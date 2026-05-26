#!/usr/bin/env python3
"""
Production-grade Etherscan transaction fetcher
High-performance + adaptive + safe

Features
--------
- True concurrent pagination
- Adaptive concurrency on rate limits
- Retry priority queue
- Progressive page discovery
- Consecutive empty-page stop logic
- Thread-local HTTP sessions
- Connection pooling
- TTL LRU cache
- Graceful shutdown
- Stable deterministic sorting
- Memory-efficient deduplication
"""

from __future__ import annotations

import os
import time
import json
import random
import signal
import logging
import threading

from dataclasses import dataclass
from typing import TypedDict, List, Dict, Set, Final
from collections import OrderedDict, deque
from concurrent.futures import (
    ThreadPoolExecutor,
    wait,
    FIRST_COMPLETED,
    Future,
)

import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# CONFIG
# ============================================================

@dataclass(frozen=True)
class Config:
    api_key: str = os.getenv("ETHERSCAN_API_KEY", "")
    base_url: str = "https://api.etherscan.io/api"

    timeout: int = 15

    transport_retries: int = 2
    api_retries: int = 5

    page_size: int = 10_000
    max_pages: int = 100_000

    initial_workers: int = 4
    max_workers: int = 8
    min_workers: int = 1

    requests_per_second: float = 4.5
    burst: int = 5

    retry_backoff_cap: int = 30

    cache_ttl: int = 600
    cache_size: int = 1000

    fail_soft: bool = True

    # stop after N consecutive empty pages
    empty_page_threshold: int = 3

    log_progress_every: int = 10


CONFIG: Final = Config()

if not CONFIG.api_key:
    raise RuntimeError("ETHERSCAN_API_KEY not set")


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s | %(message)s",
)

log = logging.getLogger("etherscan")


# ============================================================
# TYPES
# ============================================================

class Transaction(TypedDict, total=False):
    blockNumber: str
    timeStamp: str
    hash: str


# ============================================================
# GLOBAL STOP FLAG
# ============================================================

shutdown_event = threading.Event()


def signal_handler(sig, frame):
    log.warning("Shutdown requested...")
    shutdown_event.set()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ============================================================
# RATE LIMITER
# ============================================================

class TokenBucket:
    def __init__(self, rate: float, burst: int):
        self.rate = rate
        self.capacity = burst
        self.tokens = burst
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self):
        while not shutdown_event.is_set():

            with self.lock:
                now = time.monotonic()

                delta = now - self.last
                self.last = now

                self.tokens = min(
                    self.capacity,
                    self.tokens + delta * self.rate
                )

                if self.tokens >= 1:
                    self.tokens -= 1
                    return

                sleep_for = (
                    (1 - self.tokens) / self.rate
                )

            time.sleep(sleep_for)


RL = TokenBucket(
    CONFIG.requests_per_second,
    CONFIG.burst,
)


# ============================================================
# THREAD LOCAL SESSION
# ============================================================

_thread_local = threading.local()


def get_session() -> Session:
    if hasattr(_thread_local, "session"):
        return _thread_local.session

    retry = Retry(
        total=CONFIG.transport_retries,
        backoff_factor=0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        pool_connections=100,
        pool_maxsize=100,
        max_retries=retry,
    )

    session = Session()

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Connection": "keep-alive",
        "Accept": "application/json",
    })

    _thread_local.session = session
    return session


# ============================================================
# CACHE
# ============================================================

_cache: OrderedDict[
    str,
    tuple[float, List[Transaction]]
] = OrderedDict()

_cache_lock = threading.Lock()


def cache_get(key: str):
    with _cache_lock:

        item = _cache.get(key)

        if item is None:
            return None

        ts, data = item

        if time.monotonic() - ts > CONFIG.cache_ttl:
            _cache.pop(key, None)
            return None

        _cache.move_to_end(key)

        return data


def cache_set(key: str, value):
    with _cache_lock:

        _cache[key] = (
            time.monotonic(),
            value,
        )

        if len(_cache) > CONFIG.cache_size:
            _cache.popitem(last=False)


# ============================================================
# HELPERS
# ============================================================

def normalize_address(address: str) -> str:
    address = address.strip().lower()

    if (
        not address.startswith("0x")
        or len(address) != 42
    ):
        raise ValueError("Invalid address")

    return address


def backoff(attempt: int) -> float:
    base = 2 ** attempt
    jitter = random.random()

    return min(
        base + jitter,
        CONFIG.retry_backoff_cap,
    )


# ============================================================
# FETCH PAGE
# ============================================================

class RateLimitError(Exception):
    pass


def fetch_page(
    address: str,
    page: int,
) -> List[Transaction]:

    RL.acquire()

    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "page": page,
        "offset": CONFIG.page_size,
        "sort": "asc",
        "apikey": CONFIG.api_key,
    }

    session = get_session()

    try:
        response = session.get(
            CONFIG.base_url,
            params=params,
            timeout=CONFIG.timeout,
        )

    except requests.RequestException as e:
        raise RuntimeError(
            f"network_error: {e}"
        )

    if response.status_code == 429:
        raise RateLimitError()

    try:
        data = response.json()

    except json.JSONDecodeError:
        raise RuntimeError(
            "invalid_json"
        )

    status = data.get("status")
    message = (
        data.get("message", "")
        .lower()
        .strip()
    )

    result = data.get("result")

    if status == "1":
        return result

    if (
        "no transactions found"
        in message
    ):
        return []

    if (
        "rate limit" in message
        or "max rate limit" in message
    ):
        raise RateLimitError()

    raise RuntimeError(
        f"api_error: {message}"
    )


# ============================================================
# WORKER
# ============================================================

def page_worker(
    address: str,
    page: int,
):
    for attempt in range(
        CONFIG.api_retries
    ):

        if shutdown_event.is_set():
            raise RuntimeError(
                "shutdown"
            )

        try:
            txs = fetch_page(
                address,
                page,
            )

            return page, txs

        except RateLimitError:
            raise

        except Exception:

            sleep_time = backoff(
                attempt
            )

            time.sleep(
                sleep_time
            )

    raise RuntimeError(
        f"page_failed:{page}"
    )


# ============================================================
# MAIN
# ============================================================

def fetch_transactions(
    address: str
) -> List[Transaction]:

    address = normalize_address(
        address
    )

    cache_key = f"tx:{address}"

    cached = cache_get(
        cache_key
    )

    if cached is not None:
        return cached

    seen_hashes: Set[str] = set()
    results: List[
        Transaction
    ] = []

    current_workers = (
        CONFIG.initial_workers
    )

    retry_queue = deque()
    active: Dict[
        Future,
        int
    ] = {}

    next_page = 1
    empty_pages = 0
    pages_processed = 0

    executor = ThreadPoolExecutor(
        max_workers=current_workers
    )

    def submit(page: int):
        if (
            shutdown_event.is_set()
            or page
            > CONFIG.max_pages
        ):
            return

        future = executor.submit(
            page_worker,
            address,
            page,
        )

        active[
            future
        ] = page

    # warmup batch
    for _ in range(
        current_workers * 2
    ):
        submit(next_page)
        next_page += 1

    try:

        while active:

            done, _ = wait(
                active.keys(),
                return_when=FIRST_COMPLETED,
            )

            for future in done:

                page = active.pop(
                    future
                )

                try:
                    _, txs = (
                        future.result()
                    )

                    pages_processed += 1

                    if not txs:
                        empty_pages += 1
                    else:
                        empty_pages = 0

                        for tx in txs:
                            tx_hash = tx.get(
                                "hash"
                            )

                            if (
                                tx_hash
                                and tx_hash
                                not in seen_hashes
                            ):
                                seen_hashes.add(
                                    tx_hash
                                )
                                results.append(
                                    tx
                                )

                    if (
                        pages_processed
                        % CONFIG.log_progress_every
                        == 0
                    ):
                        log.info(
                            "Pages=%d | txs=%d | workers=%d",
                            pages_processed,
                            len(results),
                            current_workers,
                        )

                except RateLimitError:

                    log.warning(
                        "Rate limit hit "
                        "(workers=%d)",
                        current_workers,
                    )

                    retry_queue.appendleft(
                        page
                    )

                    current_workers = max(
                        CONFIG.min_workers,
                        current_workers - 1,
                    )

                    time.sleep(
                        random.uniform(
                            1.5,
                            3.0,
                        )
                    )

                except Exception as e:

                    if (
                        CONFIG.fail_soft
                    ):
                        log.warning(
                            "Page %d failed: %s",
                            page,
                            e,
                        )
                    else:
                        raise

            if (
                empty_pages
                >= CONFIG.empty_page_threshold
            ):
                break

            while (
                retry_queue
                and len(active)
                < current_workers
            ):
                submit(
                    retry_queue.popleft()
                )

            while (
                len(active)
                < current_workers * 2
            ):
                submit(next_page)
                next_page += 1

    finally:
        executor.shutdown(
            wait=True,
            cancel_futures=True,
        )

    results.sort(
        key=lambda tx: (
            int(
                tx.get(
                    "blockNumber",
                    0,
                )
            ),
            int(
                tx.get(
                    "timeStamp",
                    0,
                )
            ),
            tx.get(
                "hash",
                "",
            ),
        )
    )

    cache_set(
        cache_key,
        results,
    )

    log.info(
        "Finished: %d txs "
        "from %d pages",
        len(results),
        pages_processed,
    )

    return results


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    address = (
        "0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae"
    )

    txs = fetch_transactions(
        address
    )

    log.info(
        "Fetched %d transactions",
        len(txs),
    )
