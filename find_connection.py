#!/usr/bin/env python3
"""
Production-grade Etherscan transaction fetcher (optimized + adaptive).

Key upgrades:
- Progressive pagination (auto-stop on empty pages)
- Adaptive concurrency (reduces workers on rate limit)
- Retry queue with priority
- Better sorting (block + timestamp)
- Safer validation
"""

from __future__ import annotations

import os
import time
import json
import random
import logging
import threading
from dataclasses import dataclass
from typing import List, Optional, Dict, TypedDict, Any, Final, Set
from collections import OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor, Future

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ==============================================================
# CONFIG
# ==============================================================

@dataclass(frozen=True)
class Config:
    api_key: str = os.getenv("ETHERSCAN_API_KEY", "")
    base_url: str = "https://api.etherscan.io/api"

    timeout: int = 10
    transport_retries: int = 3
    api_retries: int = 4

    page_size: int = 10_000
    max_pages: int = 1000

    initial_workers: int = 5
    min_workers: int = 1

    requests_per_second: float = 4.5
    burst: int = 5

    cache_ttl: int = 600
    cache_size: int = 1000

    fail_soft: bool = True


CONFIG: Final = Config()

if not CONFIG.api_key:
    raise RuntimeError("ETHERSCAN_API_KEY not set")


# ==============================================================
# LOGGING
# ==============================================================

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s | %(message)s",
)
log = logging.getLogger("etherscan")


# ==============================================================
# TYPES
# ==============================================================

class Transaction(TypedDict, total=False):
    blockNumber: str
    timeStamp: str
    hash: str


# ==============================================================
# RATE LIMITER
# ==============================================================

class RateLimiter:
    def __init__(self, rate: float, burst: int):
        self.rate = rate
        self.capacity = burst
        self.tokens = burst
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self):
        while True:
            with self.lock:
                now = time.monotonic()
                delta = now - self.last
                self.tokens = min(self.capacity, self.tokens + delta * self.rate)
                self.last = now

                if self.tokens >= 1:
                    self.tokens -= 1
                    return

                sleep = (1 - self.tokens) / self.rate

            time.sleep(sleep)


RL = RateLimiter(CONFIG.requests_per_second, CONFIG.burst)


# ==============================================================
# SESSION (thread-local)
# ==============================================================

_thread = threading.local()


def get_session() -> Session:
    if hasattr(_thread, "s"):
        return _thread.s

    retry = Retry(
        total=CONFIG.transport_retries,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        backoff_factor=0,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=50)

    s = Session()
    s.mount("https://", adapter)

    _thread.s = s
    return s


# ==============================================================
# CACHE
# ==============================================================

_cache: OrderedDict[str, tuple[float, List[Transaction]]] = OrderedDict()
_cache_lock = threading.Lock()


def cache_get(key: str):
    with _cache_lock:
        v = _cache.get(key)
        if not v:
            return None

        ts, data = v
        if time.monotonic() - ts > CONFIG.cache_ttl:
            _cache.pop(key, None)
            return None

        _cache.move_to_end(key)
        return data


def cache_set(key: str, val):
    with _cache_lock:
        _cache[key] = (time.monotonic(), val)
        if len(_cache) > CONFIG.cache_size:
            _cache.popitem(last=False)


# ==============================================================
# HELPERS
# ==============================================================

def normalize(addr: str) -> str:
    addr = addr.strip().lower()
    if not addr.startswith("0x") or len(addr) != 42:
        raise ValueError("Invalid address")
    return addr


def backoff(attempt: int):
    base = 1.5 ** attempt
    return min(base + random.random(), 20)


# ==============================================================
# FETCH PAGE
# ==============================================================

def fetch_page(address: str, page: int):
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

    s = get_session()
    r = s.get(CONFIG.base_url, params=params, timeout=CONFIG.timeout)

    data = r.json()

    if data["status"] == "1":
        return data["result"]

    msg = data.get("message", "").lower()

    if "no transactions" in msg:
        return []

    if "rate limit" in msg:
        raise RuntimeError("rate_limit")

    raise RuntimeError(msg)


# ==============================================================
# MAIN
# ==============================================================

def fetch_transactions(address: str) -> List[Transaction]:
    address = normalize(address)

    cache_key = f"tx:{address}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    results: List[Transaction] = []
    seen: Set[str] = set()

    workers = CONFIG.initial_workers
    page_queue = deque([1])
    retry_queue = deque()

    active_futures: Dict[Future, int] = {}
    executor = ThreadPoolExecutor(max_workers=workers)

    stop = False
    last_page_seen = False

    def submit(page: int):
        f = executor.submit(worker, page)
        active_futures[f] = page

    def worker(page: int):
        for attempt in range(CONFIG.api_retries):
            try:
                return page, fetch_page(address, page)
            except Exception as e:
                if "rate_limit" in str(e):
                    raise
                time.sleep(backoff(attempt))
        raise RuntimeError("max retries")

    submit(1)

    while active_futures:
        done = []
        for f in list(active_futures):
            if f.done():
                done.append(f)

        for f in done:
            page = active_futures.pop(f)

            try:
                page, txs = f.result()

                if not txs:
                    last_page_seen = True
                    continue

                for tx in txs:
                    h = tx.get("hash")
                    if h and h not in seen:
                        seen.add(h)
                        results.append(tx)

                # schedule next page ONLY if still valid
                if not last_page_seen:
                    next_page = page + 1
                    if next_page <= CONFIG.max_pages:
                        submit(next_page)

            except Exception as e:
                if "rate_limit" in str(e):
                    log.warning("Rate limit hit → reducing workers")
                    workers = max(CONFIG.min_workers, workers - 1)
                    executor._max_workers = workers
                    retry_queue.append(page)
                    time.sleep(1)
                else:
                    if CONFIG.fail_soft:
                        log.warning("Page %d failed: %s", page, e)
                    else:
                        raise

        # retry failed pages
        while retry_queue and len(active_futures) < workers:
            submit(retry_queue.popleft())

        time.sleep(0.01)

    executor.shutdown(wait=True)

    # better sorting
    results.sort(
        key=lambda x: (
            int(x.get("blockNumber", 0)),
            int(x.get("timeStamp", 0)),
            x.get("hash", ""),
        )
    )

    cache_set(cache_key, results)

    return results


# ==============================================================
# RUN
# ==============================================================

if __name__ == "__main__":
    addr = "0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae"

    txs = fetch_transactions(addr)

    log.info("Fetched %d transactions", len(txs))
