#!/usr/bin/env python3
"""
Ultra-reliable Etherscan transaction fetcher (enhanced production version).

Major improvements:
- Parallel page fetching (ThreadPoolExecutor)
- Thread-local sessions (fix connection contention)
- Advanced circuit breaker (HALF-OPEN state)
- Smarter rate limiter (burst-safe token bucket)
- Deduplication of transactions
- Optional fail-soft mode (partial results)
- Strong validation & safer parsing
"""

from __future__ import annotations

import os
import time
import json
import random
import logging
import threading
from dataclasses import dataclass
from typing import List, Optional, Dict, TypedDict, Any, Final, Literal, Set
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

from requests import Session, RequestException
from requests.adapters import HTTPAdapter
from requests.exceptions import Timeout, HTTPError
from urllib3.util.retry import Retry


# ==============================================================
# Configuration
# ==============================================================

@dataclass(frozen=True)
class Config:
    api_key: str = os.getenv("ETHERSCAN_API_KEY", "")
    base_url: str = "https://api.etherscan.io/api"

    request_timeout: int = 10
    transport_retries: int = 3
    api_retries: int = 5

    backoff_base: float = 1.7
    max_backoff: float = 30.0

    max_pool_connections: int = 50

    page_size: int = 10_000
    max_pages: Optional[int] = 100

    # parallelism
    max_workers: int = 5

    # rate limit
    requests_per_second: float = 4.5
    burst_size: int = 5

    # cache
    cache_enabled: bool = True
    cache_ttl: int = 600
    cache_max_size: int = 1000

    # circuit breaker
    cb_fail_threshold: int = 5
    cb_reset_timeout: int = 30

    # behavior
    fail_soft: bool = True  # return partial data instead of crashing


CONFIG: Final = Config()

if not CONFIG.api_key:
    raise RuntimeError("ETHERSCAN_API_KEY is not set")


# ==============================================================
# Logging
# ==============================================================

logging.basicConfig(
    format="[%(asctime)s] %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("etherscan")


# ==============================================================
# Types
# ==============================================================

class Transaction(TypedDict, total=False):
    blockNumber: str
    timeStamp: str
    hash: str
    from_: str
    to: str
    value: str


class EtherscanResponse(TypedDict):
    status: Literal["0", "1"]
    message: str
    result: Any


# ==============================================================
# Errors
# ==============================================================

class EtherscanError(Exception):
    pass


class RateLimitError(EtherscanError):
    pass


class CircuitBreakerOpen(EtherscanError):
    pass


# ==============================================================
# Helpers
# ==============================================================

def normalize_address(addr: str) -> str:
    addr = addr.strip()
    if not (addr.startswith("0x") and len(addr) == 42):
        raise ValueError(f"Invalid address: {addr}")
    return addr.lower()


def short(addr: str) -> str:
    return f"{addr[:6]}…{addr[-4:]}"


def backoff(attempt: int) -> float:
    base = CONFIG.backoff_base ** attempt
    jitter = random.uniform(0, base * 0.3)
    return min(base + jitter, CONFIG.max_backoff)


# ==============================================================
# Rate Limiter (burst-safe token bucket)
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

                sleep_time = (1 - self.tokens) / self.rate

            time.sleep(sleep_time)


_RATE_LIMITER = RateLimiter(CONFIG.requests_per_second, CONFIG.burst_size)


# ==============================================================
# Circuit Breaker (HALF-OPEN)
# ==============================================================

class CircuitBreaker:
    def __init__(self):
        self.failures = 0
        self.state = "CLOSED"
        self.last_fail = 0.0
        self.lock = threading.Lock()

    def check(self):
        with self.lock:
            if self.state == "OPEN":
                if time.monotonic() - self.last_fail > CONFIG.cb_reset_timeout:
                    self.state = "HALF_OPEN"
                else:
                    raise CircuitBreakerOpen()

    def success(self):
        with self.lock:
            self.failures = 0
            self.state = "CLOSED"

    def fail(self):
        with self.lock:
            self.failures += 1
            self.last_fail = time.monotonic()
            if self.failures >= CONFIG.cb_fail_threshold:
                self.state = "OPEN"


_CB = CircuitBreaker()


# ==============================================================
# Thread-local session
# ==============================================================

_thread_local = threading.local()


def create_session() -> Session:
    retry = Retry(
        total=CONFIG.transport_retries,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        backoff_factor=0,
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_maxsize=CONFIG.max_pool_connections,
    )

    s = Session()
    s.mount("https://", adapter)
    return s


def get_session() -> Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = create_session()
    return _thread_local.session


# ==============================================================
# Cache (TTL + LRU)
# ==============================================================

_cache: OrderedDict[str, tuple[float, List[Transaction]]] = OrderedDict()
_cache_lock = threading.Lock()


def cache_get(key: str) -> Optional[List[Transaction]]:
    if not CONFIG.cache_enabled:
        return None

    with _cache_lock:
        item = _cache.get(key)
        if not item:
            return None

        ts, data = item
        if time.monotonic() - ts > CONFIG.cache_ttl:
            _cache.pop(key, None)
            return None

        _cache.move_to_end(key)
        return data


def cache_set(key: str, data: List[Transaction]):
    if not CONFIG.cache_enabled:
        return

    with _cache_lock:
        _cache[key] = (time.monotonic(), data)
        if len(_cache) > CONFIG.cache_max_size:
            _cache.popitem(last=False)


# ==============================================================
# Core
# ==============================================================

def _fetch_page(address: str, page: int) -> List[Transaction]:
    _CB.check()
    _RATE_LIMITER.acquire()

    session = get_session()

    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "page": page,
        "offset": CONFIG.page_size,
        "sort": "asc",
        "apikey": CONFIG.api_key,
    }

    try:
        resp = session.get(CONFIG.base_url, params=params, timeout=CONFIG.request_timeout)
        resp.raise_for_status()
    except Exception:
        _CB.fail()
        raise

    try:
        data: EtherscanResponse = resp.json()
    except json.JSONDecodeError:
        _CB.fail()
        raise EtherscanError("Invalid JSON")

    if data["status"] == "1":
        _CB.success()
        result = data["result"]

        if not isinstance(result, list):
            raise EtherscanError("Invalid result format")

        return result

    msg = data.get("message", "").lower()

    if "no transactions" in msg:
        return []

    if "rate limit" in msg:
        _CB.fail()
        raise RateLimitError(msg)

    _CB.fail()
    raise EtherscanError(f"Unexpected response: {data}")


def fetch_transactions(address: str) -> List[Transaction]:
    address = normalize_address(address)
    key = f"tx:{address}"

    cached = cache_get(key)
    if cached is not None:
        return cached

    all_txs: List[Transaction] = []
    seen_hashes: Set[str] = set()

    def worker(page: int):
        for attempt in range(CONFIG.api_retries + 1):
            try:
                return page, _fetch_page(address, page)
            except Exception as e:
                if attempt >= CONFIG.api_retries:
                    raise
                time.sleep(backoff(attempt))

    with ThreadPoolExecutor(max_workers=CONFIG.max_workers) as executor:
        futures = {
            executor.submit(worker, page): page
            for page in range(1, CONFIG.max_pages + 1)
        }

        for future in as_completed(futures):
            page = futures[future]

            try:
                _, txs = future.result()
            except Exception as e:
                logger.warning("Page %d failed: %s", page, e)
                if not CONFIG.fail_soft:
                    raise
                continue

            if not txs:
                continue

            for tx in txs:
                h = tx.get("hash")
                if h and h not in seen_hashes:
                    seen_hashes.add(h)
                    all_txs.append(tx)

    all_txs.sort(key=lambda x: int(x.get("blockNumber", "0")))
    cache_set(key, all_txs)

    return all_txs


# ==============================================================
# Example
# ==============================================================

if __name__ == "__main__":
    addr = "0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae"

    try:
        txs = fetch_transactions(addr)
        logger.info("Fetched %d txs for %s", len(txs), short(addr))
    except Exception as e:
        logger.error("Fatal: %s", e)
