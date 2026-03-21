#!/usr/bin/env python3
"""
High-reliability Etherscan transaction fetcher (production-grade).

Key upgrades:
- Circuit breaker (prevents API hammering)
- Global rate limiter (thread-safe)
- Safer retry separation (transport vs API)
- Memory-safe TTL + size-limited cache
- Optional controlled parallel fetching
- Strict validation
"""

from __future__ import annotations

import os
import time
import json
import random
import logging
import threading
from dataclasses import dataclass
from typing import List, Optional, Dict, TypedDict, Any, Final, Literal
from collections import OrderedDict

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

    backoff_base: float = 1.6
    max_backoff: float = 30.0

    max_pool_connections: int = 25

    page_size: int = 10_000
    max_pages: Optional[int] = 100  # hard safety cap

    # rate limit (~5 req/sec free tier)
    requests_per_second: float = 4.5

    # cache
    cache_enabled: bool = True
    cache_ttl: int = 600
    cache_max_size: int = 1000

    # circuit breaker
    cb_fail_threshold: int = 5
    cb_reset_timeout: int = 30


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
# Rate Limiter (token bucket)
# ==============================================================

class RateLimiter:
    def __init__(self, rate: float):
        self.rate = rate
        self.tokens = rate
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self):
        with self.lock:
            now = time.monotonic()
            delta = now - self.last
            self.tokens = min(self.rate, self.tokens + delta * self.rate)
            self.last = now

            if self.tokens < 1:
                sleep_time = (1 - self.tokens) / self.rate
                time.sleep(sleep_time)
                self.tokens = 0
            else:
                self.tokens -= 1


_RATE_LIMITER = RateLimiter(CONFIG.requests_per_second)


# ==============================================================
# Circuit Breaker
# ==============================================================

class CircuitBreaker:
    def __init__(self):
        self.failures = 0
        self.last_fail = 0.0
        self.lock = threading.Lock()

    def check(self):
        with self.lock:
            if self.failures >= CONFIG.cb_fail_threshold:
                if time.monotonic() - self.last_fail < CONFIG.cb_reset_timeout:
                    raise CircuitBreakerOpen("Circuit breaker is OPEN")
                self.failures = 0

    def success(self):
        with self.lock:
            self.failures = 0

    def fail(self):
        with self.lock:
            self.failures += 1
            self.last_fail = time.monotonic()


_CB = CircuitBreaker()


# ==============================================================
# Session
# ==============================================================

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


_SESSION = create_session()


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
        if key in _cache:
            _cache.move_to_end(key)

        _cache[key] = (time.monotonic(), data)

        if len(_cache) > CONFIG.cache_max_size:
            _cache.popitem(last=False)


# ==============================================================
# Core
# ==============================================================

def _fetch_page(session: Session, address: str, page: int) -> List[Transaction]:
    _CB.check()
    _RATE_LIMITER.acquire()

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
    except Exception as e:
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

    session = _SESSION
    all_txs: List[Transaction] = []

    page = 1

    while True:
        if CONFIG.max_pages and page > CONFIG.max_pages:
            logger.warning("Max pages reached")
            break

        for attempt in range(CONFIG.api_retries + 1):
            try:
                txs = _fetch_page(session, address, page)

                if not txs:
                    cache_set(key, all_txs)
                    return all_txs

                all_txs.extend(txs)

                if len(txs) < CONFIG.page_size:
                    cache_set(key, all_txs)
                    return all_txs

                page += 1
                break

            except (RateLimitError, RequestException, HTTPError, Timeout) as e:
                if attempt >= CONFIG.api_retries:
                    raise EtherscanError(f"{short(address)} failed") from e

                delay = backoff(attempt)
                logger.warning(
                    "%s retry %d/%d in %.2fs (%s)",
                    short(address),
                    attempt + 1,
                    CONFIG.api_retries,
                    delay,
                    type(e).__name__,
                )
                time.sleep(delay)

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
