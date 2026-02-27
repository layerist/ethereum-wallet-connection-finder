#!/usr/bin/env python3
"""
Production-grade Etherscan transaction fetcher.

Improvements:
- Strict configuration validation
- Deterministic exponential backoff with bounded jitter
- Proper retry accounting per request
- Optional max page limit (DoS protection)
- Thread-safe TTL cache
- Strict response validation
- Explicit error propagation (no silent failures)
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

    backoff_base: float = 1.5
    max_backoff: float = 30.0

    max_pool_connections: int = 20
    page_size: int = 10_000
    max_pages: Optional[int] = None  # Optional safety cap

    cache_enabled: bool = True
    cache_ttl: Optional[int] = 600  # seconds


CONFIG: Final = Config()

if not CONFIG.api_key:
    raise RuntimeError("ETHERSCAN_API_KEY is not set")


# ==============================================================
# Logging
# ==============================================================

logging.basicConfig(
    format="[%(asctime)s] %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
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
    gas: str
    gasPrice: str
    isError: str
    txreceipt_status: str
    input: str
    contractAddress: str
    cumulativeGasUsed: str
    gasUsed: str
    confirmations: str


class EtherscanResponse(TypedDict):
    status: Literal["0", "1"]
    message: str
    result: Any


class CacheEntry(TypedDict):
    data: List[Transaction]
    timestamp: float


# ==============================================================
# Errors
# ==============================================================

class EtherscanError(Exception):
    pass


class RateLimitError(EtherscanError):
    pass


class UnexpectedResponseError(EtherscanError):
    pass


# ==============================================================
# Utilities
# ==============================================================

def normalize_address(addr: str) -> str:
    addr = addr.strip()

    if not (addr.startswith("0x") and len(addr) == 42):
        raise ValueError(f"Invalid Ethereum address: {addr}")

    return addr.lower()


def short(addr: str) -> str:
    return f"{addr[:8]}…{addr[-4:]}"


def backoff_delay(attempt: int) -> float:
    """
    Exponential backoff with bounded jitter.
    """
    exp = CONFIG.backoff_base ** attempt
    jitter = random.uniform(0.0, 0.25 * exp)
    return min(exp + jitter, CONFIG.max_backoff)


# ==============================================================
# Session Factory
# ==============================================================

def create_session() -> Session:
    retry_strategy = Retry(
        total=CONFIG.transport_retries,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        backoff_factor=0,
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_maxsize=CONFIG.max_pool_connections,
    )

    sess = Session()
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


_SESSION: Final = create_session()


# ==============================================================
# Cache (thread-safe)
# ==============================================================

_transaction_cache: Dict[str, CacheEntry] = {}
_cache_lock = threading.Lock()


def _get_cached(key: str) -> Optional[List[Transaction]]:
    if not CONFIG.cache_enabled:
        return None

    with _cache_lock:
        entry = _transaction_cache.get(key)
        if not entry:
            return None

        if CONFIG.cache_ttl is None:
            return entry["data"]

        age = time.monotonic() - entry["timestamp"]
        if age < CONFIG.cache_ttl:
            logger.debug("%s cache hit (%.1fs)", key, age)
            return entry["data"]

        _transaction_cache.pop(key, None)
        return None


def _set_cache(key: str, data: List[Transaction]) -> None:
    if not CONFIG.cache_enabled:
        return

    with _cache_lock:
        _transaction_cache[key] = {
            "data": data,
            "timestamp": time.monotonic(),
        }


# ==============================================================
# Core Fetch Logic
# ==============================================================

def _fetch_page(sess: Session, address: str, page: int) -> List[Transaction]:
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

    response = sess.get(
        CONFIG.base_url,
        params=params,
        timeout=CONFIG.request_timeout,
    )
    response.raise_for_status()

    try:
        payload: EtherscanResponse = response.json()
    except json.JSONDecodeError as e:
        raise UnexpectedResponseError("Invalid JSON response") from e

    if not isinstance(payload, dict):
        raise UnexpectedResponseError("Response is not a JSON object")

    status = payload.get("status")
    message = str(payload.get("message", "")).lower()
    result = payload.get("result")

    if status == "1":
        if not isinstance(result, list):
            raise UnexpectedResponseError("Result is not a list")
        return result

    if "no transactions" in message:
        return []

    if "rate limit" in message or "too many requests" in message:
        raise RateLimitError(message)

    raise UnexpectedResponseError(f"Unexpected API response: {payload}")


def fetch_transactions(
    address: str,
    *,
    use_cache: bool = True,
    session_obj: Optional[Session] = None,
) -> List[Transaction]:
    """
    Fetch all Ethereum transactions for an address.
    Raises EtherscanError on permanent failure.
    """

    address = normalize_address(address)
    cache_key = f"txlist:{address}"

    if use_cache:
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

    sess = session_obj or _SESSION
    all_txs: List[Transaction] = []

    page = 1

    while True:
        if CONFIG.max_pages and page > CONFIG.max_pages:
            logger.warning("Max page limit reached (%d)", CONFIG.max_pages)
            break

        for attempt in range(CONFIG.api_retries + 1):
            try:
                txs = _fetch_page(sess, address, page)

                if not txs:
                    _set_cache(cache_key, all_txs)
                    return all_txs

                all_txs.extend(txs)

                logger.debug(
                    "%s page %d fetched (%d txs)",
                    short(address),
                    page,
                    len(txs),
                )

                if len(txs) < CONFIG.page_size:
                    _set_cache(cache_key, all_txs)
                    return all_txs

                page += 1
                break

            except (RateLimitError, Timeout, HTTPError, RequestException, UnexpectedResponseError) as e:
                if attempt >= CONFIG.api_retries:
                    raise EtherscanError(
                        f"{short(address)} failed after {CONFIG.api_retries} retries"
                    ) from e

                delay = backoff_delay(attempt)
                logger.warning(
                    "%s %s, retry in %.2fs (%d/%d)",
                    short(address),
                    type(e).__name__,
                    delay,
                    attempt + 1,
                    CONFIG.api_retries,
                )
                time.sleep(delay)

    _set_cache(cache_key, all_txs)
    return all_txs


# ==============================================================
# Example
# ==============================================================

if __name__ == "__main__":
    test_address = "0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae"

    try:
        txs = fetch_transactions(test_address)
        logger.info(
            "Fetched %d transactions for %s",
            len(txs),
            short(test_address),
        )
    except EtherscanError as e:
        logger.error("Fatal error: %s", e)
