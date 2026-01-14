#!/usr/bin/env python3
"""
Robust Etherscan transaction fetcher.

Features:
- Thread-safe in-memory cache with TTL
- Full pagination support (10k tx/page)
- Exponential backoff with jitter
- Explicit error modeling
- Strong typing and defensive parsing
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
    api_key: str = os.getenv("ETHERSCAN_API_KEY", "YOUR_API_KEY")
    base_url: str = "https://api.etherscan.io/api"

    request_timeout: int = 10
    max_retries: int = 3
    backoff_factor: float = 2.0
    max_pool_connections: int = 10

    page_size: int = 10_000
    cache_enabled: bool = True
    cache_ttl: Optional[int] = 600  # seconds


CONFIG: Final = Config()


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
    addr = addr.strip().lower()
    if not (addr.startswith("0x") and len(addr) == 42):
        raise ValueError(f"Invalid Ethereum address: {addr}")
    return addr


def short(addr: str) -> str:
    return f"{addr[:8]}…{addr[-4:]}"


def backoff_delay(attempt: int) -> float:
    return (CONFIG.backoff_factor ** attempt) + random.uniform(0.0, 0.5)


# ==============================================================
# Session Factory
# ==============================================================
def create_session() -> Session:
    retry_strategy = Retry(
        total=CONFIG.max_retries,
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


session: Final = create_session()


# ==============================================================
# Cache (thread-safe)
# ==============================================================
_transaction_cache: Dict[str, CacheEntry] = {}
_cache_lock = threading.Lock()


def _get_cached(address: str) -> Optional[List[Transaction]]:
    if not CONFIG.cache_enabled:
        return None

    with _cache_lock:
        entry = _transaction_cache.get(address)
        if not entry:
            return None

        if CONFIG.cache_ttl is None:
            return entry["data"]

        age = time.monotonic() - entry["timestamp"]
        if age < CONFIG.cache_ttl:
            logger.debug("%s cache hit (%.1fs)", short(address), age)
            return entry["data"]

        _transaction_cache.pop(address, None)
        return None


def _set_cache(address: str, data: List[Transaction]) -> None:
    if not CONFIG.cache_enabled:
        return

    with _cache_lock:
        _transaction_cache[address] = {
            "data": data,
            "timestamp": time.monotonic(),
        }


# ==============================================================
# Core Fetch Logic
# ==============================================================
def _fetch_page(
    sess: Session,
    address: str,
    page: int,
) -> List[Transaction]:
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
        raise UnexpectedResponseError("invalid_json") from e

    status = payload.get("status")
    message = payload.get("message", "").lower()
    result = payload.get("result")

    if status == "1" and isinstance(result, list):
        return result

    if "no transactions" in message:
        return []

    if "rate limit" in message or "too many requests" in message:
        raise RateLimitError(message)

    raise UnexpectedResponseError(payload)


def fetch_transactions(
    address: str,
    *,
    use_cache: bool = True,
    session_obj: Optional[Session] = None,
) -> List[Transaction]:
    """
    Fetch all Ethereum transactions for an address.
    """
    try:
        address = normalize_address(address)
    except ValueError as e:
        logger.error(str(e))
        return []

    if use_cache:
        cached = _get_cached(address)
        if cached is not None:
            return cached

    sess = session_obj or session
    all_txs: List[Transaction] = []

    page = 1
    attempt = 0

    while True:
        try:
            txs = _fetch_page(sess, address, page)
            attempt = 0  # reset retry counter on success

            if not txs:
                break

            all_txs.extend(txs)

            if len(txs) < CONFIG.page_size:
                break

            page += 1

        except RateLimitError:
            if attempt >= CONFIG.max_retries:
                logger.error("%s rate limit exceeded", short(address))
                break

            delay = backoff_delay(attempt)
            attempt += 1
            logger.warning(
                "%s rate limited, retrying in %.1fs (%d/%d)",
                short(address),
                delay,
                attempt,
                CONFIG.max_retries,
            )
            time.sleep(delay)

        except (Timeout, HTTPError, RequestException, UnexpectedResponseError) as e:
            if attempt >= CONFIG.max_retries:
                logger.error("%s failed: %s", short(address), e)
                break

            delay = backoff_delay(attempt)
            attempt += 1
            logger.warning(
                "%s %s retry in %.1fs (%d/%d)",
                short(address),
                type(e).__name__,
                delay,
                attempt,
                CONFIG.max_retries,
            )
            time.sleep(delay)

    _set_cache(address, all_txs)
    return all_txs


# ==============================================================
# Example
# ==============================================================
if __name__ == "__main__":
    test_address = "0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae"
    txs = fetch_transactions(test_address)
    logger.info(
        "Fetched %d transactions for %s",
        len(txs),
        short(test_address),
    )
