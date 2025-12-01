import os
import time
import json
import logging
from typing import List, Optional, Dict, TypedDict, Any
from dataclasses import dataclass
from requests import Session, RequestException, Timeout, HTTPError
from requests.adapters import HTTPAdapter
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
    retry_backoff: float = 2.0
    max_pool_connections: int = 10
    cache_enabled: bool = True
    cache_ttl: Optional[int] = 600  # seconds; None = no expiration


CONFIG = Config()


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


class CacheEntry(TypedDict):
    data: List[Transaction]
    timestamp: float


# ==============================================================
# Utils
# ==============================================================
def normalize_address(addr: str) -> str:
    """Normalize and validate Ethereum address."""
    addr = addr.strip().lower()
    if len(addr) != 42 or not addr.startswith("0x"):
        raise ValueError(f"Invalid Ethereum address: {addr}")
    return addr


def short(addr: str) -> str:
    """Shorten address for logs."""
    return addr[:10] + "..."


# ==============================================================
# Session Factory with Retry Logic
# ==============================================================
def create_session() -> Session:
    """Create a `requests.Session` with automatic retries."""
    retry_strategy = Retry(
        total=CONFIG.max_retries,
        backoff_factor=CONFIG.retry_backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
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


session: Session = create_session()
transaction_cache: Dict[str, CacheEntry] = {}


# ==============================================================
# Caching Helpers
# ==============================================================
def _get_cached_transactions(address: str) -> Optional[List[Transaction]]:
    if not CONFIG.cache_enabled:
        return None

    entry = transaction_cache.get(address)
    if not entry:
        return None

    if CONFIG.cache_ttl is None:
        return entry["data"]

    age = time.monotonic() - entry["timestamp"]
    if age < CONFIG.cache_ttl:
        logger.debug(f"{short(address)} cache hit ({age:.1f}s old)")
        return entry["data"]

    transaction_cache.pop(address, None)
    return None


def _cache_transactions(address: str, data: List[Transaction]) -> None:
    if CONFIG.cache_enabled:
        transaction_cache[address] = {
            "data": data,
            "timestamp": time.monotonic(),
        }


# ==============================================================
# Fetch Transactions
# ==============================================================
def fetch_transactions(
    address: str,
    retries: int = CONFIG.max_retries,
    delay: float = 1.0,
    session_obj: Optional[Session] = None,
    use_cache: bool = CONFIG.cache_enabled,
) -> List[Transaction]:
    """
    Fetch Ethereum transactions for an address via the Etherscan API.

    Returns:
        List of Transaction dicts (possibly empty).
    """
    try:
        address = normalize_address(address)
    except ValueError as e:
        logger.error(str(e))
        return []

    sess = session_obj or session

    # Cache?
    if use_cache:
        cached = _get_cached_transactions(address)
        if cached is not None:
            return cached

    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
        "apikey": CONFIG.api_key,
    }

    for attempt in range(1, retries + 1):
        try:
            response = sess.get(CONFIG.base_url, params=params, timeout=CONFIG.request_timeout)
            response.raise_for_status()

            data: Dict[str, Any] = response.json()
            status = data.get("status")
            message = str(data.get("message", "")).lower()

            # Success
            if status == "1":
                transactions: List[Transaction] = data.get("result", [])
                _cache_transactions(address, transactions)
                return transactions

            # No transactions
            if "no transactions" in message:
                logger.info(f"{short(address)} no transactions.")
                _cache_transactions(address, [])
                return []

            # Rate limiting
            if "rate limit" in message or "too many requests" in message:
                retry_after = int(response.headers.get("Retry-After", delay))
                logger.warning(
                    f"{short(address)} rate limit. Retry in {retry_after}s "
                    f"(attempt {attempt}/{retries})"
                )
                time.sleep(retry_after)
                delay *= CONFIG.retry_backoff
                continue

            logger.error(f"{short(address)} unexpected API response: {data}")
            break

        except (Timeout, HTTPError) as e:
            logger.warning(
                f"{short(address)} {type(e).__name__} attempt {attempt}/{retries}: {e}. "
                f"Retrying in {delay:.1f}s..."
            )
            time.sleep(delay)
            delay *= CONFIG.retry_backoff

        except (RequestException, json.JSONDecodeError) as e:
            logger.error(f"{short(address)} non-retryable error: {e}")
            break

    logger.error(f"{short(address)} failed after {retries} attempts.")
    return []


# ==============================================================
# Example Usage
# ==============================================================
if __name__ == "__main__":
    test_address = "0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae"
    txs = fetch_transactions(test_address)
    logger.info(f"Fetched {len(txs)} transactions for {short(test_address)}")
