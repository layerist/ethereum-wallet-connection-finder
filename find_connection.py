import os
import time
import json
import logging
from typing import List, Optional, Dict, TypedDict, Any
from dataclasses import dataclass
from requests import Session, RequestException, Timeout, HTTPError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# Configuration
# =========================
@dataclass(frozen=True)
class Config:
    api_key: str = os.getenv("ETHERSCAN_API_KEY", "YOUR_API_KEY")
    base_url: str = "https://api.etherscan.io/api"
    request_timeout: int = 10
    max_retries: int = 3
    retry_backoff: float = 2.0  # exponential backoff factor
    max_pool_connections: int = 10
    cache_enabled: bool = True
    cache_ttl: Optional[int] = None  # seconds, None = infinite


CONFIG = Config()

# =========================
# Logging
# =========================
logging.basicConfig(
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =========================
# Types
# =========================
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

# =========================
# Session with retry
# =========================
def create_session() -> Session:
    """Create a shared requests.Session with retry logic."""
    retry_strategy = Retry(
        total=CONFIG.max_retries,
        backoff_factor=CONFIG.retry_backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_maxsize=CONFIG.max_pool_connections)

    sess = Session()
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


session = create_session()

# =========================
# In-memory cache
# =========================
class CacheEntry(TypedDict):
    data: List[Transaction]
    timestamp: float


transaction_cache: Dict[str, CacheEntry] = {}

# =========================
# Fetch transactions
# =========================
def fetch_transactions(
    address: str,
    retries: int = CONFIG.max_retries,
    delay: float = 1.0,
    session_obj: Optional[Session] = None,
    use_cache: bool = CONFIG.cache_enabled,
) -> List[Transaction]:
    """
    Fetch all normal transactions for a given Ethereum address using the Etherscan API.
    """

    address = address.lower()
    sess = session_obj or session

    # Cache check
    if use_cache and address in transaction_cache:
        entry = transaction_cache[address]
        if CONFIG.cache_ttl is None or (time.time() - entry["timestamp"]) < CONFIG.cache_ttl:
            return entry["data"]

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

            if status == "1":
                transactions: List[Transaction] = data.get("result", [])
                if use_cache:
                    transaction_cache[address] = {"data": transactions, "timestamp": time.time()}
                return transactions

            if "no transactions" in message:
                if use_cache:
                    transaction_cache[address] = {"data": [], "timestamp": time.time()}
                return []

            if "rate limit" in message or "too many requests" in message:
                retry_after = int(response.headers.get("Retry-After", delay))
                logger.warning(
                    f"[{address[:10]}...] Rate limit hit. Retrying in {retry_after}s (attempt {attempt}/{retries})"
                )
                time.sleep(retry_after)
                delay *= CONFIG.retry_backoff
                continue

            logger.error(f"[{address[:10]}...] Unexpected response: {data}")
            break

        except (Timeout, HTTPError) as e:
            logger.warning(
                f"[{address[:10]}...] {type(e).__name__} on attempt {attempt}/{retries}: {e}. "
                f"Retrying in {delay}s..."
            )
            time.sleep(delay)
            delay *= CONFIG.retry_backoff

        except (RequestException, json.JSONDecodeError) as e:
            logger.error(f"[{address[:10]}...] Non-retryable error: {e}")
            break

    logger.error(f"[{address[:10]}...] Failed after {retries} attempts.")
    return []
