import os
import time
import json
import logging
from typing import List, Optional, Dict, TypedDict
from requests import Session, RequestException, Timeout, HTTPError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# Configuration
# =========================
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "YOUR_API_KEY")
ETHERSCAN_BASE_URL = "https://api.etherscan.io/api"
REQUEST_TIMEOUT = 10
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # exponential factor
MAX_POOL_CONNECTIONS = 10

# =========================
# Logging
# =========================
logging.basicConfig(
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# =========================
# Types
# =========================
class Transaction(TypedDict):
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
# Shared session with retries
# =========================
def create_session() -> Session:
    s = Session()
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_maxsize=MAX_POOL_CONNECTIONS)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

session = create_session()
transaction_cache: Dict[str, List[Transaction]] = {}

# =========================
# Main function
# =========================
def fetch_transactions(
    address: str,
    retries: int = MAX_RETRIES,
    delay: float = 1.0,
    session_obj: Optional[Session] = None
) -> List[Transaction]:
    """
    Fetch all normal transactions for a given Ethereum address using the Etherscan API.

    Args:
        address (str): Ethereum address to query.
        retries (int): Retry attempts on failure.
        delay (float): Initial delay between retries.
        session_obj (Optional[Session]): Custom requests.Session.

    Returns:
        List[Transaction]: List of transaction dictionaries.
    """
    address = address.lower()
    if address in transaction_cache:
        return transaction_cache[address]

    sess = session_obj or session
    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
        "apikey": ETHERSCAN_API_KEY
    }

    for attempt in range(1, retries + 1):
        try:
            response = sess.get(ETHERSCAN_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

            data = response.json()
            status = data.get("status")
            message = str(data.get("message", "")).lower()

            if status == "1":
                transactions: List[Transaction] = data.get("result", [])
                transaction_cache[address] = transactions
                return transactions

            elif "no transactions found" in message:
                transaction_cache[address] = []
                return []

            elif "rate limit" in message or "too many requests" in message:
                retry_after = int(response.headers.get("Retry-After", delay))
                logger.warning(f"Rate limit hit for {address[:10]}... Retrying in {retry_after}s...")
                time.sleep(retry_after)
                delay *= RETRY_BACKOFF
                continue

            else:
                logger.error(f"Unexpected response for {address[:10]}...: {data}")
                break

        except (Timeout, HTTPError) as e:
            logger.warning(f"[{attempt}/{retries}] Timeout/HTTP error for {address[:10]}...: {e}. Retrying in {delay}s...")
            time.sleep(delay)
            delay *= RETRY_BACKOFF

        except (RequestException, json.JSONDecodeError) as e:
            logger.error(f"Non-retryable error for {address[:10]}...: {e}")
            break

    logger.error(f"Failed to fetch transactions for {address[:10]}... after {retries} attempts.")
    return []
