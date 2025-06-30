import os
import time
import json
import logging
from typing import List, Optional, Set, Tuple, Dict
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from requests import Session, RequestException, Timeout

# Configuration
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "YOUR_API_KEY")
ETHERSCAN_BASE_URL = "https://api.etherscan.io/api"
REQUEST_TIMEOUT = 10

# Logging
logging.basicConfig(
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)

logger = logging.getLogger(__name__)
session = Session()

# Cache for fetched transactions
transaction_cache: Dict[str, List[dict]] = {}


def fetch_transactions(address: str, retries: int = 3, delay: float = 1.0) -> List[dict]:
    """
    Retrieve transactions for a given Ethereum address, with caching and
    retry/exponential backoff on errors.
    """
    address = address.lower()
    if address in transaction_cache:
        return transaction_cache[address]

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
            response = session.get(ETHERSCAN_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()

            if data.get("status") == "1":
                transactions = data.get("result", [])
                transaction_cache[address] = transactions
                return transactions
            elif data.get("message", "").lower() == "no transactions found":
                transaction_cache[address] = []
                return []
            elif "rate limit" in data.get("message", "").lower():
                logger.warning("Rate limit exceeded. Retrying after delay...")
                time.sleep(delay * 2)
            else:
                logger.warning(f"Etherscan error for {address}: {data.get('message')}")

        except (RequestException
