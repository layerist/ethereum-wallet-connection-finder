import os
import time
import json
import logging
from typing import List, Optional, Dict
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import requests
from requests import Session, RequestException, Timeout, HTTPError

# Configuration
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "YOUR_API_KEY")
ETHERSCAN_BASE_URL = "https://api.etherscan.io/api"
REQUEST_TIMEOUT = 10
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # exponential factor

# Logging
logging.basicConfig(
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Shared session and cache
session = requests.Session()
transaction_cache: Dict[str, List[dict]] = {}


def fetch_transactions(address: str, retries: int = MAX_RETRIES, delay: float = 1.0, session_obj: Optional[Session] = None) -> List[dict]:
    """
    Fetch all normal transactions for a given Ethereum address using the Etherscan API.
    Includes retry logic with exponential backoff and response caching.

    Args:
        address (str): The Ethereum address to query.
        retries (int): Number of retry attempts on failure.
        delay (float): Initial delay between retries.
        session_obj (Optional[Session]): Custom requests.Session (useful for testing).

    Returns:
        List[dict]: List of transaction dictionaries (may be empty).
    """
    address = address.lower()
    if address in transaction_cache:
        return transaction_cache[address]

    session_inst = session_obj or session

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
            response = session_inst.get(ETHERSCAN_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

            data = response.json()
            status = data.get("status")
            message = data.get("message", "").lower()

            if status == "1":
                transactions = data.get("result", [])
                transaction_cache[address] = transactions
                return transactions

            elif message == "no transactions found":
                transaction_cache[address] = []
                return []

            elif "rate limit" in message or "too many requests" in message:
                logger.warning(f"Rate limit hit for {address}. Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= RETRY_BACKOFF
            else:
                logger.error(f"Unexpected Etherscan response for {address}: {data}")
                break  # unrecoverable error, don't retry

        except (Timeout, HTTPError) as e:
            logger.warning(f"[{attempt}/{retries}] Timeout/HTTP error for {address}: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= RETRY_BACKOFF

        except (RequestException, json.JSONDecodeError) as e:
            logger.error(f"Non-retryable error for {address}: {e}")
            break  # don't retry on generic request or decoding errors

    logger.error(f"Failed to fetch transactions for {address} after {retries} attempts.")
    return []
