import os
import time
import json
import requests
from requests import Session
from requests.exceptions import RequestException, Timeout
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from typing import List, Optional, Set, Tuple

# Constants
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "YOUR_API_KEY")
ETHERSCAN_BASE_URL = "https://api.etherscan.io/api"
REQUEST_TIMEOUT = 10

# Use a session to reuse TCP connections
session = Session()

def log_message(message: str, log: Optional[List[str]] = None, verbose: bool = True) -> None:
    """
    Print and/or store a timestamped log message.
    """
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    full_message = f"[{timestamp}] {message}"
    if verbose:
        print(full_message)
    if log is not None:
        log.append(full_message)

@lru_cache(maxsize=1000)
def get_transactions(address: str, retries: int = 3, delay: int = 1) -> List[dict]:
    """
    Fetch transactions for a given Ethereum address with retry and caching.
    """
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
                return data.get("result", [])
            elif data.get("message") == "No transactions found":
                return []
            else:
                log_message(f"Etherscan error for {address}: {data.get('message')}")
        except (RequestException, Timeout) as e:
            log_message(f"Attempt {attempt}/{retries} failed for {address}: {e}")
            time.sleep(delay)
            delay *= 2  # Exponential backoff

    return []

def find_connection(
    address1: str,
    address2: str,
    max_depth: int = 3,
    log: Optional[List[str]] = None
) -> bool:
    """
    Use BFS to find a transaction path from address1 to address2.
    """
    address1, address2 = address1.lower(), address2.lower()
    visited: Set[str] = set()
    queue: deque[Tuple[str, int]] = deque([(address1, 0)])

    while queue:
        current_address, depth = queue.popleft()
        if depth >= max_depth or current_address in visited:
            continue

        visited.add(current_address)
        log_message(f"Exploring {current_address} at depth {depth}", log)

        transactions = get_transactions(current_address)
        log_message(f"→ Found {len(transactions)} transactions", log)

        for tx in transactions:
            to_address = tx.get("to", "").lower()
            if not to_address or to_address in visited:
                continue

            log_message(f"Checking tx {tx['hash']} → {to_address}", log)

            if to_address == address2:
                log_message(f"✔ Connection found via tx {tx['hash']}", log)
                return True

            queue.append((to_address, depth + 1))

    return False

def main(
    address1: str,
    address2: str,
    max_depth: int = 3,
    max_threads: int = 4,
    log_file: str = "connection_log.txt",
    verbose: bool = True
) -> None:
    """
    Search for a transaction connection between two Ethereum addresses.
    """
    log: List[str] = []
    log_message(f"🔍 Searching connection from {address1} to {address2}", log, verbose)

    with ThreadPoolExecutor(max_threads) as executor:
        future = executor.submit(find_connection, address1, address2, max_depth, log)
        connection_found = future.result()

    if connection_found:
        log_message("🎉 Connection found!", log, verbose)
    else:
        log_message("❌ No connection found.", log, verbose)

    with open(log_file, "w") as f:
        f.write("\n".join(log))

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Find transaction connection between two Ethereum addresses.")
    parser.add_argument("address1", help="Starting Ethereum address")
    parser.add_argument("address2", help="Target Ethereum address")
    parser.add_argument("--max-depth", type=int, default=3, help="Maximum search depth")
    parser.add_argument("--max-threads", type=int, default=4, help="Maximum number of threads")
    parser.add_argument("--log-file", default="connection_log.txt", help="Path to log file")
    parser.add_argument("--quiet", action="store_true", help="Suppress console output")

    args = parser.parse_args()
    main(args.address1, args.address2, args.max_depth, args.max_threads, args.log_file, not args.quiet)
