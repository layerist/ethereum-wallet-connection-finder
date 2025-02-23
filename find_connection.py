import os
import time
import json
import requests
from requests.exceptions import RequestException, HTTPError, Timeout
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Set

# Retrieve Etherscan API key from environment variable or set a placeholder
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "YOUR_API_KEY")

def log_message(message: str, log: Optional[List[str]] = None):
    """
    Logs a message with a timestamp and optionally appends it to a list.
    """
    timestamped_message = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(timestamped_message)
    if log is not None:
        log.append(timestamped_message)

@lru_cache(maxsize=None)
def get_transactions(
    address: str,
    startblock: int = 0,
    endblock: int = 99999999,
    page: int = 1,
    offset: int = 10000,
    sort: str = "asc",
    retries: int = 3,
    initial_delay: int = 1,
) -> List[dict]:
    """
    Fetch Ethereum transactions for a given address using the Etherscan API.
    Implements caching and retry logic for robustness.
    """
    url = (
        f"https://api.etherscan.io/api?module=account&action=txlist&address={address}"
        f"&startblock={startblock}&endblock={endblock}&page={page}&offset={offset}"
        f"&sort={sort}&apikey={ETHERSCAN_API_KEY}"
    )

    delay = initial_delay
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get("status") == "1":
                return data.get("result", [])
            if data.get("status") == "0" and data.get("message") == "No transactions found":
                return []  # No transactions for the address

            log_message(f"API error ({address}): {data.get('message')}")
        except (RequestException, HTTPError, Timeout) as e:
            log_message(f"Attempt {attempt}/{retries} failed for {address}: {e}")
        
        time.sleep(delay)
        delay *= 2  # Exponential backoff for retries
    
    return []

def find_connection(
    address1: str,
    address2: str,
    max_depth: int = 3,
    visited: Optional[Set[str]] = None,
    log: Optional[List[str]] = None,
) -> bool:
    """
    Recursively searches for a transaction path between two Ethereum addresses.
    """
    if max_depth <= 0:
        return False
    
    if visited is None:
        visited = set()
    if address1 in visited:
        return False
    
    visited.add(address1)
    log_message(f"Checking transactions for {address1} at depth {max_depth}", log)

    transactions = get_transactions(address1)
    log_message(f"Found {len(transactions)} transactions for {address1}", log)

    for tx in transactions:
        tx_to = tx.get("to", "").lower()
        if not tx_to or tx_to in visited:
            continue

        log_message(f"Checking transaction {tx['hash']} → {tx_to}", log)

        if tx_to == address2.lower():
            log_message(f"Direct connection found in transaction {tx['hash']}", log)
            return True

        if find_connection(tx_to, address2, max_depth - 1, visited, log):
            log_message(f"Indirect connection found via {tx_to}", log)
            return True

    return False

def main(address1: str, address2: str, max_threads: int = 4, log_file: str = "connection_log.txt"):
    """
    Main function to search for a connection between two Ethereum addresses.
    Uses multithreading to parallelize API calls and improve performance.
    """
    log = []
    log_message(f"Starting connection search: {address1} → {address2}", log)

    with ThreadPoolExecutor(max_threads) as executor:
        future = executor.submit(find_connection, address1, address2, log=log)
        connection_found = future.result()

    if connection_found:
        log_message("Connection found!", log)
    else:
        log_message("No connection found.", log)

    with open(log_file, "w") as file:
        file.write("\n".join(log))

if __name__ == "__main__":
    address1 = "0xAddress1"
    address2 = "0xAddress2"
    main(address1, address2)
