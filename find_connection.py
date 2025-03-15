import os
import time
import json
import requests
from requests.exceptions import RequestException, Timeout
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from typing import List, Optional, Set

# Retrieve Etherscan API key from environment variable or set a placeholder
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "YOUR_API_KEY")


def log_message(message: str, log: Optional[List[str]] = None, verbose: bool = True):
    """
    Logs a message with a timestamp and optionally appends it to a list.
    """
    timestamped_message = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    if verbose:
        print(timestamped_message)
    if log is not None:
        log.append(timestamped_message)


@lru_cache(maxsize=500)
def get_transactions(address: str, retries: int = 3, initial_delay: int = 1) -> List[dict]:
    """
    Fetch Ethereum transactions for a given address using the Etherscan API.
    Implements caching and retry logic for robustness.
    """
    url = (
        f"https://api.etherscan.io/api?module=account&action=txlist&address={address}"
        f"&startblock=0&endblock=99999999&sort=asc&apikey={ETHERSCAN_API_KEY}"
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
                return []

            log_message(f"API error ({address}): {data.get('message')}")
        except (RequestException, Timeout) as e:
            log_message(f"Attempt {attempt}/{retries} failed for {address}: {e}")
        
        time.sleep(delay)
        delay *= 2  # Exponential backoff
    
    return []


def find_connection(address1: str, address2: str, max_depth: int = 3, log: Optional[List[str]] = None) -> bool:
    """
    Uses a breadth-first search (BFS) to find a transaction path between two Ethereum addresses.
    """
    queue = deque([(address1, 0)])
    visited: Set[str] = set()

    while queue:
        current_address, depth = queue.popleft()
        if depth >= max_depth:
            continue

        if current_address in visited:
            continue
        visited.add(current_address)

        log_message(f"Checking {current_address} at depth {depth}", log)
        transactions = get_transactions(current_address)
        log_message(f"Found {len(transactions)} transactions for {current_address}", log)

        for tx in transactions:
            tx_to = tx.get("to", "").lower()
            if not tx_to or tx_to in visited:
                continue
            
            log_message(f"Checking transaction {tx['hash']} → {tx_to}", log)
            
            if tx_to == address2.lower():
                log_message(f"Direct connection found in transaction {tx['hash']}", log)
                return True
            
            queue.append((tx_to, depth + 1))
    
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
