import os
import time
import json
import requests
from requests.exceptions import RequestException, HTTPError, Timeout
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

# Retrieve Etherscan API key from environment variable or set a placeholder
ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY', 'YOUR_API_KEY')

@lru_cache(maxsize=None)
def get_transactions(
    address: str,
    startblock: int = 0,
    endblock: int = 99999999,
    page: int = 1,
    offset: int = 10000,
    sort: str = 'asc',
    retries: int = 3,
    delay: int = 1,
) -> List[dict]:
    """
    Fetch Ethereum transactions for a specified address from the Etherscan API.
    Caches results to reduce repeated API calls.
    """
    url = (
        f"https://api.etherscan.io/api?module=account&action=txlist&address={address}"
        f"&startblock={startblock}&endblock={endblock}&page={page}&offset={offset}"
        f"&sort={sort}&apikey={ETHERSCAN_API_KEY}"
    )

    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get('status') == '1':  # Successful API response
                return data.get('result', [])
            if data.get('status') == '0' and data.get('message') == 'No transactions found':
                return []  # No transactions for the address

            log_and_print(f"API error: {data.get('message')}")
        except (RequestException, HTTPError, Timeout) as e:
            log_and_print(f"Attempt {attempt + 1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
    return []


def find_connection(
    address1: str,
    address2: str,
    max_depth: int = 3,
    current_depth: int = 1,
    log: Optional[List[str]] = None,
) -> bool:
    """
    Recursively search for a transaction path between two Ethereum addresses.
    """
    if current_depth > max_depth:
        return False

    log_and_print(f"Depth {current_depth}: Fetching transactions for {address1}", log)
    transactions = get_transactions(address1)

    log_and_print(f"Depth {current_depth}: Found {len(transactions)} transactions for {address1}", log)

    for tx in transactions:
        tx_to = tx.get('to', '').lower()
        if not tx_to:
            continue  # Skip invalid transactions

        log_and_print(
            f"Depth {current_depth}: Checking transaction {tx['hash']} from {tx['from']} to {tx_to}",
            log,
        )

        if tx_to == address2.lower():
            log_and_print(f"Depth {current_depth}: Direct connection found in transaction {tx['hash']}", log)
            return True
        if find_connection(tx_to, address2, max_depth, current_depth + 1, log):
            log_and_print(f"Depth {current_depth}: Indirect connection found via {tx_to}", log)
            return True

    return False


def log_and_print(message: str, log: Optional[List[str]] = None):
    """
    Log a message with a timestamp and optionally append it to a list.
    """
    timestamped_message = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(timestamped_message)
    if log is not None:
        log.append(timestamped_message)


def main(address1: str, address2: str, max_threads: int = 4, log_file: str = 'connection_log.txt'):
    """
    Main function to search for a connection between two Ethereum addresses.
    """
    log = []
    log_and_print(f"Starting connection search between {address1} and {address2}", log)

    connection_found = False

    def search():
        nonlocal connection_found
        if find_connection(address1, address2, log=log):
            connection_found = True

    with ThreadPoolExecutor(max_threads) as executor:
        futures = [executor.submit(search) for _ in range(max_threads)]
        for future in futures:
            future.result()  # Wait for threads to complete

    if connection_found:
        log_and_print("Connection found!", log)
    else:
        log_and_print("No connection found.", log)

    with open(log_file, 'w') as file:
        file.write("\n".join(log))


if __name__ == "__main__":
    # Replace with actual Ethereum addresses to test
    address1 = "0xAddress1"
    address2 = "0xAddress2"
    main(address1, address2)
