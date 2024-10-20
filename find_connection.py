import requests
import json
import time
import threading
from requests.exceptions import RequestException, HTTPError, Timeout
from functools import lru_cache
import os

# Get Etherscan API key from environment variable or prompt user if not set
ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY', 'YOUR_API_KEY')

# Cache transactions to minimize API calls for the same address
@lru_cache(maxsize=None)
def get_transactions(address, startblock=0, endblock=99999999, page=1, offset=10000, sort='asc', retries=3, delay=1):
    """
    Fetch Ethereum transactions for a given address from the Etherscan API.
    
    :param address: Ethereum address to query
    :param startblock: Start block for transaction history
    :param endblock: End block for transaction history
    :param page: Page number for paginated results
    :param offset: Number of transactions per page
    :param sort: Sorting order ('asc' or 'desc')
    :param retries: Number of retry attempts in case of failure
    :param delay: Delay between retry attempts in seconds
    :return: List of transactions or empty list if unsuccessful
    """
    url = (f"https://api.etherscan.io/api?module=account&action=txlist&address={address}"
           f"&startblock={startblock}&endblock={endblock}&page={page}&offset={offset}"
           f"&sort={sort}&apikey={ETHERSCAN_API_KEY}")
    
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()  # Raise an error for bad HTTP responses
            data = response.json()
            
            if data.get('status') == '1':  # Success case
                return data.get('result', [])
            else:  # Etherscan API error response
                log_and_print(f"API error: {data.get('message')}")
                return []
        except (RequestException, HTTPError, Timeout) as e:
            log_and_print(f"Request attempt {attempt + 1}/{retries} failed: {str(e)}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                return []

def find_connection(address1, address2, max_depth=3, current_depth=1, log=None):
    """
    Recursively find a transaction path between two Ethereum addresses.
    
    :param address1: Starting Ethereum address
    :param address2: Target Ethereum address
    :param max_depth: Maximum depth for recursive search
    :param current_depth: Current depth of the search
    :param log: List to log progress (optional)
    :return: True if a connection is found, False otherwise
    """
    if current_depth > max_depth:
        return False

    log_and_print(f"Depth {current_depth}: Checking transactions for {address1}", log)
    transactions = get_transactions(address1)

    log_and_print(f"Depth {current_depth}: {len(transactions)} transactions found for {address1}", log)
    
    for tx in transactions:
        log_and_print(f"Depth {current_depth}: Checking tx {tx['hash']} from {tx['from']} to {tx['to']}", log)
        
        if tx['to'] and tx['to'].lower() == address2.lower():
            log_and_print(f"Depth {current_depth}: Direct connection found in tx {tx['hash']}", log)
            return True
        elif tx['to'] and find_connection(tx['to'], address2, max_depth, current_depth + 1, log):
            log_and_print(f"Depth {current_depth}: Indirect connection found via {tx['to']}", log)
            return True

    return False

def log_and_print(message, log=None):
    """
    Log a message with a timestamp and optionally append it to a list.
    
    :param message: Message to be logged
    :param log: List to store log messages (optional)
    """
    timestamped_message = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(timestamped_message)
    if log is not None:
        log.append(timestamped_message)

def main(address1, address2, max_threads=4, log_file='connection_log.txt'):
    """
    Main function to orchestrate the search for a connection between two Ethereum addresses.
    
    :param address1: Starting Ethereum address
    :param address2: Target Ethereum address
    :param max_threads: Number of threads for parallel search
    :param log_file: Path to save the log output
    """
    log = []
    log_and_print(f"Starting connection search between {address1} and {address2}", log)
    
    connection_found = threading.Event()

    def check_connection():
        if find_connection(address1, address2, log=log):
            connection_found.set()

    # Create threads for parallel search
    threads = []
    for _ in range(max_threads):
        thread = threading.Thread(target=check_connection)
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    if connection_found.is_set():
        log_and_print("Connection found!", log)
    else:
        log_and_print("No connection found.", log)

    # Save logs to a file
    with open(log_file, 'w') as f:
        f.write("\n".join(log))

if __name__ == "__main__":
    # Example addresses to search for a connection between
    address1 = "0xAddress1"
    address2 = "0xAddress2"
    main(address1, address2)
