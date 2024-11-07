import requests
import json
import time
import threading
from requests.exceptions import RequestException, HTTPError, Timeout
from functools import lru_cache
import os

# Retrieve Etherscan API key or set a placeholder for user to replace
ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY') or 'YOUR_API_KEY'

# Cache transactions to reduce repeated API calls for the same address
@lru_cache(maxsize=None)
def get_transactions(address, startblock=0, endblock=99999999, page=1, offset=10000, sort='asc', retries=3, delay=1):
    """
    Fetch Ethereum transactions for a specified address from the Etherscan API.
    
    :param address: Ethereum address to query
    :param startblock: Starting block for transaction history
    :param endblock: Ending block for transaction history
    :param page: Page number for pagination
    :param offset: Number of transactions per page
    :param sort: Sort order ('asc' or 'desc')
    :param retries: Number of retry attempts on failure
    :param delay: Delay between retries (seconds)
    :return: List of transactions or an empty list if unsuccessful
    """
    url = (f"https://api.etherscan.io/api?module=account&action=txlist&address={address}"
           f"&startblock={startblock}&endblock={endblock}&page={page}&offset={offset}"
           f"&sort={sort}&apikey={ETHERSCAN_API_KEY}")
    
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') == '1':  # Successful API response
                return data.get('result', [])
            else:
                log_and_print(f"API error: {data.get('message')}")
                return []
        except (RequestException, HTTPError, Timeout) as e:
            log_and_print(f"Attempt {attempt + 1}/{retries} failed: {str(e)}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                return []

def find_connection(address1, address2, max_depth=3, current_depth=1, log=None):
    """
    Recursively search for a transaction path between two Ethereum addresses.
    
    :param address1: Starting Ethereum address
    :param address2: Target Ethereum address
    :param max_depth: Max depth for recursive search
    :param current_depth: Current recursion depth
    :param log: List to log progress (optional)
    :return: True if a connection is found, False otherwise
    """
    if current_depth > max_depth:
        return False

    log_and_print(f"Depth {current_depth}: Checking transactions for {address1}", log)
    transactions = get_transactions(address1)

    log_and_print(f"Depth {current_depth}: Found {len(transactions)} transactions for {address1}", log)
    
    for tx in transactions:
        log_and_print(f"Depth {current_depth}: Checking transaction {tx['hash']} from {tx['from']} to {tx['to']}", log)
        
        if tx['to'] and tx['to'].lower() == address2.lower():
            log_and_print(f"Depth {current_depth}: Direct connection found in transaction {tx['hash']}", log)
            return True
        elif tx['to'] and find_connection(tx['to'], address2, max_depth, current_depth + 1, log):
            log_and_print(f"Depth {current_depth}: Indirect connection found via {tx['to']}", log)
            return True

    return False

def log_and_print(message, log=None):
    """
    Log a message with a timestamp and optionally append it to a list.
    
    :param message: Message to log
    :param log: List to store log messages (optional)
    """
    timestamped_message = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(timestamped_message)
    if log is not None:
        log.append(timestamped_message)

def main(address1, address2, max_threads=4, log_file='connection_log.txt'):
    """
    Main function to manage the search for a connection between two Ethereum addresses.
    
    :param address1: Starting Ethereum address
    :param address2: Target Ethereum address
    :param max_threads: Max number of threads for parallel search
    :param log_file: Path to save the log
    """
    log = []
    log_and_print(f"Starting search for a connection between {address1} and {address2}", log)
    
    connection_found = threading.Event()

    def check_connection():
        if find_connection(address1, address2, log=log):
            connection_found.set()

    # Launch threads for parallel search
    threads = [threading.Thread(target=check_connection) for _ in range(max_threads)]
    
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    if connection_found.is_set():
        log_and_print("Connection found!", log)
    else:
        log_and_print("No connection found.", log)

    # Write logs to file
    with open(log_file, 'w') as f:
        f.write("\n".join(log))

if __name__ == "__main__":
    # Example addresses to initiate search
    address1 = "0xAddress1"
    address2 = "0xAddress2"
    main(address1, address2)
