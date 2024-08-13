import requests
import json
import time
import threading
from requests.exceptions import RequestException
from functools import lru_cache

# Get Etherscan API key from environment variable or prompt user
import os
ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY', 'YOUR_API_KEY')

# Cache transactions to avoid repeated API calls
@lru_cache(maxsize=None)
def get_transactions(address, startblock=0, endblock=99999999, page=1, offset=10000, sort='asc'):
    url = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock={startblock}&endblock={endblock}&page={page}&offset={offset}&sort={sort}&apikey={ETHERSCAN_API_KEY}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if data['status'] == '1':
            return data['result']
        else:
            log_and_print(f"Error in response: {data['message']}", [])
            return []
    except RequestException as e:
        log_and_print(f"Request failed: {str(e)}", [])
        return []

def find_connection(address1, address2, max_depth=3, current_depth=1, log=[]):
    if current_depth > max_depth:
        return False

    log_and_print(f"Depth {current_depth}: Checking transactions of {address1}", log)
    transactions = get_transactions(address1)

    log_and_print(f"Depth {current_depth}: Found {len(transactions)} transactions for {address1}", log)
    
    for tx in transactions:
        log_and_print(f"Depth {current_depth}: Checking transaction {tx['hash']} from {tx['from']} to {tx['to']}", log)
        if tx['to'] == address2:
            log_and_print(f"Depth {current_depth}: Direct connection found in transaction {tx['hash']}", log)
            return True
        elif tx['to'] and find_connection(tx['to'], address2, max_depth, current_depth + 1, log):
            log_and_print(f"Depth {current_depth}: Indirect connection found through {tx['to']}", log)
            return True

    return False

def log_and_print(message, log):
    timestamped_message = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(timestamped_message)
    log.append(timestamped_message)

def main(address1, address2, log_file='connection_log.txt'):
    log = []
    log_and_print(f"Starting connection check between {address1} and {address2}", log)
    
    # Use threading for possible parallel execution
    connection_found = threading.Event()
    
    def check_connection():
        if find_connection(address1, address2, log=log):
            connection_found.set()

    threads = []
    for _ in range(4):  # Create a few threads to parallelize search
        thread = threading.Thread(target=check_connection)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    if connection_found.is_set():
        log_and_print("Connection found!", log)
    else:
        log_and_print("No connection found.", log)
    
    with open(log_file, 'w') as f:
        for line in log:
            f.write(line + '\n')

if __name__ == "__main__":
    address1 = "0xAddress1"
    address2 = "0xAddress2"
    main(address1, address2)
