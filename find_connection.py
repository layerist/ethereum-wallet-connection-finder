import requests
import json
import time

# Replace with your Etherscan API key
ETHERSCAN_API_KEY = 'YOUR_API_KEY'

# Function to get the transactions of an Ethereum address
def get_transactions(address, startblock=0, endblock=99999999, page=1, offset=10000, sort='asc'):
    url = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock={startblock}&endblock={endblock}&page={page}&offset={offset}&sort={sort}&apikey={ETHERSCAN_API_KEY}"
    response = requests.get(url)
    data = response.json()
    if data['status'] == '1':
        return data['result']
    else:
        return []

# Function to find connection between two addresses
def find_connection(address1, address2, max_depth=3, current_depth=1, log=[]):
    if current_depth > max_depth:
        return False

    log_and_print(f"Checking transactions of {address1} at depth {current_depth}", log)

    transactions = get_transactions(address1)
    log_and_print(f"Found {len(transactions)} transactions for {address1}", log)
    
    for tx in transactions:
        log_and_print(f"Checking transaction {tx['hash']} from {tx['from']} to {tx['to']}", log)
        if tx['to'] == address2:
            log_and_print(f"Direct connection found in transaction {tx['hash']}", log)
            return True
        elif find_connection(tx['to'], address2, max_depth, current_depth + 1, log):
            log_and_print(f"Indirect connection found through {tx['to']}", log)
            return True
    return False

# Function to log and print messages
def log_and_print(message, log):
    print(message)
    log.append(message)

# Main function to check connection and log the process
def main(address1, address2, log_file='connection_log.txt'):
    log = []
    log_and_print(f"Checking connection between {address1} and {address2}", log)
    
    connected = find_connection(address1, address2, log=log)

    if connected:
        log_and_print("Connection found!", log)
    else:
        log_and_print("No connection found.", log)
    
    with open(log_file, 'w') as f:
        for line in log:
            f.write(line + '\n')

if __name__ == "__main__":
    # Replace these with the Ethereum addresses you want to check
    address1 = "0xAddress1"
    address2 = "0xAddress2"
    main(address1, address2)
