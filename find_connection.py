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

# Logging setup
logging.basicConfig(
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

session = Session()
transaction_cache: Dict[str, List[dict]] = {}  # Cache to avoid redundant API calls


def fetch_transactions(address: str, retries: int = 3, delay: int = 1) -> List[dict]:
    """
    Fetch transactions for a given Ethereum address with retries and exponential backoff.
    Uses in-memory caching to avoid repeated requests for the same address.
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
                result = data.get("result", [])
                transaction_cache[address] = result
                return result
            elif data.get("message") == "No transactions found":
                transaction_cache[address] = []
                return []
            elif data.get("message", "").lower().startswith("rate limit"):
                logging.warning("Rate limit hit. Sleeping before retrying...")
                time.sleep(delay * 2)
            else:
                logging.warning(f"Etherscan error for {address}: {data.get('message')}")

        except (RequestException, Timeout) as e:
            logging.warning(f"Attempt {attempt}/{retries} failed for {address}: {e}")
            time.sleep(delay)
            delay *= 2  # Exponential backoff

    logging.error(f"Failed to fetch transactions for {address} after {retries} attempts.")
    return []


def find_connection_bfs(
    start_address: str,
    target_address: str,
    max_depth: int,
    log: Optional[List[str]] = None
) -> bool:
    """
    Perform a breadth-first search to determine if a path of transactions exists
    from start_address to target_address within a given depth.
    """
    start_address = start_address.lower()
    target_address = target_address.lower()
    visited: Set[str] = set()
    queue: deque[Tuple[str, int]] = deque([(start_address, 0)])

    while queue:
        current_address, depth = queue.popleft()
        if current_address in visited or depth > max_depth:
            continue

        visited.add(current_address)
        msg = f"[Depth {depth}] Exploring: {current_address}"
        logging.info(msg)
        if log is not None:
            log.append(msg)

        transactions = fetch_transactions(current_address)
        logging.info(f"→ {len(transactions)} transactions found for {current_address}")
        if log is not None:
            log.append(f"→ {len(transactions)} transactions")

        for tx in transactions:
            to_address = tx.get("to", "").lower()
            if not to_address or to_address in visited:
                continue

            logging.info(f"↪ Checking tx {tx['hash']} → {to_address}")
            if log is not None:
                log.append(f"↪ {tx['hash']} → {to_address}")

            if to_address == target_address:
                success_msg = f"✔ Connection found via tx {tx['hash']}"
                logging.info(success_msg)
                if log is not None:
                    log.append(success_msg)
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
    Search for a transaction-based connection between two Ethereum addresses.
    """
    if not verbose:
        logging.getLogger().setLevel(logging.WARNING)

    log: List[str] = []
    logging.info(f"🔍 Starting search from {address1} to {address2} (max depth {max_depth})")

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        future = executor.submit(find_connection_bfs, address1, address2, max_depth, log)
        connection_found = future.result()

    result_msg = "🎉 Connection found!" if connection_found else "❌ No connection found."
    logging.info(result_msg)
    log.append(result_msg)

    with open(log_file, "w", encoding="utf-8") as f:
        f.write("\n".join(log))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Find a transaction connection between two Ethereum addresses.")
    parser.add_argument("address1", help="Source Ethereum address")
    parser.add_argument("address2", help="Target Ethereum address")
    parser.add_argument("--max-depth", type=int, default=3, help="Maximum BFS search depth")
    parser.add_argument("--max-threads", type=int, default=4, help="Number of worker threads")
    parser.add_argument("--log-file", default="connection_log.txt", help="Path to output log file")
    parser.add_argument("--quiet", action="store_true", help="Suppress console output")

    args = parser.parse_args()
    main(
        address1=args.address1,
        address2=args.address2,
        max_depth=args.max_depth,
        max_threads=args.max_threads,
        log_file=args.log_file,
        verbose=not args.quiet
    )
