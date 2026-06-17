#!/usr/bin/env python3
"""
Production-grade Etherscan transaction fetcher.

What this version optimizes for
-------------------------------
- Etherscan API V2 endpoint support (`/v2/api` + `chainid`)
- Safe rate limiting with jittered retries
- Reliable large-history fetching via block-range splitting
- Concurrent range workers without unsafe "empty page" guessing
- Thread-local HTTP sessions with connection pooling
- Deterministic de-duplication and sorting
- CLI usage and optional JSON/JSONL output
- Graceful shutdown

Environment variables
---------------------
ETHERSCAN_API_KEY              required unless --api-key is passed
ETHERSCAN_CHAIN_ID             default: 1
ETHERSCAN_RPS                  default: 2.7
ETHERSCAN_PAGE_SIZE            default: 10000
ETHERSCAN_WORKERS              default: 4
ETHERSCAN_MAX_WORKERS          default: 8
ETHERSCAN_TIMEOUT              default: 20
ETHERSCAN_OUTPUT               optional output path
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import threading
import time

from collections import OrderedDict, deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Deque, Dict, Final, Iterable, List, Optional, Set, Tuple, TypedDict

import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="[%(asctime)s] %(levelname)s | %(message)s",
)

log = logging.getLogger("etherscan")


# ============================================================
# TYPES
# ============================================================

class Transaction(TypedDict, total=False):
    blockNumber: str
    timeStamp: str
    hash: str
    nonce: str
    blockHash: str
    transactionIndex: str
    from_: str
    to: str
    value: str
    gas: str
    gasPrice: str
    isError: str
    txreceipt_status: str
    input: str
    contractAddress: str
    cumulativeGasUsed: str
    gasUsed: str
    confirmations: str
    methodId: str
    functionName: str


@dataclass(frozen=True)
class Config:
    api_key: str = os.getenv("ETHERSCAN_API_KEY", "")
    base_url: str = os.getenv("ETHERSCAN_BASE_URL", "https://api.etherscan.io/v2/api")
    chain_id: str = os.getenv("ETHERSCAN_CHAIN_ID", "1")

    timeout: float = float(os.getenv("ETHERSCAN_TIMEOUT", "20"))
    transport_retries: int = int(os.getenv("ETHERSCAN_TRANSPORT_RETRIES", "2"))
    api_retries: int = int(os.getenv("ETHERSCAN_API_RETRIES", "6"))

    # 10_000 is the historical endpoint max. If your tier/network has a lower cap,
    # set ETHERSCAN_PAGE_SIZE=1000 or pass --page-size 1000.
    page_size: int = int(os.getenv("ETHERSCAN_PAGE_SIZE", "10000"))

    start_block: int = int(os.getenv("ETHERSCAN_START_BLOCK", "0"))
    end_block: int = int(os.getenv("ETHERSCAN_END_BLOCK", "99999999"))

    initial_workers: int = int(os.getenv("ETHERSCAN_WORKERS", "4"))
    max_workers: int = int(os.getenv("ETHERSCAN_MAX_WORKERS", "8"))

    # Free V2 tier is currently 3 calls/sec, so default is intentionally below it.
    requests_per_second: float = float(os.getenv("ETHERSCAN_RPS", "2.7"))
    burst: int = int(os.getenv("ETHERSCAN_BURST", "3"))

    retry_backoff_cap: float = float(os.getenv("ETHERSCAN_BACKOFF_CAP", "30"))

    cache_ttl: float = float(os.getenv("ETHERSCAN_CACHE_TTL", "600"))
    cache_size: int = int(os.getenv("ETHERSCAN_CACHE_SIZE", "256"))

    fail_soft: bool = os.getenv("ETHERSCAN_FAIL_SOFT", "1") not in {"0", "false", "False"}

    # If a range returns page_size records, the range is treated as saturated and split.
    # This avoids trusting pagination windows for very large histories.
    min_split_block_span: int = int(os.getenv("ETHERSCAN_MIN_SPLIT_BLOCK_SPAN", "1"))

    log_progress_every: int = int(os.getenv("ETHERSCAN_LOG_PROGRESS_EVERY", "10"))


CONFIG: Final = Config()


# ============================================================
# GLOBAL STOP FLAG
# ============================================================

shutdown_event = threading.Event()


def signal_handler(sig, frame) -> None:  # noqa: ANN001 - signal handler signature
    log.warning("Shutdown requested...")
    shutdown_event.set()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ============================================================
# RATE LIMITER
# ============================================================

class TokenBucket:
    def __init__(self, rate: float, burst: int) -> None:
        if rate <= 0:
            raise ValueError("rate must be > 0")
        if burst <= 0:
            raise ValueError("burst must be > 0")

        self.rate = rate
        self.capacity = float(burst)
        self.tokens = float(burst)
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        while not shutdown_event.is_set():
            with self.lock:
                now = time.monotonic()
                delta = now - self.last
                self.last = now

                self.tokens = min(self.capacity, self.tokens + delta * self.rate)

                if self.tokens >= 1:
                    self.tokens -= 1
                    return

                sleep_for = (1 - self.tokens) / self.rate

            time.sleep(min(sleep_for, 1.0))

        raise RuntimeError("shutdown")


# ============================================================
# THREAD-LOCAL SESSION
# ============================================================

_thread_local = threading.local()


def get_session(config: Config) -> Session:
    session = getattr(_thread_local, "session", None)
    if session is not None:
        return session

    retry = Retry(
        total=config.transport_retries,
        connect=config.transport_retries,
        read=config.transport_retries,
        backoff_factor=0.25,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(
        pool_connections=config.max_workers * 4,
        pool_maxsize=config.max_workers * 4,
        max_retries=retry,
    )

    session = Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": "etherscan-fetcher/2.0",
            "Accept": "application/json",
            "Connection": "keep-alive",
        }
    )

    _thread_local.session = session
    return session


# ============================================================
# TTL LRU CACHE
# ============================================================

_cache: OrderedDict[str, Tuple[float, List[Transaction]]] = OrderedDict()
_cache_lock = threading.Lock()


def cache_get(key: str, config: Config) -> Optional[List[Transaction]]:
    with _cache_lock:
        item = _cache.get(key)
        if item is None:
            return None

        ts, data = item
        if time.monotonic() - ts > config.cache_ttl:
            _cache.pop(key, None)
            return None

        _cache.move_to_end(key)
        return list(data)


def cache_set(key: str, value: List[Transaction], config: Config) -> None:
    with _cache_lock:
        _cache[key] = (time.monotonic(), list(value))
        while len(_cache) > config.cache_size:
            _cache.popitem(last=False)


# ============================================================
# HELPERS
# ============================================================

class RateLimitError(Exception):
    pass


class EtherscanAPIError(Exception):
    pass


def normalize_address(address: str) -> str:
    address = address.strip().lower()
    if not address.startswith("0x") or len(address) != 42:
        raise ValueError(f"Invalid Ethereum address: {address!r}")

    try:
        int(address[2:], 16)
    except ValueError as exc:
        raise ValueError(f"Invalid Ethereum address: {address!r}") from exc

    return address


def calc_backoff(attempt: int, cap: float) -> float:
    # Full jitter exponential backoff.
    return min(random.uniform(0, 2 ** attempt), cap)


def parse_int(value: object, default: int = 0) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


def tx_sort_key(tx: Transaction) -> Tuple[int, int, str]:
    return (
        parse_int(tx.get("blockNumber")),
        parse_int(tx.get("transactionIndex")),
        tx.get("hash", ""),
    )


def unique_transactions(txs: Iterable[Transaction]) -> List[Transaction]:
    seen: Set[str] = set()
    out: List[Transaction] = []

    for tx in sorted(txs, key=tx_sort_key):
        tx_hash = tx.get("hash")
        if not tx_hash or tx_hash in seen:
            continue

        seen.add(tx_hash)
        out.append(tx)

    return out


def build_params(config: Config, address: str, start_block: int, end_block: int, page: int) -> Dict[str, object]:
    return {
        "chainid": config.chain_id,
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": start_block,
        "endblock": end_block,
        "page": page,
        "offset": config.page_size,
        "sort": "asc",
        "apikey": config.api_key,
    }


# ============================================================
# ETHERSCAN REQUESTS
# ============================================================

def etherscan_get(config: Config, limiter: TokenBucket, params: Dict[str, object]) -> Dict[str, object]:
    session = get_session(config)

    for attempt in range(config.api_retries):
        if shutdown_event.is_set():
            raise RuntimeError("shutdown")

        limiter.acquire()

        try:
            response = session.get(config.base_url, params=params, timeout=config.timeout)
        except requests.RequestException as exc:
            sleep_for = calc_backoff(attempt, config.retry_backoff_cap)
            log.debug("Network error: %s; retrying in %.2fs", exc, sleep_for)
            time.sleep(sleep_for)
            continue

        if response.status_code == 429:
            sleep_for = calc_backoff(attempt + 1, config.retry_backoff_cap)
            time.sleep(sleep_for)
            continue

        try:
            data = response.json()
        except json.JSONDecodeError as exc:
            raise EtherscanAPIError(f"invalid_json: http={response.status_code}") from exc

        status = str(data.get("status", "")).strip()
        message = str(data.get("message", "")).strip()
        result = data.get("result")
        haystack = f"{message} {result}".lower()

        if status == "1":
            return data

        if "no transactions found" in haystack:
            return {"status": "1", "message": "OK", "result": []}

        if "rate limit" in haystack or "too many invalid api key attempts" in haystack:
            sleep_for = calc_backoff(attempt + 1, config.retry_backoff_cap)
            time.sleep(sleep_for)
            continue

        # Deprecated V1 endpoint / invalid key / invalid chainid should fail fast.
        if (
            "deprecated v1 endpoint" in haystack
            or "invalid api key" in haystack
            or "missing or unsupported chainid" in haystack
        ):
            raise EtherscanAPIError(f"fatal_api_error: {message} | {result}")

        raise EtherscanAPIError(f"api_error: {message} | {result}")

    raise RateLimitError("rate_limit_or_retries_exhausted")


def fetch_range_once(
    config: Config,
    limiter: TokenBucket,
    address: str,
    start_block: int,
    end_block: int,
    page: int = 1,
) -> List[Transaction]:
    params = build_params(config, address, start_block, end_block, page)
    data = etherscan_get(config, limiter, params)

    result = data.get("result", [])
    if not isinstance(result, list):
        raise EtherscanAPIError(f"unexpected_result_type: {type(result).__name__}")

    return result  # type: ignore[return-value]


# ============================================================
# FETCH LOGIC
# ============================================================

def fetch_transactions(address: str, config: Config = CONFIG) -> List[Transaction]:
    if not config.api_key:
        raise RuntimeError("ETHERSCAN_API_KEY not set; pass --api-key or set the env var")

    address = normalize_address(address)

    if config.start_block < 0:
        raise ValueError("start_block must be >= 0")
    if config.end_block < config.start_block:
        raise ValueError("end_block must be >= start_block")
    if config.page_size <= 0:
        raise ValueError("page_size must be > 0")
    if config.initial_workers <= 0 or config.max_workers <= 0:
        raise ValueError("workers must be > 0")

    cache_key = (
        f"tx:v2:{config.chain_id}:{address}:"
        f"{config.start_block}:{config.end_block}:{config.page_size}"
    )
    cached = cache_get(cache_key, config)
    if cached is not None:
        log.info("Cache hit: %d txs", len(cached))
        return cached

    limiter = TokenBucket(config.requests_per_second, config.burst)

    ranges: Deque[Tuple[int, int]] = deque([(config.start_block, config.end_block)])
    active: Dict[Future[List[Transaction]], Tuple[int, int]] = {}
    all_txs: List[Transaction] = []
    saturated_ranges = 0
    completed_ranges = 0

    def submit(executor: ThreadPoolExecutor, block_range: Tuple[int, int]) -> None:
        start_block, end_block = block_range
        future = executor.submit(
            fetch_range_once,
            config,
            limiter,
            address,
            start_block,
            end_block,
            1,
        )
        active[future] = block_range

    workers = min(config.initial_workers, config.max_workers)

    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        while ranges and len(active) < workers:
            submit(executor, ranges.popleft())

        while active and not shutdown_event.is_set():
            done, _ = wait(active.keys(), return_when=FIRST_COMPLETED)

            for future in done:
                start_block, end_block = active.pop(future)

                try:
                    txs = future.result()
                except RateLimitError as exc:
                    # Put it back and reduce pressure.
                    ranges.appendleft((start_block, end_block))
                    workers = max(1, workers - 1)
                    log.warning(
                        "Rate limit/retry exhaustion on range %d-%d; workers=%d; %s",
                        start_block,
                        end_block,
                        workers,
                        exc,
                    )
                    continue
                except Exception as exc:
                    if config.fail_soft:
                        log.warning("Range %d-%d failed: %s", start_block, end_block, exc)
                        continue
                    raise

                completed_ranges += 1

                if len(txs) >= config.page_size and (end_block - start_block) >= config.min_split_block_span:
                    mid = (start_block + end_block) // 2

                    if mid < end_block:
                        saturated_ranges += 1
                        ranges.appendleft((mid + 1, end_block))
                        ranges.appendleft((start_block, mid))
                    else:
                        # Degenerate single-block range. Fall back to pagination inside this block.
                        page = 1
                        while True:
                            page_txs = fetch_range_once(config, limiter, address, start_block, end_block, page)
                            all_txs.extend(page_txs)
                            if len(page_txs) < config.page_size:
                                break
                            page += 1
                else:
                    all_txs.extend(txs)

                if completed_ranges % config.log_progress_every == 0:
                    current_unique = len({tx.get("hash") for tx in all_txs if tx.get("hash")})
                    log.info(
                        "Ranges=%d | queued=%d | active=%d | saturated=%d | unique_txs~=%d | workers=%d",
                        completed_ranges,
                        len(ranges),
                        len(active),
                        saturated_ranges,
                        current_unique,
                        workers,
                    )

            # Cautiously recover concurrency when no rate-limit pressure is active.
            if workers < config.max_workers and len(ranges) > workers:
                workers += 1

            while ranges and len(active) < workers:
                submit(executor, ranges.popleft())

    if shutdown_event.is_set():
        log.warning("Stopped before completion; returning partial result")

    result = unique_transactions(all_txs)
    cache_set(cache_key, result, config)

    log.info(
        "Finished: %d unique txs | ranges=%d | saturated=%d | chainid=%s",
        len(result),
        completed_ranges,
        saturated_ranges,
        config.chain_id,
    )

    return result


# ============================================================
# OUTPUT
# ============================================================

def write_output(txs: List[Transaction], output_path: str, jsonl: bool = False) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if jsonl:
        with path.open("w", encoding="utf-8") as f:
            for tx in txs:
                f.write(json.dumps(tx, ensure_ascii=False, sort_keys=True))
                f.write("\n")
    else:
        with path.open("w", encoding="utf-8") as f:
            json.dump(txs, f, ensure_ascii=False, indent=2, sort_keys=True)

    log.info("Saved: %s", path)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch normal Etherscan transactions for an address.")
    parser.add_argument("address", help="Ethereum address, e.g. 0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae")
    parser.add_argument("--api-key", default=CONFIG.api_key, help="Etherscan API key")
    parser.add_argument("--chainid", default=CONFIG.chain_id, help="Etherscan V2 chain id; Ethereum mainnet is 1")
    parser.add_argument("--base-url", default=CONFIG.base_url, help="Etherscan API base URL")
    parser.add_argument("--start-block", type=int, default=CONFIG.start_block)
    parser.add_argument("--end-block", type=int, default=CONFIG.end_block)
    parser.add_argument("--page-size", type=int, default=CONFIG.page_size)
    parser.add_argument("--workers", type=int, default=CONFIG.initial_workers)
    parser.add_argument("--max-workers", type=int, default=CONFIG.max_workers)
    parser.add_argument("--rps", type=float, default=CONFIG.requests_per_second)
    parser.add_argument("--burst", type=int, default=CONFIG.burst)
    parser.add_argument("--timeout", type=float, default=CONFIG.timeout)
    parser.add_argument("--output", default=os.getenv("ETHERSCAN_OUTPUT", ""))
    parser.add_argument("--jsonl", action="store_true", help="Write JSON Lines instead of a JSON array")
    parser.add_argument("--strict", action="store_true", help="Fail fast on non-rate-limit range errors")

    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    config = replace(
        CONFIG,
        api_key=args.api_key,
        base_url=args.base_url,
        chain_id=str(args.chainid),
        start_block=args.start_block,
        end_block=args.end_block,
        page_size=args.page_size,
        initial_workers=args.workers,
        max_workers=args.max_workers,
        requests_per_second=args.rps,
        burst=args.burst,
        timeout=args.timeout,
        fail_soft=not args.strict,
    )

    txs = fetch_transactions(args.address, config)

    if args.output:
        write_output(txs, args.output, args.jsonl)
    else:
        print(json.dumps(txs, ensure_ascii=False, indent=2, sort_keys=True))

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        shutdown_event.set()
        raise SystemExit(130)
