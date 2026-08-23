#!/usr/bin/env python3
"""
Reliable Etherscan V2 history fetcher (v5).

Main upgrades vs a basic txlist downloader:
- Etherscan V2 endpoint support: /v2/api + chainid.
- Normal, internal, ERC-20, ERC-721 and ERC-1155 actions.
- Global token-bucket rate limiter shared by all workers.
- Retry-After aware API retry loop with full-jitter backoff.
- Block-range splitting when a range hits the API page-size ceiling.
- Safe single-block pagination for very busy addresses.
- Thread-local requests.Session with keep-alive and HTTPAdapter pools.
- Every retry passes through the shared rate limiter (no hidden adapter retries).
- Non-zero exit status after SIGINT/SIGTERM even when partial output is saved.
- Optional HTTP(S) proxy support.
- Deterministic de-duplication and sorting.
- JSON, JSONL and CSV output.
- Strict mode or fail-soft mode with failed-range reporting.
- Graceful Ctrl+C shutdown with partial-result return.

Examples:
  python etherscan_tx_fetcher_v5.py 0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae \
    --api-key YOUR_KEY --chainid 1 --action txlist --output txs.json

  python etherscan_tx_fetcher_v5.py 0x... --action tokentx --csv --output erc20.csv

Environment:
  ETHERSCAN_API_KEY
  ETHERSCAN_CHAIN_ID=1
  ETHERSCAN_BASE_URL=https://api.etherscan.io/v2/api
  ETHERSCAN_RPS=2.7
  ETHERSCAN_BURST=3
  ETHERSCAN_PAGE_SIZE=10000
  ETHERSCAN_WORKERS=4
  ETHERSCAN_MAX_WORKERS=8
  ETHERSCAN_TIMEOUT=20
  ETHERSCAN_PROXY=http://user:pass@127.0.0.1:8080
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import random
import signal
import sys
import threading
import time
from collections import OrderedDict, deque
from email.utils import parsedate_to_datetime
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, replace
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

import requests
from requests import Session
from requests.adapters import HTTPAdapter


# ============================================================
# SAFE ENV PARSING
# ============================================================

def env_str(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def env_int(name: str, default: int, *, minimum: Optional[int] = None) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} must be integer, got {raw!r}") from None
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


def env_float(name: str, default: float, *, minimum: Optional[float] = None) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"{name} must be float, got {raw!r}") from None
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=env_str("LOG_LEVEL", "INFO").upper(),
    format="[%(asctime)s] %(levelname)s | %(message)s",
)
log = logging.getLogger("etherscan_fetcher")


# ============================================================
# CONFIG / TYPES
# ============================================================

Tx = Dict[str, Any]
BlockRange = Tuple[int, int]

SUPPORTED_ACTIONS = {
    "txlist",        # normal transactions
    "txlistinternal",# internal transactions
    "tokentx",       # ERC-20 transfers
    "tokennfttx",    # ERC-721 transfers
    "token1155tx",   # ERC-1155 transfers
}


@dataclass(frozen=True)
class Config:
    api_key: str = env_str("ETHERSCAN_API_KEY", "")
    base_url: str = env_str("ETHERSCAN_BASE_URL", "https://api.etherscan.io/v2/api")
    chain_id: str = env_str("ETHERSCAN_CHAIN_ID", "1")
    action: str = env_str("ETHERSCAN_ACTION", "txlist")

    start_block: int = env_int("ETHERSCAN_START_BLOCK", 0, minimum=0)
    end_block: int = env_int("ETHERSCAN_END_BLOCK", 99_999_999, minimum=0)
    page_size: int = env_int("ETHERSCAN_PAGE_SIZE", 10_000, minimum=1)

    initial_workers: int = env_int("ETHERSCAN_WORKERS", 4, minimum=1)
    max_workers: int = env_int("ETHERSCAN_MAX_WORKERS", 8, minimum=1)

    requests_per_second: float = env_float("ETHERSCAN_RPS", 2.7, minimum=0.01)
    burst: int = env_int("ETHERSCAN_BURST", 3, minimum=1)

    timeout: float = env_float("ETHERSCAN_TIMEOUT", 20.0, minimum=0.1)
    transport_retries: int = env_int("ETHERSCAN_TRANSPORT_RETRIES", 2, minimum=0)
    api_retries: int = env_int("ETHERSCAN_API_RETRIES", 7, minimum=1)
    retry_backoff_cap: float = env_float("ETHERSCAN_BACKOFF_CAP", 35.0, minimum=0.1)

    cache_ttl: float = env_float("ETHERSCAN_CACHE_TTL", 600.0, minimum=0.0)
    cache_size: int = env_int("ETHERSCAN_CACHE_SIZE", 128, minimum=0)

    fail_soft: bool = env_bool("ETHERSCAN_FAIL_SOFT", False)
    min_split_block_span: int = env_int("ETHERSCAN_MIN_SPLIT_BLOCK_SPAN", 1, minimum=1)
    max_single_block_pages: int = env_int("ETHERSCAN_MAX_SINGLE_BLOCK_PAGES", 200, minimum=1)
    log_progress_every: int = env_int("ETHERSCAN_LOG_PROGRESS_EVERY", 25, minimum=1)
    max_range_requeues: int = env_int("ETHERSCAN_MAX_RANGE_REQUEUES", 5, minimum=0)

    proxy: str = env_str("ETHERSCAN_PROXY", "")
    user_agent: str = env_str("ETHERSCAN_USER_AGENT", "etherscan-fetcher/5.0")


CONFIG = Config()
shutdown_event = threading.Event()
_shutdown_signal: Optional[int] = None


# ============================================================
# SIGNALS
# ============================================================

def _signal_handler(signum: int, _frame: object) -> None:
    global _shutdown_signal
    _shutdown_signal = signum
    log.warning("Shutdown requested by signal %s", signum)
    shutdown_event.set()


def shutdown_exit_code() -> int:
    """Return conventional non-zero exit status after SIGINT/SIGTERM."""
    if _shutdown_signal is None:
        return 0
    return 128 + int(_shutdown_signal)


try:
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
except Exception:
    # Some embedded environments do not allow signal handlers.
    pass


# ============================================================
# EXCEPTIONS
# ============================================================

class FetcherError(Exception):
    """Base fetcher error."""


class RateLimitError(FetcherError):
    """API retries were exhausted due to throttling/transient errors."""


class FatalAPIError(FetcherError):
    """Fatal Etherscan response: invalid key, invalid chainid, deprecated endpoint, etc."""


class NonFatalAPIError(FetcherError):
    """Recoverable API response for one range."""


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
        self.updated_at = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        while not shutdown_event.is_set():
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.updated_at
                self.updated_at = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return

                sleep_for = (1.0 - self.tokens) / self.rate

            interruptible_sleep(min(max(sleep_for, 0.01), 1.0))

        raise RuntimeError("shutdown")


# ============================================================
# HTTP SESSION
# ============================================================

_thread_local = threading.local()


def get_session(config: Config) -> Session:
    """Return one keep-alive Session per worker thread.

    Important: urllib3-level retries are intentionally disabled. Every retry must
    pass through etherscan_get() so the global TokenBucket sees every HTTP attempt.
    Otherwise adapter-internal retries can silently exceed the configured RPS.
    """
    session = getattr(_thread_local, "session", None)
    if session is not None:
        return session

    adapter = HTTPAdapter(
        pool_connections=max(config.max_workers * 4, 4),
        pool_maxsize=max(config.max_workers * 4, 4),
        max_retries=0,
    )

    session = Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": config.user_agent,
            "Accept": "application/json",
            "Connection": "keep-alive",
        }
    )

    if config.proxy:
        session.proxies.update({"http": config.proxy, "https": config.proxy})

    _thread_local.session = session
    return session


# ============================================================
# TTL LRU CACHE
# ============================================================

_cache: "OrderedDict[str, Tuple[float, List[Tx]]]" = OrderedDict()
_cache_lock = threading.Lock()


def cache_get(key: str, config: Config) -> Optional[List[Tx]]:
    if config.cache_size <= 0 or config.cache_ttl <= 0:
        return None
    with _cache_lock:
        item = _cache.get(key)
        if item is None:
            return None
        ts, value = item
        if time.monotonic() - ts > config.cache_ttl:
            _cache.pop(key, None)
            return None
        _cache.move_to_end(key)
        return [dict(tx) for tx in value]


def cache_set(key: str, value: List[Tx], config: Config) -> None:
    if config.cache_size <= 0 or config.cache_ttl <= 0:
        return
    with _cache_lock:
        _cache[key] = (time.monotonic(), [dict(tx) for tx in value])
        while len(_cache) > config.cache_size:
            _cache.popitem(last=False)


# ============================================================
# HELPERS
# ============================================================

def normalize_address(address: str) -> str:
    value = address.strip().lower()
    if not value.startswith("0x") or len(value) != 42:
        raise ValueError(f"Invalid Ethereum address: {address!r}")
    try:
        int(value[2:], 16)
    except ValueError:
        raise ValueError(f"Invalid Ethereum address: {address!r}") from None
    return value


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


def full_jitter_backoff(attempt: int, cap: float) -> float:
    return min(random.uniform(0.0, 2.0 ** attempt), cap)


def interruptible_sleep(seconds: float) -> None:
    """Sleep while still reacting quickly to Ctrl+C/SIGTERM."""
    if seconds <= 0:
        return
    if shutdown_event.wait(timeout=seconds):
        raise RuntimeError("shutdown")


def retry_after_seconds(response: requests.Response) -> Optional[float]:
    """Parse Retry-After as either delta-seconds or an HTTP date."""
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        try:
            target = parsedate_to_datetime(raw)
            return max(0.0, target.timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            return None


def tx_sort_key(tx: Mapping[str, Any]) -> Tuple[int, int, int, str]:
    # Some actions do not have transactionIndex/logIndex. Missing values sort as 0.
    return (
        parse_int(tx.get("blockNumber")),
        parse_int(tx.get("transactionIndex")),
        parse_int(tx.get("logIndex")),
        str(tx.get("hash") or tx.get("transactionHash") or ""),
    )


def tx_identity(tx: Mapping[str, Any], action: str) -> str:
    # Normal/internal txs can be de-duped by hash. Token transfers need logIndex too.
    tx_hash = str(tx.get("hash") or tx.get("transactionHash") or "")
    if not tx_hash:
        return json.dumps(tx, sort_keys=True, ensure_ascii=False)

    if action in {"tokentx", "tokennfttx", "token1155tx"}:
        return f"{tx_hash}:{tx.get('logIndex', '')}:{tx.get('contractAddress', '')}:{tx.get('tokenID', '')}"

    if action == "txlistinternal":
        return f"{tx_hash}:{tx.get('traceId', '')}:{tx.get('type', '')}"

    return tx_hash


def unique_transactions(txs: Iterable[Tx], action: str) -> List[Tx]:
    seen: Set[str] = set()
    out: List[Tx] = []
    for tx in sorted(txs, key=tx_sort_key):
        identity = tx_identity(tx, action)
        if identity in seen:
            continue
        seen.add(identity)
        out.append(tx)
    return out


def build_params(config: Config, address: str, start_block: int, end_block: int, page: int) -> Dict[str, Any]:
    return {
        "chainid": config.chain_id,
        "module": "account",
        "action": config.action,
        "address": address,
        "startblock": start_block,
        "endblock": end_block,
        "page": page,
        "offset": config.page_size,
        "sort": "asc",
        "apikey": config.api_key,
    }


def validate_config(config: Config) -> None:
    if not config.api_key:
        raise ValueError("ETHERSCAN_API_KEY is empty; pass --api-key or set env var")
    if config.action not in SUPPORTED_ACTIONS:
        raise ValueError(f"Unsupported action {config.action!r}. Supported: {', '.join(sorted(SUPPORTED_ACTIONS))}")
    if config.end_block < config.start_block:
        raise ValueError("end_block must be >= start_block")
    if config.initial_workers > config.max_workers:
        raise ValueError("workers must be <= max-workers")
    try:
        chain_id_int = int(str(config.chain_id).strip())
    except ValueError:
        raise ValueError(f"chain_id must be a positive integer, got {config.chain_id!r}") from None
    if chain_id_int <= 0:
        raise ValueError(f"chain_id must be > 0, got {chain_id_int}")
    if config.requests_per_second <= 0:
        raise ValueError("requests_per_second must be > 0")
    if config.timeout <= 0:
        raise ValueError("timeout must be > 0")
    if config.page_size > 10_000:
        raise ValueError("page_size must be <= 10000 for reliable Etherscan pagination")
    if not config.base_url.lower().startswith(("https://", "http://")):
        raise ValueError("base_url must start with http:// or https://")


# ============================================================
# ETHERSCAN REQUESTS
# ============================================================

def etherscan_get(config: Config, limiter: TokenBucket, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute one logical Etherscan request with rate-limited manual retries.

    All network/HTTP/API retries go through TokenBucket.acquire(). This keeps the
    configured global RPS true even during throttling or provider instability.
    """
    session = get_session(config)
    last_error = "unknown"
    network_failures = 0
    transient_http = {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}

    for attempt in range(config.api_retries):
        if shutdown_event.is_set():
            raise RuntimeError("shutdown")

        limiter.acquire()

        try:
            response = session.get(config.base_url, params=params, timeout=config.timeout)
        except requests.RequestException as exc:
            network_failures += 1
            last_error = f"network_error: {exc}"
            if network_failures > config.transport_retries or attempt + 1 >= config.api_retries:
                break
            sleep_for = full_jitter_backoff(network_failures, config.retry_backoff_cap)
            log.debug(
                "%s; transport retry %d/%d in %.2fs",
                last_error,
                network_failures,
                config.transport_retries,
                sleep_for,
            )
            interruptible_sleep(sleep_for)
            continue

        network_failures = 0

        if response.status_code in transient_http:
            last_error = f"http_{response.status_code}"
            if attempt + 1 >= config.api_retries:
                break
            sleep_for = retry_after_seconds(response)
            if sleep_for is None:
                sleep_for = full_jitter_backoff(attempt + 1, config.retry_backoff_cap)
            log.debug("%s; retry %d/%d in %.2fs", last_error, attempt + 1, config.api_retries, sleep_for)
            interruptible_sleep(sleep_for)
            continue

        try:
            data = response.json()
        except (json.JSONDecodeError, requests.exceptions.JSONDecodeError) as exc:
            snippet = response.text[:250].replace("\n", " ")
            raise NonFatalAPIError(
                f"invalid_json: http={response.status_code}; body={snippet!r}"
            ) from exc

        if not isinstance(data, dict):
            raise NonFatalAPIError(
                f"unexpected_payload_type: http={response.status_code}; type={type(data).__name__}"
            )

        status = str(data.get("status", "")).strip()
        message = str(data.get("message", "")).strip()
        result = data.get("result")
        haystack = f"{message} {result}".lower()

        if status == "1":
            return data

        if any(x in haystack for x in ("no transactions found", "no records found")):
            return {"status": "1", "message": "OK", "result": []}

        if any(
            x in haystack
            for x in (
                "rate limit",
                "max rate limit",
                "temporarily unavailable",
                "timeout",
                "server too busy",
            )
        ):
            last_error = f"api_transient: {message} | {result}"
            if attempt + 1 >= config.api_retries:
                break
            sleep_for = retry_after_seconds(response)
            if sleep_for is None:
                sleep_for = full_jitter_backoff(attempt + 1, config.retry_backoff_cap)
            log.debug("%s; retry %d/%d in %.2fs", last_error, attempt + 1, config.api_retries, sleep_for)
            interruptible_sleep(sleep_for)
            continue

        if any(
            x in haystack
            for x in (
                "invalid api key",
                "missing or unsupported chainid",
                "deprecated v1 endpoint",
                "invalid action",
                "missing required parameter",
                "invalid address format",
            )
        ):
            raise FatalAPIError(f"fatal_api_error: http={response.status_code}; {message} | {result}")

        if response.status_code >= 400:
            raise NonFatalAPIError(f"http_api_error: http={response.status_code}; {message} | {result}")

        raise NonFatalAPIError(f"api_error: http={response.status_code}; {message} | {result}")

    raise RateLimitError(f"retries_exhausted after {config.api_retries} attempts: {last_error}")


def fetch_range_page(
    config: Config,
    limiter: TokenBucket,
    address: str,
    start_block: int,
    end_block: int,
    page: int,
) -> List[Tx]:
    params = build_params(config, address, start_block, end_block, page)
    data = etherscan_get(config, limiter, params)
    result = data.get("result", [])
    if not isinstance(result, list):
        raise NonFatalAPIError(f"unexpected_result_type: {type(result).__name__}; result={result!r}")
    return [dict(item) for item in result if isinstance(item, dict)]


def fetch_range_first_page(config: Config, limiter: TokenBucket, address: str, block_range: BlockRange) -> List[Tx]:
    start_block, end_block = block_range
    return fetch_range_page(config, limiter, address, start_block, end_block, 1)


def fetch_range_remaining_pages(
    config: Config,
    limiter: TokenBucket,
    address: str,
    start_block: int,
    end_block: int,
    first_page: List[Tx],
) -> List[Tx]:
    """Continue pagination for a range without fetching page 1 twice."""
    all_txs: List[Tx] = list(first_page)
    page = 2
    while not shutdown_event.is_set():
        if page > config.max_single_block_pages:
            raise NonFatalAPIError(
                f"range_page_limit_exceeded: blocks={start_block}-{end_block}; "
                f"pages>{config.max_single_block_pages}; increase ETHERSCAN_MAX_SINGLE_BLOCK_PAGES"
            )
        txs = fetch_range_page(config, limiter, address, start_block, end_block, page)
        all_txs.extend(txs)
        if len(txs) < config.page_size:
            return all_txs
        page += 1
    raise RuntimeError("shutdown")


def fetch_single_block_remaining_pages(
    config: Config,
    limiter: TokenBucket,
    address: str,
    block_number: int,
    first_page: List[Tx],
) -> List[Tx]:
    return fetch_range_remaining_pages(
        config, limiter, address, block_number, block_number, first_page
    )


# ============================================================
# FETCH ENGINE
# ============================================================

def fetch_transactions(address: str, config: Config = CONFIG) -> List[Tx]:
    validate_config(config)
    address = normalize_address(address)

    cache_key = ":".join(
        [
            "etherscan-v5",
            config.base_url.rstrip("/").lower(),
            config.chain_id,
            config.action,
            address,
            str(config.start_block),
            str(config.end_block),
            str(config.page_size),
        ]
    )
    cached = cache_get(cache_key, config)
    if cached is not None:
        log.info("Cache hit: %d rows", len(cached))
        return cached

    limiter = TokenBucket(config.requests_per_second, config.burst)
    queued: Deque[BlockRange] = deque([(config.start_block, config.end_block)])
    active: Dict[Future[List[Tx]], BlockRange] = {}
    all_txs: List[Tx] = []
    failed_ranges: List[Tuple[BlockRange, str]] = []
    range_requeues: Dict[BlockRange, int] = {}

    workers = min(config.initial_workers, config.max_workers)
    completed_ranges = 0
    saturated_ranges = 0
    successes_since_rate_limit = 0

    def submit(executor: ThreadPoolExecutor, block_range: BlockRange) -> None:
        future = executor.submit(fetch_range_first_page, config, limiter, address, block_range)
        active[future] = block_range

    log.info(
        "Start: address=%s chainid=%s action=%s blocks=%d-%d page_size=%d rps=%.2f workers=%d/%d",
        address,
        config.chain_id,
        config.action,
        config.start_block,
        config.end_block,
        config.page_size,
        config.requests_per_second,
        workers,
        config.max_workers,
    )

    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        while queued and len(active) < workers:
            submit(executor, queued.popleft())

        while active and not shutdown_event.is_set():
            done, _ = wait(active.keys(), return_when=FIRST_COMPLETED, timeout=1.0)
            if not done:
                continue

            for future in done:
                block_range = active.pop(future)
                start_block, end_block = block_range

                try:
                    txs = future.result()
                except RateLimitError as exc:
                    attempts = range_requeues.get(block_range, 0) + 1
                    range_requeues[block_range] = attempts
                    successes_since_rate_limit = 0
                    workers = max(1, workers - 1)
                    if attempts <= config.max_range_requeues:
                        queued.appendleft(block_range)
                        log.warning(
                            "Rate pressure on %d-%d; requeue=%d/%d; workers=%d; %s",
                            start_block,
                            end_block,
                            attempts,
                            config.max_range_requeues,
                            workers,
                            exc,
                        )
                        interruptible_sleep(min(2.0 * attempts, 15.0))
                        continue
                    if config.fail_soft:
                        failed_ranges.append((block_range, str(exc)))
                        log.warning("Range exhausted retries and was skipped: %d-%d | %s", start_block, end_block, exc)
                        continue
                    raise
                except FatalAPIError:
                    shutdown_event.set()
                    raise
                except Exception as exc:
                    if config.fail_soft:
                        failed_ranges.append((block_range, str(exc)))
                        log.warning("Range failed and skipped: %d-%d | %s", start_block, end_block, exc)
                        continue
                    raise

                completed_ranges += 1
                successes_since_rate_limit += 1
                saturated = len(txs) >= config.page_size

                if saturated:
                    saturated_ranges += 1
                    span = end_block - start_block + 1
                    if start_block < end_block and span > config.min_split_block_span:
                        # A full page is ambiguous: there may be more rows. Split the block
                        # interval and re-query both halves; duplicates are removed later.
                        mid = (start_block + end_block) // 2
                        queued.appendleft((mid + 1, end_block))
                        queued.appendleft((start_block, mid))
                    else:
                        # Once the range is too small to split further, paginate it. This
                        # preserves correctness for a single extremely busy block and also
                        # finally gives ETHERSCAN_MIN_SPLIT_BLOCK_SPAN real semantics.
                        all_txs.extend(
                            fetch_range_remaining_pages(
                                config, limiter, address, start_block, end_block, txs
                            )
                        )
                else:
                    all_txs.extend(txs)

                if completed_ranges % config.log_progress_every == 0:
                    approx_unique = len({tx_identity(tx, config.action) for tx in all_txs})
                    log.info(
                        "Progress: completed=%d queued=%d active=%d saturated=%d rows~=%d unique~=%d workers=%d failed=%d",
                        completed_ranges,
                        len(queued),
                        len(active),
                        saturated_ranges,
                        len(all_txs),
                        approx_unique,
                        workers,
                        len(failed_ranges),
                    )

            # Restore concurrency only after a few successful ranges. The token bucket
            # remains the hard RPS gate; this worker adaptation reduces pressure after
            # repeated throttling instead of bouncing back immediately.
            recovery_successes = 3
            if (
                workers < config.max_workers
                and queued
                and successes_since_rate_limit >= recovery_successes
            ):
                workers += 1
                successes_since_rate_limit = 0

            while queued and len(active) < workers:
                submit(executor, queued.popleft())

    if shutdown_event.is_set():
        log.warning("Stopped before completion; returning partial result")

    result = unique_transactions(all_txs, config.action)
    complete = not shutdown_event.is_set() and not failed_ranges and not queued and not active
    if complete:
        cache_set(cache_key, result, config)
    else:
        log.warning("Result is partial; it will not be cached")

    log.info(
        "Finished: rows=%d unique=%d completed_ranges=%d saturated=%d failed=%d chainid=%s action=%s",
        len(all_txs),
        len(result),
        completed_ranges,
        saturated_ranges,
        len(failed_ranges),
        config.chain_id,
        config.action,
    )

    if failed_ranges:
        log.warning("Failed ranges were skipped because fail_soft=True. First failures: %s", failed_ranges[:5])

    return result


# ============================================================
# OUTPUT
# ============================================================

def collect_csv_fields(rows: Sequence[Tx]) -> List[str]:
    preferred = [
        "blockNumber",
        "timeStamp",
        "hash",
        "transactionIndex",
        "logIndex",
        "from",
        "to",
        "value",
        "contractAddress",
        "tokenName",
        "tokenSymbol",
        "tokenDecimal",
        "tokenID",
        "gas",
        "gasPrice",
        "gasUsed",
        "isError",
        "txreceipt_status",
        "input",
        "methodId",
        "functionName",
    ]
    keys: Set[str] = set()
    for row in rows:
        keys.update(row.keys())
    return [k for k in preferred if k in keys] + sorted(keys - set(preferred))


def write_output(rows: List[Tx], output_path: str, *, jsonl: bool = False, csv_output: bool = False) -> None:
    """Write atomically so interruption never leaves a half-written destination file."""
    if jsonl and csv_output:
        raise ValueError("--jsonl and --csv are mutually exclusive")

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_name: Optional[str] = None
    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as f:
            temp_name = f.name
            if csv_output:
                fields = collect_csv_fields(rows)
                writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)
            elif jsonl:
                for row in rows:
                    f.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
                    f.write("\n")
            else:
                json.dump(rows, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_name, path)
    except Exception:
        if temp_name:
            try:
                Path(temp_name).unlink(missing_ok=True)
            except OSError:
                pass
        raise

    log.info("Saved atomically: %s", path)


# ============================================================
# CLI
# ============================================================

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Etherscan V2 account history for one address.")
    parser.add_argument("address", help="Ethereum-compatible address, e.g. 0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae")
    parser.add_argument("--api-key", default=CONFIG.api_key, help="Etherscan API key")
    parser.add_argument("--chainid", default=CONFIG.chain_id, help="Etherscan V2 chain id; Ethereum mainnet is 1")
    parser.add_argument("--base-url", default=CONFIG.base_url, help="Etherscan API base URL")
    parser.add_argument("--action", default=CONFIG.action, choices=sorted(SUPPORTED_ACTIONS), help="Account action to fetch")

    parser.add_argument("--start-block", type=int, default=CONFIG.start_block)
    parser.add_argument("--end-block", type=int, default=CONFIG.end_block)
    parser.add_argument("--page-size", type=int, default=CONFIG.page_size)

    parser.add_argument("--workers", type=int, default=CONFIG.initial_workers)
    parser.add_argument("--max-workers", type=int, default=CONFIG.max_workers)
    parser.add_argument("--rps", type=float, default=CONFIG.requests_per_second)
    parser.add_argument("--burst", type=int, default=CONFIG.burst)
    parser.add_argument("--timeout", type=float, default=CONFIG.timeout)
    parser.add_argument("--api-retries", type=int, default=CONFIG.api_retries)
    parser.add_argument("--transport-retries", type=int, default=CONFIG.transport_retries)
    parser.add_argument("--proxy", default=CONFIG.proxy, help="Optional proxy URL, e.g. http://user:pass@127.0.0.1:8080")

    parser.add_argument("--output", default=env_str("ETHERSCAN_OUTPUT", ""), help="Output path. If omitted, prints JSON to stdout")
    parser.add_argument("--jsonl", action="store_true", help="Write JSON Lines instead of a JSON array")
    parser.add_argument("--csv", action="store_true", help="Write CSV instead of JSON/JSONL")
    parser.add_argument("--fail-soft", action="store_true", default=CONFIG.fail_soft, help="Skip failed ranges instead of aborting")
    parser.add_argument("--strict", action="store_true", help="Abort on first failed range; overrides --fail-soft/env")
    parser.add_argument("--no-cache", action="store_true", help="Disable in-process TTL cache")
    args = parser.parse_args(argv)
    if args.jsonl and args.csv:
        parser.error("--jsonl and --csv are mutually exclusive")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    global _shutdown_signal
    shutdown_event.clear()
    _shutdown_signal = None
    args = parse_args(argv)

    config = replace(
        CONFIG,
        api_key=args.api_key,
        base_url=args.base_url,
        chain_id=str(args.chainid),
        action=args.action,
        start_block=args.start_block,
        end_block=args.end_block,
        page_size=args.page_size,
        initial_workers=args.workers,
        max_workers=args.max_workers,
        requests_per_second=args.rps,
        burst=args.burst,
        timeout=args.timeout,
        api_retries=args.api_retries,
        transport_retries=args.transport_retries,
        proxy=args.proxy,
        fail_soft=False if args.strict else bool(args.fail_soft),
        cache_size=0 if args.no_cache else CONFIG.cache_size,
    )

    rows = fetch_transactions(args.address, config)

    if args.output:
        write_output(rows, args.output, jsonl=args.jsonl, csv_output=args.csv)
    else:
        for row in rows if args.jsonl else [rows]:
            print(json.dumps(row, ensure_ascii=False, indent=None if args.jsonl else 2, sort_keys=True))

    return shutdown_exit_code()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        shutdown_event.set()
        raise SystemExit(130)
    except FatalAPIError as exc:
        log.error("Fatal API error: %s", exc)
        raise SystemExit(2)
    except Exception as exc:
        log.error("Error: %s", exc)
        raise SystemExit(1)
