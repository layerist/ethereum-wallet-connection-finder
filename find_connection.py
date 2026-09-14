#!/usr/bin/env python3
"""
Reliable Etherscan V2 history fetcher (v6).

Highlights:
- Etherscan V2 endpoint support: /v2/api + chainid.
- Normal, internal, ERC-20, ERC-721 and ERC-1155 account actions.
- Global token-bucket limiter shared by all worker threads and all retries.
- Retry-After aware retries with capped full-jitter backoff.
- Separate connect/read timeouts.
- Range splitting on saturated pages, with bounded pagination for unsplittable ranges.
- Pagination loop/stall detection to avoid silently accepting repeated API pages.
- Adaptive worker pressure after throttling without violating the global RPS cap.
- Thread-local keep-alive sessions; urllib3 retries remain disabled intentionally.
- Deterministic de-duplication and sorting.
- Atomic JSON/JSONL/CSV writes, including directory fsync on POSIX when possible.
- Strict mode and fail-soft mode with optional machine-readable failure report.
- Graceful SIGINT/SIGTERM: completed rows are returned/saved and exit status is non-zero.
- In-process TTL/LRU cache only stores complete results.

Examples:
  python etherscan_tx_fetcher_v6.py 0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae \
    --api-key YOUR_KEY --chainid 1 --action txlist --output txs.json

  python etherscan_tx_fetcher_v6.py 0x... --action tokentx --csv --output erc20.csv

Environment:
  ETHERSCAN_API_KEY
  ETHERSCAN_CHAIN_ID=1
  ETHERSCAN_BASE_URL=https://api.etherscan.io/v2/api
  ETHERSCAN_RPS=2.7
  ETHERSCAN_BURST=3
  ETHERSCAN_PAGE_SIZE=10000
  ETHERSCAN_WORKERS=4
  ETHERSCAN_MAX_WORKERS=8
  ETHERSCAN_CONNECT_TIMEOUT=10
  ETHERSCAN_READ_TIMEOUT=20
  ETHERSCAN_API_RETRIES=7
  ETHERSCAN_TRANSPORT_RETRIES=2
  ETHERSCAN_MAX_RETRY_AFTER=60
  ETHERSCAN_PROXY=http://user:pass@127.0.0.1:8080
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import os
import random
import signal
import sys
import threading
import time
from collections import OrderedDict, deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass, field, replace
from email.utils import parsedate_to_datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple
from urllib.parse import urlparse

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
        raise ValueError(f"{name} must be an integer, got {raw!r}") from None
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
        raise ValueError(f"{name} must be a float, got {raw!r}") from None
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite, got {value!r}")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean, got {raw!r}")


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
    "txlist",
    "txlistinternal",
    "tokentx",
    "tokennfttx",
    "token1155tx",
}

TRANSIENT_HTTP = {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}
FATAL_HTTP = {400, 401, 403, 404}


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

    connect_timeout: float = env_float("ETHERSCAN_CONNECT_TIMEOUT", 10.0, minimum=0.1)
    read_timeout: float = env_float("ETHERSCAN_READ_TIMEOUT", 20.0, minimum=0.1)
    transport_retries: int = env_int("ETHERSCAN_TRANSPORT_RETRIES", 2, minimum=0)
    api_retries: int = env_int("ETHERSCAN_API_RETRIES", 7, minimum=1)
    retry_backoff_cap: float = env_float("ETHERSCAN_BACKOFF_CAP", 35.0, minimum=0.1)
    max_retry_after: float = env_float("ETHERSCAN_MAX_RETRY_AFTER", 60.0, minimum=0.0)

    cache_ttl: float = env_float("ETHERSCAN_CACHE_TTL", 600.0, minimum=0.0)
    cache_size: int = env_int("ETHERSCAN_CACHE_SIZE", 128, minimum=0)

    fail_soft: bool = env_bool("ETHERSCAN_FAIL_SOFT", False)
    min_split_block_span: int = env_int("ETHERSCAN_MIN_SPLIT_BLOCK_SPAN", 1, minimum=1)
    max_range_pages: int = env_int("ETHERSCAN_MAX_RANGE_PAGES", 200, minimum=1)
    max_range_requeues: int = env_int("ETHERSCAN_MAX_RANGE_REQUEUES", 5, minimum=0)
    recovery_successes: int = env_int("ETHERSCAN_RECOVERY_SUCCESSES", 3, minimum=1)
    log_progress_every: int = env_int("ETHERSCAN_LOG_PROGRESS_EVERY", 25, minimum=1)

    proxy: str = env_str("ETHERSCAN_PROXY", "")
    user_agent: str = env_str("ETHERSCAN_USER_AGENT", "etherscan-fetcher/6.0")


CONFIG = Config()
shutdown_event = threading.Event()
_shutdown_signal: Optional[int] = None


@dataclass
class FetchStats:
    http_attempts: int = 0
    http_successes: int = 0
    http_transient: int = 0
    transport_errors: int = 0
    api_transient: int = 0
    rate_limit_exhaustions: int = 0
    completed_ranges: int = 0
    saturated_ranges: int = 0
    paginated_ranges: int = 0
    pages_fetched: int = 0


class AtomicStats:
    def __init__(self) -> None:
        self._stats = FetchStats()
        self._lock = threading.Lock()

    def inc(self, name: str, amount: int = 1) -> None:
        with self._lock:
            setattr(self._stats, name, getattr(self._stats, name) + amount)

    def snapshot(self) -> FetchStats:
        with self._lock:
            return replace(self._stats)


@dataclass
class FailedRange:
    start_block: int
    end_block: int
    error: str


@dataclass
class FetchOutcome:
    rows: List[Tx]
    complete: bool
    interrupted: bool
    failed_ranges: List[FailedRange] = field(default_factory=list)
    stats: FetchStats = field(default_factory=FetchStats)


# ============================================================
# SIGNALS
# ============================================================


def _signal_handler(signum: int, _frame: object) -> None:
    global _shutdown_signal
    if _shutdown_signal is None:
        _shutdown_signal = signum
    shutdown_event.set()


def shutdown_exit_code() -> int:
    if _shutdown_signal is None:
        return 0
    return 128 + int(_shutdown_signal)


try:
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
except (ValueError, OSError):
    pass


# ============================================================
# EXCEPTIONS
# ============================================================


class FetcherError(Exception):
    """Base fetcher error."""


class ShutdownRequested(FetcherError):
    """Raised internally to unwind promptly after SIGINT/SIGTERM."""


class RateLimitError(FetcherError):
    """Retries were exhausted due to throttling/transient errors."""


class FatalAPIError(FetcherError):
    """Fatal request/API configuration error."""


class NonFatalAPIError(FetcherError):
    """Recoverable failure local to a requested range/page."""


# ============================================================
# RATE LIMITER
# ============================================================


class TokenBucket:
    def __init__(self, rate: float, burst: int) -> None:
        if rate <= 0:
            raise ValueError("rate must be > 0")
        if burst <= 0:
            raise ValueError("burst must be > 0")
        self.rate = float(rate)
        self.capacity = float(burst)
        self.tokens = float(burst)
        self.updated_at = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            if shutdown_event.is_set():
                raise ShutdownRequested("shutdown requested")

            with self.lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self.updated_at)
                self.updated_at = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return

                sleep_for = (1.0 - self.tokens) / self.rate

            interruptible_sleep(min(max(sleep_for, 0.01), 1.0))


# ============================================================
# HTTP SESSION
# ============================================================

_thread_local = threading.local()


def _session_key(config: Config) -> Tuple[str, str, int]:
    return (config.proxy, config.user_agent, config.max_workers)


def get_session(config: Config) -> Session:
    """Return one keep-alive Session per worker thread/configuration.

    urllib3 retries are deliberately disabled: every HTTP retry must pass through
    etherscan_get() so the shared TokenBucket accounts for every network attempt.
    """
    key = _session_key(config)
    session = getattr(_thread_local, "session", None)
    old_key = getattr(_thread_local, "session_key", None)
    if session is not None and old_key == key:
        return session

    if session is not None:
        try:
            session.close()
        except Exception:
            pass

    pool_size = max(config.max_workers * 2, 4)
    adapter = HTTPAdapter(
        pool_connections=pool_size,
        pool_maxsize=pool_size,
        max_retries=0,
        pool_block=True,
    )

    session = Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": config.user_agent,
            "Accept": "application/json",
        }
    )

    if config.proxy:
        session.proxies.update({"http": config.proxy, "https": config.proxy})

    _thread_local.session = session
    _thread_local.session_key = key
    return session


# ============================================================
# TTL LRU CACHE
# ============================================================

_cache: "OrderedDict[str, Tuple[float, List[Tx]]]" = OrderedDict()
_cache_lock = threading.Lock()


def cache_get(key: str, config: Config) -> Optional[List[Tx]]:
    if config.cache_size <= 0 or config.cache_ttl <= 0:
        return None
    now = time.monotonic()
    with _cache_lock:
        item = _cache.get(key)
        if item is None:
            return None
        ts, value = item
        if now - ts > config.cache_ttl:
            _cache.pop(key, None)
            return None
        _cache.move_to_end(key)
        return [dict(tx) for tx in value]


def cache_set(key: str, value: List[Tx], config: Config) -> None:
    if config.cache_size <= 0 or config.cache_ttl <= 0:
        return
    with _cache_lock:
        _cache[key] = (time.monotonic(), [dict(tx) for tx in value])
        _cache.move_to_end(key)
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
    exponent = min(max(attempt, 0), 30)
    return random.uniform(0.0, min(2.0**exponent, cap))


def interruptible_sleep(seconds: float) -> None:
    if seconds <= 0:
        return
    if shutdown_event.wait(timeout=seconds):
        raise ShutdownRequested("shutdown requested")


def retry_after_seconds(response: requests.Response, cap: float) -> Optional[float]:
    raw = response.headers.get("Retry-After")
    if not raw:
        return None

    delay: Optional[float]
    try:
        delay = max(0.0, float(raw))
    except ValueError:
        try:
            target = parsedate_to_datetime(raw)
            if target.tzinfo is None:
                delay = None
            else:
                delay = max(0.0, target.timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            delay = None

    if delay is None:
        return None
    return min(delay, cap)


def tx_sort_key(tx: Mapping[str, Any]) -> Tuple[int, int, int, str, str]:
    return (
        parse_int(tx.get("blockNumber")),
        parse_int(tx.get("transactionIndex")),
        parse_int(tx.get("logIndex")),
        str(tx.get("traceId") or ""),
        str(tx.get("hash") or tx.get("transactionHash") or ""),
    )


def tx_identity(tx: Mapping[str, Any], action: str) -> str:
    tx_hash = str(tx.get("hash") or tx.get("transactionHash") or "").lower()
    if not tx_hash:
        return "row:" + hashlib.sha256(
            json.dumps(tx, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    if action in {"tokentx", "tokennfttx", "token1155tx"}:
        return ":".join(
            (
                tx_hash,
                str(tx.get("logIndex") or ""),
                str(tx.get("contractAddress") or "").lower(),
                str(tx.get("tokenID") or ""),
                str(tx.get("tokenValue") or "") if action == "token1155tx" else "",
            )
        )

    if action == "txlistinternal":
        return ":".join(
            (
                tx_hash,
                str(tx.get("traceId") or ""),
                str(tx.get("type") or ""),
            )
        )

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


def page_fingerprint(txs: Sequence[Tx], action: str) -> str:
    digest = hashlib.sha256()
    digest.update(str(len(txs)).encode("ascii"))
    digest.update(b"\0")
    for tx in txs:
        digest.update(tx_identity(tx, action).encode("utf-8", errors="replace"))
        digest.update(b"\n")
    return digest.hexdigest()


def build_params(
    config: Config,
    address: str,
    start_block: int,
    end_block: int,
    page: int,
) -> Dict[str, Any]:
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
        raise ValueError("ETHERSCAN_API_KEY is empty; pass --api-key or set the environment variable")
    if config.action not in SUPPORTED_ACTIONS:
        raise ValueError(
            f"Unsupported action {config.action!r}. Supported: {', '.join(sorted(SUPPORTED_ACTIONS))}"
        )
    if config.end_block < config.start_block:
        raise ValueError("end_block must be >= start_block")
    if config.start_block < 0 or config.end_block < 0:
        raise ValueError("start_block and end_block must be >= 0")
    if config.page_size < 1 or config.page_size > 10_000:
        raise ValueError("page_size must be in the range 1..10000")
    if config.initial_workers < 1 or config.max_workers < 1:
        raise ValueError("workers and max-workers must be >= 1")
    if config.initial_workers > config.max_workers:
        raise ValueError("workers must be <= max-workers")
    if config.requests_per_second <= 0 or not math.isfinite(config.requests_per_second):
        raise ValueError("rps must be a finite value > 0")
    if config.burst < 1:
        raise ValueError("burst must be >= 1")
    if config.connect_timeout <= 0 or not math.isfinite(config.connect_timeout):
        raise ValueError("connect_timeout must be a finite value > 0")
    if config.read_timeout <= 0 or not math.isfinite(config.read_timeout):
        raise ValueError("read_timeout must be a finite value > 0")
    if config.api_retries < 1:
        raise ValueError("api_retries must be >= 1")
    if config.transport_retries < 0:
        raise ValueError("transport_retries must be >= 0")
    if config.max_retry_after < 0 or not math.isfinite(config.max_retry_after):
        raise ValueError("max_retry_after must be a finite value >= 0")

    try:
        chain_id_int = int(str(config.chain_id).strip())
    except ValueError:
        raise ValueError(f"chain_id must be a positive integer, got {config.chain_id!r}") from None
    if chain_id_int <= 0:
        raise ValueError(f"chain_id must be > 0, got {chain_id_int}")

    parsed = urlparse(config.base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"Invalid base_url: {config.base_url!r}")

    if config.proxy:
        proxy = urlparse(config.proxy)
        if proxy.scheme not in {"http", "https", "socks4", "socks5", "socks5h"} or not proxy.netloc:
            raise ValueError(f"Invalid proxy URL: {config.proxy!r}")


def _payload_text(data: Mapping[str, Any]) -> Tuple[str, str, str]:
    status = str(data.get("status", "")).strip()
    message = str(data.get("message", "")).strip()
    result = data.get("result")
    if isinstance(result, str):
        result_text = result
    else:
        try:
            result_text = json.dumps(result, ensure_ascii=False, separators=(",", ":"))[:1000]
        except Exception:
            result_text = repr(result)[:1000]
    return status, message, result_text


# ============================================================
# ETHERSCAN REQUESTS
# ============================================================


def etherscan_get(
    config: Config,
    limiter: TokenBucket,
    params: Dict[str, Any],
    stats: AtomicStats,
) -> Dict[str, Any]:
    session = get_session(config)
    last_error = "unknown"
    consecutive_transport_failures = 0

    for attempt in range(1, config.api_retries + 1):
        if shutdown_event.is_set():
            raise ShutdownRequested("shutdown requested")

        limiter.acquire()
        stats.inc("http_attempts")

        try:
            response = session.get(
                config.base_url,
                params=params,
                timeout=(config.connect_timeout, config.read_timeout),
            )
        except requests.RequestException as exc:
            consecutive_transport_failures += 1
            stats.inc("transport_errors")
            last_error = f"network_error: {exc.__class__.__name__}: {exc}"
            if (
                consecutive_transport_failures > config.transport_retries
                or attempt >= config.api_retries
            ):
                break
            sleep_for = full_jitter_backoff(consecutive_transport_failures, config.retry_backoff_cap)
            log.debug(
                "%s; transport retry %d/%d in %.2fs",
                last_error,
                consecutive_transport_failures,
                config.transport_retries,
                sleep_for,
            )
            interruptible_sleep(sleep_for)
            continue

        consecutive_transport_failures = 0

        if response.status_code in TRANSIENT_HTTP:
            stats.inc("http_transient")
            last_error = f"http_{response.status_code}"
            if attempt >= config.api_retries:
                break
            sleep_for = retry_after_seconds(response, config.max_retry_after)
            if sleep_for is None:
                sleep_for = full_jitter_backoff(attempt, config.retry_backoff_cap)
            log.debug(
                "%s; retry %d/%d in %.2fs",
                last_error,
                attempt,
                config.api_retries,
                sleep_for,
            )
            interruptible_sleep(sleep_for)
            continue

        if response.status_code in FATAL_HTTP:
            snippet = response.text[:300].replace("\n", " ")
            raise FatalAPIError(f"HTTP {response.status_code}: {snippet!r}")

        try:
            data = response.json()
        except (ValueError, requests.exceptions.JSONDecodeError) as exc:
            snippet = response.text[:300].replace("\n", " ")
            if response.status_code >= 500:
                last_error = f"invalid_json_http_{response.status_code}: {snippet!r}"
                if attempt >= config.api_retries:
                    break
                stats.inc("http_transient")
                interruptible_sleep(full_jitter_backoff(attempt, config.retry_backoff_cap))
                continue
            raise NonFatalAPIError(
                f"invalid_json: http={response.status_code}; body={snippet!r}"
            ) from exc

        if not isinstance(data, dict):
            raise NonFatalAPIError(
                f"unexpected_payload_type: http={response.status_code}; type={type(data).__name__}"
            )

        status, message, result_text = _payload_text(data)
        haystack = f"{message} {result_text}".lower()

        if status == "1":
            stats.inc("http_successes")
            return data

        if any(text in haystack for text in ("no transactions found", "no records found")):
            stats.inc("http_successes")
            return {"status": "1", "message": "OK", "result": []}

        if any(
            text in haystack
            for text in (
                "rate limit",
                "max rate limit",
                "temporarily unavailable",
                "timeout",
                "server too busy",
                "query timeout",
            )
        ):
            stats.inc("api_transient")
            last_error = f"api_transient: {message} | {result_text}"
            if attempt >= config.api_retries:
                break
            sleep_for = retry_after_seconds(response, config.max_retry_after)
            if sleep_for is None:
                sleep_for = full_jitter_backoff(attempt, config.retry_backoff_cap)
            log.debug(
                "%s; retry %d/%d in %.2fs",
                last_error,
                attempt,
                config.api_retries,
                sleep_for,
            )
            interruptible_sleep(sleep_for)
            continue

        if any(
            text in haystack
            for text in (
                "invalid api key",
                "missing or unsupported chainid",
                "deprecated v1 endpoint",
                "invalid action",
                "missing required parameter",
                "invalid address format",
                "unsupported chain",
            )
        ):
            raise FatalAPIError(
                f"fatal_api_error: http={response.status_code}; {message} | {result_text}"
            )

        if response.status_code >= 400:
            raise NonFatalAPIError(
                f"http_api_error: http={response.status_code}; {message} | {result_text}"
            )

        raise NonFatalAPIError(
            f"api_error: http={response.status_code}; {message} | {result_text}"
        )

    stats.inc("rate_limit_exhaustions")
    raise RateLimitError(
        f"retries_exhausted after {config.api_retries} attempts: {last_error}"
    )


def fetch_range_page(
    config: Config,
    limiter: TokenBucket,
    address: str,
    start_block: int,
    end_block: int,
    page: int,
    stats: AtomicStats,
) -> List[Tx]:
    params = build_params(config, address, start_block, end_block, page)
    data = etherscan_get(config, limiter, params, stats)
    result = data.get("result", [])
    if not isinstance(result, list):
        raise NonFatalAPIError(
            f"unexpected_result_type: {type(result).__name__}; result={str(result)[:500]!r}"
        )
    stats.inc("pages_fetched")
    return [dict(item) for item in result if isinstance(item, dict)]


def fetch_range_first_page(
    config: Config,
    limiter: TokenBucket,
    address: str,
    block_range: BlockRange,
    stats: AtomicStats,
) -> List[Tx]:
    start_block, end_block = block_range
    return fetch_range_page(config, limiter, address, start_block, end_block, 1, stats)


def fetch_range_remaining_pages(
    config: Config,
    limiter: TokenBucket,
    address: str,
    start_block: int,
    end_block: int,
    first_page: List[Tx],
    stats: AtomicStats,
) -> List[Tx]:
    """Continue a saturated range from page 2 with loop/stall protection."""
    all_txs: List[Tx] = list(first_page)
    seen_fingerprints: Set[str] = {page_fingerprint(first_page, config.action)}
    seen_identities: Set[str] = {tx_identity(tx, config.action) for tx in first_page}

    for page in range(2, config.max_range_pages + 1):
        if shutdown_event.is_set():
            raise ShutdownRequested("shutdown requested")

        txs = fetch_range_page(
            config,
            limiter,
            address,
            start_block,
            end_block,
            page,
            stats,
        )

        fingerprint = page_fingerprint(txs, config.action)
        if fingerprint in seen_fingerprints:
            raise NonFatalAPIError(
                f"pagination_loop_detected: blocks={start_block}-{end_block}; page={page}"
            )
        seen_fingerprints.add(fingerprint)

        if txs:
            page_ids = {tx_identity(tx, config.action) for tx in txs}
            if page_ids and page_ids.issubset(seen_identities):
                raise NonFatalAPIError(
                    f"pagination_stalled: blocks={start_block}-{end_block}; page={page}; "
                    "page contained no new transaction identities"
                )
            seen_identities.update(page_ids)

        all_txs.extend(txs)
        if len(txs) < config.page_size:
            return all_txs

    raise NonFatalAPIError(
        f"range_page_limit_exceeded: blocks={start_block}-{end_block}; "
        f"pages>={config.max_range_pages}; increase ETHERSCAN_MAX_RANGE_PAGES or split more aggressively"
    )


# ============================================================
# FETCH ENGINE
# ============================================================


def make_cache_key(config: Config, address: str) -> str:
    return ":".join(
        [
            "etherscan-v6",
            config.base_url.rstrip("/").lower(),
            config.chain_id,
            config.action,
            address,
            str(config.start_block),
            str(config.end_block),
            str(config.page_size),
        ]
    )


def fetch_transactions_detailed(address: str, config: Config = CONFIG) -> FetchOutcome:
    validate_config(config)
    address = normalize_address(address)

    cache_key = make_cache_key(config, address)
    cached = cache_get(cache_key, config)
    if cached is not None:
        log.info("Cache hit: %d rows", len(cached))
        return FetchOutcome(rows=cached, complete=True, interrupted=False)

    limiter = TokenBucket(config.requests_per_second, config.burst)
    stats = AtomicStats()
    queued: Deque[BlockRange] = deque([(config.start_block, config.end_block)])
    active: Dict[Future[List[Tx]], BlockRange] = {}
    all_txs: List[Tx] = []
    failed_ranges: List[FailedRange] = []
    range_requeues: Dict[BlockRange, int] = {}

    workers = min(config.initial_workers, config.max_workers)
    successes_since_rate_limit = 0

    def submit(executor: ThreadPoolExecutor, block_range: BlockRange) -> None:
        future = executor.submit(
            fetch_range_first_page,
            config,
            limiter,
            address,
            block_range,
            stats,
        )
        active[future] = block_range

    log.info(
        "Start: address=%s chainid=%s action=%s blocks=%d-%d page_size=%d "
        "rps=%.2f burst=%d workers=%d/%d",
        address,
        config.chain_id,
        config.action,
        config.start_block,
        config.end_block,
        config.page_size,
        config.requests_per_second,
        config.burst,
        workers,
        config.max_workers,
    )

    executor = ThreadPoolExecutor(max_workers=config.max_workers, thread_name_prefix="etherscan")
    try:
        while queued and len(active) < workers:
            submit(executor, queued.popleft())

        while active and not shutdown_event.is_set():
            done, _ = wait(tuple(active), return_when=FIRST_COMPLETED, timeout=0.5)
            if not done:
                continue

            for future in done:
                block_range = active.pop(future)
                start_block, end_block = block_range

                try:
                    txs = future.result()
                except ShutdownRequested:
                    shutdown_event.set()
                    continue
                except RateLimitError as exc:
                    attempts = range_requeues.get(block_range, 0) + 1
                    range_requeues[block_range] = attempts
                    successes_since_rate_limit = 0
                    workers = max(1, workers - 1)

                    if attempts <= config.max_range_requeues and not shutdown_event.is_set():
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
                        try:
                            interruptible_sleep(min(2.0 * attempts, 15.0))
                        except ShutdownRequested:
                            shutdown_event.set()
                        continue

                    if config.fail_soft:
                        failed_ranges.append(FailedRange(start_block, end_block, str(exc)))
                        log.warning("Range skipped after retry exhaustion: %d-%d | %s", start_block, end_block, exc)
                        continue
                    raise
                except FatalAPIError:
                    shutdown_event.set()
                    raise
                except Exception as exc:
                    if config.fail_soft:
                        failed_ranges.append(FailedRange(start_block, end_block, str(exc)))
                        log.warning("Range failed and was skipped: %d-%d | %s", start_block, end_block, exc)
                        continue
                    raise

                stats.inc("completed_ranges")
                successes_since_rate_limit += 1
                saturated = len(txs) >= config.page_size

                if saturated:
                    stats.inc("saturated_ranges")
                    span = end_block - start_block + 1
                    if start_block < end_block and span > config.min_split_block_span:
                        mid = (start_block + end_block) // 2
                        queued.appendleft((mid + 1, end_block))
                        queued.appendleft((start_block, mid))
                    else:
                        try:
                            paginated = fetch_range_remaining_pages(
                                config,
                                limiter,
                                address,
                                start_block,
                                end_block,
                                txs,
                                stats,
                            )
                        except ShutdownRequested:
                            shutdown_event.set()
                            all_txs.extend(txs)
                            continue
                        except Exception as exc:
                            if config.fail_soft:
                                # Preserve page 1 as known-good partial data, but mark the range incomplete.
                                all_txs.extend(txs)
                                failed_ranges.append(FailedRange(start_block, end_block, str(exc)))
                                log.warning(
                                    "Pagination failed; preserved first page only: %d-%d | %s",
                                    start_block,
                                    end_block,
                                    exc,
                                )
                                continue
                            raise

                        stats.inc("paginated_ranges")
                        all_txs.extend(paginated)
                else:
                    all_txs.extend(txs)

                snap = stats.snapshot()
                if snap.completed_ranges % config.log_progress_every == 0:
                    log.info(
                        "Progress: completed=%d queued=%d active=%d saturated=%d rows=%d "
                        "workers=%d failed=%d http_attempts=%d",
                        snap.completed_ranges,
                        len(queued),
                        len(active),
                        snap.saturated_ranges,
                        len(all_txs),
                        workers,
                        len(failed_ranges),
                        snap.http_attempts,
                    )

            if (
                workers < config.max_workers
                and queued
                and successes_since_rate_limit >= config.recovery_successes
            ):
                workers += 1
                successes_since_rate_limit = 0

            while queued and len(active) < workers and not shutdown_event.is_set():
                submit(executor, queued.popleft())

    finally:
        if shutdown_event.is_set():
            for future in active:
                future.cancel()
        # Running requests cannot be force-cancelled safely; wait for them to leave requests.get().
        executor.shutdown(wait=True, cancel_futures=True)

    if shutdown_event.is_set():
        log.warning("Stopped before completion; returning partial result")

    result = unique_transactions(all_txs, config.action)
    complete = (
        not shutdown_event.is_set()
        and not failed_ranges
        and not queued
        and not active
    )

    if complete:
        cache_set(cache_key, result, config)
    else:
        log.warning("Result is partial; it will not be cached")

    final_stats = stats.snapshot()
    log.info(
        "Finished: rows=%d unique=%d completed_ranges=%d saturated=%d paginated=%d "
        "failed=%d http_attempts=%d transport_errors=%d transient_http=%d transient_api=%d",
        len(all_txs),
        len(result),
        final_stats.completed_ranges,
        final_stats.saturated_ranges,
        final_stats.paginated_ranges,
        len(failed_ranges),
        final_stats.http_attempts,
        final_stats.transport_errors,
        final_stats.http_transient,
        final_stats.api_transient,
    )

    return FetchOutcome(
        rows=result,
        complete=complete,
        interrupted=shutdown_event.is_set(),
        failed_ranges=failed_ranges,
        stats=final_stats,
    )


def fetch_transactions(address: str, config: Config = CONFIG) -> List[Tx]:
    """Backward-compatible API returning rows only.

    Use fetch_transactions_detailed() when the caller needs completeness/failure metadata.
    """
    return fetch_transactions_detailed(address, config).rows


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
        "traceId",
        "from",
        "to",
        "value",
        "contractAddress",
        "tokenName",
        "tokenSymbol",
        "tokenDecimal",
        "tokenID",
        "tokenValue",
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
    return [key for key in preferred if key in keys] + sorted(keys - set(preferred))


def fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    flags = getattr(os, "O_DIRECTORY", 0) | os.O_RDONLY
    try:
        fd = os.open(str(path), flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write_text(path: Path, writer_fn: Any) -> None:
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
        ) as handle:
            temp_name = handle.name
            writer_fn(handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
        fsync_directory(path.parent)
    except Exception:
        if temp_name:
            try:
                Path(temp_name).unlink(missing_ok=True)
            except OSError:
                pass
        raise


def write_output(
    rows: List[Tx],
    output_path: str,
    *,
    jsonl: bool = False,
    csv_output: bool = False,
) -> None:
    if jsonl and csv_output:
        raise ValueError("--jsonl and --csv are mutually exclusive")

    path = Path(output_path)

    def writer(handle: Any) -> None:
        if csv_output:
            fields = collect_csv_fields(rows)
            if not fields:
                return
            csv_writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
            csv_writer.writeheader()
            csv_writer.writerows(rows)
        elif jsonl:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
                handle.write("\n")
        else:
            json.dump(rows, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")

    atomic_write_text(path, writer)
    log.info("Saved atomically: %s", path)


def write_failure_report(outcome: FetchOutcome, report_path: str) -> None:
    path = Path(report_path)
    payload = {
        "complete": outcome.complete,
        "interrupted": outcome.interrupted,
        "failed_ranges": [asdict(item) for item in outcome.failed_ranges],
        "stats": asdict(outcome.stats),
        "row_count": len(outcome.rows),
    }

    def writer(handle: Any) -> None:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")

    atomic_write_text(path, writer)
    log.info("Saved failure report: %s", path)


# ============================================================
# CLI
# ============================================================


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Etherscan V2 account history for one address."
    )
    parser.add_argument(
        "address",
        help="Ethereum-compatible address, e.g. 0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae",
    )
    parser.add_argument("--api-key", default=CONFIG.api_key, help="Etherscan API key")
    parser.add_argument("--chainid", default=CONFIG.chain_id, help="Etherscan V2 chain id")
    parser.add_argument("--base-url", default=CONFIG.base_url, help="Etherscan API V2 base URL")
    parser.add_argument(
        "--action",
        default=CONFIG.action,
        choices=sorted(SUPPORTED_ACTIONS),
        help="Account action to fetch",
    )

    parser.add_argument("--start-block", type=int, default=CONFIG.start_block)
    parser.add_argument("--end-block", type=int, default=CONFIG.end_block)
    parser.add_argument("--page-size", type=int, default=CONFIG.page_size)

    parser.add_argument("--workers", type=int, default=CONFIG.initial_workers)
    parser.add_argument("--max-workers", type=int, default=CONFIG.max_workers)
    parser.add_argument("--rps", type=float, default=CONFIG.requests_per_second)
    parser.add_argument("--burst", type=int, default=CONFIG.burst)
    parser.add_argument("--connect-timeout", type=float, default=CONFIG.connect_timeout)
    parser.add_argument("--read-timeout", type=float, default=CONFIG.read_timeout)
    parser.add_argument("--api-retries", type=int, default=CONFIG.api_retries)
    parser.add_argument("--transport-retries", type=int, default=CONFIG.transport_retries)
    parser.add_argument("--max-retry-after", type=float, default=CONFIG.max_retry_after)
    parser.add_argument("--proxy", default=CONFIG.proxy, help="Optional HTTP/HTTPS/SOCKS proxy URL")

    parser.add_argument(
        "--output",
        default=env_str("ETHERSCAN_OUTPUT", ""),
        help="Output path. If omitted, prints to stdout",
    )
    parser.add_argument("--jsonl", action="store_true", help="Write JSON Lines")
    parser.add_argument("--csv", action="store_true", help="Write CSV")
    parser.add_argument(
        "--failure-report",
        default=env_str("ETHERSCAN_FAILURE_REPORT", ""),
        help="Optional JSON report with completeness, failures and request statistics",
    )
    parser.add_argument(
        "--fail-soft",
        action="store_true",
        default=CONFIG.fail_soft,
        help="Skip failed ranges and return a partial result",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Abort on the first failed range; overrides --fail-soft/environment",
    )
    parser.add_argument("--no-cache", action="store_true", help="Disable in-process TTL cache")

    args = parser.parse_args(argv)
    if args.jsonl and args.csv:
        parser.error("--jsonl and --csv are mutually exclusive")
    return args


def build_config_from_args(args: argparse.Namespace) -> Config:
    return replace(
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
        connect_timeout=args.connect_timeout,
        read_timeout=args.read_timeout,
        api_retries=args.api_retries,
        transport_retries=args.transport_retries,
        max_retry_after=args.max_retry_after,
        proxy=args.proxy,
        fail_soft=False if args.strict else bool(args.fail_soft),
        cache_size=0 if args.no_cache else CONFIG.cache_size,
    )


def emit_stdout(rows: List[Tx], *, jsonl: bool, csv_output: bool) -> None:
    if csv_output:
        fields = collect_csv_fields(rows)
        if not fields:
            return
        writer = csv.DictWriter(sys.stdout, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        return

    if jsonl:
        for row in rows:
            print(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    else:
        print(json.dumps(rows, ensure_ascii=False, indent=2, sort_keys=True))


def main(argv: Optional[Sequence[str]] = None) -> int:
    global _shutdown_signal
    shutdown_event.clear()
    _shutdown_signal = None

    args = parse_args(argv)
    config = build_config_from_args(args)
    outcome = fetch_transactions_detailed(args.address, config)

    if args.output:
        write_output(outcome.rows, args.output, jsonl=args.jsonl, csv_output=args.csv)
    else:
        emit_stdout(outcome.rows, jsonl=args.jsonl, csv_output=args.csv)

    if args.failure_report:
        write_failure_report(outcome, args.failure_report)

    signal_code = shutdown_exit_code()
    if signal_code:
        return signal_code
    if not outcome.complete:
        return 3
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BrokenPipeError:
        try:
            sys.stdout.close()
        except Exception:
            pass
        raise SystemExit(0)
    except KeyboardInterrupt:
        shutdown_event.set()
        raise SystemExit(130)
    except FatalAPIError as exc:
        log.error("Fatal API error: %s", exc)
        raise SystemExit(2)
    except Exception as exc:
        log.error("Error: %s", exc)
        raise SystemExit(1)
