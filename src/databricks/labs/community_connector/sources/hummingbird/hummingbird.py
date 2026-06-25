"""Hummingbird Case Management connector for Lakeflow Community Connectors.

Ported from the in-house CopyAPI connector in empower-data-infra
(``tilt_data/bronze/hummingbird/{copy_hummingbird,copy_hummingbird_history_events}.py``).

Two tables:
  * ``cases``          — two-phase: paginate ``/cases?updatedBetween=...`` (relay
                         cursor) to collect tokens, then ``GET /cases/{token}``
                         (parallel) for full detail. Incremental on ``updated_at``.
  * ``history_events`` — single-phase paginated
                         ``/history_events?created_at=<from>...<to>``.
                         Incremental on ``created_at``.

Auth: OAuth 2.0 client-credentials. The UC connection (or local ``-o`` options /
``HUMMINGBIRD_CLIENT_ID`` / ``HUMMINGBIRD_CLIENT_SECRET`` env vars) carries
``client_id`` / ``client_secret`` (+ optional ``base_url``); the token POST and
~60s-early refresh are done in-process, cached at module scope.

Incremental model: the framework's offset replaces CopyAPI's ``bronze_metadata``
highwater. The offset is ``{"cursor": "<ISO8601 window end>"}``; each
``read_table`` call walks forward in ``window_days`` windows (default 1, matching
CopyAPI), processing up to ``max_windows_per_batch`` windows per call, and caps at
the connector's init time so ``Trigger.AvailableNow`` converges.
"""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.hummingbird.hummingbird_schemas import (
    API_VERSION,
    DEFAULT_BASE_URL,
    DEFAULT_START_DATE,
    ENRICH_WORKERS,
    INITIAL_BACKOFF,
    MAX_RETRIES,
    PAGE_SIZE,
    REQUEST_TIMEOUT,
    RETRIABLE_STATUS_CODES,
    TABLE_COLUMNS,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)

_TS_FMT = "%Y-%m-%dT%H:%M:%SZ"

# Module-level OAuth token cache. No threading.Lock here: in the merged
# single-file SDP module a module-level Lock becomes a captured closure variable
# and Spark cannot pickle it when shipping the data source to executors. A benign
# race on concurrent first-fetch (the cases enrichment pool) at worst does one
# extra token POST, so a lock-free cache is fine.
_token_cache: dict = {"access_token": None, "expires_at": 0.0}


def _get_access_token(base_url: str, client_id: str, client_secret: str) -> str:
    """Return a cached OAuth client-credentials token, refreshing ~60s before expiry."""
    token = _token_cache.get("access_token")
    if token and time.time() < _token_cache["expires_at"] - 60:
        return token

    resp = requests.post(
        f"{base_url}/oauth/token",
        json={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    _token_cache["access_token"] = data["access_token"]
    _token_cache["expires_at"] = time.time() + data.get("expires_in", 3600)
    return _token_cache["access_token"]


def _normalize_ts(value: str) -> str:
    """Coerce a date or ISO timestamp string to the API's ``%Y-%m-%dT%H:%M:%SZ`` form.

    All cursors share this fixed UTC format, so lexicographic string comparison is
    equivalent to chronological comparison.
    """
    v = value.strip()
    if len(v) == 10:  # YYYY-MM-DD
        return f"{v}T00:00:00Z"
    dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime(_TS_FMT)


def _advance(from_ts: str, window_days: int, cap_ts: str) -> str:
    """Return ``from_ts`` + ``window_days``, clamped to ``cap_ts``."""
    dt = datetime.strptime(from_ts, _TS_FMT).replace(tzinfo=timezone.utc)
    end = (dt + timedelta(days=window_days)).strftime(_TS_FMT)
    return min(end, cap_ts)


class HummingbirdLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for the Hummingbird Case Management API."""

    def __init__(self, options: dict) -> None:
        super().__init__(options)
        self._base_url = options.get("base_url", DEFAULT_BASE_URL).rstrip("/")
        self._client_id = options.get("client_id") or os.environ.get("HUMMINGBIRD_CLIENT_ID")
        self._client_secret = options.get("client_secret") or os.environ.get(
            "HUMMINGBIRD_CLIENT_SECRET"
        )
        if not self._client_id or not self._client_secret:
            raise ValueError(
                "Hummingbird connector requires 'client_id' and 'client_secret' "
                "(via connection options or HUMMINGBIRD_CLIENT_ID / "
                "HUMMINGBIRD_CLIENT_SECRET env vars)."
            )
        # Cap the cursor at init time so a trigger never chases data that lands
        # mid-run; the next trigger creates a fresh instance and resumes here.
        self._init_ts = datetime.now(timezone.utc).strftime(_TS_FMT)

    # ---- LakeflowConnect interface ----------------------------------------

    def list_tables(self) -> list:
        return list(TABLE_SCHEMAS.keys())

    def get_table_schema(self, table_name: str, table_options: dict) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)

        start = (start_offset or {}).get("cursor")
        if not start:
            start = table_options.get("start_date", DEFAULT_START_DATE)
        start = _normalize_ts(start)

        # Upper bound: never past the connector's init time (so AvailableNow
        # converges); optionally clamp tighter with an explicit end_date to bound
        # a backfill to a known-populated window.
        cap = self._init_ts
        end_date = table_options.get("end_date")
        if end_date:
            cap = min(_normalize_ts(end_date), cap)

        # Caught up to the cap — return the offset unchanged so the framework
        # sees equality and Trigger.AvailableNow terminates.
        if start >= cap:
            return iter([]), (start_offset or {"cursor": start})

        window_days = int(table_options.get("window_days", "1"))
        max_windows = int(table_options.get("max_windows_per_batch", "31"))

        records: list = []
        cursor = start
        for _ in range(max_windows):
            if cursor >= cap:
                break
            window_end = _advance(cursor, window_days, cap)
            if table_name == "cases":
                records.extend(self._fetch_cases(cursor, window_end))
            else:
                records.extend(self._fetch_history_events(cursor, window_end))
            cursor = window_end

        end_offset = {"cursor": cursor}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    # ---- HTTP -------------------------------------------------------------

    def _auth_headers(self) -> dict:
        token = _get_access_token(self._base_url, self._client_id, self._client_secret)
        return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    def _get(self, url: str, params: dict) -> dict:
        """GET with Retry-After handling on 429 and bounded backoff on 5xx.

        Uses module-level ``requests`` (no shared Session) so it's safe to call
        concurrently from the enrichment thread pool.
        """
        backoff = INITIAL_BACKOFF
        attempt = 0
        while True:
            resp = requests.get(
                url, params=params, headers=self._auth_headers(), timeout=REQUEST_TIMEOUT
            )
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 60))
                time.sleep(retry_after)
                continue  # rate-limit waits don't count as retries (matches CopyAPI)
            if resp.status_code in RETRIABLE_STATUS_CODES and attempt < MAX_RETRIES - 1:
                attempt += 1
                time.sleep(backoff)
                backoff *= 2
                continue
            resp.raise_for_status()
            return resp.json()

    # ---- Per-table fetch --------------------------------------------------

    def _fetch_cases(self, from_ts: str, to_ts: str) -> list:
        """Two-phase: page ``/cases`` for tokens, then enrich ``/cases/{token}``."""
        tokens: list = []
        cursor = None
        while True:
            params = {
                "updatedBetween.begin": from_ts,
                "updatedBetween.end": to_ts,
                "first": PAGE_SIZE,
                "apiVersion": API_VERSION,
            }
            if cursor:
                params["after"] = cursor
            page = self._get(f"{self._base_url}/cases", params)
            tokens.extend(item["case"]["token"] for item in page.get("cases", []))
            page_info = page.get("page_info", {})
            if not page_info.get("has_next_page"):
                break
            cursor = page_info.get("end_cursor")

        def _detail(token: str):
            detail = self._get(
                f"{self._base_url}/cases/{token}", {"apiVersion": API_VERSION}
            )
            return detail.get("case")

        out: list = []
        with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as pool:
            futures = {pool.submit(_detail, t): t for t in tokens}
            for fut in as_completed(futures):
                case = fut.result()
                if case:
                    out.append(self._project("cases", case))
        return out

    def _fetch_history_events(self, from_ts: str, to_ts: str) -> list:
        """Single-phase paginated read over the created_at half-open range."""
        out: list = []
        page_token = None
        while True:
            params = {
                "created_at": f"{from_ts}...{to_ts}",
                "first": PAGE_SIZE,
                "apiVersion": API_VERSION,
            }
            if page_token:
                params["page_token"] = page_token
            page = self._get(f"{self._base_url}/history_events", params)
            out.extend(self._project("history_events", e) for e in page.get("history_events", []))
            page_token = page.get("next_page_token")
            if not page_token:
                break
        return out

    # ---- Helpers ----------------------------------------------------------

    def _project(self, table_name: str, raw: dict) -> dict:
        """Pick the schema columns from a raw API object; JSON-encode nested
        arrays/objects so they fit their ``StringType`` column (mirrors
        ``ParseHummingbird.spark_transform``)."""
        rec = {}
        for col in TABLE_COLUMNS[table_name]:
            val = raw.get(col)
            if isinstance(val, (list, dict)):
                val = json.dumps(val)
            rec[col] = val
        return rec

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {list(TABLE_SCHEMAS)}"
            )
