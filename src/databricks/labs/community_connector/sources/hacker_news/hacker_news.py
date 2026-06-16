"""Hacker News connector for the Firebase HN API.

The Hacker News API (https://hacker-news.firebaseio.com/v0/) is fully public:
no auth, no API key, no credentials. It is **id-addressed**, not
list-paginated — items are fetched one id at a time and ``maxitem`` is the
monotonic high-water mark of the id space.

The ``items`` table is the headline incremental (append) stream. Because the
source supports range queries over the id space — ``(start_id, end_id]`` via
``maxitem`` + per-id ``/item/{id}.json`` fetches — it is implemented as a
``SupportsPartitionedStream`` so the id range fans out across Spark executors.
The four snapshot tables (``updates``, ``topstories``, ``newstories``,
``beststories``) are full-refresh each run and fall back to the single-driver
``simpleStreamReader`` path via ``is_partitioned() == False``.

Termination (Trigger.AvailableNow): the connector snapshots ``maxitem`` once
at init time (``self._init_max_id``) and never lets ``latest_offset`` exceed
it. Once the stream catches up to that snapshot, ``latest_offset`` stops
advancing and the trigger terminates. New ids that appear after init are
picked up by the next trigger, which creates a fresh connector instance with a
newer snapshot. This is the id-based analogue of the timestamp ``_init_ts``
cap used by time-cursor connectors.
"""

import concurrent.futures
import time
from datetime import datetime, timezone
from typing import Iterator, Optional

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.hacker_news.hacker_news_schemas import (
    BASE_URL,
    DEFAULT_FETCH_CONCURRENCY,
    DEFAULT_MAX_RECORDS_PER_BATCH,
    DEFAULT_PARTITION_SIZE,
    DEFAULT_START_LOOKBACK,
    INITIAL_BACKOFF,
    ITEMS_TABLE,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    RETRIABLE_STATUS_CODES,
    STORY_LIST_ENDPOINTS,
    STORY_LIST_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    TABLES,
    UPDATES_TABLE,
)


class HackerNewsLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for the public Hacker News API."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        # No credentials of any kind — the API is fully public.
        self._base_url = options.get("base_url", BASE_URL).rstrip("/")
        # Snapshot the id high-water mark once so a trigger never chases
        # ids created after this instance was constructed. Resolved lazily
        # (the first time it is needed) so __init__ stays cheap and the
        # instance carries no eager network state.
        self._init_max_id_cache: Optional[int] = None

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------
    def _new_session(self) -> requests.Session:
        """Create a fresh requests Session.

        Sessions are created per-call (and inside ``read_partition`` on
        executors) rather than stored on ``self`` so the connector instance
        stays picklable when Spark ships the reader to workers.
        """
        return requests.Session()

    def _get_json(self, session: requests.Session, path: str):
        """GET ``{base_url}{path}`` and return parsed JSON, retrying on 429/5xx.

        ``path`` must start with ``/`` (e.g. ``/maxitem.json``). Returns the
        decoded JSON value, which may be ``None`` for missing/gap item ids.
        """
        url = f"{self._base_url}{path}"
        backoff = INITIAL_BACKOFF
        resp = None
        for attempt in range(MAX_RETRIES):
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                break
            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2
        if resp is None or resp.status_code != 200:
            status = resp.status_code if resp is not None else "no response"
            raise RuntimeError(f"GET {url} failed with status {status}")
        return resp.json()

    def _fetch_maxitem(self, session: requests.Session) -> int:
        """Return the current ``maxitem`` high-water mark as an int."""
        value = self._get_json(session, "/maxitem.json")
        if value is None:
            raise RuntimeError("maxitem.json returned null")
        return int(value)

    def _init_max_id(self) -> int:
        """Init-time snapshot of ``maxitem`` (cached for this instance).

        This is the cap that guarantees Trigger.AvailableNow termination:
        ``latest_offset`` never exceeds it, so once the stream catches up the
        offset stops advancing.
        """
        if self._init_max_id_cache is None:
            session = self._new_session()
            try:
                self._init_max_id_cache = self._fetch_maxitem(session)
            finally:
                session.close()
        return self._init_max_id_cache

    # ------------------------------------------------------------------
    # LakeflowConnect interface
    # ------------------------------------------------------------------
    def list_tables(self) -> list[str]:
        """Static table set — the HN API has no discovery endpoint."""
        return list(TABLES)

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLES:
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {TABLES}"
            )

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Single-driver read path.

        Used for the snapshot tables (which opt out of partitioning via
        ``is_partitioned``) and as the sequential fallback for ``items`` when
        the framework reads a non-partitioned stream.
        """
        self._validate_table(table_name)
        if table_name == UPDATES_TABLE:
            return self._read_updates_snapshot(table_options)
        if table_name in STORY_LIST_TABLES:
            return self._read_story_list_snapshot(table_name, table_options)
        if table_name == ITEMS_TABLE:
            return self._read_items_sequential(start_offset, table_options)
        raise ValueError(f"Unhandled table '{table_name}'")

    # ------------------------------------------------------------------
    # SupportsPartitionedStream interface
    # ------------------------------------------------------------------
    def is_partitioned(self, table_name: str) -> bool:
        """Only ``items`` partitions; snapshot tables use simpleStreamReader."""
        return table_name == ITEMS_TABLE

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Return the end offset for the next micro-batch of ``items``.

        Lightweight metadata-only call: it determines *which* ids exist, not
        the records themselves. The returned ``max_id`` is bounded by
        ``max_records_per_batch`` (so each batch is a finite id window) and
        capped at the init-time ``maxitem`` snapshot (so the trigger
        terminates). When the start offset already sits at the snapshot, the
        same value is returned and Spark stops issuing micro-batches.
        """
        if table_name != ITEMS_TABLE:
            # Snapshot tables don't partition; offset machinery is unused.
            return {}

        init_max = self._init_max_id()
        max_records = self._max_records_per_batch(table_options)

        start_max_id = self._resolve_start_max_id(start_offset, init_max, table_options)

        # Already caught up to (or past) the init-time snapshot — stop.
        if start_max_id >= init_max:
            return {"max_id": init_max}

        end_max_id = min(init_max, start_max_id + max_records)
        return {"max_id": end_max_id}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> list[dict]:
        """Split the id range ``(start, end]`` into per-partition id windows.

        For batch reads (both offsets ``None``) the whole admission-controlled
        range from the resolved first id to the init-time snapshot is split.
        Each descriptor is ``{"start_id": a, "end_id": b}`` covering the
        half-open id range ``(a, b]`` — small and fully self-contained.
        """
        if table_name != ITEMS_TABLE:
            return []

        init_max = self._init_max_id()

        if start_offset is None and end_offset is None:
            # Batch mode: cover the full admission-controlled id range.
            start_max_id = self._resolve_start_max_id(None, init_max, table_options)
            end_max_id = init_max
        else:
            start_off = start_offset or {}
            end_off = end_offset or {}
            if start_off == end_off:
                return []
            start_max_id = self._resolve_start_max_id(start_off, init_max, table_options)
            end_max_id = int(end_off.get("max_id", init_max))

        if end_max_id <= start_max_id:
            return []

        partition_size = self._partition_size(table_options)
        partitions: list[dict] = []
        lower = start_max_id
        while lower < end_max_id:
            upper = min(lower + partition_size, end_max_id)
            partitions.append({"start_id": lower, "end_id": upper})
            lower = upper
        return partitions

    def read_partition(
        self, table_name: str, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        """Fetch every item in the partition's ``(start_id, end_id]`` range.

        Runs on a Spark executor — self-contained, with its own session.
        Null responses (gap / deleted ids that return ``null``) are skipped.
        """
        if table_name != ITEMS_TABLE:
            return iter([])

        start_id = int(partition["start_id"])
        end_id = int(partition["end_id"])
        ids = range(start_id + 1, end_id + 1)  # half-open (start_id, end_id]
        records = self._fetch_items(ids, table_options)
        return iter(records)

    # ------------------------------------------------------------------
    # items helpers
    # ------------------------------------------------------------------
    def _max_records_per_batch(self, table_options: dict[str, str]) -> int:
        return int(
            table_options.get(
                "max_records_per_batch", str(DEFAULT_MAX_RECORDS_PER_BATCH)
            )
        )

    def _partition_size(self, table_options: dict[str, str]) -> int:
        return int(table_options.get("partition_size", str(DEFAULT_PARTITION_SIZE)))

    def _fetch_concurrency(self, table_options: dict[str, str]) -> int:
        return int(
            table_options.get("fetch_concurrency", str(DEFAULT_FETCH_CONCURRENCY))
        )

    def _resolve_start_max_id(
        self,
        start_offset: dict | None,
        init_max: int,
        table_options: dict[str, str],
    ) -> int:
        """Resolve the exclusive lower bound (``max_id``) of the id range.

        Resolution order:
          1. A populated prior offset's ``max_id`` (resume from checkpoint).
          2. ``start_item_id`` table option — begin ingestion at that id, so
             the exclusive lower bound is ``start_item_id - 1``.
          3. Default first-run bound: ``init_max - start_item_id_lookback``
             (lookback default 1000) so a cold start does not read from id 0.
        """
        if start_offset and start_offset.get("max_id") is not None:
            return int(start_offset["max_id"])

        if "start_item_id" in table_options:
            return max(0, int(table_options["start_item_id"]) - 1)

        lookback = int(
            table_options.get("start_item_id_lookback", str(DEFAULT_START_LOOKBACK))
        )
        return max(0, init_max - lookback)

    def _fetch_items(
        self, ids, table_options: dict[str, str]
    ) -> list[dict]:
        """Fetch ``/item/{id}.json`` for each id, skipping nulls.

        Uses a bounded thread pool to fetch concurrently (the API documents no
        rate limit, but we stay polite with bounded concurrency). Results are
        returned in ascending id order so any downstream cursor logic sees a
        monotonic id sequence.
        """
        id_list = list(ids)
        if not id_list:
            return []

        concurrency = max(1, self._fetch_concurrency(table_options))
        session = self._new_session()
        results: dict[int, dict] = {}
        try:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=concurrency
            ) as pool:
                future_to_id = {
                    pool.submit(self._get_json, session, f"/item/{item_id}.json"): item_id
                    for item_id in id_list
                }
                for future in concurrent.futures.as_completed(future_to_id):
                    item = future.result()
                    if item is None:  # gap / deleted id — skip, do not emit
                        continue
                    item_id = item.get("id")
                    if item_id is None:
                        continue
                    results[int(item_id)] = item
        finally:
            session.close()

        return [results[i] for i in sorted(results)]

    def _read_items_sequential(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Sequential (single-driver) incremental read of ``items``.

        Mirrors the partitioned path's offset semantics so the connector still
        works when the framework reads ``items`` as a non-partitioned stream.
        Returns a moving end offset ``{"max_id": <last id read>}``; when the
        offset stops advancing (caught up to the init snapshot), the framework
        sees ``end_offset == start_offset`` and the trigger terminates.
        """
        init_max = self._init_max_id()
        max_records = self._max_records_per_batch(table_options)
        start_max_id = self._resolve_start_max_id(start_offset, init_max, table_options)

        # Defensive: maxitem may not have advanced — return cleanly.
        if start_max_id >= init_max:
            current = {"max_id": start_max_id}
            # Preserve the incoming offset exactly so the framework's
            # equality-based termination check fires.
            return iter([]), (dict(start_offset) if start_offset else current)

        end_max_id = min(init_max, start_max_id + max_records)
        ids = range(start_max_id + 1, end_max_id + 1)
        records = self._fetch_items(ids, table_options)
        end_offset = {"max_id": end_max_id}
        return iter(records), end_offset

    # ------------------------------------------------------------------
    # snapshot helpers
    # ------------------------------------------------------------------
    def _read_updates_snapshot(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read ``/updates.json`` as a single snapshot row.

        The endpoint returns ``{"items": [...], "profiles": [...]}``; we emit
        one row holding both arrays plus the run's snapshot timestamp. Snapshot
        reads carry no offset (return ``{}``).
        """
        session = self._new_session()
        try:
            payload = self._get_json(session, "/updates.json")
        finally:
            session.close()

        payload = payload or {}
        row = {
            "items": payload.get("items"),
            "profiles": payload.get("profiles"),
            "snapshot_time": datetime.now(timezone.utc).isoformat(),
        }
        return iter([row]), {}

    def _read_story_list_snapshot(
        self, table_name: str, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read a ``{top,new,best}stories.json`` array as one row per id.

        Each row carries the story id, its 0-based rank in the ranked list,
        and the run's snapshot timestamp. Full re-read each run; no offset.
        """
        endpoint = STORY_LIST_ENDPOINTS[table_name]
        session = self._new_session()
        try:
            ids = self._get_json(session, f"/{endpoint}.json")
        finally:
            session.close()

        ids = ids or []
        snapshot_time = datetime.now(timezone.utc).isoformat()
        records = [
            {
                "story_id": int(story_id),
                "rank": rank,
                "snapshot_time": snapshot_time,
            }
            for rank, story_id in enumerate(ids)
        ]
        return iter(records), {}
