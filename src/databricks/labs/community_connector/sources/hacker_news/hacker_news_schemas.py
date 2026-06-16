"""Schemas, metadata, and constants for the Hacker News connector.

The Hacker News Firebase API exposes a fixed, static set of resources with
no schema-discovery endpoint, so every table schema is hard-coded here
(faithful to the official field × type matrix documented in
``hacker_news_api_doc.md``).

All ``integer`` API fields map to ``LongType`` (not ``IntegerType``): item
IDs are already ~42M and climb monotonically forever, and ``score`` /
``descendants`` can be large on popular stories.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ---- table set -------------------------------------------------------------

ITEMS_TABLE = "items"
UPDATES_TABLE = "updates"
TOPSTORIES_TABLE = "topstories"
NEWSTORIES_TABLE = "newstories"
BESTSTORIES_TABLE = "beststories"

STORY_LIST_TABLES = (TOPSTORIES_TABLE, NEWSTORIES_TABLE, BESTSTORIES_TABLE)

# Map each story-list table to its endpoint path segment.
STORY_LIST_ENDPOINTS = {
    TOPSTORIES_TABLE: "topstories",
    NEWSTORIES_TABLE: "newstories",
    BESTSTORIES_TABLE: "beststories",
}

TABLES = [
    ITEMS_TABLE,
    UPDATES_TABLE,
    TOPSTORIES_TABLE,
    NEWSTORIES_TABLE,
    BESTSTORIES_TABLE,
]

# ---- item schema -----------------------------------------------------------
# Every item carries ``id`` and ``type``; all other fields are type-conditional
# and may be absent. Absent struct fields are emitted as ``None`` (never {}),
# and the raw parsed JSON is returned from read_table — the framework coerces
# types per this schema.
ITEM_SCHEMA = StructType(
    [
        StructField("id", LongType(), nullable=False),
        StructField("type", StringType(), nullable=True),
        StructField("by", StringType(), nullable=True),
        StructField("time", LongType(), nullable=True),  # unix seconds
        StructField("deleted", BooleanType(), nullable=True),
        StructField("dead", BooleanType(), nullable=True),
        StructField("title", StringType(), nullable=True),
        StructField("url", StringType(), nullable=True),
        StructField("text", StringType(), nullable=True),
        StructField("score", LongType(), nullable=True),
        StructField("descendants", LongType(), nullable=True),
        StructField("kids", ArrayType(LongType()), nullable=True),
        StructField("parent", LongType(), nullable=True),
        StructField("poll", LongType(), nullable=True),
        StructField("parts", ArrayType(LongType()), nullable=True),
    ]
)

# ---- updates schema --------------------------------------------------------
# A single snapshot row per run holding the two recent-changes arrays.
UPDATES_SCHEMA = StructType(
    [
        StructField("items", ArrayType(LongType()), nullable=True),
        StructField("profiles", ArrayType(StringType()), nullable=True),
        # ISO-8601 timestamp of the run that captured this snapshot.
        StructField("snapshot_time", StringType(), nullable=True),
    ]
)

# ---- story-list schema -----------------------------------------------------
# One row per story id with its 0-based rank in the ranked list, plus the
# snapshot timestamp of the run.
STORY_LIST_SCHEMA = StructType(
    [
        StructField("story_id", LongType(), nullable=False),
        StructField("rank", LongType(), nullable=True),
        StructField("snapshot_time", StringType(), nullable=True),
    ]
)

TABLE_SCHEMAS = {
    ITEMS_TABLE: ITEM_SCHEMA,
    UPDATES_TABLE: UPDATES_SCHEMA,
    TOPSTORIES_TABLE: STORY_LIST_SCHEMA,
    NEWSTORIES_TABLE: STORY_LIST_SCHEMA,
    BESTSTORIES_TABLE: STORY_LIST_SCHEMA,
}

# ---- metadata --------------------------------------------------------------
# items is the headline incremental (append) table; the cursor is the item id
# high-water mark. The snapshot tables have no row-level primary key and no
# cursor — they are fully re-read each run.
TABLE_METADATA = {
    ITEMS_TABLE: {
        "primary_keys": ["id"],
        "cursor_field": "id",
        "ingestion_type": "append",
    },
    # ``updates`` emits exactly one row per run holding the recent-changes
    # arrays; the snapshot timestamp uniquely identifies that row.
    UPDATES_TABLE: {
        "primary_keys": ["snapshot_time"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    # The ranked-story lists emit one row per story id; ``story_id`` is the
    # natural per-row key within a snapshot.
    TOPSTORIES_TABLE: {
        "primary_keys": ["story_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    NEWSTORIES_TABLE: {
        "primary_keys": ["story_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    BESTSTORIES_TABLE: {
        "primary_keys": ["story_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
}

# ---- admission control / batching defaults --------------------------------
# maxitem is ~42M, so a cold start must NOT read from id 0. On first run we
# default the starting offset to (maxitem - DEFAULT_START_LOOKBACK); a user
# can override with the ``start_item_id`` table option to backfill further.
DEFAULT_START_LOOKBACK = 1000
# Cap ids fetched per read_table / micro-batch call so each batch reads a
# bounded id window: effective end = min(maxitem, start.max_id + this).
DEFAULT_MAX_RECORDS_PER_BATCH = 10000
# Number of ids each partition descriptor covers when fanning out a
# micro-batch across executors.
DEFAULT_PARTITION_SIZE = 1000
# Bounded concurrency for per-id fetches within a single partition.
DEFAULT_FETCH_CONCURRENCY = 16

# ---- HTTP behaviour --------------------------------------------------------
BASE_URL = "https://hacker-news.firebaseio.com/v0"
REQUEST_TIMEOUT = 20  # seconds; every request must set an explicit timeout
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
INITIAL_BACKOFF = 0.5  # seconds; doubled after each retry
