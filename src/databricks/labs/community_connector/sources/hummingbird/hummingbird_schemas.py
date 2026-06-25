"""Schemas, metadata, and constants for the Hummingbird connector.

Ported from the in-house CopyAPI implementation in empower-data-infra
(``tilt_data/bronze/hummingbird/``). The column sets mirror the Liquibase silver
tables ``etl_hummingbird.cases`` / ``etl_hummingbird.history_events``:

  * ``id`` is omitted on cases (the API always returns NULL — dropped in silver).
  * The nested ``reviews`` / ``filings`` arrays on cases are serialized to JSON
    strings (``StringType``) exactly as ``ParseHummingbird.spark_transform`` does,
    to keep the Delta schema stable.
  * Silver-only audit columns (``source_file_*``, ``ingested_at``) are not produced
    by the connector — the SDP pipeline owns its own ingestion metadata.
"""

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

# ---- Hummingbird API ------------------------------------------------------
DEFAULT_BASE_URL = "https://api.hummingbird.co"
API_VERSION = "2025-09-02"
PAGE_SIZE = 100
REQUEST_TIMEOUT = 60
ENRICH_WORKERS = 5  # matches CopyHummingbird's per-token enrichment fan-out

# Backfill lower bound used only when no offset has been checkpointed yet.
# Override per run with the ``start_date`` table option (the local spike passes
# a recent date so it converges fast).
DEFAULT_START_DATE = "2024-01-01"

# ---- Retry / rate limiting ------------------------------------------------
RETRIABLE_STATUS_CODES = {500, 502, 503}  # 429 handled separately (Retry-After)
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry

# ---- Schemas --------------------------------------------------------------
CASES_SCHEMA = StructType(
    [
        StructField("token", StringType(), nullable=False),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("reviews", StringType(), nullable=True),  # JSON-encoded array
        StructField("filings", StringType(), nullable=True),  # JSON-encoded array
    ]
)

HISTORY_EVENTS_SCHEMA = StructType(
    [
        StructField("token", StringType(), nullable=False),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("case_token", StringType(), nullable=True),
        StructField("review_token", StringType(), nullable=True),
        StructField("ip_address", StringType(), nullable=True),
        StructField("user_agent", StringType(), nullable=True),
        StructField("account", StringType(), nullable=True),
    ]
)

TABLE_SCHEMAS = {
    "cases": CASES_SCHEMA,
    "history_events": HISTORY_EVENTS_SCHEMA,
}

# Columns projected out of each raw API record; everything else is dropped.
TABLE_COLUMNS = {name: [f.name for f in schema.fields] for name, schema in TABLE_SCHEMAS.items()}

TABLE_METADATA = {
    "cases": {
        "primary_keys": ["token"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "history_events": {
        # History events are immutable, but keying the upsert on token keeps
        # re-pulled windows idempotent (SCD1 last-write-wins).
        "primary_keys": ["token"],
        "cursor_field": "created_at",
        "ingestion_type": "cdc",
    },
}
