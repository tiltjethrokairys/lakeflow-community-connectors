# Lakeflow Hacker News Community Connector

This documentation describes how to configure and use the **Hacker News** Lakeflow community connector to ingest data from the public [Hacker News Firebase API](https://github.com/HackerNews/API) into Databricks.

The Hacker News API is **fully public**: it serves stories, comments, jobs, polls, and ranked story lists as plain HTTPS `GET` requests returning JSON. The connector exposes five tables, the headline one being `items` — an incremental, append-only stream over the entire Hacker News item id space.

## Prerequisites

- **No account, API key, token, or credentials of any kind.** The Hacker News API has no authentication. You do not configure any secrets for this connector.
- **Network access**: The environment running the connector must be able to reach `https://hacker-news.firebaseio.com`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Authentication

There is nothing to authenticate. The base URL is `https://hacker-news.firebaseio.com/v0`. Every endpoint is an unauthenticated `GET` — no `Authorization` header, no query-string token, no request signing. You will not be prompted for any credential, and the Unity Catalog connection for this source carries **zero credential fields**.

### Required Connection Parameters

The connector has no required connection parameters. The only connection-level option is optional:

| Name       | Type   | Required | Description                                                                                                                                                              | Example                                 |
| ---------- | ------ | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------- |
| `base_url` | string | No       | Base URL for the Hacker News Firebase API. Almost always left at the default; override only for testing or a proxy. Defaults to `https://hacker-news.firebaseio.com/v0`. | `https://hacker-news.firebaseio.com/v0` |

### `externalOptionsAllowList` (required for `items` tuning)

The `items` table accepts table-specific options that control admission control and batching (see [The `items` incremental stream](#the-items-incremental-stream)). To pass any of these through, you must set the `externalOptionsAllowList` connection option to the full, comma-separated list of allowed option names:

```
start_item_id,start_item_id_lookback,max_records_per_batch,partition_size,fetch_concurrency
```

> **Note**: These are **table** options, not connection parameters. They are supplied per-table under `table_configuration` in the pipeline spec, but their names must appear in `externalOptionsAllowList` on the connection for the connection to allow them. If you only ingest the snapshot tables (`updates`, `topstories`, `newstories`, `beststories`) and accept the `items` defaults, you do not need to set any of them — but including the allowlist is harmless and recommended so you can tune `items` later.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `start_item_id,start_item_id_lookback,max_records_per_batch,partition_size,fetch_concurrency` so the `items` table options can be passed through.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Hacker News API has no discovery endpoint, so the connector exposes a fixed, **static list** of five tables. Use these exact names as `source_table`:

| Table         | Description                                                                                          | Ingestion Type | Primary Key         | Cursor (incremental)     |
| ------------- | ---------------------------------------------------------------------------------------------------- | -------------- | ------------------- | ------------------------ |
| `items`       | Individual HN items — stories, comments, jobs, polls, and poll options — fetched one id at a time.   | `append`       | `id`                | `id` (monotonic integer) |
| `updates`     | The recent-changes feed: ids of recently modified items plus usernames of recently changed profiles. | `snapshot`     | — (one row per run) | —                        |
| `topstories`  | Current top story ids (up to ~500), ranked. Includes jobs.                                           | `snapshot`     | —                   | —                        |
| `newstories`  | Newest story ids (up to ~500), newest first.                                                         | `snapshot`     | —                   | —                        |
| `beststories` | Best-ranked story ids (up to ~500), by score.                                                        | `snapshot`     | —                   | —                        |

### `items`

The headline table — an append-only stream keyed by the monotonically increasing integer `id`. It mixes **all item types** in one table: `story`, `comment`, `job`, `poll`, and `pollopt`. Because fields are type-conditional, **not every column is populated for every row** — for example, `url` only appears on stories, `parent` only on comments and poll options, and `parts` only on polls. Deleted items return only `id`, `type`, and `deleted: true`. The connector preserves whatever the API returns and leaves absent fields as `null`.

The id space contains gaps (not every allocated id is a public item), and the API returns `null` for those ids. **The connector skips nulls** — they are never emitted as rows — so the table can have gaps in the `id` sequence even though ingestion is contiguous over the id range.

See [The `items` incremental stream](#the-items-incremental-stream) below for how the moving cursor and admission control work — this is the most important thing to understand before configuring the table.

### `updates`

A single snapshot row per run. The row holds two arrays — `items` (ids of recently changed items) and `profiles` (usernames of recently changed user profiles) — plus a `snapshot_time` recording when the run captured them. The endpoint only ever returns the current recent-changes window; there is no history, so the table is fully re-read each run.

### `topstories` / `newstories` / `beststories`

Each is the ranked story-id list from its endpoint, **expanded to one row per id**. Each row carries `story_id`, its 0-based `rank` in the list, and a `snapshot_time`. These lists change continuously, so each table is fully re-read on every run (snapshot). They contain only the story ids and rank — join to `items` on `story_id = id` to get the story content.

### How the snapshot tables are read

All four snapshot tables are **full-refresh**: every run re-reads the entire current list. The recommended SDP deployment reads them via a batch full-refresh (`apply_changes_from_snapshot`), and that is what the generated pipeline does. They also run correctly on the streaming path (`spark.readStream` + `Trigger.AvailableNow`): because a snapshot has no natural cursor, each run stamps a synthetic per-run offset (`{"snapshot": <run-time>}`) that advances once — emitting the full snapshot — and then converges so the trigger terminates. A later run re-reads the snapshot in full. You do not configure any of this; it is automatic. Unlike `items`, snapshot tables do **not** resume incrementally — each run is a complete re-read by design.

### Why there is no `users` table

Hacker News users are reachable only at `/v0/user/{username}.json` by a **known** username. There is no list endpoint, no cursor, and no way to enumerate all users — they only appear as references (the `by` field on items, or the `updates.profiles` array). A first-class `users` stream is therefore not feasible, and the connector does not expose one. (The `askstories`, `showstories`, and `jobstories` snapshot lists also exist in the API but are not exposed in this connector.)

## The `items` incremental stream

`items` is incremental and append-only. Understanding its cursor and admission control is essential, because the live id space is enormous (`maxitem` is roughly 42 million as of mid-2026) and a naive cold start would try to read every item.

### How the cursor works

The Hacker News API is **id-addressed, not list-paginated**. The connector tracks a moving high-water mark on the integer `id`:

1. It reads `/maxitem.json` — a single integer, the current highest item id.
2. The Spark checkpoint stores the last id read as the offset `{"max_id": N}`.
3. Each run reads only the ids **past** that checkpoint, in the half-open range `(last_max_id, end]`, fetching `/item/{id}.json` for each.
4. The offset advances to the last id read, so the next run resumes from there. **A resumed pipeline never re-reads ids it has already ingested.**

The connector snapshots `maxitem` once when the run starts and never reads past it, so a `Trigger.AvailableNow` run terminates cleanly once it catches up. Ids created after the run started are picked up by the next run.

### Admission control (cold-start safety)

Because the live `maxitem` is ~42M, the connector will **not** start from id 0 on a first run — that would attempt tens of millions of HTTP requests. Instead, the first run's starting point and per-run batch size are governed by these table options:

| Option                   | Type    | Default   | Description                                                                                                                                                                                                                |
| ------------------------ | ------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `start_item_id`          | integer | _(unset)_ | Explicit first item id to begin ingesting from on the first run. Use this to backfill from a chosen historical id. When set, ingestion begins at this id (the offset's exclusive lower bound becomes `start_item_id - 1`). |
| `start_item_id_lookback` | integer | `1000`    | Used only when `start_item_id` is **not** set. The first run starts at `maxitem - start_item_id_lookback`, so a cold start reads just the most recent ~1000 ids rather than the whole history.                             |
| `max_records_per_batch`  | integer | `10000`   | Caps the id window read per micro-batch / run: the batch covers `(start, min(start + max_records_per_batch, maxitem)]`. Bounds how much each run does so large backfills progress in predictable chunks across runs.       |
| `partition_size`         | integer | `1000`    | Number of ids each Spark partition covers when a batch is fanned out across executors. The `items` id range is split into windows of this size for parallel fetching.                                                      |
| `fetch_concurrency`      | integer | `16`      | Bounded number of concurrent `/item/{id}.json` requests within a single partition. The API documents no rate limit, but the connector stays a polite client with bounded concurrency.                                      |

**Resolution order for the first-run starting id:**

1. A stored checkpoint offset, if one exists (resume from where the last run left off).
2. Otherwise, `start_item_id` if provided.
3. Otherwise, `maxitem - start_item_id_lookback` (default lookback `1000`).

### Example: backfill `items` from a chosen id

To backfill from a specific historical id and progress in 50,000-id chunks per run:

```json
{
  "pipeline_spec": {
    "connection_name": "hacker_news_connection",
    "object": [
      {
        "table": {
          "source_table": "items",
          "table_configuration": {
            "start_item_id": "40000000",
            "max_records_per_batch": "50000",
            "partition_size": "2000",
            "fetch_concurrency": "16"
          }
        }
      }
    ]
  }
}
```

On the first run this starts at id `40000000` and reads forward in batches of up to 50,000 ids, fanning each batch out across executors in 2,000-id partitions. Subsequent runs continue from the stored offset until they catch up to `maxitem`. To instead just keep up with the firehose from the most recent items, omit all of these and accept the default `start_item_id_lookback` of `1000`.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option                | Required | Description                                                                                     |
| --------------------- | -------- | ----------------------------------------------------------------------------------------------- |
| `source_table`        | Yes      | Table name in the source system (`items`, `updates`, `topstories`, `newstories`, `beststories`) |
| `destination_catalog` | No       | Target catalog (defaults to pipeline's default)                                                 |
| `destination_schema`  | No       | Target schema (defaults to pipeline's default)                                                  |
| `destination_table`   | No       | Target table name (defaults to `source_table`)                                                  |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option         | Required | Description                                                                                                                                                     |
| -------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scd_type`     | No       | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables (`items`) do not support this option. |
| `primary_keys` | No       | List of columns to override the connector's default primary keys                                                                                                |
| `sequence_by`  | No       | Column used to order records for SCD Type 2 change tracking                                                                                                     |
| `cluster_by`   | No       | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source.                           |

### Source-specific `table_configuration` options

All source-specific options apply **only** to the `items` table; the four snapshot tables take no options (each is a full re-read).

| Option                   | Applicable Object | Required | Description                                                                            | Default                                   |
| ------------------------ | ----------------- | -------- | -------------------------------------------------------------------------------------- | ----------------------------------------- |
| `start_item_id`          | `items`           | No       | Explicit first item id for the first run. Use to backfill from a chosen historical id. | _(unset → uses `start_item_id_lookback`)_ |
| `start_item_id_lookback` | `items`           | No       | First-run start = `maxitem - this` when `start_item_id` is unset.                      | `1000`                                    |
| `max_records_per_batch`  | `items`           | No       | Max id window read per micro-batch / run.                                              | `10000`                                   |
| `partition_size`         | `items`           | No       | Ids per Spark partition when fanning a batch across executors.                         | `1000`                                    |
| `fetch_concurrency`      | `items`           | No       | Concurrent per-id fetches within a partition.                                          | `16`                                      |

## Data Type Mapping

The API uses a small set of JSON types. All integer fields map to `LongType` (never `IntegerType`): item ids are already ~42M and climb forever, and `score` / `descendants` can be large on popular stories.

| API JSON type    | Example fields                                                             | Spark type              | Notes                                                                   |
| ---------------- | -------------------------------------------------------------------------- | ----------------------- | ----------------------------------------------------------------------- |
| integer          | `id`, `time`, `score`, `descendants`, `parent`, `poll`, `story_id`, `rank` | `LongType`              | `time` is unix seconds; cast to `TimestampType` downstream if needed.   |
| string           | `type`, `by`, `title`, `url`, `text`, `snapshot_time`, `profiles[]`        | `StringType`            | `title`/`text` are HTML-encoded. `snapshot_time` is an ISO-8601 string. |
| boolean          | `deleted`, `dead`                                                          | `BooleanType`           | Absent ≈ `false`.                                                       |
| array of integer | `kids`, `parts`, `updates.items`                                           | `ArrayType(LongType)`   | Preserved as nested arrays, not flattened.                              |
| array of string  | `updates.profiles`                                                         | `ArrayType(StringType)` |                                                                         |

### Schema highlights

- **`items`**: `id` (non-null) and `type` are always present; everything else is type-conditional and may be `null` for a given row. Notable fields: `by` (author), `time` (unix seconds), `title`/`url`/`text`, `score`, `descendants` (recursive comment count), `kids` (child ids), `parent`, `poll`, `parts`, and the `deleted`/`dead` flags.
- **`updates`**: `items` (`array<long>`), `profiles` (`array<string>`), `snapshot_time` (string).
- **`topstories` / `newstories` / `beststories`**: `story_id` (`long`), `rank` (`long`, 0-based), `snapshot_time` (string).

The schema is static and driven by the connector; you do not normally need to customize it.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Hacker News connector source in your workspace. This places the connector code under a project path that Lakeflow can load.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Add a `table` entry per object you want to ingest. The `items` table accepts the admission-control options above; the snapshot tables take none.

```json
{
  "pipeline_spec": {
    "connection_name": "hacker_news_connection",
    "object": [
      {
        "table": {
          "source_table": "items",
          "table_configuration": {
            "start_item_id_lookback": "1000",
            "max_records_per_batch": "10000"
          }
        }
      },
      {
        "table": {
          "source_table": "topstories"
        }
      },
      {
        "table": {
          "source_table": "updates"
        }
      }
    ]
  }
}
```

3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start small**: Begin with the snapshot tables (`topstories`, `updates`) and a small `items` window (default `start_item_id_lookback` of `1000`) to validate data shape before committing to a large backfill.
- **Backfill in chunks**: For a historical `items` backfill, set `start_item_id` and keep `max_records_per_batch` bounded so each run finishes in a predictable window. The checkpoint advances each run, so the backfill completes incrementally across runs without re-reading.
- **Keep `items` incremental afterward**: Once caught up, each run reads only ids past the last checkpoint — cheap and fast. Schedule it to balance freshness against request volume.
- **Tune concurrency politely**: `fetch_concurrency` defaults to `16`. The API documents no rate limit, but it is a public Firebase service with no SLA — avoid issuing thousands of parallel requests. Increase concurrency cautiously.
- **Refresh snapshots on a schedule**: `updates`, `topstories`, `newstories`, and `beststories` are full re-reads with no history; run them as often as you need current rankings, but don't expect a back-history.

#### Troubleshooting

**Common issues:**

- **First `items` run reads almost nothing**: Expected. With no `start_item_id`, the first run starts at `maxitem - start_item_id_lookback` (default `1000`), so it ingests only the most recent ~1000 ids. Set `start_item_id` to a historical id to backfill further.
- **Gaps in the `items` id sequence**: Expected. The id space has gaps (not every id is a public item) and the API returns `null` for those ids; the connector skips nulls, so contiguous ingestion still yields a sparse `id` column.
- **Columns are null for many rows**: Expected. `items` mixes all item types and fields are type-conditional — e.g. `url` only on stories, `parent` only on comments/poll options, `parts` only on polls. Deleted items return only `id`, `type`, and `deleted`.
- **Backfill seems slow**: Each run is bounded by `max_records_per_batch`. A large backfill completes over multiple runs; raise `max_records_per_batch` and/or `partition_size` to do more per run, within polite limits.
- **Transient HTTP errors**: The connector retries `429`/`500`/`502`/`503`/`504` with exponential backoff (up to 5 attempts). Transient failures are expected and handled transparently.
- **Table not found**: Ensure `source_table` is exactly one of `items`, `updates`, `topstories`, `newstories`, or `beststories`.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/hacker_news/hacker_news.py`
- Connector schemas and metadata: `src/databricks/labs/community_connector/sources/hacker_news/hacker_news_schemas.py`
- Connector API documentation: `src/databricks/labs/community_connector/sources/hacker_news/hacker_news_api_doc.md`
- Official Hacker News API documentation: `https://github.com/HackerNews/API`
- [Lakeflow Community Connectors Repository](https://github.com/databrickslabs/lakeflow-community-connectors)
