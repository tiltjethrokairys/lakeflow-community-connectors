# Hummingbird connector

Ingests the [Hummingbird](https://docs.hummingbird.co/reference) Case Management
API into Databricks via the Lakeflow Community Connector framework.

Ported from the in-house `CopyAPI` connector in `empower-data-infra`
(`tilt_data/bronze/hummingbird/`), which lands raw NDJSON to a volume + a
separate ParseV2 silver job. This connector collapses both into a single SDP
pipeline: `read_table` performs the fetch and projection, and SDP `apply_changes`
upserts (SCD1) keyed on `token`.

## Tables

| Table            | Ingestion | Cursor       | Notes                                                                                  |
| ---------------- | --------- | ------------ | -------------------------------------------------------------------------------------- |
| `cases`          | `cdc`     | `updated_at` | Two-phase: page `/cases?updatedBetween=…` for tokens, then `GET /cases/{token}` (5-way parallel). `reviews`/`filings` JSON-encoded to `StringType`. |
| `history_events` | `cdc`     | `created_at` | Single-phase paginated `/history_events?created_at=<from>...<to>`.                      |

## Connection parameters

| Name            | Required | Description                                              |
| --------------- | -------- | -------------------------------------------------------- |
| `client_id`     | yes      | OAuth 2.0 client-credentials client ID.                  |
| `client_secret` | yes      | OAuth 2.0 client-credentials client secret.              |
| `base_url`      | no       | API base URL. Defaults to `https://api.hummingbird.co`.  |

Locally, `client_id`/`client_secret` can also be supplied via the
`HUMMINGBIRD_CLIENT_ID` / `HUMMINGBIRD_CLIENT_SECRET` env vars (keeps secrets off
the command line — the local file-sink runner prints its `-o` options).

## Per-table options (`external_options_allowlist`)

| Option                  | Default      | Description                                                          |
| ----------------------- | ------------ | -------------------------------------------------------------------- |
| `start_date`            | `2024-01-01` | Backfill lower bound when no offset is checkpointed (date or ISO ts).|
| `end_date`              | _(none)_     | Optional upper bound; clamps the backfill to a known-populated window (also capped at the connector init time). |
| `window_days`           | `1`          | Days per query window (matches CopyAPI's 1-day increments).          |
| `max_windows_per_batch` | `31`         | Windows processed per `read_table` call before yielding an offset.   |

## Local run

```bash
export HUMMINGBIRD_CLIENT_ID=...      # do not pass via -o (the runner prints options)
export HUMMINGBIRD_CLIENT_SECRET=...
.venv/bin/python tools/scripts/stream_to_files_local.py hummingbird history_events \
    -o start_date=2024-01-01 -o window_days=7 --rounds 2 --keep
```

## Notes / gaps vs the CopyAPI original

- **PII tagging**: the CopyAPI path applies UC `pii` tags to `name`/`reviews`/`filings`
  (cases) and `description`/`ip_address`/`user_agent`/`account` (history_events) via
  Liquibase. The community framework has no PII story — tagging must be bolted on
  post-pipeline (e.g. an `ALTER TABLE … SET TAGS` reconcile step).
- **Simulator corpus**: a `source_simulator` spec + corpus (for offline CI tests via
  `LakeflowConnectTests`) is a follow-up; this connector currently ships with an
  offline structural test only.
