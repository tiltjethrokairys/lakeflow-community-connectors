# Hacker News API Documentation

## Authorization

**No authentication required.** The Hacker News Firebase API is fully public. Every endpoint is a plain HTTPS GET returning JSON. There are no API keys, no OAuth flows, no tokens, no request signing, and no credentials of any kind.

The connector spec will contain **zero credential fields**.

```
GET https://hacker-news.firebaseio.com/v0/maxitem.json
```

No `Authorization` header. No query-string tokens. That is the complete auth model.

## Object List

The API exposes a fixed, static set of resources. There is no "list objects" discovery endpoint.

| Resource | Type | Description |
|---|---|---|
| `items` | Incremental (append) | Individual HN items: stories, comments, jobs, polls, pollopts. ID-addressed; `maxitem` is the high-water mark. |
| `updates` | Snapshot | Recently changed item IDs and profile usernames. |
| `topstories` | Snapshot | Up to 500 current top story IDs (includes jobs). |
| `newstories` | Snapshot | Up to 500 newest story IDs. |
| `beststories` | Snapshot | Up to 500 best-ranked story IDs. |
| `askstories` | Snapshot | Up to 200 latest Ask HN IDs. (Out of scope for this batch — see Deferred Tables.) |
| `showstories` | Snapshot | Up to 200 latest Show HN IDs. (Out of scope for this batch — see Deferred Tables.) |
| `jobstories` | Snapshot | Up to 200 latest job posting IDs. (Out of scope for this batch — see Deferred Tables.) |
| `users` | Not enumerable | Fetchable by known username only — no `users` table. See note below. |

**Why there is no `users` table:** The `/v0/user/{username}.json` endpoint requires a known username. There is no list endpoint, no cursor, and no way to enumerate all users. Users are only reachable as references from items (`by` field) or from the `updates.profiles` array. A `users` table is therefore not feasible as a first-class connector stream.

## Object Schema

Schemas are static (documented in the official README; no schema-discovery endpoint exists).

### Item schema — `/v0/item/{id}.json`

Every item has `id` and `type`. All other fields are type-conditional and may be absent or null.

| Field | Type | Required | Appears on types | Description |
|---|---|---|---|---|
| `id` | integer | yes | all | Monotonically increasing unique item ID. |
| `type` | string | yes | all | One of: `story`, `comment`, `job`, `poll`, `pollopt`. |
| `by` | string | no | all | Author username. Absent on deleted items. |
| `time` | integer | no | all | Unix timestamp (seconds) of creation. |
| `deleted` | boolean | no | all | `true` if the item has been deleted. Fields beyond `id`/`type` may be missing. |
| `dead` | boolean | no | story, comment, poll | `true` if the item has been killed ("dead"). |
| `title` | string | no | story, poll, job | Title text (HTML-encoded). |
| `url` | string | no | story | External URL for the story. Absent on text posts. |
| `text` | string | no | story, comment, poll, job | Body text (HTML-encoded). |
| `score` | integer | no | story, poll, pollopt | Points for stories/polls; vote count for pollopts. |
| `descendants` | integer | no | story, poll | Total comment count (recursive). |
| `kids` | array[integer] | no | story, comment, poll | Child item IDs in ranked order. |
| `parent` | integer | no | comment, pollopt | ID of the parent item (comment or story). |
| `poll` | integer | no | pollopt | ID of the poll this option belongs to. |
| `parts` | array[integer] | no | poll | Ordered list of pollopt IDs for this poll. |

**Field × type matrix:**

| Field | story | comment | job | poll | pollopt |
|---|---|---|---|---|---|
| `id` | Y | Y | Y | Y | Y |
| `type` | Y | Y | Y | Y | Y |
| `by` | Y | Y | Y | Y | Y |
| `time` | Y | Y | Y | Y | Y |
| `deleted` | Y | Y | Y | Y | Y |
| `dead` | Y | Y | — | Y | — |
| `title` | Y | — | Y | Y | — |
| `url` | Y | — | — | — | — |
| `text` | Y | Y | Y | Y | — |
| `score` | Y | — | — | Y | Y |
| `descendants` | Y | — | — | Y | — |
| `kids` | Y | Y | — | Y | — |
| `parent` | — | Y | — | — | Y |
| `poll` | — | — | — | — | Y |
| `parts` | — | — | — | Y | — |

Example response for a story:

```json
{
  "id": 8863,
  "type": "story",
  "by": "dhouston",
  "time": 1175714200,
  "title": "My YC app: Dropbox - Throw away your USB drive",
  "url": "http://www.getdropbox.com/u/2/screencast.html",
  "score": 111,
  "descendants": 71,
  "kids": [8952, 9224, 8917, 8884, 8887, 8943, 8869, 8958, 9005, 9671, 8940, 9067, 8908, 9055, 8865, 8881, 8872, 8873]
}
```

Example response for a comment:

```json
{
  "id": 2921983,
  "type": "comment",
  "by": "norvig",
  "time": 1314211127,
  "text": "Aw shucks, guys ...",
  "parent": 2921506,
  "kids": [2922097, 2922429, 2924562, 2922065, 2922138, 2922428, 2922650]
}
```

Example response for a deleted item (minimal fields returned):

```json
{
  "id": 12345,
  "type": "story",
  "deleted": true
}
```

### maxitem schema — `/v0/maxitem.json`

Returns a single JSON integer. No wrapper object.

```json
41876345
```

### updates schema — `/v0/updates.json`

```json
{
  "items": [8423305, 8420805, 8423379, 8422504],
  "profiles": ["thefox", "mdavis"]
}
```

| Field | Type | Description |
|---|---|---|
| `items` | array[integer] | IDs of recently changed items. |
| `profiles` | array[string] | Usernames of recently changed user profiles. |

### stories list schema — `/v0/topstories.json`, `/v0/newstories.json`, `/v0/beststories.json`

Returns a JSON array of integers. No wrapper object.

```json
[9129911, 9129199, 9127761, 9128141, 9128264, 9127792, 9129248, 9129055, ...]
```

| Field | Type | Description |
|---|---|---|
| *(array element)* | integer | Story ID. |

## Get Object Primary Keys

All primary keys are static.

| Table | Primary Key | Notes |
|---|---|---|
| `items` | `id` | Monotonically increasing integer. Never reused. |
| `updates` | *(no row-level PK)* | Entire payload is one snapshot record per run. |
| `topstories` | *(no row-level PK)* | Entire payload is one snapshot per run. |
| `newstories` | *(no row-level PK)* | Entire payload is one snapshot per run. |
| `beststories` | *(no row-level PK)* | Entire payload is one snapshot per run. |

## Object Ingestion Type

| Table | Ingestion Type | Cursor Field | Rationale |
|---|---|---|---|
| `items` | `append` | `id` | IDs are monotonically increasing. New items are always at the top. Existing items can be edited (score, descendants, kids change), but the append model captures the insert event; see CDC note below. |
| `updates` | `snapshot` | — | The endpoint returns only the current recent-changes window; there is no history. Full re-read each run. |
| `topstories` | `snapshot` | — | Ranked list changes continuously. Full re-read each run. |
| `newstories` | `snapshot` | — | Same as above. |
| `beststories` | `snapshot` | — | Same as above. |

**CDC note for `items`:** The `updates.items` array surfaces IDs of recently modified items (score changes, new comments, kills). An optional CDC pass can re-fetch those IDs and upsert the updated records into the items table. This is an enhancement path, not the baseline.

## Read API for Data Retrieval

### `items` — Incremental append via id range

**Core endpoints:**

| Endpoint | Method | Returns |
|---|---|---|
| `GET /v0/maxitem.json` | GET | Single integer — current highest item ID. |
| `GET /v0/item/{id}.json` | GET | Single item object or `null` if not found. |

**Incremental read strategy:**

The HN API is id-addressed, not list-paginated. Ingestion works by walking an integer range:

1. Call `/v0/maxitem.json` → `current_max`.
2. Read `start_id` from the persisted offset (`max_id`). On first run, compute `start_id = current_max - start_item_id_lookback` (see table options below).
3. For each `id` in `(start_id, current_max]`, call `/v0/item/{id}.json`.
4. Collect non-null responses. Persist `{"max_id": current_max}` as the new offset.
5. Next run begins at step 1 with `start_id = persisted max_id`.

**Offset structure:**

```json
{"max_id": 41876345}
```

The offset advances by setting `max_id` to the last id read. When `max_id == current_max`, ingestion is caught up and the run yields zero records.

**Admission control — this is mandatory:**

As of mid-2026, `maxitem` is approximately 41–42 million. Without a bound on first-run backfill:

- A cold start would attempt ~42 million HTTP requests. This is unusable.
- Fetching even 1 000 items/second takes 11+ hours.

Two required table options:

| Option | Type | Default | Description |
|---|---|---|---|
| `start_item_id` | integer | `maxitem - 1000` | Absolute item ID to begin ingestion from on first run. Set to a historical ID to backfill further. |
| `max_records_per_batch` | integer | `10000` | Maximum number of item IDs to fetch in a single run. Bounds batch size so each execution completes in a predictable window. |

With `max_records_per_batch`, the per-run id window is `(start_id, min(start_id + max_records_per_batch, current_max)]`. The offset advances to the last ID processed; subsequent runs continue from there.

**Null / gap handling:**

`/v0/item/{id}.json` returns `null` for IDs that do not correspond to items (gaps in the ID space are normal — IDs are allocated, not all items are public). Null responses must be skipped, not emitted as records.

**Example request sequence:**

```
GET https://hacker-news.firebaseio.com/v0/maxitem.json
→ 41876345

GET https://hacker-news.firebaseio.com/v0/item/41876100.json
→ {"id": 41876100, "type": "story", "by": "user123", ...}

GET https://hacker-news.firebaseio.com/v0/item/41876101.json
→ null   (skip)

GET https://hacker-news.firebaseio.com/v0/item/41876102.json
→ {"id": 41876102, "type": "comment", "by": "user456", ...}
```

---

### `updates` — Snapshot

**Endpoint:** `GET /v0/updates.json`

Returns the current recent-changes window. No parameters. Full read each run; the response is written as one record (or two records: one for `items`, one for `profiles`, depending on schema design).

```
GET https://hacker-news.firebaseio.com/v0/updates.json
→ {"items": [41876100, 41876200, ...], "profiles": ["pg", "tptacek"]}
```

---

### `topstories` — Snapshot

**Endpoint:** `GET /v0/topstories.json`

Returns up to 500 story IDs as a JSON array. Full read each run. No parameters.

```
GET https://hacker-news.firebaseio.com/v0/topstories.json
→ [41876200, 41876100, 41875900, ...]
```

---

### `newstories` — Snapshot

**Endpoint:** `GET /v0/newstories.json`

Returns up to 500 story IDs ordered newest-first. Full read each run. No parameters.

---

### `beststories` — Snapshot

**Endpoint:** `GET /v0/beststories.json`

Returns up to 500 story IDs ordered by score. Full read each run. No parameters.

---

### Rate Limits

**The official docs state: "There is currently no rate limit."**

That said, this is a public service backed by Firebase with no SLA. Practical guidance:

- Use concurrent fetches with a bounded thread pool (e.g., 10–20 concurrent item requests) to respect fair-use expectations.
- Do not issue thousands of parallel requests per second. The `max_records_per_batch` cap naturally limits throughput.
- The Firebase hosting infrastructure will throttle or block abusive patterns even without a formal rate limit.
- Add a small sleep (e.g., 10–50 ms) between item fetches if running at high concurrency to be a polite client.

No `Retry-After` header or 429 responses are documented.

## Field Type Mapping

| API field | API type | Spark/Delta type | Notes |
|---|---|---|---|
| `id` | integer | `LongType` | Primary key. Up to ~42M currently; fits in 32-bit but use 64-bit for safety. |
| `type` | string (enum) | `StringType` | Values: `story`, `comment`, `job`, `poll`, `pollopt`. |
| `by` | string | `StringType` | HN username. Case-sensitive. Absent on deleted items. |
| `time` | integer (unix seconds) | `LongType` (raw) or `TimestampType` (derived) | Seconds since epoch. Cast to `TimestampType` in the pipeline. |
| `text` | string | `StringType` | HTML-encoded. May contain `<p>`, `<a>`, `<i>` etc. |
| `url` | string | `StringType` | External URL. May be very long. |
| `title` | string | `StringType` | HTML-encoded. |
| `score` | integer | `LongType` | Can be large for very popular stories. |
| `descendants` | integer | `LongType` | Total recursive comment count. |
| `kids` | array[integer] | `ArrayType(LongType)` | Ordered list of child item IDs. |
| `parent` | integer | `LongType` | ID of parent item. |
| `poll` | integer | `LongType` | ID of the parent poll (on `pollopt` items). |
| `parts` | array[integer] | `ArrayType(LongType)` | Ordered poll option IDs. |
| `dead` | boolean | `BooleanType` | Default absent = `false`. |
| `deleted` | boolean | `BooleanType` | Default absent = `false`. |

**`updates` table field types:**

| API field | API type | Spark/Delta type |
|---|---|---|
| `items` | array[integer] | `ArrayType(LongType)` |
| `profiles` | array[string] | `ArrayType(StringType)` |

**Story list tables (`topstories`, `newstories`, `beststories`):**

The raw response is an array of integers. The connector should expand each element to a row with a single column:

| Column | Type |
|---|---|
| `story_id` | `LongType` |

Optionally include a `rank` column (0-based position in the array) and a `snapshot_time` column (timestamp of the run).

## Simulator Notes (for in-process test harness)

The id-addressed design is ideal for deterministic simulation:

- **`maxitem`**: Return a fixed integer (e.g., `1000`). Advance it between simulated runs to test incremental progression.
- **`/item/{id}`**: Maintain a dict mapping id → item dict. Return `null` for unknown IDs to test gap-skipping.
- **Item variety**: Include at least one of each type (`story`, `comment`, `job`, `poll`, `pollopt`) and one `deleted: true` item in the fixture set.
- **Offset state**: The simulator should accept and return `{"max_id": N}` offsets; verify each run starts at `N+1`.
- **`updates`**: Return a fixed payload with a small `items` array and a `profiles` array.
- **Story lists**: Return a short fixed array (e.g., 5 IDs) pointing to items in the fixture dict.
- **Admission control**: The simulator should accept `start_item_id` and `max_records_per_batch` options and respect them, so the default first-run bound can be tested without a 42M-item stub.

## Deferred Tables

The following resources exist in the HN API but are out of scope for this batch. They share the same snapshot pattern as `topstories`/`newstories`/`beststories` and can be added in a future batch with minimal additional research.

| Table | Endpoint | Max IDs | Complexity | Reason deferred |
|---|---|---|---|---|
| `askstories` | `/v0/askstories.json` | 200 | Low | Same snapshot pattern; narrower scope (Ask HN only). Lower priority. |
| `showstories` | `/v0/showstories.json` | 200 | Low | Same snapshot pattern; narrower scope (Show HN only). Lower priority. |
| `jobstories` | `/v0/jobstories.json` | 200 | Low | Same snapshot pattern; narrower scope (jobs only). Lower priority. |
| `users` | `/v0/user/{username}.json` | Not enumerable | High | No list endpoint. Only reachable by known username from `by` fields or `updates.profiles`. Not feasible as a first-class stream without building a username accumulator layer first. |

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| Official docs (GitHub README) | https://github.com/HackerNews/API | 2026-06-16 | Highest | All endpoints, field names and types, rate limit statement ("no rate limit"), item type enum, user schema, maxitem semantics, updates structure, story list max sizes |
| Official docs (raw README) | https://raw.githubusercontent.com/HackerNews/API/master/README.md | 2026-06-16 | Highest | Confirmed field-by-field schema verbatim; "There is currently no rate limit"; breaking change policy; Firebase client library recommendation |
| Orchestrator-provided authoritative facts | (inline in task prompt) | 2026-06-16 | Highest | Base URL, auth model (none), incremental design via maxitem, admission control requirements, offset structure, table options |
