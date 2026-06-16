"""Custom simulator handlers for the Hacker News Firebase API.

The HN API does not fit the declarative ``corpus + pagination_style`` model:

* ``/maxitem.json`` returns a **bare integer**, not a record array, and the
  connector snapshots it once per run as the id high-water mark. To exercise
  the incremental ``items`` path the simulator must let this value **advance
  between runs** so a later run sees new ids past the previous checkpoint.
* ``/item/{id}.json`` returns a **single item dict keyed by the path id**, or
  ``null`` for gap / deleted ids. The connector picks the exact id range it
  requests (driven by the maxitem the simulator reports), so the handler
  synthesizes a deterministic item per requested id rather than relying on the
  bootstrapped flat corpus.
* ``/{top,new,best}stories.json`` return **bare integer arrays**.
* ``/updates.json`` returns ``{"items": [...], "profiles": [...]}``.

These handlers live under ``source_simulator/specs/hacker_news/handlers/`` (not
under ``sources/hacker_news/``) so they are never inlined by
``merge_python_source.py`` — exactly how the gmail and qualtrics handlers are
organized. They are simulator-only test fixtures, not connector runtime code.

Monotonic ``maxitem`` across runs
---------------------------------
The connector fetches ``/maxitem.json`` exactly once per trigger run (it caches
the snapshot on the instance). Each fresh run is therefore one fresh
``/maxitem.json`` request. The handler returns ``_MAXITEM_BASE`` on the first
request of the process and advances by ``_MAXITEM_STEP`` on every subsequent
request, so run N+1 sees ``_MAXITEM_STEP`` new ids past run N's checkpoint —
which is exactly what proves the incremental append reads only new ids on the
second run. The cap is the connector's own init-time snapshot, so each run
still terminates under Trigger.AvailableNow.
"""

from __future__ import annotations

import json
import re
import threading
from typing import Any

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

# ---------------------------------------------------------------------------
# Deterministic fixture parameters
# ---------------------------------------------------------------------------

# First run sees maxitem = _MAXITEM_BASE; each later run advances by _STEP so
# new ids become available past the prior checkpoint.
_MAXITEM_BASE = 1000
_MAXITEM_STEP = 10

# Every Nth id is a "gap" and returns null (exercises null-skipping). Keep this
# coprime-ish with the type cycle so gaps and types interleave naturally.
_GAP_EVERY = 7

# Every Nth (non-gap) item is flagged ``dead: true`` so the ``dead`` column is
# populated in at least some sampled records. Coprime-ish with _GAP_EVERY and
# the type cycle so dead items span several types and dodge most gap ids.
_DEAD_EVERY = 5

# Rotating item types so the fixture covers all five item shapes. The deleted
# slot returns a minimal ``{id, type, deleted: true}`` item.
_TYPE_CYCLE = ["story", "comment", "job", "poll", "pollopt", "deleted"]

# Story-list fixtures point at ids guaranteed to resolve to non-null items
# (i.e. not gap ids). Sized small per the simulator notes.
_STORY_LIST_IDS = [101, 102, 103, 104, 105]

_UPDATES_PAYLOAD = {
    "items": [201, 202, 203, 204],
    "profiles": ["pg", "tptacek", "dang"],
}

_ITEM_PATH_RE = re.compile(r"/item/(?P<id>\d+)\.json")

# ---------------------------------------------------------------------------
# Process-local mutable state (maxitem progression)
# ---------------------------------------------------------------------------

_lock = threading.Lock()
_state = {"maxitem_calls": 0}


def _next_maxitem() -> int:
    """Return the maxitem for this run; advance for the next run.

    Thread-safe so concurrent ``__init__`` snapshots within a single run still
    observe a consistent value (the connector caches it anyway, so within a run
    only one call actually reaches here).
    """
    with _lock:
        calls = _state["maxitem_calls"]
        _state["maxitem_calls"] = calls + 1
        return _MAXITEM_BASE + calls * _MAXITEM_STEP


def reset_state() -> None:
    """Reset maxitem progression — useful between independent test scenarios."""
    with _lock:
        _state["maxitem_calls"] = 0


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def maxitem(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """Serve ``/maxitem.json`` — a bare, monotonically-advancing integer."""
    return _json_response(prep, _next_maxitem())


def item(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """Serve ``/item/{id}.json`` — a deterministic item or ``null`` for gaps."""
    match = _ITEM_PATH_RE.search(prep.url or "")
    item_id = int(match.group("id")) if match else 0
    return _json_response(prep, _synthesize_item(item_id))


def updates(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """Serve ``/updates.json`` — recent-changes ``{items, profiles}`` dict."""
    return _json_response(prep, dict(_UPDATES_PAYLOAD))


def topstories(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    return _json_response(prep, list(_STORY_LIST_IDS))


def newstories(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    # Newest-first ordering — reverse the shared id list.
    return _json_response(prep, list(reversed(_STORY_LIST_IDS)))


def beststories(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    return _json_response(prep, list(_STORY_LIST_IDS))


# ---------------------------------------------------------------------------
# Synthesis
# ---------------------------------------------------------------------------


def _synthesize_item(item_id: int) -> dict[str, Any] | None:
    """Build a deterministic item for ``item_id``, or ``None`` for gap ids.

    The shape matches the connector's expected fields per the item × type
    matrix. Type and gap selection are pure functions of the id, so the same
    id always yields the same response across runs (deterministic).
    """
    if item_id <= 0:
        return None
    if item_id % _GAP_EVERY == 0:
        return None  # gap / non-existent id — connector must skip nulls

    kind = _TYPE_CYCLE[item_id % len(_TYPE_CYCLE)]
    base = {
        "id": item_id,
        "by": f"user{item_id % 97}",
        "time": 1175714200 + item_id,
    }
    # Per the HN API, a live item can be flagged ``dead`` (killed / flagged)
    # while still being present — distinct from ``deleted``. Mark a
    # deterministic subset so the ``dead`` column is exercised. Coprime-ish
    # with the gap/type cycles so dead items still cover multiple types.
    if item_id % _DEAD_EVERY == 0:
        base["dead"] = True

    if kind == "deleted":
        # Deleted items return only id/type/deleted per the API docs.
        return {"id": item_id, "type": "story", "deleted": True}

    if kind == "story":
        return {
            **base,
            "type": "story",
            "title": f"Story {item_id}",
            "url": f"https://example.com/{item_id}",
            "score": 10 + (item_id % 200),
            "descendants": item_id % 50,
            "kids": [item_id + 1, item_id + 2],
        }
    if kind == "comment":
        return {
            **base,
            "type": "comment",
            "text": f"Comment body for {item_id}",
            "parent": max(1, item_id - 1),
            "kids": [item_id + 3],
        }
    if kind == "job":
        return {
            **base,
            "type": "job",
            "title": f"Job posting {item_id}",
            "text": f"We are hiring ({item_id})",
        }
    if kind == "poll":
        return {
            **base,
            "type": "poll",
            "title": f"Poll {item_id}",
            "text": f"Poll prompt {item_id}",
            "score": item_id % 100,
            "descendants": item_id % 20,
            "parts": [item_id + 10, item_id + 11],
            "kids": [item_id + 4],
        }
    # pollopt
    return {
        **base,
        "type": "pollopt",
        "poll": max(1, item_id - 10),
        "score": item_id % 30,
    }


# ---------------------------------------------------------------------------
# Response building
# ---------------------------------------------------------------------------


def _json_response(prep: PreparedRequest, payload: Any) -> Response:
    """Encode ``payload`` (including bare ints, arrays, and ``null``) as JSON.

    ``requests``' ``.json()`` parses bare scalars and ``null`` fine, so the HN
    connector's ``resp.json()`` round-trips integers, arrays, dicts, and
    ``None`` exactly as the live API would.
    """
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    rec = ResponseRecord(
        status_code=200,
        headers={"Content-Type": "application/json"},
        body_text=body.decode("utf-8"),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)
