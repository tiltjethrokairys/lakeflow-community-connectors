"""Tests for the Hacker News LakeflowConnect connector.

Runs against the in-process source simulator described by
``source_simulator/specs/hacker_news/``. The HN Firebase API is fully public
(no auth), so ``replay_config`` carries no credentials — the connector's
``__init__`` reads no secret fields, and the simulator never validates the
dict anyway. A single ``base_url`` knob is supplied so the value is explicit.

The connector is a ``SupportsPartitionedStream`` whose headline table,
``items``, is an incremental **append** stream keyed by the monotonic integer
``id``. The four snapshot tables (``updates``, ``topstories``, ``newstories``,
``beststories``) opt out of partitioning (``is_partitioned() == False``) and
use the single-driver ``read_table`` path, so the base
``LakeflowConnectTests.test_read_table`` / ``test_read_terminates`` /
``test_every_column_populated_by_at_least_one_record`` exercise them while the
``SupportsPartitionedStreamTests`` mixin covers ``items``.

The custom simulator handler (``specs/hacker_news/handlers/hn.py``) advances
``/maxitem.json`` by ``_MAXITEM_STEP`` on every request, so a freshly-built
connector instance sees a higher high-water mark than the previous one. The
incremental-contract tests below build *fresh* instances around an explicit
``reset_state()`` so the maxitem progression is deterministic and isolated
from whatever order the rest of the suite runs in.
"""

from __future__ import annotations

import pytest

from databricks.labs.community_connector.source_simulator.specs.hacker_news.handlers import (
    hn as hn_handler,
)
from databricks.labs.community_connector.sources.hacker_news.hacker_news import (
    HackerNewsLakeflowConnect,
)
from databricks.labs.community_connector.sources.hacker_news.hacker_news_schemas import (
    ITEMS_TABLE,
    STORY_LIST_TABLES,
    UPDATES_TABLE,
)
from tests.unit.sources.test_partition_suite import SupportsPartitionedStreamTests
from tests.unit.sources.test_suite import LakeflowConnectTests

# The maxitem progression baked into the simulator handler. Mirrors the
# private constants in handlers/hn.py — kept here so the incremental tests can
# compute exact expected windows without importing handler internals beyond
# the documented reset hook.
MAXITEM_BASE = hn_handler._MAXITEM_BASE  # first run's maxitem
MAXITEM_STEP = hn_handler._MAXITEM_STEP  # advance per /maxitem.json request
GAP_EVERY = hn_handler._GAP_EVERY        # every Nth id returns null


class TestHackerNewsConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    connector_class = HackerNewsLakeflowConnect
    simulator_source = "hacker_news"
    sample_records = 200

    # The HN API takes no credentials. ``base_url`` is the only knob the
    # connector reads; the simulator routes by path regardless of host, so the
    # value just has to be a well-formed prefix. No secret fields exist.
    replay_config = {
        "base_url": "https://hacker-news.firebaseio.com/v0",
    }

    @classmethod
    def setup_class(cls):
        # Start every test class from a clean maxitem progression so the
        # shared ``self.connector`` (built inside super().setup_class()) and
        # the standard suite see a predictable high-water mark sequence.
        hn_handler.reset_state()
        super().setup_class()


# ---------------------------------------------------------------------------
# Incremental-append contract — the headline feature of this connector.
#
# These tests deliberately build their own fresh connector instances rather
# than reusing ``self.connector``: the maxitem high-water mark advances on
# every ``/maxitem.json`` request, and a connector snapshots maxitem once per
# instance. A fresh instance therefore models a fresh trigger run, which is
# the only way to exercise "run N+1 reads only ids past run N's checkpoint".
# ---------------------------------------------------------------------------


def _fresh_connector(base_url: str = "https://hacker-news.firebaseio.com/v0"):
    return HackerNewsLakeflowConnect({"base_url": base_url})


def _ids_from_partition(partition: dict) -> range:
    """The half-open id range ``(start_id, end_id]`` a partition covers."""
    return range(int(partition["start_id"]) + 1, int(partition["end_id"]) + 1)


class TestHackerNewsIncrementalContract:
    """Prove the incremental ``items`` append contract end-to-end.

    Uses the live simulator installed by ``TestHackerNewsConnector.setup_class``
    — but pytest runs each class's ``setup_class`` independently, so this class
    needs the simulator patch active too. We piggy-back on the shared
    simulator by declaring our own minimal class-level setup that reuses the
    base harness machinery.
    """

    @classmethod
    def setup_class(cls):
        # Install the simulator (simulate mode, hacker_news spec/corpus) the
        # same way LakeflowConnectTests does, but standalone for this class so
        # the incremental tests don't depend on the other class's lifecycle.
        from databricks.labs.community_connector.source_simulator import (
            MODE_SIMULATE,
            Simulator,
        )
        from tests.unit.sources.test_suite import LakeflowConnectTests as _Base

        spec_path = (
            _Base._simulator_specs_root() / "hacker_news" / "endpoints.yaml"
        )
        corpus_dir = _Base._simulator_specs_root() / "hacker_news" / "corpus"
        cls._patch = Simulator(
            mode=MODE_SIMULATE, spec_path=spec_path, corpus_dir=corpus_dir
        )
        cls._patch.__enter__()

    @classmethod
    def teardown_class(cls):
        patch = getattr(cls, "_patch", None)
        if patch is not None:
            patch.__exit__(None, None, None)
            cls._patch = None

    def setup_method(self):
        # Each incremental test owns the maxitem progression from id 0.
        hn_handler.reset_state()

    # ------------------------------------------------------------------
    # Round 1: cold start under admission control, moving + converging offset
    # ------------------------------------------------------------------

    def test_round1_cold_start_lower_bound_admission_control(self):
        """Round 1 must NOT read from id 0 — the cold-start lower bound is
        ``maxitem - start_item_id_lookback``, not the bottom of the id space.

        We use an explicit ``start_item_id_lookback`` strictly less than the
        first maxitem so the lower bound is provably > 0; with the default
        lookback (1000) and base maxitem (1000) the bound would coincide with
        0 and the assertion would be vacuous.
        """
        lookback = 200
        opts = {"start_item_id_lookback": str(lookback)}
        connector = _fresh_connector()

        # First instance => first /maxitem.json request => base high-water mark.
        init_max = connector._init_max_id()
        assert init_max == MAXITEM_BASE, (
            f"expected first run to snapshot maxitem={MAXITEM_BASE}, got {init_max}"
        )
        expected_lower = init_max - lookback  # exclusive lower bound
        assert expected_lower > 0, "fixture must keep the cold-start bound off id 0"

        partitions = connector.get_partitions(ITEMS_TABLE, opts)
        assert partitions, "cold start should produce at least one partition"

        # Every partition's covered id range must sit strictly above the
        # admission-control lower bound — never reaching down toward id 0.
        min_id_touched = min(min(_ids_from_partition(p)) for p in partitions)
        max_id_touched = max(max(_ids_from_partition(p)) for p in partitions)
        assert min_id_touched == expected_lower + 1, (
            f"cold start read down to id {min_id_touched}; admission control "
            f"should start at {expected_lower + 1} (= maxitem - lookback + 1)"
        )
        assert max_id_touched == init_max, (
            f"cold start should read up to the init snapshot {init_max}, "
            f"got {max_id_touched}"
        )
        # The partition descriptors collectively must never touch id 0.
        assert min(int(p["start_id"]) for p in partitions) >= expected_lower

    def test_round1_default_lookback_never_reads_from_zero(self):
        """Even with the *default* lookback, the cold-start lower bound is
        ``max(0, maxitem - 1000)`` and the connector never enumerates id 0."""
        from databricks.labs.community_connector.sources.hacker_news.hacker_news_schemas import (
            DEFAULT_START_LOOKBACK,
        )

        connector = _fresh_connector()
        init_max = connector._init_max_id()
        lower = connector._resolve_start_max_id({}, init_max, {})
        assert lower == max(0, init_max - DEFAULT_START_LOOKBACK)
        # The first id actually fetched is lower + 1 — strictly positive even
        # when lower clamps to 0 (id 0 is never a valid item).
        partitions = connector.get_partitions(ITEMS_TABLE, {})
        first_id = min(min(_ids_from_partition(p)) for p in partitions)
        assert first_id >= 1, "id 0 must never be enumerated"

    def test_round1_moving_offset_converges_under_available_now(self):
        """latest_offset returns a MOVING ``{"max_id": N}`` then converges.

        Models Trigger.AvailableNow: feed each end offset back as the next
        start. The end offset advances on the first batch, then equals the
        start (== init snapshot) and the trigger terminates.
        """
        lookback = 200
        opts = {"start_item_id_lookback": str(lookback)}
        connector = _fresh_connector()
        init_max = connector._init_max_id()
        assert init_max == MAXITEM_BASE

        # First micro-batch: start at initialOffset {}, expect a moving end.
        end1 = connector.latest_offset(ITEMS_TABLE, opts, start_offset={})
        assert end1 == {"max_id": init_max}, (
            f"first end offset should advance to the init snapshot, got {end1}"
        )

        # Second call resuming from end1: caught up, so end == start (converge).
        end2 = connector.latest_offset(ITEMS_TABLE, opts, start_offset=end1)
        assert end2 == end1, (
            f"offset must stop advancing once caught up to the init snapshot; "
            f"got {end2} after {end1}"
        )

        # And get_partitions over the equal range is empty — Spark stops.
        assert connector.get_partitions(
            ITEMS_TABLE, opts, start_offset=end1, end_offset=end2
        ) == []

    # ------------------------------------------------------------------
    # Round 2: fresh instance after maxitem advanced — read ONLY new ids
    # ------------------------------------------------------------------

    def test_round2_resumes_from_checkpoint_reads_only_new_ids(self):
        """Round 2 (fresh instance, maxitem advanced) resumes from Round 1's
        checkpoint and reads ONLY ids past it — never re-reading earlier ids.
        """
        lookback = 200
        opts = {"start_item_id_lookback": str(lookback)}

        # --- Round 1 -------------------------------------------------------
        c1 = _fresh_connector()
        init_max_1 = c1._init_max_id()
        assert init_max_1 == MAXITEM_BASE
        checkpoint = c1.latest_offset(ITEMS_TABLE, opts, start_offset={})
        assert checkpoint == {"max_id": init_max_1}
        # Collect the ids Round 1 actually read.
        r1_parts = c1.get_partitions(
            ITEMS_TABLE, opts, start_offset=None, end_offset=checkpoint
        )
        r1_ids = set()
        for p in r1_parts:
            for rec in c1.read_partition(ITEMS_TABLE, p, opts):
                r1_ids.add(rec["id"])
        assert r1_ids, "Round 1 should have read some ids"
        assert max(r1_ids) <= init_max_1

        # --- Round 2 (fresh instance => maxitem advanced by one step) ------
        c2 = _fresh_connector()
        init_max_2 = c2._init_max_id()
        assert init_max_2 == MAXITEM_BASE + MAXITEM_STEP, (
            f"a fresh instance should snapshot an advanced maxitem; "
            f"got {init_max_2}, expected {MAXITEM_BASE + MAXITEM_STEP}"
        )

        # Resume from Round 1's checkpoint.
        end2 = c2.latest_offset(ITEMS_TABLE, opts, start_offset=checkpoint)
        assert end2 == {"max_id": init_max_2}, (
            f"Round 2 should advance the end offset to the new snapshot "
            f"{init_max_2}, got {end2}"
        )

        r2_parts = c2.get_partitions(
            ITEMS_TABLE, opts, start_offset=checkpoint, end_offset=end2
        )
        r2_ids = set()
        for p in r2_parts:
            # No partition may reach at or below the checkpoint id.
            assert int(p["start_id"]) >= checkpoint["max_id"], (
                f"Round 2 partition {p} starts at/below the checkpoint "
                f"{checkpoint['max_id']} — would re-read committed ids"
            )
            for rec in c2.read_partition(ITEMS_TABLE, p, opts):
                r2_ids.add(rec["id"])

        # The core invariant: Round 2 reads strictly past the checkpoint and
        # never re-reads anything Round 1 already committed.
        assert r2_ids, "Round 2 should have read the newly-available ids"
        assert min(r2_ids) > checkpoint["max_id"], (
            f"Round 2 read id {min(r2_ids)} <= checkpoint "
            f"{checkpoint['max_id']} — re-reading committed data"
        )
        assert r1_ids.isdisjoint(r2_ids), (
            f"Round 2 re-read ids already committed in Round 1: "
            f"{sorted(r1_ids & r2_ids)}"
        )
        # Round 2's window is exactly the new id band (checkpoint, new_max].
        assert max(r2_ids) <= init_max_2

    def test_round2_sequential_read_path_only_new_ids(self):
        """The single-driver ``read_table`` path mirrors the same incremental
        semantics: Round 2 resumes from the checkpoint and emits only new ids.
        """
        opts = {"start_item_id_lookback": "200"}

        c1 = _fresh_connector()
        _, checkpoint = c1.read_table(ITEMS_TABLE, {}, opts)
        assert checkpoint == {"max_id": MAXITEM_BASE}

        c2 = _fresh_connector()
        records, end2 = c2.read_table(ITEMS_TABLE, checkpoint, opts)
        ids = [r["id"] for r in records]
        assert end2 == {"max_id": MAXITEM_BASE + MAXITEM_STEP}
        assert ids, "Round 2 sequential read should emit the new ids"
        assert min(ids) > checkpoint["max_id"], (
            f"sequential Round 2 re-read committed id {min(ids)}"
        )

    # ------------------------------------------------------------------
    # Null / gap id skipping
    # ------------------------------------------------------------------

    def test_gap_ids_are_skipped(self):
        """Ids that return ``null`` (gaps / deleted) are never emitted."""
        opts = {"start_item_id_lookback": "200"}
        connector = _fresh_connector()
        init_max = connector._init_max_id()
        lower = connector._resolve_start_max_id({}, init_max, opts)

        partitions = connector.get_partitions(ITEMS_TABLE, opts)
        emitted_ids = set()
        for p in partitions:
            for rec in connector.read_partition(ITEMS_TABLE, p, opts):
                emitted_ids.add(rec["id"])

        # Every requested id that is a "gap" (id % GAP_EVERY == 0) must be
        # absent, and at least one gap id must fall inside the read window so
        # the assertion is non-vacuous.
        requested = range(lower + 1, init_max + 1)
        gap_ids = {i for i in requested if i % GAP_EVERY == 0}
        assert gap_ids, "fixture window must contain at least one gap id"
        leaked = gap_ids & emitted_ids
        assert not leaked, f"gap ids leaked into the output: {sorted(leaked)}"

        # And no emitted id is a gap id.
        assert all(i % GAP_EVERY != 0 for i in emitted_ids), (
            "an emitted id collides with a gap slot"
        )


# ---------------------------------------------------------------------------
# Snapshot tables — shapes returned by the four full-refresh tables.
# These run on the shared simulator from TestHackerNewsConnector; declare a
# tiny standalone class so they don't depend on suite ordering either.
# ---------------------------------------------------------------------------


class TestHackerNewsSnapshotTables:
    @classmethod
    def setup_class(cls):
        from databricks.labs.community_connector.source_simulator import (
            MODE_SIMULATE,
            Simulator,
        )
        from tests.unit.sources.test_suite import LakeflowConnectTests as _Base

        spec_path = _Base._simulator_specs_root() / "hacker_news" / "endpoints.yaml"
        corpus_dir = _Base._simulator_specs_root() / "hacker_news" / "corpus"
        cls._patch = Simulator(
            mode=MODE_SIMULATE, spec_path=spec_path, corpus_dir=corpus_dir
        )
        cls._patch.__enter__()
        hn_handler.reset_state()
        cls.connector = _fresh_connector()

    @classmethod
    def teardown_class(cls):
        patch = getattr(cls, "_patch", None)
        if patch is not None:
            patch.__exit__(None, None, None)
            cls._patch = None

    def test_updates_snapshot_shape(self):
        records, offset = self.connector.read_table(UPDATES_TABLE, {}, {})
        rows = list(records)
        assert offset == {}, "snapshot tables carry no offset"
        assert len(rows) == 1, "updates emits exactly one snapshot row"
        row = rows[0]
        assert isinstance(row.get("items"), list) and row["items"], (
            "updates row must carry the recent-changes 'items' array"
        )
        assert isinstance(row.get("profiles"), list) and row["profiles"], (
            "updates row must carry the recent-changes 'profiles' array"
        )
        assert all(isinstance(i, int) for i in row["items"])
        assert all(isinstance(p, str) for p in row["profiles"])
        assert isinstance(row.get("snapshot_time"), str) and row["snapshot_time"]

    @pytest.mark.parametrize("table", list(STORY_LIST_TABLES))
    def test_story_list_snapshot_shape(self, table):
        records, offset = self.connector.read_table(table, {}, {})
        rows = list(records)
        assert offset == {}, "snapshot tables carry no offset"
        assert rows, f"{table} should return at least one ranked-story row"
        for expected_rank, row in enumerate(rows):
            assert isinstance(row["story_id"], int)
            assert row["rank"] == expected_rank, (
                f"{table} rank should be the 0-based position in the ranked list"
            )
            assert isinstance(row["snapshot_time"], str) and row["snapshot_time"]
        # Ranks are dense 0..N-1 in order.
        assert [r["rank"] for r in rows] == list(range(len(rows)))
