#!/usr/bin/env python3
"""Run any community connector through the streaming -> file-sink path locally.

This is a *local dev harness* (not a deployment tool). It wires a connector's
``lakeflow_connect`` Spark streaming Data Source straight into a file sink
(parquet/json/csv) with a ``checkpointLocation``, using ``Trigger.AvailableNow``
-- the "Option A" pattern for sending ingestion output to files instead of Delta
tables, with no Databricks runtime and no Unity Catalog.

What it's good for:
  * Sanity-checking a connector's streaming/offset path end-to-end on a laptop.
  * *Seeing* incremental behaviour: run several rounds with ``--rounds`` and watch
    whether the checkpoint suppresses already-seen rows (a real incremental
    source with a moving cursor) or re-reads the whole table each round (a
    snapshot source, whose offset is just an init-time token).

No Unity Catalog locally, so there is no connection to inject. Pass the
connector's own options directly with ``-o key=value`` instead of
``.option("databricks.connection", ...)``. Credential-free sources (pokeapi,
example) need only their plain options.

Two environment foot-guns this script handles for you:
  * Spark would otherwise launch its Python workers with the system ``python3``
    (often < 3.10), and the framework's ``X | None`` type hints raise
    ``TypeError: unsupported operand type(s) for |`` on worker import. We pin
    ``PYSPARK_PYTHON``/``PYSPARK_DRIVER_PYTHON`` to the interpreter running this
    script (run it with the project venv's python).
  * The Python Data Source API needs ``pyarrow`` for worker<->JVM transfer; it is
    not in the ``dev`` extra. We check for it and print an install hint.

Examples (run with the project venv, e.g. ``.venv/bin/python``):

    # Snapshot source (pokeapi, live, no auth): each round re-reads -> rows grow.
    .venv/bin/python tools/scripts/stream_to_files_local.py pokeapi generation \\
        -o base_url=https://pokeapi.co/api/v2 --rounds 2

    # Incremental source (example: in-process synthetic, no auth, offline):
    # round 2 only picks up rows past the checkpointed cursor.
    .venv/bin/python tools/scripts/stream_to_files_local.py example orders \\
        -o username=simulator-user -o password=simulator-fake-password --rounds 2

    # Write JSON and keep the output dir for inspection.
    .venv/bin/python tools/scripts/stream_to_files_local.py pokeapi type \\
        -o base_url=https://pokeapi.co/api/v2 --format json --keep
"""
import argparse
import os
import shutil
import sys
import tempfile

# Pin the workers to *this* interpreter BEFORE pyspark spawns them. Must happen
# before the SparkSession is built. Running this script with the project venv's
# python therefore makes the workers use the same (3.10+) interpreter.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

VALID_FORMATS = ("parquet", "json", "csv")


def _parse_options(pairs):
    """Turn ['k=v', ...] into {'k': 'v', ...}; error on a missing '='."""
    opts = {}
    for pair in pairs or []:
        if "=" not in pair:
            raise SystemExit(f"--option must be key=value, got: {pair!r}")
        key, value = pair.split("=", 1)
        opts[key] = value
    return opts


def _require_pyarrow():
    try:
        import pyarrow  # noqa: F401
    except ModuleNotFoundError:
        raise SystemExit(
            "pyarrow is required for the Spark Python Data Source API but is not "
            "installed in this environment.\n"
            "  Install it with:  uv pip install pyarrow   (or: pip install pyarrow)"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Run a connector's streaming read into a local file sink (Option A).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("source", help="Connector source name, e.g. pokeapi, example")
    parser.add_argument("table", help="Source table name to read")
    parser.add_argument(
        "-o", "--option", action="append", metavar="KEY=VALUE", default=[],
        help="Connector option (repeatable). Passed straight to the reader, "
             "e.g. -o base_url=https://pokeapi.co/api/v2",
    )
    parser.add_argument(
        "--format", default="parquet", choices=VALID_FORMATS,
        help="File sink format (default: parquet)",
    )
    parser.add_argument(
        "--rounds", type=int, default=1,
        help="Number of availableNow rounds to run against the SAME checkpoint. "
             "Use >1 to observe incremental vs snapshot behaviour (default: 1)",
    )
    parser.add_argument("--out", help="Output dir (default: a temp dir)")
    parser.add_argument("--checkpoint", help="Checkpoint dir (default: a temp dir)")
    parser.add_argument("--master", default="local[2]", help="Spark master (default: local[2])")
    parser.add_argument(
        "--keep", action="store_true",
        help="Keep the output and checkpoint dirs instead of deleting them",
    )
    args = parser.parse_args()

    _require_pyarrow()

    options = _parse_options(args.option)
    options["tableName"] = args.table

    out_dir = args.out or tempfile.mkdtemp(prefix=f"{args.source}_{args.table}_files_")
    chk_dir = args.checkpoint or tempfile.mkdtemp(prefix=f"{args.source}_{args.table}_chk_")
    created_temp = (args.out is None, args.checkpoint is None)

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master(args.master)
        .appName(f"stream_to_files_local::{args.source}.{args.table}")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Source-agnostic registration: resolve <Source>DataSource by name and
    # register it under the "lakeflow_connect" format.
    from databricks.labs.community_connector.sparkpds import find_data_source

    data_source_cls = find_data_source(args.source)
    spark.dataSource.register(data_source_cls)
    fmt_name = data_source_cls.name()

    print(f"source       : {args.source}  ({data_source_cls.__name__}, format={fmt_name!r})")
    print(f"table        : {args.table}")
    print(f"options      : { {k: v for k, v in options.items() if k != 'tableName'} }")
    print(f"sink format  : {args.format}")
    print(f"output dir   : {out_dir}")
    print(f"checkpoint   : {chk_dir}")
    print(f"rounds       : {args.rounds}")
    print("-" * 70)

    def read_back_count():
        reader = spark.read.format(args.format)
        if args.format == "csv":
            reader = reader.option("header", "true")
        return reader.load(out_dir).count()

    try:
        prev = 0
        for r in range(1, args.rounds + 1):
            reader = (
                spark.readStream.format(fmt_name)
                .options(**options)
            )
            writer = (
                reader.load()
                .writeStream.format(args.format)
                .option("path", out_dir)
                .option("checkpointLocation", chk_dir)
            )
            if args.format == "csv":
                writer = writer.option("header", "true")
            query = writer.trigger(availableNow=True).start()
            query.awaitTermination()

            total = read_back_count()
            delta = total - prev
            prev = total
            print(f"[round {r}] availableNow done -> file sink total rows = {total}  (new this round = {delta})")

        print("-" * 70)
        if args.rounds > 1:
            print(
                "Interpretation: a steady or zero 'new this round' after round 1 means the\n"
                "checkpoint is suppressing already-seen rows (incremental cursor). A 'new this\n"
                "round' roughly equal to the table size means the source re-reads in full each\n"
                "round (snapshot — the offset is just an init-time token)."
            )
        if args.keep:
            print(f"\nKept output    : {out_dir}\nKept checkpoint: {chk_dir}")
    finally:
        spark.stop()
        if not args.keep:
            if created_temp[0]:
                shutil.rmtree(out_dir, ignore_errors=True)
            if created_temp[1]:
                shutil.rmtree(chk_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
