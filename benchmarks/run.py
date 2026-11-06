"""Reproducible local benchmark; observations are not production guarantees."""

import argparse
import json
import platform
import sqlite3
import statistics
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter

from trustflow import __version__
from trustflow.demo import generate
from trustflow.pipeline import ingest
from trustflow.report import report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflows", type=int, default=5000)
    parser.add_argument("--trials", type=int, choices=range(1, 11), default=3)
    args = parser.parse_args()
    trials = []
    with tempfile.TemporaryDirectory(prefix="trustflow-benchmark-") as temporary:
        root = Path(temporary)
        generated = generate(root / "input", count=args.workflows, seed=42)
        partitions = sorted((root / "input").glob("*.ndjson"))
        for index in range(args.trials):
            warehouse = root / f"warehouse-{index}"
            start = perf_counter()
            for source in partitions:
                ingest(source, warehouse)
            ingestion_seconds = perf_counter() - start
            start = perf_counter()
            result = report(warehouse, generated["suggested_as_of"])
            trials.append(
                {
                    "ingestion_seconds": round(ingestion_seconds, 6),
                    "report_seconds": round(perf_counter() - start, 6),
                    "event_snapshot_sha256": result["event_snapshot_sha256"],
                }
            )
    print(
        json.dumps(
            {
                "dataset_kind": "synthetic",
                "trustflow_version": __version__,
                "measured_at": datetime.now(UTC).isoformat(),
                "python": platform.python_version(),
                "sqlite": sqlite3.sqlite_version,
                "os": platform.system(),
                "architecture": platform.machine(),
                "workflows": args.workflows,
                "events": generated["events"],
                "partitions": generated["partitions"],
                "median_ingestion_seconds": statistics.median(
                    row["ingestion_seconds"] for row in trials
                ),
                "median_report_seconds": statistics.median(row["report_seconds"] for row in trials),
                "identical_fact_snapshots": len({row["event_snapshot_sha256"] for row in trials})
                == 1,
                "trials": trials,
                "limitations": "Local filesystem and synthetic data; excludes generation, audit, network, and export. No production throughput claim.",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
