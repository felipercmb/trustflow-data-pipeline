import argparse
import json
import sqlite3
import sys
from pathlib import Path

from .demo import generate
from .errors import PipelineError
from .pipeline import ingest
from .report import export_report, report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="TrustFlow: auditable digital-signature event pipeline."
    )
    sub = parser.add_subparsers(dest="command", required=True)
    demo = sub.add_parser(
        "generate-demo", help="Generate explicitly synthetic, partitioned NDJSON."
    )
    demo.add_argument("--output", type=Path, default=Path("data/demo"))
    demo.add_argument("--count", type=int, default=240)
    demo.add_argument("--seed", type=int, default=42)
    load = sub.add_parser("ingest", help="Load one file or a folder of NDJSON partitions.")
    load.add_argument("source", type=Path)
    load.add_argument("--data-dir", type=Path, default=Path("data/local"))
    load.add_argument("--dataset-kind", choices=["synthetic", "operational"], default="synthetic")
    load.add_argument("--sla-seconds", type=int, default=3600)
    query = sub.add_parser("report", help="Export metrics at an explicit observation time.")
    query.add_argument("--data-dir", type=Path, default=Path("data/local"))
    query.add_argument("--as-of", required=True, help="ISO timestamp including timezone.")
    query.add_argument("--channel", choices=["web", "mobile", "api"])
    query.add_argument("--output", type=Path, default=Path("reports/latest"))
    args = parser.parse_args(argv)
    try:
        if args.command == "generate-demo":
            result = generate(args.output, args.count, args.seed)
        elif args.command == "ingest":
            sources = (
                sorted(args.source.glob("*.ndjson")) if args.source.is_dir() else [args.source]
            )
            if not sources:
                raise PipelineError("NO_FILES", "No .ndjson partitions found.")
            batches = []
            for source in sources:
                batch = ingest(source, args.data_dir, args.dataset_kind, args.sla_seconds)
                batches.append(batch)
                print(json.dumps({"event": "batch_accepted", **batch}), file=sys.stderr)
            result = {
                "status": "accepted",
                "batches": len(batches),
                "inserted_rows": sum(b["inserted_rows"] for b in batches),
                "duplicate_rows": sum(b["duplicate_rows"] for b in batches),
            }
        else:
            result = report(args.data_dir, args.as_of, args.channel)
            export_report(result, args.output)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0
    except (PipelineError, OSError, sqlite3.DatabaseError) as exc:
        code = exc.code if isinstance(exc, PipelineError) else "IO_ERROR"
        message = (
            str(exc)
            if isinstance(exc, PipelineError)
            else "Local IO failed; check file access and available storage."
        )
        print(
            json.dumps({"status": "error", "code": code, "message": message}),
            file=sys.stderr,
        )
        return 1
