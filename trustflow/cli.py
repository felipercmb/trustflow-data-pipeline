import argparse
import json
import sqlite3
import sys
from pathlib import Path

from .audit import audit
from .demo import generate
from .errors import PipelineError
from .export import verify_export
from .pipeline import ingest
from .quality import evaluate, load_policy
from .recovery import rebuild
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
    load.add_argument(
        "--skip-accepted",
        action="store_true",
        help="Skip already accepted raw hashes without another audit attempt.",
    )
    load.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Process later files after rejection; still return exit code 1.",
    )
    query = sub.add_parser("report", help="Export metrics at an explicit observation time.")
    query.add_argument("--data-dir", type=Path, default=Path("data/local"))
    query.add_argument("--as-of", required=True, help="ISO timestamp including timezone.")
    query.add_argument("--channel", choices=["web", "mobile", "api"])
    query.add_argument(
        "--known-at", help="Only include events first accepted by this ingestion timestamp."
    )
    query.add_argument("--document-type", choices=["contract", "authorization", "service_order"])
    query.add_argument("--output", type=Path, default=Path("reports/latest"))
    inspect = sub.add_parser(
        "audit", help="Verify raw hashes, accounting, lifecycle and silver lineage."
    )
    inspect.add_argument("--data-dir", type=Path, default=Path("data/local"))
    restore = sub.add_parser("rebuild", help="Rebuild accepted facts into a new destination.")
    restore.add_argument("--data-dir", type=Path, default=Path("data/local"))
    restore.add_argument("--destination", type=Path, required=True)
    check = sub.add_parser(
        "check",
        help="Evaluate explicit quality/service thresholds; failed checks return exit code 3.",
    )
    check.add_argument("--data-dir", type=Path, default=Path("data/local"))
    check.add_argument("--as-of", required=True)
    check.add_argument("--known-at")
    check.add_argument("--policy", type=Path, required=True)
    verify = sub.add_parser("verify-export", help="Verify hashes in a published report bundle.")
    verify.add_argument("directory", type=Path)
    args = parser.parse_args(argv)
    exit_code = 0
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
            failures = []
            for source in sources:
                try:
                    batch = ingest(
                        source,
                        args.data_dir,
                        args.dataset_kind,
                        args.sla_seconds,
                        skip_accepted=args.skip_accepted,
                    )
                except (PipelineError, OSError, sqlite3.DatabaseError) as exc:
                    if not args.continue_on_error:
                        raise
                    failure = {"code": exc.code if isinstance(exc, PipelineError) else "IO_ERROR"}
                    failures.append(failure)
                    print(json.dumps({"event": "batch_error", **failure}), file=sys.stderr)
                    continue
                batches.append(batch)
                print(json.dumps({"event": f"batch_{batch['status']}", **batch}), file=sys.stderr)
            result = {
                "status": "partial_failure" if failures else "accepted",
                "batches": len(batches),
                "skipped_batches": sum(b["status"] == "skipped" for b in batches),
                "failed_batches": len(failures),
                "inserted_rows": sum(b["inserted_rows"] for b in batches),
                "duplicate_rows": sum(b["duplicate_rows"] for b in batches),
                "failures": failures,
            }
            exit_code = 1 if failures else 0
        elif args.command == "audit":
            result = audit(args.data_dir)
            exit_code = 3 if result["status"] == "failed" else 0
        elif args.command == "rebuild":
            result = rebuild(args.data_dir, args.destination)
        elif args.command == "check":
            policy = load_policy(args.policy)
            result = evaluate(report(args.data_dir, args.as_of, known_at=args.known_at), policy)
            exit_code = 3 if result["status"] == "failed" else 0
        elif args.command == "verify-export":
            result = verify_export(args.directory)
            exit_code = 3 if result["status"] == "failed" else 0
        elif args.command == "report":
            result = report(
                args.data_dir,
                args.as_of,
                args.channel,
                known_at=args.known_at,
                document_type=args.document_type,
            )
            export_report(result, args.output)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return exit_code
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
