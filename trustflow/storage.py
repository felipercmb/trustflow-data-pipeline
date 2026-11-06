"""Immutable content-addressed raw files and a small transactional warehouse."""

import hashlib
import os
import sqlite3
import tempfile
from importlib.resources import files
from pathlib import Path

from .errors import PipelineError

MAX_BYTES = 10 * 1024 * 1024


def sql_text(name: str) -> str:
    return files("trustflow").joinpath("sql", name).read_text(encoding="utf-8")


def connect(root: Path, dataset_kind: str, sla_seconds: int) -> sqlite3.Connection:
    if (
        dataset_kind not in {"synthetic", "operational"}
        or type(sla_seconds) is not int
        or not 1 <= sla_seconds <= 31_536_000
    ):
        raise PipelineError("INVALID_CONFIG", "Invalid dataset kind or SLA seconds.")
    root.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(root / "warehouse.sqlite", timeout=30)
    db.row_factory = sqlite3.Row
    try:
        db.executescript(sql_text("schema.sql"))
        with db:
            db.execute(
                "INSERT OR IGNORE INTO settings VALUES (1, 1, ?, ?)", (dataset_kind, sla_seconds)
            )
        config = db.execute(
            "SELECT schema_version, dataset_kind, sla_seconds FROM settings WHERE singleton=1"
        ).fetchone()
        if config is None or config["schema_version"] != 1:
            raise PipelineError("WAREHOUSE_VERSION", "Unsupported warehouse schema version.")
        if (config["dataset_kind"], config["sla_seconds"]) != (dataset_kind, sla_seconds):
            raise PipelineError(
                "CONFIG_MISMATCH", "Use the dataset kind and SLA configured for this warehouse."
            )
    except Exception:
        db.close()
        raise
    return db


def readonly(root: Path) -> sqlite3.Connection:
    warehouse = root / "warehouse.sqlite"
    if not warehouse.exists():
        raise PipelineError("NO_WAREHOUSE", "Ingest a batch before opening this warehouse.")
    db = sqlite3.connect(warehouse.resolve().as_uri() + "?mode=ro", uri=True, timeout=30)
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA temp_store=MEMORY")
        db.execute("BEGIN")
        config = db.execute("SELECT * FROM settings WHERE singleton=1").fetchone()
        if config is None or config["schema_version"] != 1:
            raise PipelineError("WAREHOUSE_VERSION", "Unsupported warehouse schema version.")
    except Exception:
        db.close()
        raise
    return db


def read_bounded(path: Path) -> bytes:
    with path.open("rb") as source:
        body = source.read(MAX_BYTES + 1)
    if len(body) > MAX_BYTES:
        raise PipelineError("BATCH_TOO_LARGE", "Batch exceeds 10 MiB. Partition the source file.")
    return body


def preserve_raw(root: Path, body: bytes) -> tuple[str, str]:
    digest = hashlib.sha256(body).hexdigest()
    key = f"bronze/{digest[:2]}/{digest}.ndjson"
    target = root / key
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".raw-", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(body)
            output.flush()
            os.fsync(output.fileno())
        try:
            os.link(temporary, target)  # Atomic create; never overwrite a prior snapshot.
        except FileExistsError:
            pass
        if hashlib.sha256(target.read_bytes()).hexdigest() != digest:
            raise PipelineError(
                "RAW_INTEGRITY", "Existing bronze file failed SHA-256 verification."
            )
    finally:
        Path(temporary).unlink(missing_ok=True)
    return digest, key
