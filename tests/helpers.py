import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5


def event(flow=1, kind="requested", at="2025-01-01T00:00:00Z", **changes):
    value = {
        "schema_version": 1,
        "event_id": str(uuid5(NAMESPACE_URL, f"test:{flow}:{kind}")),
        "workflow_id": str(uuid5(NAMESPACE_URL, f"test:{flow}")),
        "event_type": kind,
        "occurred_at": at,
        "channel": "api",
        "document_type": "contract",
        "dataset_kind": "synthetic",
        "failure_code": "TIMEOUT" if kind == "failed" else None,
    }
    return value | changes


class WarehouseTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.root = self.path / "warehouse"
        self.source = self.path / "input.ndjson"

    def write(self, events):
        self.source.write_text("".join(json.dumps(e) + "\n" for e in events), encoding="utf-8")
        return self.source

    def query(self, sql):
        db = sqlite3.connect(self.root / "warehouse.sqlite")
        try:
            return db.execute(sql).fetchall()
        finally:
            db.close()
