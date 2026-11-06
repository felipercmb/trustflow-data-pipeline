from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

from helpers import WarehouseTest, event

from trustflow.audit import audit
from trustflow.errors import PipelineError
from trustflow.pipeline import ingest


class ConcurrencyTests(WarehouseTest):
    def test_concurrent_duplicate_deliveries_commit_one_fact(self):
        self.write([event()])
        barrier = Barrier(2)

        def deliver(_: int):
            barrier.wait(timeout=5)
            return ingest(self.source, self.root)

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(deliver, range(2)))
        self.assertEqual(sum(row["inserted_rows"] for row in results), 1)
        self.assertEqual(sum(row["duplicate_rows"] for row in results), 1)
        self.assertEqual(audit(self.root)["status"], "passed")

    def test_conflicting_concurrent_events_leave_one_accepted_fact(self):
        first = self.path / "first.ndjson"
        first.write_bytes(self.write([event(channel="api")]).read_bytes())
        second = self.path / "second.ndjson"
        second.write_bytes(self.write([event(channel="web")]).read_bytes())
        barrier = Barrier(2)

        def deliver(path: Path):
            barrier.wait(timeout=5)
            try:
                return ingest(path, self.root)["status"]
            except PipelineError as exc:
                return exc.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(deliver, [first, second]))
        self.assertCountEqual(results, ["accepted", "EVENT_ID_CONFLICT"])
        self.assertEqual(self.query("SELECT COUNT(*) FROM events"), [(1,)])
        self.assertEqual(audit(self.root)["status"], "passed")
