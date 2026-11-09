import concurrent.futures
from dataclasses import asdict
import multiprocessing
from pathlib import Path
import tempfile
import unittest

from lab import Consumer, ContractError, ERP, Event, RetryableError, SimulatedCrash, Transport, connect, demo


def process_in_child(path, payload):
    return Consumer(path).process(Event(**payload))


class ReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.consumer = Consumer(self.root / "consumer.db")
        self.erp = ERP(self.root / "erp.db")
        self.transport = Transport(self.root / "transport.db")
        self.first = Event("e1", "synthetic-1", 1, "CREATED")

    def tearDown(self):
        self.tmp.cleanup()

    def test_concurrent_duplicates_produce_one_effect(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(lambda _: self.consumer.process(self.first), range(16)))
        self.assertEqual(outcomes.count("applied"), 1)
        self.assertEqual(outcomes.count("duplicate"), 15)
        self.assertEqual(len(self.consumer.snapshot()["effects"]), 1)

    def test_failure_before_commit_rolls_back_effect_and_inbox(self):
        with self.assertRaises(SimulatedCrash):
            self.consumer.process(self.first, fail_before_commit=True)
        self.assertEqual(self.consumer.snapshot(), {"orders": [], "inbox": [], "effects": []})
        self.assertEqual(self.consumer.process(self.first), "applied")

    def test_commit_before_ack_survives_new_process(self):
        delivery = self.transport.enqueue(asdict(self.first))
        with self.assertRaises(SimulatedCrash):
            self.transport.deliver(delivery, self.consumer, fail_after_commit=True)
        # Processo novo efetivo, usando spawn, com conexão própria.
        with concurrent.futures.ProcessPoolExecutor(max_workers=1,
                mp_context=multiprocessing.get_context("spawn")) as pool:
            result = pool.submit(process_in_child, str(self.consumer.path), asdict(self.first)).result()
        self.assertEqual(result, "duplicate")
        self.assertEqual(self.transport.deliver(delivery, Consumer(self.consumer.path)), "duplicate")
        self.assertEqual(len(self.consumer.snapshot()["effects"]), 1)

    def test_outbox_is_atomic_with_erp_state(self):
        with self.assertRaises(SimulatedCrash):
            self.erp.change(self.first, fail_before_commit=True)
        with connect(self.erp.path) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM orders").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0], 0)
        self.erp.change(self.first)
        self.assertEqual(self.erp.change(self.first), "already_recorded")

    def test_relay_failure_creates_duplicate_delivery_not_effect(self):
        self.erp.change(self.first)
        with self.assertRaises(SimulatedCrash):
            self.erp.relay(self.transport, fail_after_enqueue=True)
        self.erp.relay(self.transport)
        self.assertEqual(self.transport.deliver(1, self.consumer), "applied")
        self.assertEqual(self.transport.deliver(2, self.consumer), "duplicate")
        self.assertEqual(len(self.consumer.snapshot()["effects"]), 1)

    def test_old_event_cannot_regress_cancelled_order(self):
        for event in (self.first, Event("e2", "synthetic-1", 2, "SHIPPED"),
                      Event("e3", "synthetic-1", 3, "CANCELLED")):
            self.consumer.process(event)
        self.assertEqual(self.consumer.process(Event("old", "synthetic-1", 1, "CREATED")), "stale")
        self.assertEqual(self.consumer.snapshot()["orders"][0]["status"], "CANCELLED")
        self.assertEqual(len(self.consumer.snapshot()["effects"]), 3)

    def test_gap_is_retryable_then_recovers(self):
        later = Event("e2", "synthetic-1", 2, "SHIPPED")
        with self.assertRaises(RetryableError):
            self.consumer.process(later)
        self.assertEqual(len(self.consumer.snapshot()["inbox"]), 0)
        self.consumer.process(self.first)
        self.assertEqual(self.consumer.process(later), "applied")

    def test_event_id_reuse_with_different_payload_is_rejected(self):
        self.consumer.process(self.first)
        with self.assertRaises(ContractError):
            self.consumer.process(Event("e1", "synthetic-1", 2, "SHIPPED"))
        self.assertEqual(len(self.consumer.snapshot()["effects"]), 1)

    def test_same_version_with_divergent_state_is_rejected(self):
        self.consumer.process(self.first)
        with self.assertRaises(ContractError):
            self.consumer.process(Event("other", "synthetic-1", 1, "CANCELLED"))

    def test_new_version_cannot_reopen_cancelled_order(self):
        self.consumer.process(self.first)
        self.consumer.process(Event("e2", "synthetic-1", 2, "CANCELLED"))
        with self.assertRaises(ContractError):
            self.consumer.process(Event("e3", "synthetic-1", 3, "SHIPPED"))

    def test_invalid_event_goes_to_dead_letter_after_three_attempts(self):
        delivery = self.transport.enqueue({**asdict(self.first), "schema_version": 99})
        for _ in range(3):
            with self.assertRaises(ContractError):
                self.transport.deliver(delivery, self.consumer)
        self.assertEqual(self.transport.deliver(delivery, self.consumer), "dead_letter")
        with connect(self.transport.path) as db:
            row = db.execute("SELECT * FROM dead_letters").fetchone()
            self.assertEqual(row["attempts"], 3)
        self.assertEqual(self.consumer.snapshot()["effects"], [])

    def test_replay_preserves_identity_after_dependency_is_recovered(self):
        second = Event("e2", "synthetic-1", 2, "SHIPPED")
        delivery = self.transport.enqueue(asdict(second))
        for _ in range(3):
            with self.assertRaises(RetryableError):
                self.transport.deliver(delivery, self.consumer)
        self.consumer.process(self.first)
        replay = self.transport.replay(delivery)
        self.assertEqual(self.transport.deliver(replay, self.consumer), "applied")
        with connect(self.transport.path) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM dead_letters").fetchone()[0], 1)
        self.assertEqual([r["event_id"] for r in self.consumer.snapshot()["effects"]], ["e1", "e2"])

    def test_contract_rejects_boolean_version(self):
        with self.assertRaises(ContractError):
            self.consumer.process(Event("bad", "synthetic-1", True, "CREATED"))

    def test_demo_reconciles_state_and_effects(self):
        result = demo(self.root / "demo")
        self.assertEqual(result["assertions"]["effects"], 3)
        self.assertEqual(result["assertions"]["final_status"], "CANCELLED")


if __name__ == "__main__":
    unittest.main()
