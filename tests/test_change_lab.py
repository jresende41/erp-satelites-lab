import sqlite3
import tempfile
import unittest
from pathlib import Path
from change_lab import ChangeLab, demo


class ChangeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.lab = ChangeLab(Path(self.tmp.name) / 'live.db')
        self.lab.write('a', 'Fictício A')

    def tearDown(self):
        self.lab.close()
        self.tmp.cleanup()

    def test_expand_preserves_old_reader(self):
        old = self.lab.read(1)
        self.lab.expand()
        self.assertEqual(old, self.lab.read(1))
        self.assertEqual(old, self.lab.read(2))

    def test_dual_write_survives_application_rollback(self):
        self.lab.expand()
        self.lab.switch(2)
        self.lab.write('b', 'Fictício B')
        self.lab.switch(1)
        self.assertEqual([('a', 'Fictício A'), ('b', 'Fictício B')], self.lab.read())

    def test_divergence_blocks_switch_without_changing_flag(self):
        self.lab.expand()
        with self.lab.db:
            self.lab.db.execute('UPDATE orders SET display_name=NULL')
        with self.assertRaisesRegex(RuntimeError, 'Reconciliação'):
            self.lab.switch(2)
        self.assertEqual(1, self.lab.app)

    def test_contract_blocks_old_application(self):
        self.lab.expand()
        self.lab.switch(2)
        self.lab.contract()
        with self.assertRaisesRegex(RuntimeError, 'incompatível'):
            self.lab.switch(1)
        self.assertEqual([('a', 'Fictício A')], self.lab.read(2))

    def test_snapshot_does_not_contain_later_writes(self):
        self.lab.expand()
        path = Path(self.tmp.name) / 'snapshot.db'
        self.lab.snapshot(path)
        self.lab.write('b', 'Fictício B')
        with sqlite3.connect(path) as restored:
            self.assertEqual([('a',)], list(restored.execute('SELECT id FROM orders')))
        self.assertEqual(2, len(self.lab.read()))

    def test_contract_requires_new_app_and_demo_reports_data_gap(self):
        self.lab.expand()
        with self.assertRaisesRegex(RuntimeError, 'antiga'):
            self.lab.contract()
        report = demo()
        self.assertEqual(['pedido-002'], report['writes_not_in_snapshot'])
        self.assertFalse(report['rpo_measured'])
        self.assertEqual(2, report['application_rollback_preserved_orders'])
