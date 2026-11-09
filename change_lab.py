"""Mudança de contrato fictício: rollback da aplicação não restaura dados."""
import json
import sqlite3
import tempfile
from pathlib import Path


class ChangeLab:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute('CREATE TABLE IF NOT EXISTS orders(id TEXT PRIMARY KEY, legacy_name TEXT NOT NULL)')
        self.db.commit()
        self.app = 1

    def close(self):
        self.db.close()

    def columns(self):
        return {r[1] for r in self.db.execute('PRAGMA table_info(orders)')}

    def expand(self):
        with self.db:
            self.db.execute('ALTER TABLE orders ADD COLUMN display_name TEXT')
            self.db.execute('UPDATE orders SET display_name=legacy_name')

    def write(self, key, name):
        cols = self.columns()
        with self.db:
            if {'legacy_name', 'display_name'} <= cols:
                self.db.execute('INSERT INTO orders VALUES(?,?,?)', (key, name, name))
            else:
                self.db.execute('INSERT INTO orders VALUES(?,?)', (key, name))

    def read(self, version=None):
        version = self.app if version is None else version
        col = 'legacy_name' if version == 1 else 'display_name'
        if col not in self.columns():
            raise RuntimeError('Contrato incompatível com a versão solicitada')
        return list(self.db.execute(f'SELECT id,{col} FROM orders ORDER BY id'))

    def switch(self, version):
        if version not in (1, 2):
            raise ValueError('Versão deve ser 1 ou 2')
        self.read(version)
        if {'legacy_name', 'display_name'} <= self.columns():
            divergent = self.db.execute('SELECT COUNT(*) FROM orders WHERE display_name IS NULL OR display_name != legacy_name').fetchone()[0]
            if divergent:
                raise RuntimeError('Reconciliação pendente; troca bloqueada')
        self.app = version

    def contract(self):
        if self.app != 2:
            raise RuntimeError('Aplicação antiga ainda ativa')
        self.switch(2)
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            self.db.execute('CREATE TABLE orders_new(id TEXT PRIMARY KEY, display_name TEXT NOT NULL)')
            self.db.execute('INSERT INTO orders_new SELECT id,display_name FROM orders')
            self.db.execute('DROP TABLE orders')
            self.db.execute('ALTER TABLE orders_new RENAME TO orders')

    def snapshot(self, path):
        target = sqlite3.connect(path)
        try:
            self.db.backup(target)
        finally:
            target.close()


def demo():
    with tempfile.TemporaryDirectory(prefix='change-lab-') as tmp:
        lab = ChangeLab(Path(tmp) / 'live.db')
        try:
            lab.write('pedido-001', 'Cliente fictício A')
            lab.expand()
            lab.snapshot(Path(tmp) / 'snapshot.db')
            lab.switch(2)
            lab.write('pedido-002', 'Cliente fictício B')
            lab.switch(1)
            rollback_rows = lab.read()
            backup = sqlite3.connect(Path(tmp) / 'snapshot.db')
            try:
                snapshot_ids = {r[0] for r in backup.execute('SELECT id FROM orders')}
            finally:
                backup.close()
            missing = sorted({r[0] for r in rollback_rows} - snapshot_ids)
            lab.switch(2)
            lab.contract()
            try:
                lab.switch(1)
            except RuntimeError as exc:
                blocked = str(exc)
            else:
                raise AssertionError('Rollback deveria estar bloqueado')
            return {'synthetic': True, 'application_rollback_preserved_orders': len(rollback_rows),
                    'writes_not_in_snapshot': missing, 'rollback_after_contract': blocked,
                    'rto_measured': False, 'rpo_measured': False,
                    'external_effects_reconciled': False}
        finally:
            lab.close()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['demo'])
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    report = json.dumps(demo(), ensure_ascii=False, indent=2)
    print(report)
    if args.output:
        args.output.write_text(report + '\n', encoding='utf-8')
