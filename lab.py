"""Laboratório didático: outbox, transporte local e consumidor idempotente.

Python 3.10+; apenas biblioteca padrão. Não se conecta a ERP ou mensageria reais.
"""
from __future__ import annotations

import argparse
from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile


class RetryableError(Exception):
    """Dependência ainda indisponível ou lacuna de versão."""


class ContractError(Exception):
    """Contrato inválido ou identidade reutilizada com conteúdo divergente."""


class SimulatedCrash(Exception):
    """Ponto de falha controlado, usado nos testes."""


@dataclass(frozen=True)
class Event:
    event_id: str
    order_id: str
    version: int
    status: str
    schema_version: int = 1

    def validate(self):
        if (not isinstance(self.event_id, str) or not self.event_id.strip()
                or not isinstance(self.order_id, str) or not self.order_id.strip()
                or type(self.version) is not int or self.version < 1
                or type(self.schema_version) is not int or self.schema_version != 1
                or not isinstance(self.status, str)
                or self.status not in {"CREATED", "SHIPPED", "CANCELLED"}):
            raise ContractError("Evento incompatível com o contrato v1")

    def payload(self):
        self.validate()
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))

    def fingerprint(self):
        return hashlib.sha256(self.payload().encode()).hexdigest()


def connect(path):
    db = sqlite3.connect(str(path), timeout=10, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=10000")
    return db


@contextmanager
def transaction(path):
    db = connect(path)
    try:
        db.execute("BEGIN IMMEDIATE")
        yield db
        db.execute("COMMIT")
    except BaseException:
        if db.in_transaction:
            db.execute("ROLLBACK")
        raise
    finally:
        db.close()


class ERP:
    def __init__(self, path):
        self.path = Path(path)
        with closing(connect(self.path)) as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY, version INTEGER NOT NULL,
                status TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox (
                event_id TEXT PRIMARY KEY, payload TEXT NOT NULL,
                published INTEGER NOT NULL DEFAULT 0);
            """)

    def change(self, event, fail_before_commit=False):
        event.validate()
        with transaction(self.path) as db:
            prior = db.execute("SELECT payload FROM outbox WHERE event_id=?",
                               (event.event_id,)).fetchone()
            if prior:
                if prior["payload"] != event.payload():
                    raise ContractError("Identificador reutilizado com conteúdo diferente")
                return "already_recorded"
            current = db.execute("SELECT * FROM orders WHERE order_id=?",
                                 (event.order_id,)).fetchone()
            expected = current["version"] + 1 if current else 1
            if event.version != expected:
                raise ContractError("O produtor exige versão consecutiva por pedido")
            if not current and event.status != "CREATED":
                raise ContractError("Pedido começa em CREATED")
            if current and current["status"] == "CANCELLED":
                raise ContractError("Cancelamento é terminal neste laboratório")
            db.execute("INSERT INTO orders VALUES(?,?,?) ON CONFLICT(order_id) "
                       "DO UPDATE SET version=excluded.version,status=excluded.status",
                       (event.order_id, event.version, event.status))
            db.execute("INSERT INTO outbox(event_id,payload) VALUES(?,?)",
                       (event.event_id, event.payload()))
            if fail_before_commit:
                raise SimulatedCrash("Falha antes do commit do ERP")
        return "recorded"

    def relay(self, transport, fail_after_enqueue=False):
        with closing(connect(self.path)) as db:
            pending = db.execute("SELECT * FROM outbox WHERE published=0 ORDER BY rowid").fetchall()
        for row in pending:
            transport.enqueue(json.loads(row["payload"]))
            if fail_after_enqueue:
                raise SimulatedCrash("Transporte recebeu; outbox ainda não marcada")
            with transaction(self.path) as db:
                db.execute("UPDATE outbox SET published=1 WHERE event_id=?", (row["event_id"],))
        return len(pending)


class Consumer:
    name = "shipping-v1"

    def __init__(self, path):
        self.path = Path(path)
        with closing(connect(self.path)) as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY, version INTEGER NOT NULL,
                status TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS inbox (
                consumer TEXT NOT NULL, event_id TEXT NOT NULL,
                fingerprint TEXT NOT NULL, outcome TEXT NOT NULL,
                PRIMARY KEY(consumer,event_id));
            CREATE TABLE IF NOT EXISTS effects (
                event_id TEXT PRIMARY KEY, order_id TEXT NOT NULL,
                status TEXT NOT NULL);
            """)

    def process(self, event, fail_before_commit=False):
        event.validate()
        with transaction(self.path) as db:
            prior = db.execute("SELECT * FROM inbox WHERE consumer=? AND event_id=?",
                               (self.name, event.event_id)).fetchone()
            if prior:
                if prior["fingerprint"] != event.fingerprint():
                    raise ContractError("Mesmo event_id com conteúdo divergente")
                return "duplicate"
            current = db.execute("SELECT * FROM orders WHERE order_id=?",
                                 (event.order_id,)).fetchone()
            version = current["version"] if current else 0
            if event.version < version:
                outcome = "stale"
            elif event.version == version:
                if event.status != current["status"]:
                    raise ContractError("Mesma versão com estados divergentes")
                outcome = "same_state"
            else:
                if event.version != version + 1:
                    raise RetryableError("Lacuna de versão: recuperar eventos anteriores")
                if not current and event.status != "CREATED":
                    raise ContractError("Primeiro estado deve ser CREATED")
                if current and current["status"] == "CANCELLED":
                    raise ContractError("Evento tenta alterar pedido cancelado")
                db.execute("INSERT INTO orders VALUES(?,?,?) ON CONFLICT(order_id) "
                           "DO UPDATE SET version=excluded.version,status=excluded.status",
                           (event.order_id, event.version, event.status))
                db.execute("INSERT INTO effects VALUES(?,?,?)",
                           (event.event_id, event.order_id, event.status))
                outcome = "applied"
            if fail_before_commit:
                raise SimulatedCrash("Efeito e inbox serão revertidos")
            db.execute("INSERT INTO inbox VALUES(?,?,?,?)",
                       (self.name, event.event_id, event.fingerprint(), outcome))
        return outcome

    def snapshot(self):
        with closing(connect(self.path)) as db:
            return {table: [dict(r) for r in db.execute(f"SELECT * FROM {table} ORDER BY rowid")]
                    for table in ("orders", "inbox", "effects")}


class Transport:
    """Fila local persistida; não simula leases, rede ou toda a semântica de SQS."""
    def __init__(self, path):
        self.path = Path(path)
        with closing(connect(self.path)) as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS deliveries (
                delivery_id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload TEXT NOT NULL, acked INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0, error TEXT);
            CREATE TABLE IF NOT EXISTS dead_letters (
                delivery_id INTEGER PRIMARY KEY, payload TEXT NOT NULL,
                error TEXT NOT NULL, attempts INTEGER NOT NULL);
            """)

    def enqueue(self, payload):
        with transaction(self.path) as db:
            return db.execute("INSERT INTO deliveries(payload) VALUES(?)",
                              (json.dumps(payload, sort_keys=True),)).lastrowid

    def deliver(self, delivery_id, consumer, fail_before_commit=False,
                fail_after_commit=False, max_attempts=3):
        with transaction(self.path) as db:
            row = db.execute("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)).fetchone()
            if not row:
                raise KeyError(delivery_id)
            if row["acked"]:
                return "acked"
            if db.execute("SELECT 1 FROM dead_letters WHERE delivery_id=?", (delivery_id,)).fetchone():
                return "dead_letter"
            db.execute("UPDATE deliveries SET attempts=attempts+1 WHERE delivery_id=?", (delivery_id,))
        try:
            try:
                payload = json.loads(row["payload"])
                if not isinstance(payload, dict) or set(payload) != {
                        "event_id", "order_id", "version", "status", "schema_version"}:
                    raise ContractError("Campos obrigatórios ausentes ou desconhecidos")
                event = Event(**payload)
            except (TypeError, ValueError) as exc:
                raise ContractError("Payload inválido") from exc
            outcome = consumer.process(event, fail_before_commit=fail_before_commit)
            if fail_after_commit:
                raise SimulatedCrash("Commit do consumidor ocorreu; ACK ainda não")
        except Exception as exc:
            with transaction(self.path) as db:
                db.execute("UPDATE deliveries SET error=? WHERE delivery_id=?", (str(exc), delivery_id))
                latest = db.execute("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)).fetchone()
                if latest["attempts"] >= max_attempts:
                    db.execute("INSERT OR IGNORE INTO dead_letters VALUES(?,?,?,?)",
                               (delivery_id, latest["payload"], str(exc), latest["attempts"]))
            raise
        with transaction(self.path) as db:
            db.execute("UPDATE deliveries SET acked=1,error=NULL WHERE delivery_id=?", (delivery_id,))
        return outcome

    def replay(self, delivery_id):
        with closing(connect(self.path)) as db:
            row = db.execute("SELECT payload FROM dead_letters WHERE delivery_id=?", (delivery_id,)).fetchone()
        if not row:
            raise KeyError(delivery_id)
        # Nova entrega, preservando event_id; histórico da falha continua intacto.
        return self.enqueue(json.loads(row["payload"]))


def demo(directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    if any((directory / name).exists() for name in ("erp.db", "consumer.db", "transport.db")):
        raise FileExistsError("Use um diretório novo para não misturar execuções")
    (directory / ".lab-runtime").write_text("erp-satelites-lab-v1\n")
    erp, consumer, transport = ERP(directory / "erp.db"), Consumer(directory / "consumer.db"), Transport(directory / "transport.db")
    events = [Event("evt-001", "pedido-demo-001", 1, "CREATED"),
              Event("evt-002", "pedido-demo-001", 2, "SHIPPED"),
              Event("evt-003", "pedido-demo-001", 3, "CANCELLED")]
    log = []
    for event in events:
        erp.change(event)
        erp.relay(transport)
    log.append({"delivery": 1, "outcome": transport.deliver(1, consumer)})
    try:
        transport.deliver(2, consumer, fail_after_commit=True)
    except SimulatedCrash:
        log.append({"delivery": 2, "outcome": "crash_after_commit_before_ack"})
    # Nova instância: a evidência está no banco, não na memória do processo.
    consumer = Consumer(directory / "consumer.db")
    log.append({"delivery": 2, "outcome": transport.deliver(2, consumer)})
    log.append({"delivery": 3, "outcome": transport.deliver(3, consumer)})
    old = transport.enqueue(asdict(Event("evt-late-001", "pedido-demo-001", 1, "CREATED")))
    log.append({"delivery": old, "outcome": transport.deliver(old, consumer)})
    snap = consumer.snapshot()
    assert snap["orders"][0]["status"] == "CANCELLED"
    assert len(snap["effects"]) == 3
    return {"kind": "synthetic_local_lab", "log": log, "final_state": snap,
            "assertions": {"effects": 3, "final_version": 3, "final_status": "CANCELLED",
                           "replayed_delivery": "duplicate", "late_event": "stale"}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("demo")
    run.add_argument("--directory", type=Path)
    run.add_argument("--output", type=Path)
    clean = sub.add_parser("clean")
    clean.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "clean":
        marker = args.directory / ".lab-runtime"
        if not marker.exists() or marker.read_text() != "erp-satelites-lab-v1\n":
            parser.error("Diretório sem marcador do laboratório; nada foi removido")
        for name in ("erp.db", "consumer.db", "transport.db"):
            (args.directory / name).unlink(missing_ok=True)
        marker.unlink()
        print("Somente os três bancos gerados e o marcador foram removidos.")
        return
    if args.directory:
        result = demo(args.directory)
    else:
        with tempfile.TemporaryDirectory() as tmp:
            result = demo(tmp)
    payload = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n")
    print(payload)


if __name__ == "__main__":
    main()
