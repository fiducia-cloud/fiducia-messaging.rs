#!/usr/bin/env python3
"""Real transactional outbox -> fiducia-relay -> NATS JetStream acceptance.

Requires a DISPOSABLE Postgres database with the tracked migrations applied
and a built production relay: cargo build --features postgres,nats --bin fiducia-relay.
DATABASE_URL=postgres://.../fiducia_messaging_e2e NATS_URL=nats://127.0.0.1:4222 \
FIDUCIA_E2E_RELAY_BIN=./target/debug/fiducia-relay FIDUCIA_E2E_ALLOW_WRITES=1 \
python3 tests/live_postgres_jetstream_relay_e2e.py

A row may be marked 'published' only after production NatsPublisher receives
JetStream's publish ACK. No fake broker or mocked database allowed.
"""
import datetime
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import time
import urllib.parse
import uuid


def pg_env():
    url = urllib.parse.urlsplit(os.environ["DATABASE_URL"])
    db = urllib.parse.unquote(url.path.lstrip("/"))
    if url.scheme not in ("postgres", "postgresql") or not url.hostname or not db:
        raise RuntimeError("DATABASE_URL must point to Postgres")
    if not any(word in db.lower() for word in ("test", "e2e")):
        raise RuntimeError("refusing to mutate a non-test database")
    env = os.environ.copy()
    env.update(PGHOST=url.hostname, PGPORT=str(url.port or 5432),
               PGDATABASE=db, PGUSER=urllib.parse.unquote(url.username or ""))
    if url.password:
        env["PGPASSWORD"] = urllib.parse.unquote(url.password)
    return env


def psql(statement, env):
    # Credentials stay in environment, not arguments, output, or logs.
    result = subprocess.run(["psql", "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1",
                             "-c", statement], env=env, capture_output=True,
                            text=True, timeout=12)
    if result.returncode:
        raise AssertionError("Postgres E2E operation failed (details intentionally redacted)")
    return result.stdout.strip()


def sql(value):
    return "'" + value.replace("'", "''") + "'"


def dedup_id(tenant, key):
    h = hashlib.sha256()
    h.update(b"fiducia-messaging:nats-dedup:v1\0")
    h.update(b"tenant\0")
    h.update(uuid.UUID(tenant).bytes)
    encoded = key.encode("utf-8")
    h.update(len(encoded).to_bytes(8, "big"))
    h.update(encoded)
    return "v1-" + h.hexdigest()


def insert_sql(identifier, tenant, key):
    envelope = {
        "envelope_version": 1, "message_id": identifier,
        "message_type": "tests.requested", "schema_version": 1,
        "correlation_id": identifier, "tenant_id": tenant,
        "idempotency_key": key, "fencing_token": 1,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "payload": {"kind": "e2e-relay", "tenant": tenant},
    }
    return ("INSERT INTO message_outbox "
            "(id, subject, tenant_id, idempotency_key, dedup_id, payload) "
            "VALUES (%s::uuid, 'fiducia.tests.requested.v1', %s::uuid, %s, %s, %s::jsonb)" %
            (sql(identifier), sql(tenant), sql(key), sql(dedup_id(tenant, key)),
             sql(json.dumps(envelope))))


def statuses(ids, env):
    query = ("SELECT id::text || ':' || status || ':' || attempts "
             "FROM message_outbox WHERE id IN (" +
             ",".join(sql(identifier) for identifier in ids) + ") ORDER BY id")
    raw = psql(query, env)
    return {parts[0]: (parts[1], int(parts[2]))
            for line in raw.splitlines() if line
            for parts in [line.split(":")]}


def main():
    if os.getenv("FIDUCIA_E2E_ALLOW_WRITES") != "1":
        raise RuntimeError("explicit nonproduction write opt-in required")
    if not os.getenv("NATS_URL"):
        raise RuntimeError("real NATS JetStream URL required")
    binary = pathlib.Path(os.environ["FIDUCIA_E2E_RELAY_BIN"]).resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError("prebuilt production fiducia-relay executable required")
    if not shutil.which("psql"):
        raise RuntimeError("psql is required for real Postgres test")
    pg = pg_env()
    assert psql("SELECT current_database()", pg) == pg["PGDATABASE"]
    assert psql("SELECT to_regclass('message_outbox') IS NOT NULL", pg) == "t"
    tenant_a, tenant_b = str(uuid.uuid4()), str(uuid.uuid4())
    ids = [str(uuid.uuid4()), str(uuid.uuid4())]
    rolled_back = str(uuid.uuid4())
    key = "e2e-" + uuid.uuid4().hex
    process = None
    try:
        # Atomicity negative: a ROLLBACK must never yield a publishable row.
        psql("BEGIN; " + insert_sql(rolled_back, tenant_a, key + "-rollback")
             + "; ROLLBACK;", pg)
        assert rolled_back not in statuses([rolled_back], pg)
        # Identical business key in different tenants: independent effects.
        psql("BEGIN; " + insert_sql(ids[0], tenant_a, key) + "; "
             + insert_sql(ids[1], tenant_b, key) + "; COMMIT;", pg)
        before = statuses(ids, pg)
        assert len(before) == 2 and all(before[i][0] == "pending" for i in ids), before
        process = subprocess.Popen([str(binary)], env=os.environ.copy(),
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise AssertionError("relay exited before broker ACK")
            after = statuses(ids, pg)
            if len(after) == 2 and all(after[i][0] == "published" for i in ids):
                assert all(after[i][1] >= 1 for i in ids), after
                print("PASS: two tenant-isolated DB rows received real JetStream ACKs")
                return
            time.sleep(0.3)
        raise AssertionError("relay failed to publish both rows to JetStream")
    finally:
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        psql("DELETE FROM message_outbox WHERE id IN (" +
             ",".join(sql(i) for i in ids + [rolled_back]) + ")", pg)


if __name__ == "__main__":
    main()
