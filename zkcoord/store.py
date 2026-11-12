"""One shared SQLite database: immutable chunks and receiver-side fencing.

Both coordinators must use the same local database file (not NFS). ZooKeeper
stores only election metadata; potentially large counters stay out of znodes.
"""

import json
import sqlite3
import zlib
from contextlib import contextmanager
from pathlib import Path

from kazoo.exceptions import BadVersionError


def pack(value):
    return zlib.compress(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())


def unpack(value):
    return json.loads(zlib.decompress(value))


class Store:
    def __init__(self, path):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                    name TEXT PRIMARY KEY, spec TEXT NOT NULL,
                    active_epoch INTEGER NOT NULL DEFAULT 0,
                    owner TEXT, result_version INTEGER NOT NULL DEFAULT 0,
                    result_id INTEGER
                );
                CREATE TABLE IF NOT EXISTS chunks (
                    job TEXT NOT NULL REFERENCES jobs(name),
                    chunk_id INTEGER NOT NULL, payload BLOB NOT NULL,
                    epoch INTEGER NOT NULL, owner TEXT NOT NULL,
                    PRIMARY KEY(job, chunk_id)
                );
                CREATE TABLE IF NOT EXISTS publications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job TEXT NOT NULL REFERENCES jobs(name),
                    version INTEGER NOT NULL, epoch INTEGER NOT NULL,
                    owner TEXT NOT NULL, payload BLOB NOT NULL,
                    created_at TEXT NOT NULL DEFAULT
                        (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    UNIQUE(job, version)
                );
            """)

    @contextmanager
    def connection(self, write=False):
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA synchronous=FULL")
        try:
            if write:
                db.execute("BEGIN IMMEDIATE")
            yield db
            if write:
                db.commit()
        except BaseException:
            if write:
                db.rollback()
            raise
        finally:
            db.close()

    def prepare(self, job, spec):
        encoded = json.dumps(spec, sort_keys=True, ensure_ascii=False)
        with self.connection(write=True) as db:
            old = db.execute("SELECT spec FROM jobs WHERE name=?", (job,)).fetchone()
            if old and old[0] != encoded:
                raise ValueError("Job already exists with another dataset/config; use a new job name")
            db.execute("INSERT OR IGNORE INTO jobs(name, spec) VALUES (?, ?)", (job, encoded))

    def job(self, job):
        with self.connection() as db:
            row = db.execute("SELECT * FROM jobs WHERE name=?", (job,)).fetchone()
        if row is None:
            raise ValueError(f"Unknown job {job!r}; run prepare first")
        value = dict(row)
        value["spec"] = json.loads(value["spec"])
        return value

    def claim(self, job, epoch, owner):
        """Register the monotonically increasing election token at the receiver."""
        with self.connection(write=True) as db:
            updated = db.execute(
                "UPDATE jobs SET active_epoch=?, owner=? WHERE name=? AND active_epoch < ?",
                (epoch, owner, job, epoch),
            )
            if updated.rowcount != 1:
                raise BadVersionError("Leader epoch is already superseded")
            return db.execute("SELECT result_version FROM jobs WHERE name=?", (job,)).fetchone()[0]

    def chunk(self, job, chunk_id):
        with self.connection() as db:
            row = db.execute("SELECT payload FROM chunks WHERE job=? AND chunk_id=?", (job, chunk_id)).fetchone()
        return unpack(row[0]) if row else None

    def checkpoint(self, job, chunk_id, payload, epoch, owner):
        """Atomically append once. Stale writers cannot create checkpoints."""
        encoded = pack(payload)
        with self.connection(write=True) as db:
            current = db.execute("SELECT * FROM jobs WHERE name=?", (job,)).fetchone()
            if current is None or current["active_epoch"] != epoch:
                raise BadVersionError("Checkpoint belongs to a superseded epoch")
            count = json.loads(current["spec"])["chunk_count"]
            if not 1 <= chunk_id <= count:
                raise ValueError("Chunk is outside the prepared input")
            old = db.execute("SELECT payload FROM chunks WHERE job=? AND chunk_id=?", (job, chunk_id)).fetchone()
            if old:
                if old[0] != encoded:
                    raise ValueError("Non-deterministic chunk result")
                return False
            db.execute(
                "INSERT INTO chunks(job, chunk_id, payload, epoch, owner) VALUES (?, ?, ?, ?, ?)",
                (job, chunk_id, encoded, epoch, owner),
            )
            return True

    def checkpoints(self, job):
        with self.connection() as db:
            rows = db.execute("SELECT chunk_id, payload FROM chunks WHERE job=? ORDER BY chunk_id", (job,)).fetchall()
        return [(row[0], unpack(row[1])) for row in rows]

    def publish(self, job, epoch, owner, expected_version, payload):
        """CAS and artifact insertion share ONE SQLite transaction.

        BadVersionError is raised only after a real conditional UPDATE affects
        zero rows. With FENCING=0 the intentionally unconditional UPDATE permits
        the stale writer. Checkpoints remain protected in both experiments.
        """
        encoded = pack(payload)
        with self.connection(write=True) as db:
            row = db.execute("SELECT * FROM jobs WHERE name=?", (job,)).fetchone()
            if row is None:
                raise ValueError("Unknown job")
            spec = json.loads(row["spec"])
            done = db.execute("SELECT count(*) FROM chunks WHERE job=?", (job,)).fetchone()[0]
            if done != spec["chunk_count"]:
                raise ValueError("Cannot publish an incomplete calculation")
            query = "UPDATE jobs SET result_version=result_version+1 WHERE name=?"
            args = (job,)
            if spec["fencing"]:
                query += " AND active_epoch=? AND result_version=?"
                args += (epoch, expected_version)
            if db.execute(query, args).rowcount != 1:
                raise BadVersionError("Atomic result CAS rejected a stale epoch/version")
            version = row["result_version"] + 1
            cursor = db.execute(
                "INSERT INTO publications(job, version, epoch, owner, payload) VALUES (?, ?, ?, ?, ?)",
                (job, version, epoch, owner, encoded),
            )
            db.execute("UPDATE jobs SET result_id=? WHERE name=?", (cursor.lastrowid, job))
            return version

    def result(self, job):
        with self.connection() as db:
            row = db.execute(
                "SELECT p.* FROM jobs j JOIN publications p ON p.id=j.result_id WHERE j.name=?", (job,),
            ).fetchone()
        if row is None:
            return None
        value = dict(row)
        value["payload"] = unpack(value["payload"])
        return value

    def history(self, job):
        with self.connection() as db:
            rows = db.execute(
                "SELECT version, epoch, owner, created_at FROM publications WHERE job=? ORDER BY version", (job,),
            ).fetchall()
        return [dict(row) for row in rows]
