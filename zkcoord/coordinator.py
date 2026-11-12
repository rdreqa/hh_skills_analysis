import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import BadVersionError, NoNodeError
from kazoo.retry import KazooRetry

import mapreduce
from .data import aggregate, chunks, encode, sha256, summary, valid_name

SESSION_TIMEOUT = 10.0
ZK_ROOT = "/hh-skills/v1"


class Journal:
    def __init__(self, directory, owner, job):
        valid_name(owner)
        Path(directory).mkdir(parents=True, exist_ok=True)
        self.owner, self.job = owner, job
        self.state, self.epoch = "INITIAL", None
        self.logger = logging.getLogger(f"coordinator.{owner}.{job}")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        self.logger.handlers.clear()
        handler = logging.FileHandler(Path(directory) / f"{owner}.jsonl", encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(message)s"))
        self.logger.addHandler(handler)

    def emit(self, event, **fields):
        row = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "unix_time": time.time(), "monotonic": time.monotonic(),
            "pid": os.getpid(), "coordinator_id": self.owner, "job": self.job,
            "zk_state": self.state, "epoch": self.epoch, "event": event,
        }
        row.update(fields)
        self.logger.info(json.dumps(row, ensure_ascii=False))


class SessionLost(RuntimeError):
    pass


def elect(client, job, owner, journal, lost):
    """Ephemeral sequential election; watch only the immediate predecessor.

    The parent's persistent sequence is never reset. The candidate's creation
    zxid is a monotonically increasing epoch across the entire ensemble. A
    process with a lost session never creates another candidate in this run.
    """
    parent = f"{ZK_ROOT}/{job}/election"
    client.ensure_path(parent)
    node = client.create(
        f"{parent}/candidate-", json.dumps({"id": owner, "pid": os.getpid()}).encode(),
        ephemeral=True, sequence=True,
    )
    name = node.rsplit("/", 1)[1]
    journal.emit("candidate_created", node=node)
    while not lost.is_set():
        children = sorted(child for child in client.get_children(parent) if child.startswith("candidate-"))
        if name not in children:
            raise SessionLost("Election candidate disappeared")
        index = children.index(name)
        if index == 0 and client.state == KazooState.CONNECTED:
            stat = client.exists(node)
            if stat is None or lost.is_set():
                raise SessionLost("Session expired during election")
            return node, stat.czxid
        journal.emit("waiting_for_leader", predecessor=children[index - 1] if index else None)
        changed = threading.Event()
        if index and client.exists(f"{parent}/{children[index - 1]}", watch=lambda _: changed.set()) is None:
            continue
        changed.wait(0.5)
    raise SessionLost("Session lost while waiting for leadership")


def wait_for_checkpoints(store, job, count, journal, timeout=180):
    deadline = time.monotonic() + timeout
    journal.emit("stale_worker_waiting_for_checkpoints")
    while time.monotonic() < deadline:
        values = store.checkpoints(job)
        if len(values) == count:
            return values
        time.sleep(0.2)
    raise TimeoutError("New leader did not complete checkpoints in 180 seconds")


def compute(store, job, owner, epoch, delay, journal):
    spec = store.job(job)["spec"]
    done = store.checkpoints(job)
    completed = {chunk_id for chunk_id, _ in done}
    journal.emit("checkpoint_restored", completed_chunks=len(done), total_chunks=spec["chunk_count"])
    with chunks(spec["input_path"]) as stream:
        for chunk_id, chunk in enumerate(stream, 1):
            if chunk_id in completed or store.chunk(job, chunk_id) is not None:
                continue
            # No correctness decision relies on client.connected. In the demo
            # SIGCONT deliberately leaves the old in-flight attempt alive so
            # the receiver must reject its original epoch, even after reconnect.
            if store.job(job)["active_epoch"] != epoch:
                break
            journal.emit("chunk_started", chunk_id=chunk_id)
            if delay:
                time.sleep(delay)
            for attempt in range(1, mapreduce.MAX_RETRIES + 1):
                try:
                    partial, count = mapreduce.mapper(chunk, chunk_id, attempt, mapreduce.TEST_FAILURE)
                    break
                except Exception as error:
                    journal.emit("mapper_retry", chunk_id=chunk_id, attempt=attempt, error=str(error))
                    if attempt == mapreduce.MAX_RETRIES:
                        raise
            try:
                saved = store.checkpoint(job, chunk_id, encode(partial, count), epoch, owner)
                journal.emit("checkpoint_saved", chunk_id=chunk_id, inserted=saved)
            except BadVersionError as error:
                journal.emit("checkpoint_rejected", chunk_id=chunk_id, error=type(error).__name__, message=str(error))
                break
    values = store.checkpoints(job)
    if len(values) != spec["chunk_count"]:
        values = wait_for_checkpoints(store, job, spec["chunk_count"], journal)
    result = aggregate(values)
    journal.emit("calculation_completed", chunks=len(values), **summary(result))
    return result


def run(store, job, owner, hosts, log_dir, delay=0):
    if delay < 0:
        raise ValueError("Chunk delay must be nonnegative")
    journal = Journal(log_dir, owner, job)
    spec = store.job(job)["spec"]
    if sha256(spec["input_path"]) != spec["sha256"]:
        raise ValueError("Input snapshot changed after prepare")
    if sha256(mapreduce.__file__) != spec["mapper_sha256"]:
        raise ValueError("Original mapper changed after prepare; use a new job")
    lost = threading.Event()
    client = KazooClient(
        hosts=hosts, timeout=SESSION_TIMEOUT,
        connection_retry=KazooRetry(max_tries=-1, max_delay=1),
        command_retry=KazooRetry(max_tries=3, max_delay=1),
    )

    def connection_state(state):
        journal.state = state
        if state == KazooState.LOST:
            lost.set()  # Never cleared: this attempt cannot get a new epoch.
        journal.emit("zk_state_changed")

    client.add_listener(connection_state)
    node = None
    try:
        journal.emit("coordinator_started", fencing=spec["fencing"], session_timeout=SESSION_TIMEOUT)
        client.start(timeout=30)
        node, epoch = elect(client, job, owner, journal, lost)
        journal.epoch = epoch
        if store.result(job) is not None:
            journal.emit("job_already_published")
            return 0
        version = store.claim(job, epoch, owner)
        journal.emit("leader_acquired", expected_version=version)
        result = compute(store, job, owner, epoch, delay, journal)
        journal.emit("publication_attempt", expected_version=version)
        try:
            version = store.publish(job, epoch, owner, version, result)
        except BadVersionError as error:
            journal.emit("publication_rejected", error=type(error).__name__, message=str(error))
            return 0
        journal.emit("result_published", message="записал результат", result_version=version, **summary(result))
        return 0
    except Exception as error:
        journal.emit("coordinator_failed", error=type(error).__name__, message=str(error))
        raise
    finally:
        if node and client.connected:
            try:
                client.delete(node)
            except NoNodeError:
                pass
            except Exception as error:
                journal.emit("candidate_cleanup_failed", error=type(error).__name__)
        client.stop()
        client.close()
        journal.emit("coordinator_stopped")
