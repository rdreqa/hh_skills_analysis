"""Real SIGSTOP/SIGCONT failover. Run against an available ZooKeeper ensemble."""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kazoo.client import KazooClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from zkcoord.__main__ import verify
from zkcoord.store import Store, unpack


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def events(path):
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:  # A concurrent final write may be incomplete.
            pass
    return rows


def wait_event(path, event, process, deadline, **conditions):
    while time.monotonic() < deadline:
        for row in events(path):
            if row["event"] == event and all(row.get(k) == v for k, v in conditions.items()):
                return row
        if process.poll() is not None:
            raise RuntimeError(f"Coordinator exited {process.returncode}; see {path} and console logs")
        time.sleep(0.02)
    raise TimeoutError(f"Timed out waiting for {event} in {path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job", required=True)
    parser.add_argument("--state-dir", default=os.getenv("STATE_DIR", ".state"))
    parser.add_argument("--pause-seconds", type=float, default=40)
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()
    if args.pause_seconds < 20 or args.timeout <= args.pause_seconds:
        parser.error("Use pause >=20 seconds and timeout > pause")
    store = Store(Path(args.state_dir) / "checkpoints.sqlite3")
    job = store.job(args.job)
    if job["result_version"] or job["active_epoch"] or store.checkpoints(args.job):
        parser.error("Demo requires a fresh prepared job; choose a new --job")
    if job["spec"]["chunk_count"] < 2:
        parser.error("Demo requires at least 201 nonempty JSONL lines")
    mode = "with_fencing" if job["spec"]["fencing"] else "no_fencing"
    logs = Path("logs") / mode / args.job
    logs.mkdir(parents=True, exist_ok=True)
    if any(logs.glob("*.jsonl")):
        parser.error("Log directory already contains a run; choose a new job")
    processes, handles = [], []
    stopped = None
    observer = KazooClient(hosts=os.getenv("ZK_HOSTS", "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"), timeout=10)
    evidence = {"job": args.job, "fencing": job["spec"]["fencing"], "log_dir": str(logs)}

    def evidence_event(event, **fields):
        row = {"event": event, "timestamp": datetime.now(timezone.utc).isoformat(),
               "monotonic": time.monotonic(), **fields}
        with (logs / "experiment.jsonl").open("a", encoding="utf-8") as output:
            output.write(json.dumps(row, ensure_ascii=False) + "\n")
        return row

    def start(owner, delay):
        handle = (logs / f"{owner}.console.txt").open("w", encoding="utf-8")
        handles.append(handle)
        process = subprocess.Popen(
            [sys.executable, "-m", "zkcoord", "--state-dir", args.state_dir, "run",
             "--job", args.job, "--id", owner, "--log-dir", str(logs), "--chunk-delay", str(delay)],
            stdout=handle, stderr=subprocess.STDOUT,
        )
        processes.append(process)
        (logs / f"{owner}.pid").write_text(str(process.pid) + "\n")
        return process

    try:
        deadline = time.monotonic() + args.timeout
        observer.start(timeout=15)
        first = start("c1", 2)
        first_leader = wait_event(logs / "c1.jsonl", "leader_acquired", first, deadline)
        first_candidate = wait_event(logs / "c1.jsonl", "candidate_created", first, deadline)["node"]
        first_stat = observer.exists(first_candidate)
        require(first_stat is not None and first_stat.ephemeralOwner != 0, "Leader candidate must be ephemeral")
        evidence["old_candidate"] = first_candidate
        evidence["old_ephemeral_owner"] = first_stat.ephemeralOwner
        second = start("c2", 0)
        wait_event(logs / "c2.jsonl", "candidate_created", second, deadline)
        wait_event(logs / "c1.jsonl", "chunk_started", first, deadline, chunk_id=2)
        os.kill(first.pid, signal.SIGSTOP)
        stopped = first
        pause = evidence_event("leader_sigstop", coordinator_id="c1", pid=first.pid)
        print(f"SIGSTOP c1 pid={first.pid}; waiting for c2", flush=True)
        leader = wait_event(logs / "c2.jsonl", "leader_acquired", second, deadline)
        observer.sync(first_candidate.rsplit("/", 1)[0])
        require(observer.exists(first_candidate) is None, "Old leader's candidate did not expire")
        evidence["old_candidate_absent_after_failover"] = True
        evidence["failover_seconds"] = leader["monotonic"] - pause["monotonic"]
        evidence_event("failover_measured", coordinator_id="c2", epoch=leader["epoch"],
                       failover_seconds=evidence["failover_seconds"])
        restored = wait_event(logs / "c2.jsonl", "checkpoint_restored", second, deadline)
        if restored["completed_chunks"] < 1 or leader["epoch"] <= first_leader["epoch"]:
            raise AssertionError("No checkpoint restored or epoch did not increase")
        evidence["restored_chunks"] = restored["completed_chunks"]
        wait_event(logs / "c2.jsonl", "result_published", second, deadline)
        # At least 40s since SIGSTOP, and only after the new result is published.
        resume_at = pause["monotonic"] + args.pause_seconds
        while time.monotonic() < resume_at:
            time.sleep(max(0, min(0.2, resume_at - time.monotonic())))
        os.kill(first.pid, signal.SIGCONT)
        stopped = None
        resumed = evidence_event("leader_sigcont", coordinator_id="c1", pid=first.pid)
        evidence["paused_seconds"] = resumed["monotonic"] - pause["monotonic"]
        for process in processes:
            if process.wait(timeout=max(1, deadline - time.monotonic())) != 0:
                raise RuntimeError("Coordinator failed; inspect console logs")
        all_events = events(logs / "c1.jsonl") + events(logs / "c2.jsonl")
        published = [row for row in all_events if row["event"] == "result_published"]
        rejected = [row for row in all_events if row["event"] == "publication_rejected"]
        attempts = [row for row in all_events if row["event"] == "publication_attempt"]
        started = [row for row in all_events if row["event"] == "coordinator_started"]
        result = store.result(args.job)
        fenced = job["spec"]["fencing"]
        require(len(started) == 2 and all(row["fencing"] == fenced for row in started),
                "Both coordinators must use the prepared fencing mode")
        evidence["coordinator_fencing"] = {row["coordinator_id"]: int(row["fencing"]) for row in started}
        saved_before_pause = {row["chunk_id"] for row in all_events
                              if row["event"] == "checkpoint_saved" and row["coordinator_id"] == "c1"
                              and row["monotonic"] <= pause["monotonic"]}
        new_mapped = {row["chunk_id"] for row in all_events
                      if row["event"] == "chunk_started" and row["coordinator_id"] == "c2"}
        require(saved_before_pause.isdisjoint(new_mapped), "New leader recomputed a completed chunk")
        evidence["new_leader_skipped_completed_chunks"] = sorted(saved_before_pause)
        require(len(attempts) == 2, "Both leaders must actually attempt publication")
        require(len(published) == (1 if fenced else 2), "Unexpected number of successful publications")
        require(result["version"] == (1 if fenced else 2), "Unexpected pointer version")
        require(result["owner"] == ("c2" if fenced else "c1"), "Unexpected pointer owner")
        require(len(rejected) == int(fenced), "Unexpected number of rejected publications")
        if fenced:
            require(rejected[0]["coordinator_id"] == "c1" and rejected[0]["error"] == "BadVersionError",
                    "Stale leader must receive a real BadVersionError")
        # Check the contents of every successful publication, including the stale one.
        baseline = verify(store, args.job)
        with store.connection() as db:
            payloads = db.execute("SELECT payload FROM publications WHERE job=?", (args.job,)).fetchall()
        require(all(unpack(row[0]) == result["payload"] for row in payloads), "Published payloads differ")
        evidence.update({"status": "passed", "successful_publications": len(published),
                         "rejected_publications": len(rejected), "pointer_version": result["version"],
                         "pointer_owner": result["owner"], "verification": baseline})
        print(json.dumps(evidence, ensure_ascii=False, indent=2))
    except BaseException as error:
        evidence.update({"status": "failed", "error": f"{type(error).__name__}: {error}"})
        raise
    finally:
        observer.stop()
        observer.close()
        if stopped and stopped.poll() is None:
            os.kill(stopped.pid, signal.SIGCONT)
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        for handle in handles:
            handle.close()
        (logs / "evidence.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
