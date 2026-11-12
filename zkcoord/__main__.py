import argparse
import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from kazoo.client import KazooClient

import mapreduce
from .coordinator import SESSION_TIMEOUT, run
from .data import (FILENAMES, aggregate, chunks, csv_bytes, decode, encode,
                   prepare, s3_client, summary, valid_name)
from .store import Store


def print_json(value):
    print(json.dumps(value, ensure_ascii=False, indent=2))


def verify(store, job, original_s3=False):
    row = store.job(job)
    result = store.result(job)
    if result is None:
        raise ValueError("No published result")
    partials = []
    # Reference: original mapper and reducer, all chunks processed from scratch.
    with chunks(row["spec"]["input_path"]) as stream:
        for chunk_id, chunk in enumerate(stream, 1):
            partial, count = mapreduce.mapper(chunk, chunk_id, 1, False)
            partials.append((chunk_id, encode(partial, count)))
    baseline = aggregate(partials)
    if baseline != result["payload"]:
        raise AssertionError("Published result differs from original mapper/reducer")
    s3_checks = {}
    if original_s3:
        client = s3_client()
        for size, filename in FILENAMES.items():
            response = client.get_object(
                Bucket=os.getenv("S3_BUCKET", "hh-lake"), Key=f"results/mapreduce/{filename}",
            )
            with response["Body"] as body:
                actual = body.read()
            expected = csv_bytes(decode(baseline)[size])
            if actual != expected:
                raise AssertionError(f"Original S3 {filename} differs (including row order)")
            s3_checks[filename] = True
    return {"job": job, "matches_original_mapper": True, "original_s3": s3_checks,
            "dataset_sha256": row["spec"]["sha256"], **summary(baseline)}


def main(argv=None):
    load_dotenv()
    parser = argparse.ArgumentParser(description="ZooKeeper homework coordinator")
    parser.add_argument("--state-dir", default=os.getenv("STATE_DIR", ".state"))
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("prepare", "run", "inspect", "verify", "export"):
        cmd = sub.add_parser(command)
        cmd.add_argument("--job", required=True, type=valid_name)
        if command == "prepare":
            cmd.add_argument("--input", type=Path, help="Local JSONL; otherwise download from RustFS")
            cmd.add_argument("--fencing", type=int, choices=(0, 1), default=int(os.getenv("FENCING", "1")))
        elif command == "run":
            cmd.add_argument("--id", required=True, type=valid_name)
            cmd.add_argument("--hosts", default=os.getenv("ZK_HOSTS", "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"))
            cmd.add_argument("--log-dir")
            cmd.add_argument("--chunk-delay", type=float, default=float(os.getenv("CHUNK_DELAY_SECONDS", "0")))
        elif command == "verify":
            cmd.add_argument("--original-s3", action="store_true")
        elif command == "export":
            cmd.add_argument("--output", type=Path)
    ephemeral = sub.add_parser("ephemeral")
    ephemeral.add_argument("--hosts", default=os.getenv("ZK_HOSTS", "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"))
    ephemeral.add_argument("--path", default="/hh-skills-demo-ephemeral")
    ephemeral.add_argument("--hold", type=float, default=60)
    ephemeral.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    if args.command == "ephemeral":
        client = KazooClient(hosts=args.hosts, timeout=SESSION_TIMEOUT)
        try:
            client.start(timeout=15)
            if args.check:
                print_json({"path": args.path, "exists": client.exists(args.path) is not None})
            else:
                client.create(args.path, f"pid={os.getpid()}".encode(), ephemeral=True)
                print_json({"path": args.path, "pid": os.getpid(), "session_timeout": SESSION_TIMEOUT})
                sys.stdout.flush()
                time.sleep(args.hold)
        finally:
            client.stop()
            client.close()
        return 0

    store = Store(Path(args.state_dir) / "checkpoints.sqlite3")
    if args.command == "prepare":
        print_json(prepare(store, args.state_dir, args.job, args.fencing, args.input))
    elif args.command == "run":
        mode = "with_fencing" if store.job(args.job)["spec"]["fencing"] else "no_fencing"
        return run(store, args.job, args.id, args.hosts,
                   args.log_dir or f"logs/{mode}/{args.job}", args.chunk_delay)
    elif args.command == "inspect":
        result = store.result(args.job)
        print_json({"job": store.job(args.job), "completed_chunks": len(store.checkpoints(args.job)),
                    "publications": store.history(args.job),
                    "pointer": {key: result[key] for key in ("id", "version", "epoch", "owner")} if result else None,
                    "summary": summary(result["payload"]) if result else None})
    elif args.command == "verify":
        print_json(verify(store, args.job, args.original_s3))
    elif args.command == "export":
        result = store.result(args.job)
        if result is None:
            raise ValueError("No published result")
        output = args.output or Path(args.state_dir) / args.job / f"result-v{result['version']}"
        output.mkdir(parents=True, exist_ok=True)
        for size, counter in decode(result["payload"]).items():
            (output / FILENAMES[size]).write_bytes(csv_bytes(counter))
        print_json({"output": str(output.resolve()), "version": result["version"], "owner": result["owner"]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
