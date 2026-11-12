"""Reuse mapper/read_chunks from homework 3 without changing their logic."""

import csv
import hashlib
import io
import os
import re
import shutil
import tempfile
from collections import Counter
from contextlib import contextmanager
from pathlib import Path

import boto3
from botocore.response import StreamingBody

import mapreduce

FILENAMES = {1: "skills.csv", 2: "pairs.csv", 3: "triples.csv"}


def valid_name(value):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", value):
        raise ValueError("Use 1..80 ASCII letters, digits, '_' or '-' for job/coordinator ID")
    return value


def s3_client():
    return boto3.client(
        "s3", endpoint_url=os.getenv("S3_ENDPOINT"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


@contextmanager
def chunks(path):
    with Path(path).open("rb") as source:
        stream = StreamingBody(source, Path(path).stat().st_size)
        try:
            yield mapreduce.read_chunks(stream)
        finally:
            stream.close()


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def prepare(store, state_dir, job, fencing, input_path=None):
    valid_name(job)
    root = Path(state_dir).resolve() / "inputs"
    root.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(dir=root, prefix="snapshot-", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as output:
            if input_path:
                with Path(input_path).open("rb") as source:
                    shutil.copyfileobj(source, output)
            else:
                response = s3_client().get_object(
                    Bucket=os.getenv("S3_BUCKET", "hh-lake"),
                    Key=os.getenv("S3_OBJECT", "vacancies.jsonl"),
                )
                with response["Body"] as body:
                    shutil.copyfileobj(body, output)
            output.flush()
            os.fsync(output.fileno())
        digest = sha256(temp)
        path = root / f"{digest}.jsonl"
        # Never replace an existing immutable snapshot under running readers.
        try:
            os.link(temp, path)
        except FileExistsError:
            if sha256(path) != digest:
                raise ValueError("Corrupt existing input snapshot")
        with chunks(path) as stream:
            count = sum(1 for _ in stream)
        spec = {
            "input_path": str(path), "sha256": digest,
            "chunk_size": mapreduce.CHUNK_SIZE, "chunk_count": count,
            "mapper_sha256": sha256(mapreduce.__file__), "fencing": bool(fencing),
        }
        store.prepare(job, spec)
        return spec
    finally:
        Path(temp).unlink(missing_ok=True)


def encode(partial, count):
    # Lists preserve Counter insertion order, including ties in most_common().
    return {"devops": count, "counters": {
        str(size): [[list(combo), value] for combo, value in partial[size].items()]
        for size in (1, 2, 3)
    }}


def decode(payload):
    return {size: Counter({tuple(combo): count for combo, count in payload["counters"][str(size)]})
            for size in (1, 2, 3)}


def aggregate(checkpoints):
    totals = {size: Counter() for size in (1, 2, 3)}
    devops = 0
    for _, value in checkpoints:
        devops += value["devops"]
        partial = decode(value)
        for size in totals:
            totals[size].update(partial[size])
    return encode(totals, devops)


def csv_bytes(counter):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["skills", "count"])
    for combo, count in counter.most_common():
        writer.writerow([" + ".join(combo), count])
    return output.getvalue().encode("utf-8")


def summary(payload):
    counters = decode(payload)
    return {
        "devops": payload["devops"],
        "unique": {str(size): len(counter) for size, counter in counters.items()},
        "occurrences": {str(size): sum(counter.values()) for size, counter in counters.items()},
        "csv_sha256": {str(size): hashlib.sha256(csv_bytes(counter)).hexdigest()
                       for size, counter in counters.items()},
    }
