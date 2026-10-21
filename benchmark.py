import csv
import os
import subprocess
import sys
import time
from pathlib import Path

import boto3
import psutil
from dotenv import load_dotenv

load_dotenv()

RUNS = 3

SCRIPTS = {
    "MapReduce": "mapreduce.py",
    "DuckDB": "duckdb_analysis.py",
}

results = []

def measure(script):
    start = time.perf_counter()

    process = subprocess.Popen(
        [sys.executable, script],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=os.environ.copy(),
    )

    ps_process = psutil.Process(process.pid)
    peak_memory = 0

    while process.poll() is None:
        try:
            processes = [ps_process] + ps_process.children(recursive=True)
            memory = sum(
                p.memory_info().rss
                for p in processes
                if p.is_running()
            )
            peak_memory = max(peak_memory, memory)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

        time.sleep(0.01)

    output, _ = process.communicate()
    elapsed = time.perf_counter() - start

    if process.returncode != 0:
        print(output)
        raise RuntimeError(f"Ошибка запуска {script}")

    return elapsed, peak_memory / 1024**2

for name, script in SCRIPTS.items():
    print(f"\n=== {name} ===")

    for run in range(1, RUNS + 1):
        elapsed, memory = measure(script)

        results.append({
            "method": name,
            "run": run,
            "time_sec": round(elapsed, 3),
            "peak_memory_mib": round(memory, 2),
        })

        print(
            f"Запуск {run}: "
            f"{elapsed:.3f} сек, "
            f"RAM: {memory:.2f} MiB"
        )

filename = Path("benchmark_results.csv")

with filename.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=results[0].keys())
    writer.writeheader()
    writer.writerows(results)

print("\n=== Средние результаты ===")

for name in SCRIPTS:
    rows = [r for r in results if r["method"] == name]

    avg_time = sum(r["time_sec"] for r in rows) / RUNS
    avg_memory = sum(r["peak_memory_mib"] for r in rows) / RUNS

    print(
        f"{name}: {avg_time:.3f} сек, "
        f"RAM: {avg_memory:.2f} MiB"
    )

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

s3.upload_file(
    str(filename),
    os.getenv("S3_BUCKET", "hh-lake"),
    "results/benchmark_results.csv",
)

print("\nРезультаты сохранены в CSV и RustFS")
