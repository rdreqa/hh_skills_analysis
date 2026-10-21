
import csv
import io
import json
import os
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from itertools import combinations

import boto3
from dotenv import load_dotenv

load_dotenv()

BUCKET = os.getenv("S3_BUCKET", "hh-lake")
FILE = os.getenv("S3_OBJECT", "vacancies.jsonl")

CHUNK_SIZE = 200
WORKERS = 2
MAX_RETRIES = 3


TEST_FAILURE = os.getenv("TEST_MAPPER_FAILURE") == "1"


def is_devops(vacancy):
    roles = vacancy.get("professional_roles") or []
    return any(str(r.get("id")) == "160" for r in roles)


def mapper(chunk, chunk_id, attempt, test_failure):
    if test_failure and chunk_id == 2 and attempt == 1:
        raise RuntimeError("Simulated mapper failure")

    result = {1: Counter(), 2: Counter(), 3: Counter()}
    devops_count = 0

    for line in chunk:
        vacancy = json.loads(line)

        if not is_devops(vacancy):
            continue

        devops_count += 1

        skills = sorted({
            skill.strip()
            for skill in (vacancy.get("key_skills") or [])
            if isinstance(skill, str) and skill.strip()
        })

        for size in (1, 2, 3):
            result[size].update(combinations(skills, size))

    return result, devops_count


def read_chunks(stream):
    chunk = []

    for line in stream.iter_lines(chunk_size=1024 * 1024):
        if not line:
            continue

        chunk.append(line)

        if len(chunk) >= CHUNK_SIZE:
            yield chunk
            chunk = []

    if chunk:
        yield chunk


def save_csv(s3, filename, counter):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["skills", "count"])

    for skills, count in counter.most_common():
        writer.writerow([" + ".join(skills), count])

    s3.put_object(
        Bucket=BUCKET,
        Key=f"results/mapreduce/{filename}",
        Body=output.getvalue().encode("utf-8"),
    )


def main():
    start = time.perf_counter()

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    response = s3.get_object(Bucket=BUCKET, Key=FILE)

    counters = {1: Counter(), 2: Counter(), 3: Counter()}
    total_devops = 0
    total_chunks = 0
    retries = 0

    with ProcessPoolExecutor(max_workers=WORKERS) as pool:
        for chunk_id, chunk in enumerate(
            read_chunks(response["Body"]), start=1
        ):
            total_chunks += 1

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    future = pool.submit(
                        mapper,
                        chunk,
                        chunk_id,
                        attempt,
                        TEST_FAILURE,
                    )
                    partial, count = future.result()
                    break

                except Exception as error:
                    print(
                        f"Чанк {chunk_id}: ошибка "
                        f"(попытка {attempt}): {error}"
                    )
                    retries += 1

                    if attempt == MAX_RETRIES:
                        raise

            # Reduce: объединяем результаты мапперов
            total_devops += count

            for size in (1, 2, 3):
                counters[size].update(partial[size])

    response["Body"].close()

    for size, filename in [
        (1, "skills.csv"),
        (2, "pairs.csv"),
        (3, "triples.csv"),
    ]:
        save_csv(s3, filename, counters[size])

    elapsed = time.perf_counter() - start

    print(f"\nDevOps-вакансий: {total_devops}")
    print(f"Обработано чанков: {total_chunks}")
    print(f"Повторных попыток: {retries}")

    for size in (1, 2, 3):
        print(f"\nУникальных комбинаций ({size}): "
              f"{len(counters[size])}")

        for skills, count in counters[size].most_common(5):
            print(f"  {' + '.join(skills)}: {count}")

    print(f"\nВремя: {elapsed:.3f} сек")
    print("Результаты сохранены в RustFS")


if __name__ == "__main__":
    main()
