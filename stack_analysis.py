import csv
import io
import json
import os
from collections import Counter
from itertools import combinations

import boto3
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

BUCKET = os.getenv("S3_BUCKET", "hh-lake")
FILE = os.getenv("S3_OBJECT", "vacancies.jsonl")

STACK_SIZES = [3, 5, 10]

counters = {
    limit: {1: Counter(), 2: Counter(), 3: Counter()}
    for limit in STACK_SIZES
}

response = s3.get_object(Bucket=BUCKET, Key=FILE)

vacancies_count = 0

for line in response["Body"].iter_lines():
    if not line:
        continue

    vacancy = json.loads(line)

    roles = vacancy.get("professional_roles") or []
    if not any(str(r.get("id")) == "160" for r in roles):
        continue

    vacancies_count += 1

    skills = sorted({
        skill.strip()
        for skill in (vacancy.get("key_skills") or [])
        if isinstance(skill, str) and skill.strip()
    })

    for limit in STACK_SIZES:
        selected = skills[:limit]

        for size in (1, 2, 3):
            counters[limit][size].update(
                combinations(selected, size)
            )

response["Body"].close()

def save_csv(key, counter):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["skills", "count"])

    for skills, count in counter.most_common():
        writer.writerow([" + ".join(skills), count])

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=output.getvalue().encode("utf-8"),
    )

print(f"DevOps-вакансий: {vacancies_count}")
print("\nРазмер | Навыки | Пары | Тройки")
print("-" * 42)

for limit in STACK_SIZES:
    counts = counters[limit]

    print(
        f"{limit:6} | "
        f"{sum(counts[1].values()):6} | "
        f"{sum(counts[2].values()):6} | "
        f"{sum(counts[3].values()):6}"
    )

    for size, name in [
        (1, "skills"),
        (2, "pairs"),
        (3, "triples"),
    ]:
        save_csv(
            f"results/stacks/stack_{limit}/{name}.csv",
            counts[size],
        )

print("\nРезультаты сохранены в RustFS")
