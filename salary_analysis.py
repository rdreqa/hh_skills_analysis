import csv
import io
import json
import os
from collections import Counter, defaultdict
from itertools import combinations
from statistics import median

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
MIN_COUNT = 3

skill_counts = Counter()
salaries = defaultdict(list)

total_devops = 0
with_salary = 0


def get_salary(vacancy):
    salary = vacancy.get("salary")

    if not isinstance(salary, dict):
        return None

    if salary.get("currency") not in ("RUR", "RUB"):
        return None

    low = salary.get("from")
    high = salary.get("to")

    low = float(low) if low not in (None, "") else None
    high = float(high) if high not in (None, "") else None

    if low is not None and high is not None:
        return (low + high) / 2

    return low if low is not None else high

response = s3.get_object(Bucket=BUCKET, Key=FILE)

for line in response["Body"].iter_lines():
    if not line:
        continue

    vacancy = json.loads(line)

    roles = vacancy.get("professional_roles") or []
    if not any(str(r.get("id")) == "160" for r in roles):
        continue

    total_devops += 1

    skills = sorted({
        s.strip()
        for s in (vacancy.get("key_skills") or [])
        if isinstance(s, str) and s.strip()
    })

    salary = get_salary(vacancy)

    if salary is None:
        continue

    with_salary += 1

    skill_counts.update(skills)

    for size in (1, 2, 3):
        for combo in combinations(skills, size):
            salaries[combo].append(salary)

response["Body"].close()

results = []

for combo, values in salaries.items():
    if len(combo) == 1 or len(values) < MIN_COUNT:
        continue

    base_skill = max(
        combo,
        key=lambda skill: (skill_counts[skill], skill)
    )

    base_values = salaries[(base_skill,)]

    base_median = median(base_values)
    combo_median = median(values)

    difference = combo_median - base_median

    results.append({
        "skills": " + ".join(combo),
        "size": len(combo),
        "count": len(values),
        "base_skill": base_skill,
        "base_count": len(base_values),
        "base_median": round(base_median, 2),
        "combo_median": round(combo_median, 2),
        "difference": round(difference, 2),
    })

results.sort(key=lambda x: x["difference"], reverse=True)

print(f"Всего DevOps-вакансий: {total_devops}")
print(f"С зарплатой в рублях: {with_salary}")
print(f"Комбинаций с минимум {MIN_COUNT} вакансиями: {len(results)}")

for size in (2, 3):
    subset = [r for r in results if r["size"] == size]

    print(f"\nПАРЫ / ТРОЙКИ: {size} навыка")

    print("\nНаибольшее повышение:")
    for r in subset[:5]:
        print(
            f"{r['skills']}: {r['difference']:+,.0f} ₽ "
            f"(n={r['count']}, база: {r['base_skill']})"
        )

    print("\nНаибольшее понижение:")
    for r in sorted(
        subset, key=lambda x: x["difference"]
    )[:5]:
        print(
            f"{r['skills']}: {r['difference']:+,.0f} ₽ "
            f"(n={r['count']}, база: {r['base_skill']})"
        )

output = io.StringIO()
fields = [
    "skills", "size", "count", "base_skill",
    "base_count", "base_median", "combo_median",
    "difference"
]
writer = csv.DictWriter(output, fieldnames=fields)
writer.writeheader()
writer.writerows(results)

s3.put_object(
    Bucket=BUCKET,
    Key="results/salary_analysis.csv",
    Body=output.getvalue().encode("utf-8"),
)

print("\nРезультаты сохранены: results/salary_analysis.csv")
