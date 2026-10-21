import csv
import io
import json
import os
from collections import Counter
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

CITIES = ["Москва", "Санкт-Петербург", "Нижний Новгород"]

regions = {
    city: {
        "count": 0,
        "salaries": [],
        "skills": Counter(),
        "pairs": Counter(),
        "triples": Counter(),
    }
    for city in CITIES
}

def get_salary(vacancy):
    salary = vacancy.get("salary")
    if not isinstance(salary, dict):
        return None

    if salary.get("currency") not in ("RUR", "RUB"):
        return None

    low = salary.get("from")
    high = salary.get("to")

    if low is not None and high is not None:
        return (float(low) + float(high)) / 2

    value = low if low is not None else high
    return float(value) if value is not None else None

response = s3.get_object(Bucket=BUCKET, Key=FILE)

for line in response["Body"].iter_lines():
    if not line:
        continue

    vacancy = json.loads(line)

    roles = vacancy.get("professional_roles") or []
    if not any(str(r.get("id")) == "160" for r in roles):
        continue

    city = (vacancy.get("area") or {}).get("name")
    if city not in regions:
        continue

    data = regions[city]
    data["count"] += 1

    salary = get_salary(vacancy)
    if salary is not None:
        data["salaries"].append(salary)

    skills = sorted({
        s.strip()
        for s in (vacancy.get("key_skills") or [])
        if isinstance(s, str) and s.strip()
    })

    data["skills"].update(skills)
    data["pairs"].update(combinations(skills, 2))
    data["triples"].update(combinations(skills, 3))

response["Body"].close()

def top(counter):
    if not counter:
        return "Нет данных"
    combination, count = counter.most_common(1)[0]
    if isinstance(combination, tuple):
        combination = " + ".join(combination)
    return f"{combination} ({count})"

summary = []
skills_summary = []

for city, data in regions.items():
    salary_values = data["salaries"]

    summary.append({
        "city": city,
        "vacancies": data["count"],
        "with_salary": len(salary_values),
        "median_salary": round(median(salary_values), 2)
        if salary_values else "",
    })

    skills_summary.append({
        "city": city,
        "top_skill": top(data["skills"]),
        "top_pair": top(data["pairs"]),
        "top_triple": top(data["triples"]),
    })

print("ТАБЛИЦА 1. Общая статистика")
for row in summary:
    print(row)

print("\nТАБЛИЦА 2. Востребованность навыков")
for row in skills_summary:
    print(row)

def save_csv(filename, rows):
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=list(rows[0].keys())
    )
    writer.writeheader()
    writer.writerows(rows)

    s3.put_object(
        Bucket=BUCKET,
        Key=f"results/{filename}",
        Body=output.getvalue().encode("utf-8"),
    )

save_csv("regions_summary.csv", summary)
save_csv("regions_skills.csv", skills_summary)

print("\nРезультаты сохранены в RustFS")
