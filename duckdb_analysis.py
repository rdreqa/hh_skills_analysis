import csv
import io
import os
import time

import boto3
import duckdb
from dotenv import load_dotenv

load_dotenv()

BUCKET = os.getenv("S3_BUCKET", "hh-lake")
FILE = os.getenv("S3_OBJECT", "vacancies.jsonl")
ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
SECRET_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]


def escape(value):
    return value.replace("'", "''")


s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

db = duckdb.connect()
db.execute("INSTALL httpfs")
db.execute("LOAD httpfs")

db.execute(f"""
    CREATE SECRET rustfs (
        TYPE S3,
        KEY_ID '{escape(ACCESS_KEY)}',
        SECRET '{escape(SECRET_KEY)}',
        REGION 'us-east-1',
        ENDPOINT '{escape(ENDPOINT.replace("http://", "").replace("https://", ""))}',
        URL_STYLE 'path',
        USE_SSL false
    )
""")

start = time.perf_counter()

db.execute(f"""
    CREATE TEMP TABLE vacancies AS
    SELECT
        ROW_NUMBER() OVER () AS id,
        key_skills
    FROM read_json_auto('s3://{BUCKET}/{FILE}')
    WHERE list_contains(
        list_transform(professional_roles, r -> r.id),
        '160'
    )
""")

db.execute("""
    CREATE TEMP TABLE skills AS
    SELECT DISTINCT
        v.id,
        TRIM(s.skill) AS skill
    FROM vacancies v,
         UNNEST(v.key_skills) AS s(skill)
    WHERE s.skill IS NOT NULL
      AND TRIM(s.skill) <> ''
""")

queries = {
    1: """
        SELECT skill AS combination, COUNT(*) AS count
        FROM skills
        GROUP BY skill
    """,
    2: """
        SELECT
            a.skill || ' + ' || b.skill AS combination,
            COUNT(*) AS count
        FROM skills a
        JOIN skills b
          ON a.id = b.id AND a.skill < b.skill
        GROUP BY a.skill, b.skill
    """,
    3: """
        SELECT
            a.skill || ' + ' || b.skill || ' + ' ||
            c.skill AS combination,
            COUNT(*) AS count
        FROM skills a
        JOIN skills b
          ON a.id = b.id AND a.skill < b.skill
        JOIN skills c
          ON b.id = c.id AND b.skill < c.skill
        GROUP BY a.skill, b.skill, c.skill
    """,
}

filenames = {
    1: "skills.csv",
    2: "pairs.csv",
    3: "triples.csv",
}

all_match = True

print("DevOps вакансий:", db.execute(
    "SELECT COUNT(*) FROM vacancies"
).fetchone()[0])

for size, query in queries.items():
    rows = db.execute(query).fetchall()
    duck_results = dict(rows)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["skills", "count"])

    for name, count in sorted(
        rows, key=lambda x: (-x[1], x[0])
    ):
        writer.writerow([name, count])

    s3.put_object(
        Bucket=BUCKET,
        Key=f"results/duckdb/{filenames[size]}",
        Body=output.getvalue().encode("utf-8"),
    )

    response = s3.get_object(
        Bucket=BUCKET,
        Key=f"results/mapreduce/{filenames[size]}",
    )

    text = response["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))

    mr_results = {
        row["skills"]: int(row["count"])
        for row in reader
    }

    matches = duck_results == mr_results
    all_match = all_match and matches

    print(f"\nКомбинации из {size} навыков:")
    print(f"MapReduce: {len(mr_results)}")
    print(f"DuckDB:    {len(duck_results)}")
    print(f"Совпадают: {matches}")

    if not matches:
        differences = [
            key for key in (mr_results.keys() | duck_results.keys())
            if mr_results.get(key) != duck_results.get(key)
        ]
        print("Примеры расхождений:")
        for key in sorted(differences)[:5]:
            print(
                key,
                "MR =", mr_results.get(key),
                "DuckDB =", duck_results.get(key),
            )

    print("Топ-5:")
    for name, count in sorted(
        rows, key=lambda x: (-x[1], x[0])
    )[:5]:
        print(f"  {name}: {count}")

elapsed = time.perf_counter() - start

print(f"\nВремя DuckDB и проверки: {elapsed:.3f} сек")
print(f"Все результаты совпали: {all_match}")

db.close()
