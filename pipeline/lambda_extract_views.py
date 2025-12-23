import json
import boto3
import requests
from datetime import datetime, timedelta

S3_WIKI_BUCKET = "etnav-wikidata"
S3_PREFIX = "raw-views/"

def lambda_handler(event, context):
    s3 = boto3.client("s3")

    # Date handling
    if "date" in event:
        date_str = event["date"]
        date = datetime.strptime(date_str, "%Y-%m-%d")
    else:
        date = datetime.utcnow() - timedelta(days=21)
        date_str = date.strftime("%Y-%m-%d")

    year = date.strftime("%Y")
    month = date.strftime("%m")
    day = date.strftime("%d")

    url = (
        f"https://wikimedia.org/api/rest_v1/metrics/pageviews/"
        f"top/en.wikipedia/all-access/{year}/{month}/{day}"
    )

    headers = {
        "User-Agent": "ECBS5147-DataEngineering-Student (etnav@student.ceu.edu)"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    records = []
    retrieved_at = datetime.utcnow().isoformat()

    for item in data["items"][0]["articles"]:
        records.append({
            "title": item["article"],
            "views": item["views"],
            "rank": item["rank"],
            "date": date_str,
            "retrieved_at": retrieved_at
        })

    key = f"{S3_PREFIX}raw-views-{date_str}.json"

    body = "\n".join(json.dumps(r) for r in records)

    s3.put_object(
        Bucket=S3_WIKI_BUCKET,
        Key=key,
        Body=body.encode("utf-8")
    )

    return {
        "statusCode": 200,
        "body": f"Uploaded {len(records)} records to s3://{S3_WIKI_BUCKET}/{key}"
    }
