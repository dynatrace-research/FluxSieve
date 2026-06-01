# Copyright (c) 2026 Dynatrace LLC. All rights reserved.
# Licensed under the terms in the repository LICENSE file.

import os
import json
import csv
import boto3
import requests

# Load environment variables
BROKER_IP = os.getenv("PINOT_BROKER_IP", "localhost")
QUERY_LIST_JSON = os.getenv("PINOT_QUERIES", "[]")
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "test")
S3_KEY_PREFIX = os.getenv("S3_KEY_PREFIX", "pinot-results/")
CSV_FILENAME = os.getenv("CSV_FILENAME", "query_results.csv")

# Parse query scenarios
try:
    QUERY_SCENARIOS = json.loads(QUERY_LIST_JSON)
except json.JSONDecodeError:
    print("Failed to parse PINOT_QUERIES environment variable.")
    QUERY_SCENARIOS = []

results = []

for scenario in QUERY_SCENARIOS:
    comment = scenario.get("comment", "")
    query = scenario.get("query", "")
    raw_response_text = ""
    try:
        response = requests.post(
            f"http://{BROKER_IP}/query/sql",
            headers={"Content-Type": "application/json"},
            json={"sql": query}
        )

        # Capture and print raw response
        raw_response_text = response.text
        print(f"\nRaw response for scenario '{comment}': {query}")
        print(raw_response_text)

        response_json = response.json()
        time_used = response_json.get("timeUsedMs", "N/A")
        status = "Success" if response.status_code == 200 else f"Error {response.status_code}"
    except Exception as e:
        time_used = "N/A"
        status = f"Exception: {str(e)}"

    # Add comment as first column
    results.append([comment, query, time_used, status, raw_response_text])

# Print processed results before writing to CSV
print("\nProcessed results to be written to CSV and uploaded to S3:")
for row in results:
    print(row)

# Append results to CSV
file_exists = os.path.isfile(CSV_FILENAME)
with open(CSV_FILENAME, "a", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    if not file_exists:
        writer.writerow(["Comment", "Query", "TimeUsedMs", "Status", "RawResponse"])
    writer.writerows(results)

# Upload to S3
if S3_BUCKET:
    s3_client = boto3.client("s3")
    s3_key = os.path.join(S3_KEY_PREFIX, CSV_FILENAME)
    try:
        s3_client.upload_file(CSV_FILENAME, S3_BUCKET, s3_key)
        print(f"\nUploaded {CSV_FILENAME} to s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload to S3: {str(e)}")
else:
    print("S3_BUCKET_NAME environment variable not set.")