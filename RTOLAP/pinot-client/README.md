# Pinot Client Job

This folder contains a small Python-based query runner for Apache Pinot. It is designed to run as a Kubernetes Job, execute a list of SQL queries against a Pinot broker, write query timing/results to a CSV file, and upload the CSV to Amazon S3.

## What It Does

- Reads Pinot connection and query scenarios from environment variables.
- Executes each SQL query via Pinot's `/query/sql` endpoint.
- Captures:
  - Scenario comment label
  - SQL query text
  - `timeUsedMs` from Pinot response (when available)
  - Status (`Success`, HTTP error, or exception)
  - Full raw response body
- Appends all results to a CSV file.
- Uploads the CSV to S3 under a configurable key prefix.

## Files Included

- `pinot_client.py`
  - Main program.
  - Reads env vars (`PINOT_BROKER_IP`, `PINOT_QUERIES`, `S3_BUCKET_NAME`, `S3_KEY_PREFIX`, `CSV_FILENAME`).
  - Sends POST requests to `http://<PINOT_BROKER_IP>/query/sql`.
  - Writes and appends CSV output.
  - Uploads output file to S3 using `boto3`.

- `deployment.yaml`
  - Kubernetes Job manifest for running the client in-cluster.
  - Includes example `PINOT_QUERIES` JSON list.
  - Uses placeholders for image and output naming.

- `Dockerfile`
  - Builds a Python 3.9 slim image.
  - Installs dependencies from `requirements.txt`.
  - Runs `pinot_client.py` as container entrypoint.

- `requirements.txt`
  - Python dependencies: `requests`, `boto3`.

- `run.sh`
  - Helper script for repeated benchmark runs with different output filenames.
  - Applies and deletes Kubernetes manifests in a loop.
  - Note: this script references `deployment-lowselectivity.yaml` and `deployment-lowselectivity-count.yaml`, which are not present in this folder.

## Environment Variables

- `PINOT_BROKER_IP`
  - Pinot broker host:port.
  - Example: `pinot-broker.default.svc.cluster.local:8099`

- `PINOT_QUERIES`
  - JSON array string with query scenarios.
  - Each item should contain:
    - `comment`: label for output
    - `query`: SQL string

- `S3_BUCKET_NAME`
  - Destination S3 bucket.

- `S3_KEY_PREFIX`
  - S3 key prefix (folder path) for uploaded CSV.

- `CSV_FILENAME`
  - Local CSV filename and uploaded object name.

## Example Query Payload

```json
[
  {"comment": "Q1", "query": "SELECT COUNT(*) FROM my_table"},
  {"comment": "Q2", "query": "SELECT * FROM my_table WHERE col = 'value' LIMIT 1000"}
]
```

## Build and Run

### 1) Build container image

```bash
docker build -t <your-registry>/pinot-client:latest .
```

### 2) Push image

```bash
docker push <your-registry>/pinot-client:latest
```

### 3) Update Kubernetes manifest

Edit `deployment.yaml` and set:

- `image: ADD-yours` -> your pushed image
- `S3_BUCKET_NAME` -> your bucket
- Optional: `S3_KEY_PREFIX` and `CSV_FILENAME`

### 4) Run Job

```bash
kubectl apply -f deployment.yaml
```

### 5) Check logs

```bash
kubectl logs job/pinot-client-job
```

## Notes

- The script appends to `CSV_FILENAME` if it already exists in the container filesystem.
- If S3 upload fails, query execution results are still printed and written locally.
- Ensure your Kubernetes environment provides AWS credentials/permissions for `s3:PutObject`.

## License

This component is available under the terms in [../../LICENSE](../../LICENSE).
Use is limited to internal, non-production, non-commercial purposes.
Redistribution, sublicensing, modification, sale, and use for training or improving AI/ML models are not permitted.
