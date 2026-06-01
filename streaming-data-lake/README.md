# Kafka Topic to Parquet Reproduction Guide

This guide documents how to:
- ingest records from Kafka topic `test` using both direct Parquet programs,
- write Parquet files into this `streaming-data-lake` workspace,
- count and inspect records in Parquet with DuckDB,
- verify that input and output counts match.

## Scope

Programs covered:
- `com.dynatrace.research.matchAndEnrichRecordsWriteParquet.KafkaStreamsBaselineWriteParquetDirect`
- `com.dynatrace.research.matchAndEnrichRecordsWriteParquet.KafkaStreamsMatchToParquetDirect`

Output target root (required):
- `streaming-data-lake/`

Recommended Parquet output directories:
- Baseline: `parquet_direct_baseline`
- Enriched: `parquet_direct_enriched`

## Prerequisites

1. Kafka is running and reachable at `localhost:9092`.
2. The load generator in `load-generator/` has been run so the input topic `test` contains records.
3. The sibling repository `../streaming-data-plane` exists and builds.
4. Python 3.10+ is available for record counting.

## 1) Build once

From `streaming-data-lake`:

```bash
(cd ../streaming-data-plane && ./gradlew :kstreams:compileJava)
```

## 2) Populate the Kafka topic with test records

You must run the load generator before executing either Parquet writer. These jobs only consume what is already present in Kafka; they do not create input data themselves. If you skip this step, the topic may exist but remain empty, and the Parquet output will also be empty.

From `load-generator/`:

```bash
podman-compose -f docker-compose.yaml up --build
```

This produces records into topic `test` on `localhost:9092`.

## 3) Count records in input Kafka topic

This is the baseline count to compare against Parquet.

```bash
kcat -b localhost:9092 -t test -C -o beginning -e -q | wc -l
```

## 4) Run baseline direct writer (output to streaming-data-lake)

Run with an isolated consumer group so it re-reads from earliest:

```bash
(cd ../streaming-data-plane && PARQUET_DIR="${OLDPWD}/parquet_direct_baseline" \
timeout 90s ./gradlew :kstreams:run \
  -PmainClass=com.dynatrace.research.matchAndEnrichRecordsWriteParquet.KafkaStreamsBaselineWriteParquetDirect \
  -Dconsumer.group.id=baseline-repro-$(date +%s) \
  -Dkafka.bootstrap.servers=localhost:9092 \
  -Dkafka.topic.input=test \
  --console=plain)
```

Expected output files:
- `parquet_direct_baseline/parquet/sink/*.parquet`

## 5) Run enriched direct writer (output to streaming-data-lake)

Run with an isolated consumer group so it re-reads from earliest:

```bash
(cd ../streaming-data-plane && PARQUET_DIR="${OLDPWD}/parquet_direct_enriched" \
timeout 90s ./gradlew :kstreams:run \
  -PmainClass=com.dynatrace.research.matchAndEnrichRecordsWriteParquet.KafkaStreamsMatchToParquetDirect \
  -Dconsumer.group.id=enriched-repro-$(date +%s) \
  -Dkafka.bootstrap.servers=localhost:9092 \
  -Dkafka.topic.input=test \
  --console=plain)
```

Expected output files:
- `parquet_direct_enriched/parquet/sink/*.parquet`

## 6) Install Python requirements for Parquet counting

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r scripts/requirements-parquet-tools.txt
```

## 7) Count records in Parquet and inspect schema

Use the script:

```bash
source .venv/bin/activate
python3 scripts/count_parquet_records.py \
  --path "parquet_direct_baseline/parquet/sink/*.parquet" \
  --path "parquet_direct_enriched/parquet/sink/*.parquet"
```

Optional minimum file size filter (default is 1024 bytes):

```bash
python3 scripts/count_parquet_records.py \
  --path "parquet_direct_baseline/parquet/sink/*.parquet" \
  --path "parquet_direct_enriched/parquet/sink/*.parquet" \
  --min-bytes 1024
```

## 8) Verification checklist

1. Kafka input count from step 2 is `N`.
2. Baseline Parquet count is `N`.
3. Enriched Parquet count is `N`.
4. Enriched schema contains `matchedRuleIds`.

If counts do not match, re-run both jobs with fresh consumer group ids and make sure the timeout is long enough for full topic consumption.

## Companion files

- Commands helper: `scripts/run_ingestion_to_data_lake.sh`
- Requirements: `scripts/requirements-parquet-tools.txt`
- Counting script: `scripts/count_parquet_records.py`

## 9) Query performance comparison experiments

You can run example experiments comparing query performance between the Fluxsieve-enriched dataset and the baseline dataset using:

- `scripts/queries-comparison.py`

Example run:

```bash
source .venv/bin/activate
python3 scripts/queries-comparison.py
```

This script is intended as an example workflow for DuckDB-based analysis and can be adapted for your own query set and measurement setup.

Important: the input path patterns in `scripts/queries-comparison.py` must be aligned with the output written by the kstreams app from step 5. If your enriched output directory is `parquet_direct_enriched/parquet/sink/*.parquet`, update the script paths accordingly before running the benchmark.

Potential dependencies (depending on script version and selected output format):

- `duckdb`
- `pandas`
- `pyarrow`

You can install them with:

```bash
source .venv/bin/activate
pip install duckdb pandas pyarrow
```

Note: cold-run cache clearing in the script uses `sudo` (`sync` and `drop_caches`). If you cannot or do not want to use `sudo`, set `CLEAR_CACHE = False` in the script.

In our analysis, we executed these comparisons with DuckDB, collected the results, and stored the outputs under:

- `experimental-results/`

This folder also includes plots generated from the collected measurements.

## License

This component is available under the terms in [../LICENSE](../LICENSE).
Use is limited to internal, non-production, non-commercial purposes.
Redistribution, sublicensing, modification, sale, and use for training or improving AI/ML models are not permitted.
