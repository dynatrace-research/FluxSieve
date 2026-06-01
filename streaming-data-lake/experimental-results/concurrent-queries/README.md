# Concurrent Experiments: How to Run

This folder contains two concurrent benchmark experiments:

- `concurrency-benchmark.py`
  - Fixed query mix (Q1..Q4 and count variants), round-robin by client ID.
- `concurrency-benchmark-unique-terms.py`
  - Unique per-client terms for baseline (from TXT files) and unique random rule IDs for FluxSieve.

Both scripts support:

- `--stop-mode soft|strict`
  - `soft`: stop launching new queries at deadline (in-flight query can finish).
  - `strict`: at deadline, signal stop and interrupt active DuckDB queries.
- `--output-dir`
  - Base output folder. A timestamped run folder is always created under it.

`concurrency-benchmark.py` also supports adaptive timing for fairer query-mix coverage:

- `--ensure-all-queries-once` (default enabled)
  - After the minimum run time, continue until all 8 query IDs have at least one successful completion.
- `--max-extension-seconds`
  - Safety cap for extra time beyond `--duration-seconds` (default 300s).

## 1) Run from this directory

```bash
cd streaming-data-lake/experimental-results/concurrent-queries
```

## 2) Experiment A: fixed query mix

### Recommended strict run

```bash
python3 concurrency-benchmark.py \
  --stop-mode strict \
  --scenario 10M-concurrency-fixed \
  --output-dir ./concurrency-results
```

### Custom run settings (Used in the paper)

```bash
python3 concurrency-benchmark.py \
  --stop-mode strict \
  --concurrency-levels 8,16,32 \
  --repetitions 5 \
  --duration-seconds 120 \
  --ensure-all-queries-once \
  --max-extension-seconds 300 \
  --warmup-seconds 30 \
  --idle-between-runs-seconds 30 \
  --duckdb-threads 4 \
  --memory-limit 16GB \
  --output-dir ./concurrency-results \
  --scenario 10M-concurrency-fixed
```

## 3) Experiment B: unique per-client terms

### Recommended strict run

```bash
python3 concurrency-benchmark-unique-terms.py \
  --stop-mode strict \
  --scenario 10M-concurrency-unique-terms \
  --output-dir ./concurrency-results
```

### Optional custom run settings

```bash
python3 concurrency-benchmark-unique-terms.py \
  --stop-mode strict \
  --concurrency-levels 8,16,32 \
  --repetitions 5 \
  --duration-seconds 120 \
  --ensure-all-queries-once \
  --max-extension-seconds 300 \
  --warmup-seconds 30 \
  --idle-between-runs-seconds 60 \
  --duckdb-threads 4 \
  --memory-limit 16GB \
  --baseline-terms-file1 streaming-data-plane/kstreams/src/test/resources/random_strings_custom-twofields1.txt \
  --baseline-terms-file2 streaming-data-plane/kstreams/src/test/resources/random_strings_custom-twofields2.txt \
  --baseline-max-terms-file1 800 \
  --baseline-max-terms-file2 200 \
  --random-seed 42 \
  --output-dir ./concurrency-results \
  --scenario 10M-concurrency-unique-terms
```
### Custom run settings (Used in the paper)

```bash
python3 concurrency-benchmark-unique-terms.py \
  --stop-mode strict \
  --concurrency-levels 8,16,32 \
  --repetitions 5 \
  --duration-seconds 120 \
  --ensure-all-queries-once \
  --max-extension-seconds 300 \
  --warmup-seconds 30 \
  --idle-between-runs-seconds 30 \
  --duckdb-threads 4 \
  --memory-limit 16GB \
  --baseline-terms-file1 random_strings_custom-twofields1.txt \
  --baseline-terms-file2 random_strings_custom-twofields2.txt \
  --baseline-max-terms-file1 800 \
  --baseline-max-terms-file2 200 \
  --output-dir ./concurrency-results \
  --scenario 10M-concurrency-unique-terms
```

## 4) Where results are saved

For each run, output is created at:

- `./concurrency-results/<scenario>_YYYYMMDD_HHMMSS/`

Files produced:

- `raw_latencies.csv`
- `cpu_per_run.csv`
- `summary_overall.csv`
- `summary_by_query.csv`

## 5) Quick checks

Show CLI options:

```bash
python3 concurrency-benchmark.py --help
python3 concurrency-benchmark-unique-terms.py --help
```

Syntax check:

```bash
python3 -m py_compile concurrency-benchmark.py
python3 -m py_compile concurrency-benchmark-unique-terms.py
```
