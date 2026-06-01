# FluxSieve Replication Package

This repository is the replication package for the paper `FluxSieve: Unifying Streaming and Analytical Data Planes for Scalable Cloud Observability`.

It contains:
- implementation artifacts for the FluxSieve data-plane mechanisms,
- two experimental tracks (RTOLAP with Apache Pinot and data-lake analytics with DuckDB/Parquet),
- raw and processed experimental results, and
- plotting scripts used to generate the figures.

## 1) Package Overview

- `streaming-data-plane/`
  - Java/Kafka Streams implementation of FluxSieve core mechanisms:
    - in-stream multi-pattern matching and enrichment,
    - dynamic pattern-matching engine updates via object storage and notifications,
    - direct Parquet writers used by the data-lake experiments.

- `streaming-data-lake/`
  - Reproduction assets for the DuckDB/Parquet evaluation track.
  - Includes local load generation, benchmark scripts, result folders, and plotting scripts.

- `RTOLAP/`
  - Reproduction assets for the Apache Pinot (RTOLAP) evaluation track.
  - Includes Kubernetes deployment assets, load generation, Pinot query client, schemas/tables, and plotting scripts.

## 2) Paper-to-Repository Map

Use this section as an index from paper concepts/results to concrete artifacts.

| Paper topic | Repository artifacts |
|---|---|
| FluxSieve architecture and core stream-processing approach | `streaming-data-plane/kstreams/src/main/java/com/dynatrace/research/matchAndEnrichRecords/KStreamsMatchingHyperscanEnrich5Dbs.java` |
| Dynamic adaptation and matcher hot-swap protocol | `streaming-data-plane/kstreams/src/main/java/com/dynatrace/research/dynamicPatternMatchingEngine/PatternMatchingEngineUpdater.java`, `streaming-data-plane/kstreams/src/main/java/com/dynatrace/research/dynamicPatternMatchingEngine/KStreamsDynamicPatternFiltering.java` |
| Kafka Streams integration details and tests | `streaming-data-plane/README.md`, `streaming-data-plane/kstreams/src/test/` |
| Data-lake benchmark implementation (baseline vs FluxSieve-style enriched data) | `streaming-data-plane/kstreams/src/main/java/com/dynatrace/research/matchAndEnrichRecordsWriteParquet/KafkaStreamsBaselineWriteParquetDirect.java`, `streaming-data-plane/kstreams/src/main/java/com/dynatrace/research/matchAndEnrichRecordsWriteParquet/KafkaStreamsMatchToParquetDirect.java` |
| Data-lake evaluation methodology (cold/hot, count vs return rows, file-layout/parallelism sweep) | `streaming-data-lake/scripts/queries-comparison.py`, `streaming-data-lake/README.md` |
| Data-lake result artifacts (raw benchmark metrics) | `streaming-data-lake/experimental-results/final-10M-records_ultra-high-selectivity_parquet_zstd-10reps_parallelism1_~1k-parquet-files_~10k-records-each/results/10M-unified_benchmark_parquet_zstd/`, `streaming-data-lake/experimental-results/final-10M-records_ultra-high-selectivity_parquet_zstd-10reps_parallelism1_~5k-parquet-files_~2k-records-each/results/10M-unified_benchmark_parquet_zstd/`, `streaming-data-lake/experimental-results/final-10M-records_ultra-high-selectivity_parquet_zstd-10reps_parallelism4_~1k-parquet-files_~10k-records-each/results/10M-unified_benchmark_parquet_zstd/`, `streaming-data-lake/experimental-results/final-10M-records_ultra-high-selectivity_parquet_zstd-10reps_parallelism4_~5k-parquet-files_~2k-records-each/results/10M-unified_benchmark_parquet_zstd/` |
| Data-lake figure generation | `streaming-data-lake/experimental-results/plot-general-scaling-speedup.py` |
| RTOLAP setup and distributed-system assumptions | `RTOLAP/README.md`, `RTOLAP/tables/` |
| RTOLAP ingestion/query workload execution | `RTOLAP/load-generator/`, `RTOLAP/pinot-client/` |
| RTOLAP result artifacts (high-selectivity and ultra-high-selectivity runs) | `RTOLAP/experimental-results/High-Selectivity/`, `RTOLAP/experimental-results/Ultra-High-Selectivity/` |
| RTOLAP figure generation | `RTOLAP/experimental-results/High-Selectivity/plot-general-scaling-speedup.py`, `RTOLAP/experimental-results/Ultra-High-Selectivity/plot-general-scaling-speedup_high-selectivity.py` |

## 3) Fast Reproduction Paths

Choose one of the tracks below depending on what you want to replicate.

### A. DuckDB/Data-Lake Track (local-friendly)

Primary docs:
- `streaming-data-lake/README.md`
- Concurrency experiments: `streaming-data-lake/experimental-results/concurrent-queries/` (see `streaming-data-lake/experimental-results/concurrent-queries/README.md`)

Typical flow:
1. Build the streaming module once:
   ```bash
   (cd streaming-data-plane && ./gradlew :kstreams:compileJava)
   ```
2. Generate Kafka input records:
   ```bash
   (cd streaming-data-lake/load-generator && podman-compose -f docker-compose.yaml up --build)
   ```
3. Run baseline and enriched Parquet writers (from `streaming-data-lake/README.md`).
4. Run benchmark and analysis scripts:
   ```bash
   cd streaming-data-lake
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r scripts/requirements-parquet-tools.txt
   python3 scripts/queries-comparison.py
   ```
5. Inspect generated and archived metrics in `streaming-data-lake/experimental-results/`.

### B. RTOLAP/Pinot Track (distributed/Kubernetes)

Primary docs:
- `RTOLAP/README.md`
- `RTOLAP/load-generator/README.md`
- `RTOLAP/pinot-client/README.md`

Typical flow:
1. Deploy Kafka + Pinot cluster (as described in `RTOLAP/README.md`).
2. Produce workload data with `RTOLAP/load-generator/`.
3. Configure Pinot schema/table definitions from `RTOLAP/tables/`.
4. Execute query scenarios with `RTOLAP/pinot-client/`.
5. Analyze archived CSVs and regenerate plots from:
   - `RTOLAP/experimental-results/High-Selectivity/`
   - `RTOLAP/experimental-results/Ultra-High-Selectivity/`

## 4) Where to Find Implementations Quickly

- Core FluxSieve stream matching + enrichment:
  - `streaming-data-plane/kstreams/src/main/java/com/dynatrace/research/matchAndEnrichRecords/KStreamsMatchingHyperscanEnrich5Dbs.java`
- Dynamic matcher update path:
  - `streaming-data-plane/kstreams/src/main/java/com/dynatrace/research/dynamicPatternMatchingEngine/PatternMatchingEngineUpdater.java`
  - `streaming-data-plane/kstreams/src/main/java/com/dynatrace/research/dynamicPatternMatchingEngine/KStreamsDynamicPatternFiltering.java`
- Parquet writer variants used by data-lake experiments:
  - `streaming-data-plane/kstreams/src/main/java/com/dynatrace/research/matchAndEnrichRecordsWriteParquet/KafkaStreamsBaselineWriteParquetDirect.java`
  - `streaming-data-plane/kstreams/src/main/java/com/dynatrace/research/matchAndEnrichRecordsWriteParquet/KafkaStreamsMatchToParquetDirect.java`

## 5) Where to Find Experimental Results Quickly

- DuckDB/data-lake track:
  - `streaming-data-lake/experimental-results/`
  - Key outputs per scenario:
    - `cold_metrics_unified.csv`
    - `hot_metrics_unified.csv`
    - `10M-unified_benchmark_parquet_zstd.log`

- RTOLAP/Pinot track:
  - `RTOLAP/experimental-results/High-Selectivity/`
  - `RTOLAP/experimental-results/Ultra-High-Selectivity/`
  - Includes per-dataset-size CSVs (5M/10M/20M/40M), with multiple repetitions and query variants.

## 6) Suggested Reading Order

For a quick but complete understanding of the replication package:

1. Read this file (`README.md`).
2. Read `FluxSieve` paper for the full methodology and findings.
3. Follow one track end-to-end:
   - local: `streaming-data-lake/README.md`
   - distributed: `RTOLAP/README.md`
4. Use plotting scripts in each track to regenerate figures from raw result CSVs.

## 7) Notes on Reproducibility

- The RTOLAP track depends on distributed infrastructure (Kubernetes, Kafka, Pinot, object storage).
- The data-lake track is easier to run locally and includes reproducible benchmark scripts and archived outputs.
- Existing result folders in this repository can be used directly to reproduce plots even if you do not rerun full ingestion workloads.

## License

This repository is available under the terms in [LICENSE](LICENSE).
Use is limited to internal, non-production, non-commercial purposes.
Redistribution, sublicensing, modification, sale, and use for training or improving AI/ML models are not permitted.
