#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
INPUT_TOPIC="${INPUT_TOPIC:-test}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_LAKE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
STREAMING_DATA_PLANE_DIR="${STREAMING_DATA_PLANE_DIR:-${DATA_LAKE_DIR}/../streaming-data-plane}"

if [[ ! -x "${STREAMING_DATA_PLANE_DIR}/gradlew" ]]; then
  echo "Could not find executable gradlew at: ${STREAMING_DATA_PLANE_DIR}/gradlew"
  echo "Set STREAMING_DATA_PLANE_DIR to your streaming-data-plane path."
  exit 1
fi

BASELINE_OUT="${DATA_LAKE_DIR}/parquet_direct_baseline"
ENRICHED_OUT="${DATA_LAKE_DIR}/parquet_direct_enriched"

echo "Compiling Java sources..."
(cd "${STREAMING_DATA_PLANE_DIR}" && ./gradlew :kstreams:compileJava)

echo "Kafka input count (topic=${INPUT_TOPIC}):"
kcat -b "${BOOTSTRAP}" -t "${INPUT_TOPIC}" -C -o beginning -e -q | wc -l

echo "Running baseline writer..."
(cd "${STREAMING_DATA_PLANE_DIR}" && PARQUET_DIR="${BASELINE_OUT}" timeout 90s ./gradlew :kstreams:run \
  -PmainClass=com.dynatrace.research.matchAndEnrichRecordsWriteParquet.KafkaStreamsBaselineWriteParquetDirect \
  -Dconsumer.group.id=baseline-repro-$(date +%s) \
  -Dkafka.bootstrap.servers="${BOOTSTRAP}" \
  -Dkafka.topic.input="${INPUT_TOPIC}" \
  --console=plain)

echo "Running enriched writer..."
(cd "${STREAMING_DATA_PLANE_DIR}" && PARQUET_DIR="${ENRICHED_OUT}" timeout 90s ./gradlew :kstreams:run \
  -PmainClass=com.dynatrace.research.matchAndEnrichRecordsWriteParquet.KafkaStreamsMatchToParquetDirect \
  -Dconsumer.group.id=enriched-repro-$(date +%s) \
  -Dkafka.bootstrap.servers="${BOOTSTRAP}" \
  -Dkafka.topic.input="${INPUT_TOPIC}" \
  --console=plain)

echo "Done. Parquet outputs are in:"
echo "  ${BASELINE_OUT}/parquet/sink"
echo "  ${ENRICHED_OUT}/parquet/sink"
