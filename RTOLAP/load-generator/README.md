# Load Generator

This folder contains a Kafka load generator that produces synthetic log-like records for benchmark/testing workloads.

It is designed to:
- generate high-throughput records with multiple string fields,
- control how often specific terms appear,
- publish directly to a Kafka topic,
- run either as a plain Python script or as a containerized Kubernetes Job.

## What It Generates

The script `generator5.py` sends JSON records with this shape:

```json
{
  "timestamp": 1716630000000,
  "content1": "...",
  "status": "INFO",
  "eventType": "LOG",
  "content2": "...",
  "content3": "...",
  "content4": "...",
  "content5": "..."
}
```

String payload fields are:
- `content1`
- `content2`
- `content3`
- `content4`
- `content5`

Each field is generated as random lowercase words, and selected keywords are injected at configurable frequencies.

## Keyword / Term Occurrence Control

The generator supports deterministic term insertion:
- a frequency `N` means the term is injected every `N`th message,
- frequencies are independently configurable for different fields,
- this allows creating low/high-selectivity terms for search/index experiments.

Examples:
- `SPECIAL_FREQUENCY2=50000` injects `MATCHEABLE2` in `content2` every 50,000 messages.
- `SPECIAL_FREQUENCY3=999` injects `MATCHEABLE3` in `content3` every 999 messages.

## Configuration (Environment Variables)

Core runtime:
- `KAFKA_TOPIC` (default: `test`)
- `KAFKA_BROKER` (default: `localhost:9092`)
- `MESSAGES_PER_SECOND` (default: `10000`)
- `DURATION_SECONDS` (default: `10`)
- `STRINGSIZE` words per generated string (default: `60`)

Term controls:
- `SPECIAL_FREQUENCY1` + `MATCHEABLE1`
- `SPECIAL_FREQUENCY2` + `MATCHEABLE2`
- `SPECIAL_FREQUENCY3` + `MATCHEABLE3`
- `SPECIAL_FREQUENCY4` (uses same term as `MATCHEABLE2`)
- `SPECIAL_FREQUENCY5` + `MATCHEABLE5`
- `SPECIAL_FREQUENCY6` (uses same term as `MATCHEABLE1`)

Notes:
- `content4` uses the same keyword as `MATCHEABLE2`.
- `content5` fallback injection uses the same keyword as `MATCHEABLE1`.

## Run as Python

### 1) Install dependencies

```bash
pip install -r requirements.txt
```

### 2) Run

```bash
export KAFKA_BROKER=localhost:9092
export KAFKA_TOPIC=test
export MESSAGES_PER_SECOND=10000
export DURATION_SECONDS=60
python generator5.py
```

## Run as Docker Image

Build image:

```bash
docker build -t your-registry/load-generator:latest .
```

Run container:

```bash
docker run --rm \
  -e KAFKA_BROKER=broker:9092 \
  -e KAFKA_TOPIC=test \
  -e MESSAGES_PER_SECOND=10000 \
  -e DURATION_SECONDS=60 \
  your-registry/load-generator:latest
```

## Run on Kubernetes (Current Setup)

In this repository setup, the generator is typically deployed as a Kubernetes Job using `deployment.yaml`.

1. Build and push image:

```bash
docker build -t your-registry/load-generator:latest .
docker push your-registry/load-generator:latest
```

2. Update `deployment.yaml`:
- set `image: YOUR_IMAGE` to your pushed image,
- set Kafka/topic env vars to your cluster values.

3. Apply job:

```bash
kubectl apply -f deployment.yaml
```

4. Check execution:

```bash
kubectl get jobs
kubectl logs job/load-generator
```

## Files

- `generator5.py`: message generation + Kafka publishing logic
- `requirements.txt`: Python dependency (`kafka_python`)
- `Dockerfile`: container image definition
- `deployment.yaml`: Kubernetes Job template

## License

This component is available under the terms in [../../LICENSE](../../LICENSE).
Use is limited to internal, non-production, non-commercial purposes.
Redistribution, sublicensing, modification, sale, and use for training or improving AI/ML models are not permitted.
