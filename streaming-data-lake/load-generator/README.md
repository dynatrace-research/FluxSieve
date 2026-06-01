# Load Generator (Podman + Local Kafka)

This folder runs a Kafka message generator with Podman Compose.

## Why this setup

- Uses a non-Docker-Hub Python base image in `Dockerfile` to avoid Docker Hub rate limits.
- Uses host networking in compose so the container can reach Kafka running on the same machine via `localhost:9092`.
- Uses `pull_policy: never` so Podman does not try to pull a local image from `https://localhost/v2/`.

## Prerequisites

- Podman installed (`podman --version`)
- podman-compose installed (`podman-compose --version`)
- Kafka running locally on the host at `localhost:9092`

## Run

You need to run the load generator to create records in the Kafka topic. Kafka itself only provides the broker and topic storage; it does not create test messages on its own. If the generator is not running, the topic may exist but remain empty, which means there is no data for consumers, stream jobs, or reproduction steps to read.

In practice, this folder is the data source for local reproduction. Starting it produces the events that are written to the configured topic (`test` by default), so any downstream component can observe, consume, and process real records during your test run.

From this directory:

```bash
podman-compose -f docker-compose.yaml up --build
```

Detached mode:

```bash
podman-compose -f docker-compose.yaml up --build -d
```

## Stop and cleanup

```bash
podman-compose -f docker-compose.yaml down
```

Remove local image too:

```bash
podman rmi load-generator_message-generator:latest
```

## Validate Kafka connectivity from host

Quick check that something listens on 9092:

```bash
ss -lntp | grep 9092
```

## Reproduce and understand prior failures

1. Docker-compose provider issue
- Symptom: `podman compose` delegated to `/usr/bin/docker-compose` and failed looking for Docker socket.
- Cause: wrong compose provider (Docker Compose expected Docker daemon).
- Fix: use `podman-compose` directly.

2. Docker Hub pull rate-limit
- Symptom: `toomanyrequests` while pulling `python:3.11-slim`.
- Cause: unauthenticated Docker Hub pull limits.
- Fix applied: switched Dockerfile base image to `quay.io/fedora/python-311:latest`.

3. Localhost registry pull error
- Symptom: Podman tried to pull `localhost/load-generator_message-generator:latest`.
- Cause: `pull_policy: always` with a local image name.
- Fix applied: set `pull_policy: never`.

## Notes

- Current compose uses `deploy.replicas: 10`. If your machine is small, lower it to reduce load.
- If Kafka is not actually on localhost, update `KAFKA_BROKER` in compose.

## License

This component is available under the terms in [../../LICENSE](../../LICENSE).
Use is limited to internal, non-production, non-commercial purposes.
Redistribution, sublicensing, modification, sale, and use for training or improving AI/ML models are not permitted.
