# Streaming Data Plane

## Prerequisites

- Java 17+ (or the Java version required by this project)
- Gradle (only needed if the Gradle wrapper JAR is missing)

## Build

From the project root:

```bash
./gradlew build
```

## Running

The project contains multiple entry points. By default, `./gradlew :kstreams:run` executes `KafkaStreamsMatchToParquetDirect`. To run a different main class, pass it as a property:

```bash
./gradlew :kstreams:run -PmainClass=com.dynatrace.research.dynamicPatternMatchingEngine.PatternMatchingEngineUpdater
```

Without `-PmainClass`, the default main class is used:

```bash
./gradlew :kstreams:run
```

## Integration Test: Dynamic Matcher DB Reload

The integration test validates the full update flow for dynamic matcher conditions:

1. Start the KStreams filtering app.
2. Run the matcher updater to build and upload a new Hyperscan DB to S3.
3. Notify workers through the matcher Kafka topic.
4. Verify the worker reloads the DB from S3.
5. Assert the old matcher is no longer applied and the new matcher is active.

Test class:

- `kstreams/src/test/java/com/dynatrace/research/dynamicPatternMatchingEngine/KStreamsDynamicPatternFilteringIntegrationTest.java`

### Prerequisites

- Docker running locally (required by Testcontainers)
- Access to pull container images used by the test (Kafka + LocalStack)

### Run only this integration test

From the project root:

```bash
./gradlew :kstreams:test --tests com.dynatrace.research.dynamicPatternMatchingEngine.KStreamsDynamicPatternFilteringIntegrationTest --rerun-tasks
```

### Run all tests in the kstreams module

```bash
./gradlew :kstreams:test
```

### Behavior when Docker is not available

This integration test is guarded by a Docker availability assumption and will be skipped automatically when Docker is not configured or not running.

## Integration Test Without Containers (External Kafka/S3)

Use this when you want to run the same matcher DB reload flow without Docker/Testcontainers.

Test class:

- `kstreams/src/test/java/com/dynatrace/research/dynamicPatternMatchingEngine/KStreamsDynamicPatternFilteringExternalIntegrationTest.java`

What this external integration test verifies:

1. Updater builds a new matcher DB and uploads it to S3.
2. KStreams worker fetches and applies the new DB.
3. Updater deletes the uploaded DB object from S3 at the end of the test flow.
4. KStreams worker removes its local cached matcher DB file on shutdown.

Required system properties:

- `it.kafka.bootstrap.servers`
- `it.s3.bucket.name`
- `it.s3.region` (required for real AWS usage)

Important:

- `it.s3.bucket.name` must point to a bucket you own and can write to in your target environment.
- Use a dedicated bucket for this test flow (or a dedicated prefix policy) to avoid collisions with other runs.
- The external test expects the bucket to already exist and be accessible. It does not create buckets for you.

Optional system properties:

- `it.s3.key.name` (defaults to a random test object key)
- `it.s3.region` (default: `us-east-1`)
- `it.s3.endpoint.override` (for S3-compatible endpoints such as MinIO or LocalStack)
- `it.s3.access.key`
- `it.s3.secret.key`
- `it.kafka.topic.prefix` (default: `external-it`)

Run example (real AWS S3 bucket):

```bash
./gradlew :kstreams:test \
	--tests com.dynatrace.research.dynamicPatternMatchingEngine.KStreamsDynamicPatternFilteringExternalIntegrationTest \
	--rerun-tasks \
	--info \
	-Dit.kafka.bootstrap.servers=localhost:9092 \
	-Dit.s3.bucket.name=your-real-bucket-name \
	-Dit.s3.region=us-east-1
```

The test uses AWS SDK default credentials provider chain when `it.s3.endpoint.override` is not set (for example, environment variables, `~/.aws/credentials`, or IAM role credentials).

Run example (Kafka + S3-compatible endpoint):

```bash
./gradlew :kstreams:test \
	--tests com.dynatrace.research.dynamicPatternMatchingEngine.KStreamsDynamicPatternFilteringExternalIntegrationTest \
	--rerun-tasks \
	-Dit.kafka.bootstrap.servers=localhost:9092 \
	-Dit.s3.bucket.name=rta-matcher-updater \
	-Dit.s3.region=us-east-1 \
	-Dit.s3.endpoint.override=http://localhost:4566 \
	-Dit.s3.access.key=test \
	-Dit.s3.secret.key=test
```

If required properties are missing, this test is skipped automatically.

For example, if you run against your own AWS account, replace `rta-matcher-updater` with your own bucket name in the `-Dit.s3.bucket.name=...` property.

How to confirm it ran correctly:

- In the test output, check for worker reload log: `Finished updating new DB matcher file`.
- In the test output, check for worker confirmation log: `Matcher update confirmation from worker`.
- Test result should be `PASSED` (not `SKIPPED`).

## License

This component is available under the terms in [../LICENSE](../LICENSE).
Use is limited to internal, non-production, non-commercial purposes.
Redistribution, sublicensing, modification, sale, and use for training or improving AI/ML models are not permitted.