# RTOLAP Deployment Overview

Before running workloads in this repository, you need the following distributed services deployed and reachable from your client jobs:

1. A distributed Apache Pinot deployment (controller, broker, and server nodes).
2. Apache Kafka brokers for real-time ingestion into Pinot.

## Required Infrastructure

This setup was deployed on Kubernetes with storage and compute sized for distributed query execution and ingestion throughput.

### Apache Pinot Cluster

- Storage: Amazon EBS gp3 volumes formatted with ext4.
- Topology: 1 Pinot controller, 1 Pinot broker, and 4 Pinot server instances.
- Compute placement:
	- Controller and broker pods share one m6i.xlarge instance.
	- Each Pinot server pod runs on a dedicated m6i.xlarge instance.

This controller-broker-server architecture provides horizontal server-side query parallelism while keeping cluster management simple.

### Kafka Ingestion Layer

- Deployment model: 2 Apache Kafka brokers on Kubernetes using Strimzi.
- Compute placement: each broker runs on m6i.2xlarge.
- Topic configuration: ingestion topics use 40 partitions.

Using 40 partitions enables parallel consumption by Pinot and supports higher ingest rates for real-time analytical workloads.

## End-to-End Experiment Workflow

This repository evaluates different approaches for real-time query performance on large streaming log datasets.

1. Generate records into Kafka using the tools in the `load-generator` folder.
2. Process/enrich records with the streaming application at `../streaming-data-plane/kstreams/src/main/java/com/dynatrace/research/matchAndEnrichRecords/KStreamsMatchingHyperscanEnrich5Dbs.java`.
3. Ingest data into Pinot and run query experiments.

### Why the Load Generator Matters

The `load-generator` component is the input driver for all scenarios. It produces the Kafka event stream used by both:

- The baseline approach: Pinot ingests the generated records directly from Kafka.
- The FluxSieve-style approach: records are first matched/enriched by the streaming app, then queried after ingestion.

Without a controlled generator, the comparison between ingestion/query strategies would not be reproducible.

### Streaming App Role (Match + Enrich)

The referenced Java streaming app (`KStreamsMatchingHyperscanEnrich5Dbs.java`) performs online matching and enrichment, then writes enriched events back to a Kafka topic. This path represents the FluxSieve approach used in the comparison:

- Match records against predefined rules/patterns.
- Enrich matched records with derived fields.
- Publish enriched output to Kafka for downstream Pinot ingestion and querying.

### Pinot Table and Schema Setup

After generating and optionally enriching records, create the Pinot tables and schemas. Example configurations are provided in the `tables` folder.

These examples show how to define:

- Real-time table ingestion settings.
- Schema fields for raw and enriched records.
- Optional text-indexed variants used in experiments.

### Query Execution and Result Collection

The `pinot-client` folder contains the query runner used for experiments. It demonstrates how we:

- Query the Pinot broker directly inside the Kubernetes deployment.
- Execute equivalent query sets for different approaches.
- Run cold queries only after redeploying the Kubernetes infrastructure to clear caches.
- Save query timing/response data to CSV.
- Upload experiment outputs to an S3 bucket.

### Experimental Results and Comparison Focus

The `experimental-results` folder contains the collected outputs across scenarios.

Our primary comparison focuses on:

- Pinot text-indexed querying.
- The FluxSieve approach (match + enrich first, then query).

We also ran default Pinot full-scan queries, but those runs showed unstable behavior with frequent timeouts due to lower performance at the tested scales.

## Official Documentation

- Apache Pinot documentation: <https://pinot.apache.org/>
- Pinot Kubernetes quickstart and operations guides: <https://docs.pinot.apache.org/start-here/install/kubernetes>
- Apache Kafka documentation: <https://kafka.apache.org/documentation/>
- Strimzi documentation: <https://strimzi.io/documentation/>

## License

This component is available under the terms in [../LICENSE](../LICENSE).
Use is limited to internal, non-production, non-commercial purposes.
Redistribution, sublicensing, modification, sale, and use for training or improving AI/ML models are not permitted.

