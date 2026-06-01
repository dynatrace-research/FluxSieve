#!/bin/bash
#this is just a helper script to set up the local kafka topics for the kstreams example. It deletes any existing topics and creates new ones with the specified partitions and replication factor. A local running kafka instance is required for this script to work. You can run a local kafka instance using docker or by downloading and running kafka locally.

/usr/local/kafka/bin/kafka-topics.sh --delete --topic input --bootstrap-server localhost:9092

/usr/local/kafka/bin/kafka-topics.sh --delete --topic matcher --bootstrap-server localhost:9092

/usr/local/kafka/bin/kafka-topics.sh --delete --topic matcher.output --bootstrap-server localhost:9092

/usr/local/kafka/bin/kafka-topics.sh --delete --topic output --bootstrap-server localhost:9092

/usr/local/kafka/bin/kafka-topics.sh --delete --topic performance --bootstrap-server localhost:9092

/usr/local/kafka/bin/kafka-topics.sh --delete --topic test --bootstrap-server localhost:9092

/usr/local/kafka/bin/kafka-topics.sh --create --topic input --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

/usr/local/kafka/bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1

/usr/local/kafka/bin/kafka-topics.sh --create --topic matcher --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

/usr/local/kafka/bin/kafka-topics.sh --create --topic matcher.output --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

/usr/local/kafka/bin/kafka-topics.sh --create --topic output --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1

/usr/local/kafka/bin/kafka-topics.sh --create --topic performance --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

