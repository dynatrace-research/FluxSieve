/*
 * Copyright (c) 2026 Dynatrace LLC. All rights reserved.
 * Licensed under the terms in the repository LICENSE file.
 */

package com.dynatrace.research.dynamicPatternMatchingEngine;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class KStreamsDynamicPatternFilteringIntegrationTest {

    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.6.1");
    private static final DockerImageName LOCALSTACK_IMAGE = DockerImageName.parse("localstack/localstack:3.8.1");
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private static KafkaContainer KAFKA;
    private static LocalStackContainer LOCALSTACK;

    private KStreamsDynamicPatternFiltering app;
    private KafkaProducer<String, byte[]> inputProducer;

    private String topicInput;
    private String topicOutput;
    private String topicMatcher;
    private String topicMatcherOutput;
    private String topicPerformance;
    private String bucketName;
    private String dbObjectKey;

    @BeforeAll
    static void startContainers() {
        Assumptions.assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is required for integration test");

        KAFKA = new KafkaContainer(KAFKA_IMAGE);
        LOCALSTACK = new LocalStackContainer(LOCALSTACK_IMAGE).withServices(LocalStackContainer.Service.S3);

        KAFKA.start();
        LOCALSTACK.start();
    }

    @AfterAll
    static void stopContainers() {
        if (LOCALSTACK != null) {
            LOCALSTACK.stop();
        }
        if (KAFKA != null) {
            KAFKA.stop();
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        topicInput = "test-input-" + suffix;
        topicOutput = "test-output-" + suffix;
        topicMatcher = "test-matcher-" + suffix;
        topicMatcherOutput = "test-matcher-output-" + suffix;
        topicPerformance = "test-performance-" + suffix;
        bucketName = "matcher-bucket-" + suffix;
        dbObjectKey = "matcher-db-" + suffix;

        createTopics(topicInput, topicOutput, topicMatcher, topicMatcherOutput, topicPerformance);
        createBucket(bucketName);
        configureSystemProperties();

        inputProducer = createInputProducer();
        app = new KStreamsDynamicPatternFiltering();
        app.start();
    }

    @AfterEach
    void tearDown() {
        if (app != null) {
            app.stop();
        }
        if (inputProducer != null) {
            inputProducer.close();
        }
        clearSystemProperties();
    }

    @Test
    void shouldApplyUpdatedMatcherDbFromS3() throws Exception {
        String baselineInput = "The number is 42";

        assertThat(sendAndWaitForOutputContaining(
                baselineInput,
                "matched pattern: The number is ([0-9]+)",
                Duration.ofSeconds(30)))
                .isTrue();

        int randomPatternLength = 7;
        System.setProperty("generate.string.size", Integer.toString(randomPatternLength));

        long seed = 123456L;
        setUpdaterRandomSeed(seed);
        String expectedNewPattern = deterministicRandomString(seed, randomPatternLength);

        String updaterTag = PatternMatchingEngineUpdater.runUpdate(
                1,
                KAFKA.getBootstrapServers(),
                topicMatcher,
                topicPerformance,
                "1",
                bucketName,
                dbObjectKey);

        assertThat(updaterTag).isNotBlank();
        assertThat(PatternMatchingEngineUpdater.assertThatMatcherWasUpdatedOnWorkers(
                KAFKA.getBootstrapServers(),
                topicMatcherOutput,
                updaterTag,
                1)).isTrue();

        assertThat(sendAndWaitForOutputContaining(
                baselineInput,
                "matched pattern: The number is ([0-9]+)",
                Duration.ofSeconds(6)))
                .isFalse();

        assertThat(sendAndWaitForOutputContaining(
                expectedNewPattern,
                "matched pattern: " + expectedNewPattern,
                Duration.ofSeconds(30)))
                .isTrue();
    }

    private KafkaProducer<String, byte[]> createInputProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private void createTopics(String... topics) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(props)) {
            List<NewTopic> newTopics = java.util.Arrays.stream(topics)
                    .map(topic -> new NewTopic(topic, 1, (short) 1))
                    .toList();
            adminClient.createTopics(newTopics).all().get(30, TimeUnit.SECONDS);
        }
    }

    private void createBucket(String bucket) {
        try (S3Client s3 = S3Client.builder()
                .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
                .region(Region.of(LOCALSTACK.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .forcePathStyle(true)
                .build()) {
            s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        }
    }

    private void configureSystemProperties() {
        System.setProperty("kafka.bootstrap.servers", KAFKA.getBootstrapServers());
        System.setProperty("kafka.topic.input", topicInput);
        System.setProperty("kafka.topic.output", topicOutput);
        System.setProperty("kafka.topic.matcher", topicMatcher);
        System.setProperty("kafka.topic.matcheroutput", topicMatcherOutput);
        System.setProperty("kafka.topic.performance", topicPerformance);
        System.setProperty("s3.bucket.name", bucketName);
        System.setProperty("s3.key.name", dbObjectKey);
        System.setProperty("config.parallelism", "1");
        System.setProperty("config.commit.interval.ms", "200");
        System.setProperty("s3.endpoint.override", LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString());
        System.setProperty("s3.region", LOCALSTACK.getRegion());
        System.setProperty("s3.access.key", LOCALSTACK.getAccessKey());
        System.setProperty("s3.secret.key", LOCALSTACK.getSecretKey());
    }

    private void clearSystemProperties() {
        Set<String> keys = Set.of(
                "kafka.bootstrap.servers",
                "kafka.topic.input",
                "kafka.topic.output",
                "kafka.topic.matcher",
                "kafka.topic.matcheroutput",
                "kafka.topic.performance",
                "s3.bucket.name",
                "s3.key.name",
                "config.parallelism",
                "config.commit.interval.ms",
                "generate.string.size",
                "s3.endpoint.override",
                "s3.region",
                "s3.access.key",
                "s3.secret.key"
        );
        keys.forEach(System::clearProperty);
    }

    private boolean sendAndWaitForOutputContaining(String inputValue, String expectedValueFragment, Duration timeout) throws Exception {
        String groupId = "assert-group-" + UUID.randomUUID();

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(topicOutput));
            consumer.poll(Duration.ofMillis(250));

            inputProducer.send(new ProducerRecord<>(topicInput, "test-key", inputValue.getBytes(StandardCharsets.UTF_8))).get(10, TimeUnit.SECONDS);

            long end = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < end) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(250));
                for (var record : records) {
                    String value = new String(record.value(), StandardCharsets.UTF_8);
                    if (value.contains(expectedValueFragment)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private static void setUpdaterRandomSeed(long seed) throws Exception {
        Field randomField = PatternMatchingEngineUpdater.class.getDeclaredField("RANDOM");
        randomField.setAccessible(true);
        Random random = (Random) randomField.get(null);
        random.setSeed(seed);
    }

    private static String deterministicRandomString(long seed, int length) {
        Random random = new Random(seed);
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
        }
        return sb.toString();
    }
}
