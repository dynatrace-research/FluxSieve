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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.lang.reflect.Field;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class KStreamsDynamicPatternFilteringExternalIntegrationTest {

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private KStreamsDynamicPatternFiltering app;
    private KafkaProducer<String, byte[]> inputProducer;

    private String kafkaBootstrapServers;
    private String topicInput;
    private String topicOutput;
    private String topicMatcher;
    private String topicMatcherOutput;
    private String topicPerformance;
    private String bucketName;
    private String dbObjectKey;

    @BeforeEach
    void setUp() throws Exception {
        kafkaBootstrapServers = System.getProperty("it.kafka.bootstrap.servers", "").trim();
        bucketName = System.getProperty("it.s3.bucket.name", "").trim();

        Assumptions.assumeTrue(!kafkaBootstrapServers.isEmpty(),
                "Missing required system property: it.kafka.bootstrap.servers");
        Assumptions.assumeTrue(!bucketName.isEmpty(),
                "Missing required system property: it.s3.bucket.name");

        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String topicPrefix = System.getProperty("it.kafka.topic.prefix", "external-it");
        topicInput = topicPrefix + "-input-" + suffix;
        topicOutput = topicPrefix + "-output-" + suffix;
        topicMatcher = topicPrefix + "-matcher-" + suffix;
        topicMatcherOutput = topicPrefix + "-matcher-output-" + suffix;
        topicPerformance = topicPrefix + "-performance-" + suffix;
        dbObjectKey = System.getProperty("it.s3.key.name", "matcher-db-" + suffix);

        createTopics(topicInput, topicOutput, topicMatcher, topicMatcherOutput, topicPerformance);
        assertBucketIsAccessible(bucketName);
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
    void shouldApplyUpdatedMatcherDbFromExternalS3() throws Exception {
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
                kafkaBootstrapServers,
                topicMatcher,
                topicPerformance,
                "1",
                bucketName,
                dbObjectKey);

        assertThat(updaterTag).isNotBlank();
        assertThat(s3ObjectExists(bucketName, dbObjectKey)).isTrue();

        assertThat(PatternMatchingEngineUpdater.assertThatMatcherWasUpdatedOnWorkers(
                kafkaBootstrapServers,
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

        PatternMatchingEngineUpdater.deleteMatcherFromS3(bucketName, dbObjectKey);
        assertThat(s3ObjectExists(bucketName, dbObjectKey)).isFalse();

        Path workerDbPath = Paths.get("").toAbsolutePath().resolve(Paths.get(dbObjectKey).getFileName());
        app.stop();
        app = null;
        assertThat(Files.exists(workerDbPath)).isFalse();
    }

    private KafkaProducer<String, byte[]> createInputProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private void createTopics(String... topics) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);

        try (AdminClient adminClient = AdminClient.create(props)) {
            List<NewTopic> newTopics = java.util.Arrays.stream(topics)
                    .map(topic -> new NewTopic(topic, 1, (short) 1))
                    .toList();
            adminClient.createTopics(newTopics).all().get(30, TimeUnit.SECONDS);
        }
    }

    private void assertBucketIsAccessible(String bucket) {
        try (S3Client s3 = buildExternalS3Client()) {
            try {
                s3.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
            } catch (S3Exception ex) {
                throw new IllegalStateException(
                        "Configured S3 bucket is not accessible. Set -Dit.s3.bucket.name to an existing bucket you can read/write.",
                        ex);
            }
        }
    }

    private S3Client buildExternalS3Client() {
        String region = System.getProperty("it.s3.region", "us-east-1").trim();
        String endpointOverride = System.getProperty("it.s3.endpoint.override", "").trim();
        String accessKey = System.getProperty("it.s3.access.key", "").trim();
        String secretKey = System.getProperty("it.s3.secret.key", "").trim();

        S3ClientBuilder builder = S3Client.builder().region(Region.of(region));

        if (!endpointOverride.isEmpty()) {
            builder.endpointOverride(URI.create(endpointOverride));
            if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
                builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
            } else {
                builder.credentialsProvider(DefaultCredentialsProvider.create());
            }
            builder.forcePathStyle(true);
            return builder.build();
        }

        builder.credentialsProvider(DefaultCredentialsProvider.create());
        return builder.build();
    }

    private boolean s3ObjectExists(String bucket, String key) {
        try (S3Client s3 = buildExternalS3Client()) {
            s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
            return true;
        } catch (S3Exception ex) {
            if (ex.statusCode() == 404) {
                return false;
            }
            throw ex;
        }
    }

    private void configureSystemProperties() {
        System.setProperty("kafka.bootstrap.servers", kafkaBootstrapServers);
        System.setProperty("kafka.topic.input", topicInput);
        System.setProperty("kafka.topic.output", topicOutput);
        System.setProperty("kafka.topic.matcher", topicMatcher);
        System.setProperty("kafka.topic.matcheroutput", topicMatcherOutput);
        System.setProperty("kafka.topic.performance", topicPerformance);
        System.setProperty("s3.bucket.name", bucketName);
        System.setProperty("s3.key.name", dbObjectKey);
        System.setProperty("config.parallelism", "1");
        System.setProperty("config.commit.interval.ms", "200");

        setOptionalTestProperty("it.s3.endpoint.override", "s3.endpoint.override");
        setOptionalTestProperty("it.s3.region", "s3.region");
        setOptionalTestProperty("it.s3.access.key", "s3.access.key");
        setOptionalTestProperty("it.s3.secret.key", "s3.secret.key");
    }

    private void setOptionalTestProperty(String sourceKey, String targetKey) {
        String value = System.getProperty(sourceKey, "").trim();
        if (!value.isEmpty()) {
            System.setProperty(targetKey, value);
        }
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
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
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
