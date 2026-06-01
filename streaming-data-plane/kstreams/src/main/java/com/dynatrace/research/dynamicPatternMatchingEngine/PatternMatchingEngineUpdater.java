/*
 * Copyright (c) 2026 Dynatrace LLC. All rights reserved.
 * Licensed under the terms in the repository LICENSE file.
 */

package com.dynatrace.research.dynamicPatternMatchingEngine;

import com.gliwka.hyperscan.wrapper.CompileErrorException;
import com.gliwka.hyperscan.wrapper.Database;
import com.gliwka.hyperscan.wrapper.Expression;
import com.gliwka.hyperscan.wrapper.ExpressionFlag;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.config.SmallRyeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PatternMatchingEngineUpdater {

    private static final Logger LOGGER = LoggerFactory.getLogger(PatternMatchingEngineUpdater.class);
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final Random RANDOM = new Random();

    private static boolean isInteger(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Waits for worker confirmations that the matcher was updated.
     */
    public static boolean assertThatMatcherWasUpdatedOnWorkers(String kafkaServer, String topic, String matcherTag, int expectedCounts) {
        final long TIMEOUT_MS = 30_000;
        String expectedMatcherTag = normalizeMatcherTag(matcherTag);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            LOGGER.info("Checking worker confirmations on broker: {} and topic: {}", kafkaServer, topic);

            int matchCount = 0;
            long startTime = System.currentTimeMillis();
            while (matchCount < expectedCounts) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (expectedMatcherTag.equals(normalizeMatcherTag(record.key()))) {
                        matchCount++;
                        LOGGER.info("Matcher update confirmation from worker - topic: {}, key: {}, value: {}, offset: {}",
                                topic, record.key(), record.value(), record.offset());
                    }
                }
                if (System.currentTimeMillis() - startTime > TIMEOUT_MS) {
                    LOGGER.warn("Timeout reached waiting for worker confirmations");
                    return false;
                }
            }
            return matchCount == expectedCounts;
        }
    }

    private static String normalizeMatcherTag(String matcherTag) {
        if (matcherTag == null) {
            return "";
        }
        return matcherTag.replace("\"", "").trim();
    }

    public static void run(String key) throws InterruptedException, IOException {
        final SmallRyeConfig config = ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getValue("kafka.bootstrap.servers", String.class));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        final String kafkaTopicMatcher = config.getValue("kafka.topic.matcher", String.class);
        final String kafkaTopicMatcherOutput = config.getValue("kafka.topic.matcheroutput", String.class);
        final String kafkaTopicPerformance = config.getValue("kafka.topic.performance", String.class);
        final String bucketName = config.getValue("s3.bucket.name", String.class);
        final String keyName = config.getValue("s3.key.name", String.class);
        final int parallelism = config.getValue("config.parallelism", int.class);

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            MeterRegistry registry = new SimpleMeterRegistry();
            Timer.Sample sample = Timer.start(registry);

            String updatedMatcher;
            try {
                updatedMatcher = runUpdate(
                        parallelism,
                        config.getValue("kafka.bootstrap.servers", String.class),
                        kafkaTopicMatcher,
                        kafkaTopicPerformance,
                        key,
                        bucketName,
                        keyName);
            } catch (CompileErrorException e) {
                throw new RuntimeException(e);
            }

            Timer timer = registry.timer("custom.block.timer");
            sample.stop(timer);
            double duration = timer.totalTime(TimeUnit.MILLISECONDS);

            String timestamp = Instant.now().toString();
            String value = "timestamp: " + timestamp + ", duration: " + duration + ", matcherSize: " + key + ", parallelism: " + parallelism;
            producer.send(new ProducerRecord<>(kafkaTopicPerformance, "executeMatcherFunctionUpdate", value.getBytes()));

            LOGGER.info("Matcher updater executed for: {}", updatedMatcher);

            // Measure end-to-end latency including worker confirmation
            MeterRegistry e2eRegistry = new SimpleMeterRegistry();
            Timer.Sample e2eSample = Timer.start(e2eRegistry);

            boolean matcherAssertion = assertThatMatcherWasUpdatedOnWorkers(
                    config.getValue("kafka.bootstrap.servers", String.class),
                    kafkaTopicMatcherOutput,
                    updatedMatcher,
                    parallelism);

            if (matcherAssertion) {
                Timer e2eTimer = e2eRegistry.timer("custom.block.timer");
                e2eSample.stop(e2eTimer);
                duration = e2eTimer.totalTime(TimeUnit.MILLISECONDS);

                timestamp = Instant.now().toString();
                value = "timestamp: " + timestamp + ", duration: " + duration + ", matcherSize: " + key + ", parallelism: " + parallelism;
                producer.send(new ProducerRecord<>(kafkaTopicPerformance, "matchere2elatency", value.getBytes()));
            }
        }
    }

    public static String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(CHARACTERS.charAt(RANDOM.nextInt(CHARACTERS.length())));
        }
        return sb.toString();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final SmallRyeConfig config = ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);
        final String key = config.getValue("matcher.version", String.class);
        final int repetitions = config.getValue("config.repetitions", int.class);
        final int sleepInterval = config.getValue("config.sleep.interval", int.class);
        final String bucketName = config.getValue("s3.bucket.name", String.class);
        final String keyName = config.getValue("s3.key.name", String.class);

        for (int i = 0; i < repetitions; i++) {
            Thread.sleep(sleepInterval);
            run(key);
        }

        deleteMatcherFromS3(bucketName, keyName);
        LOGGER.info("Done - deleted object from S3");
    }

    public static void deleteMatcherFromS3(String bucketName, String keyName) {
        final SmallRyeConfig config = ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);
        try (S3Client s3 = buildS3Client(config)) {
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .build();
            s3.deleteObject(deleteObjectRequest);
        }
    }

    public static String runUpdate(int parallelism, String kafkaBootstrapServer, String kafkaTopicMatcher, String kafkaTopicPerformance, String key, String bucketName, String keyName) throws IOException, CompileErrorException {
        final SmallRyeConfig config = ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        final int stringSize = Integer.parseInt(config.getValue("generate.string.size", String.class));

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            LinkedList<Expression> expressions = new LinkedList<>();
            final AtomicInteger idCounter = new AtomicInteger(0);

            if ("matcher".equals(key)) {
                expressions.add(new Expression("The number is ([0-9]+)", EnumSet.of(ExpressionFlag.SOM_LEFTMOST), 0));
            } else if (isInteger(key)) {
                final int count = Integer.parseInt(key);
                double genDuration = timeBlock(() -> {
                    for (int i = 0; i < count; i++) {
                        String randomString = generateRandomString(stringSize);
                        int id = idCounter.getAndIncrement();
                        expressions.add(new Expression(randomString, EnumSet.of(ExpressionFlag.SOM_LEFTMOST), id));
                    }
                });
                sendPerformanceRecord(producer, kafkaTopicPerformance, "UpdaterGenerateNewMatcher", genDuration, key, parallelism);
            }

            LOGGER.info("Starting compilation of new matcher with {} expressions", expressions.size());

            // Compile outside lambda since Database.compile throws checked exceptions
            MeterRegistry compileRegistry = new SimpleMeterRegistry();
            Timer.Sample compileSample = Timer.start(compileRegistry);
            Database db = Database.compile(expressions);
            Timer compileTimer = compileRegistry.timer("custom.block.timer");
            compileSample.stop(compileTimer);
            double compileDuration = compileTimer.totalTime(TimeUnit.MILLISECONDS);

            LOGGER.info("Compiled rules - size: {}, DbSize(bytes): {}", key, db.getSize());
            sendPerformanceRecord(producer, kafkaTopicPerformance, "UpdaterCompileNewDB", compileDuration, key, parallelism);

            // Save database to file
            MeterRegistry saveRegistry = new SimpleMeterRegistry();
            Timer.Sample saveSample = Timer.start(saveRegistry);
            try (OutputStream out = new FileOutputStream(keyName)) {
                db.save(out);
            }
            Timer saveTimer = saveRegistry.timer("custom.block.timer");
            saveSample.stop(saveTimer);
            double saveDuration = saveTimer.totalTime(TimeUnit.MILLISECONDS);
            sendPerformanceRecord(producer, kafkaTopicPerformance, "UpdaterSaveNewDbToFile", saveDuration, key, parallelism);

            // Report DB size
            long dbSize = db.getSize();
            String timestamp = Instant.now().toString();
            String sizeValue = "timestamp: " + timestamp + ", DbSize(bytes): " + dbSize + ", matcherSize: " + key + ", parallelism: " + parallelism;
            producer.send(new ProducerRecord<>(kafkaTopicPerformance, "DbSize", sizeValue.getBytes()));

            db.close();

            if (dbSize <= 0) {
                LOGGER.warn("New matcher is invalid (size <= 0)");
                return "";
            }

            // Upload to S3
                try (S3Client s3 = buildS3Client(config)) {

                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
                        .build();

                LOGGER.info("Uploading new matcher to S3 bucket: {}", bucketName);
                Path path = Paths.get(keyName);

                MeterRegistry uploadRegistry = new SimpleMeterRegistry();
                Timer.Sample uploadSample = Timer.start(uploadRegistry);
                PutObjectResponse response = s3.putObject(putObjectRequest, path);
                Timer uploadTimer = uploadRegistry.timer("custom.block.timer");
                uploadSample.stop(uploadTimer);
                double uploadDuration = uploadTimer.totalTime(TimeUnit.MILLISECONDS);

                Files.delete(path);
                sendPerformanceRecord(producer, kafkaTopicPerformance, "UpdaterNewMatcherPutObjectToS3", uploadDuration, key, parallelism);
                LOGGER.info("Uploaded matcher (tag: {}) to S3 bucket: {} in {} ms", response.eTag(), bucketName, uploadDuration);

                // Notify workers
                String data = response.eTag();
                LOGGER.info("Notifying {} worker(s) with eTag: {}", parallelism, data);
                if (parallelism > 1) {
                    for (int i = 0; i < parallelism; i++) {
                        producer.send(new ProducerRecord<>(kafkaTopicMatcher, i, data, key.getBytes()));
                    }
                } else {
                    producer.send(new ProducerRecord<>(kafkaTopicMatcher, data, key.getBytes()));
                }
                LOGGER.info("Sent update notifications to workers");
                return data;
            }
        }
    }

    private static S3Client buildS3Client(SmallRyeConfig config) {
        String regionName = config.getOptionalValue("s3.region", String.class).orElse("us-east-1");
        S3ClientBuilder builder = S3Client.builder().region(Region.of(regionName));

        String endpointOverride = config.getOptionalValue("s3.endpoint.override", String.class).orElse("").trim();
        if (!endpointOverride.isEmpty()) {
            String accessKey = config.getOptionalValue("s3.access.key", String.class).orElse("test");
            String secretKey = config.getOptionalValue("s3.secret.key", String.class).orElse("test");

            builder.endpointOverride(URI.create(endpointOverride));
            builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
            builder.forcePathStyle(true);
            return builder.build();
        }

        builder.credentialsProvider(DefaultCredentialsProvider.create());
        return builder.build();
    }

    private static double timeBlock(Runnable block) {
        MeterRegistry registry = new SimpleMeterRegistry();
        Timer.Sample sample = Timer.start(registry);
        block.run();
        Timer timer = registry.timer("custom.block.timer");
        sample.stop(timer);
        return timer.totalTime(TimeUnit.MILLISECONDS);
    }

    private static void sendPerformanceRecord(KafkaProducer<String, byte[]> producer, String topic, String metricName, double duration, String matcherSize, int parallelism) {
        String timestamp = Instant.now().toString();
        String value = "timestamp: " + timestamp + ", duration: " + duration + ", matcherSize: " + matcherSize + ", parallelism: " + parallelism;
        producer.send(new ProducerRecord<>(topic, metricName, value.getBytes()));
    }

}
