/*
 * Copyright (c) 2026 Dynatrace LLC. All rights reserved.
 * Licensed under the terms in the repository LICENSE file.
 */

package com.dynatrace.research.dynamicPatternMatchingEngine;

import com.gliwka.hyperscan.wrapper.CompileErrorException;
import com.gliwka.hyperscan.wrapper.Database;
import com.gliwka.hyperscan.wrapper.Expression;
import com.gliwka.hyperscan.wrapper.ExpressionFlag;
import com.gliwka.hyperscan.wrapper.Scanner;
import io.smallrye.config.SmallRyeConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class KStreamsDynamicPatternFiltering {

    private static final String APPLICATION_ID = "KStreamsDynamicRulesMatchingHyperscan";
    private static final String PERF_WORKER_GET_OBJECT = "WorkerGetObjectFromS3";
    private static final String PERF_WORKER_LOAD_DB = "WorkerLoadNewDB";
    private static final String PERF_WORKER_TOTAL = "WorkerTotal";

    private static final Logger LOGGER = LoggerFactory.getLogger(KStreamsDynamicPatternFiltering.class);
    private static final ThreadLocal<StringBuilder> THREAD_LOCAL_BUILDER =
            ThreadLocal.withInitial(() -> new StringBuilder(256));

    private final KafkaStreams kafkaStreamsApp;
    private final KafkaProducer<String, String> producer;
    private final S3Client s3Client;
    private final String kafkaTopicPerformance;
    private final String kafkaTopicMatcherOutput;
    private final int numStreamThreads;
    private final Path localMatcherDbFilePath;
    private final AtomicReference<Database> loadedDatabase = new AtomicReference<>();
    private final ThreadLocal<ScannerState> scannerState = ThreadLocal.withInitial(ScannerState::new);

    private volatile String currentMatcher = "";
    private volatile boolean isReady;

    public KStreamsDynamicPatternFiltering() throws CompileErrorException {
        final SmallRyeConfig config = ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getValue("kafka.bootstrap.servers", String.class));

        final String kafkaTopicInput = config.getValue("kafka.topic.input", String.class);
        final String kafkaTopicOutput = config.getValue("kafka.topic.output", String.class);
        final String kafkaTopicMatcher = config.getValue("kafka.topic.matcher", String.class);
        this.kafkaTopicPerformance = config.getValue("kafka.topic.performance", String.class);
        this.kafkaTopicMatcherOutput = config.getValue("kafka.topic.matcheroutput", String.class);
        final String bucketName = config.getValue("s3.bucket.name", String.class);
        final String keyName = config.getValue("s3.key.name", String.class);
        this.numStreamThreads = config.getValue("config.parallelism", int.class);
        this.localMatcherDbFilePath = resolveLocalMatcherDbPath(keyName);

        if (numStreamThreads > 1) {
            LOGGER.info("Parallel execution with {} stream threads/tasks.", numStreamThreads);
            props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numStreamThreads);
        }

        this.producer = new KafkaProducer<>(props);
        this.s3Client = buildS3Client(config);

        createDefaultDatabaseFile(keyName);
        loadedDatabase.set(loadDatabaseFromFile(keyName));

        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream(kafkaTopicMatcher).foreach((key, value) -> {
            String matcherKey = key == null ? "" : key.toString();

            if (Objects.equals(currentMatcher, matcherKey)) {
                return;
            }

            LOGGER.info("#----1_ stream task received message from kafka matcher topic");
            long totalStartNanos = System.nanoTime();

            try {
                LOGGER.info("#----2_ stream task triggered to fetch new object for matcher key: {}", matcherKey);
                long s3StartNanos = System.nanoTime();
                String etag = syncMatcherDatabaseFromS3(bucketName, keyName, matcherKey);
                publishTiming(PERF_WORKER_GET_OBJECT, elapsedMillis(s3StartNanos));

                long loadStartNanos = System.nanoTime();
                LOGGER.info("#----4_ Loading the new DB matcher file");
                loadedDatabase.set(loadDatabaseFromFile(keyName));
                publishTiming(PERF_WORKER_LOAD_DB, elapsedMillis(loadStartNanos));

                producer.send(new ProducerRecord<>(kafkaTopicMatcherOutput, etag, "emptyvalue"),
                        (metadata, exception) -> {
                            if (exception != null) {
                                LOGGER.error("Failed to publish matcher update output.", exception);
                                return;
                            }
                            LOGGER.info(" --- Message sent to topic: {} partition: {} offset: {}",
                                    metadata.topic(), metadata.partition(), metadata.offset());
                        });

                publishTiming(PERF_WORKER_TOTAL, elapsedMillis(totalStartNanos));
                currentMatcher = matcherKey;
                LOGGER.info("#----5_ Finished updating new DB matcher file");
            } catch (Exception e) {
                LOGGER.error("Failed to update matcher DB from S3 for key {}.", matcherKey, e);
            }
        });

        builder.stream(kafkaTopicInput, Consumed.with(Serdes.String(), Serdes.ByteArray()))
                .flatMapValues(value -> {
                    StringBuilder sb = THREAD_LOCAL_BUILDER.get();
                    sb.setLength(0);

                    String valueStr = new String(value, StandardCharsets.UTF_8);
                    List<byte[]> enrichedRecords = new ArrayList<>(4);

                    Database currentDb = loadedDatabase.get();
                    if (currentDb == null) {
                        return enrichedRecords;
                    }

                    ScannerState state = scannerState.get();
                    state.ensureBound(currentDb);

                    state.scanner.scan(currentDb, value, (expression, fromByteIdx, toByteIdxExclusive) -> {
                        sb.setLength(0);
                        sb.append(valueStr)
                                .append(" -- matched pattern: ")
                                .append(expression.getExpression());
                        enrichedRecords.add(sb.toString().getBytes(StandardCharsets.UTF_8));
                        return true;
                    });

                    return enrichedRecords;
                })
                .to(kafkaTopicOutput, Produced.with(Serdes.String(), Serdes.ByteArray()));

        Topology topology = builder.build();
        this.kafkaStreamsApp = new KafkaStreams(topology, props);
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    public static void main(String[] args) throws CompileErrorException {
        KStreamsDynamicPatternFiltering app = new KStreamsDynamicPatternFiltering();
        app.start();
    }

    public void start() {
        if (isReady) {
            return;
        }

        kafkaStreamsApp.start();
        isReady = true;
    }

    public void stop() {
        if (!isReady) {
            return;
        }

        kafkaStreamsApp.close();
        producer.close();
        s3Client.close();
        try {
            Files.deleteIfExists(localMatcherDbFilePath);
        } catch (IOException e) {
            LOGGER.warn("Failed to delete local matcher DB file {} during shutdown.", localMatcherDbFilePath, e);
        }
        isReady = false;
    }

    private static final class ScannerState {
        private Database boundDatabase;
        private Scanner scanner;

        private void ensureBound(Database database) {
            if (scanner == null || boundDatabase != database) {
                scanner = new Scanner();
                scanner.allocScratch(database);
                boundDatabase = database;
            }
        }
    }

    private void createDefaultDatabaseFile(String keyName) throws CompileErrorException {
        LinkedList<Expression> expressions = new LinkedList<>();
        expressions.add(new Expression("The number is ([0-9]+)", EnumSet.of(ExpressionFlag.SOM_LEFTMOST), 0));

        try (Database db = Database.compile(expressions);
             OutputStream out = new FileOutputStream(keyName)) {
            db.save(out);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize default matcher database file.", e);
        }
    }

    private Database loadDatabaseFromFile(String keyName) {
        try (InputStream in = new FileInputStream(keyName)) {
            return Database.load(in);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load matcher database file.", e);
        }
    }

    private String syncMatcherDatabaseFromS3(String bucketName, String keyName, String matcherKey) {
        Path filePath = localMatcherDbFilePath;

        try {
            String s3ETag = getS3ETag(bucketName, keyName);
            boolean downloadDb = shouldDownloadDatabaseFile(filePath, s3ETag);

            if (!downloadDb) {
                LOGGER.info("#----3_ object downloaded was skipped");
                return s3ETag;
            }

            GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyName);

            if (matcherKey != null && !matcherKey.isBlank()) {
                requestBuilder.ifMatch(matcherKey);
            }

            GetObjectResponse response = s3Client.getObject(requestBuilder.build(), filePath);
            String etag = response.eTag() == null ? "" : response.eTag().replace("\"", "");
            LOGGER.info("#----3_ stream task's object downloaded successfully with ETag: {}", etag);
            return etag;
        } catch (IOException e) {
            throw new RuntimeException("I/O failure while synchronizing matcher database.", e);
        } catch (S3Exception e) {
            String message = e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage();
            throw new RuntimeException("S3 failure while synchronizing matcher database: " + message, e);
        }
    }

    private String getS3ETag(String bucketName, String keyName) {
        HeadObjectRequest headRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .build();
        HeadObjectResponse headResponse = s3Client.headObject(headRequest);
        return headResponse.eTag() == null ? "" : headResponse.eTag().replace("\"", "");
    }

    private boolean shouldDownloadDatabaseFile(Path filePath, String s3ETag) throws IOException {
        if (Files.exists(filePath)) {
            String localChecksum = computeMD5Checksum(filePath);
            if (localChecksum.equals(s3ETag)) {
                return false;
            }
            Files.delete(filePath);
            return true;
        }

        Path parentPath = filePath.getParent();
        if (parentPath != null) {
            Files.createDirectories(parentPath);
        }
        return true;
    }

    private void publishTiming(String metricName, double durationMs) {
        String payload = "timestamp: " + Instant.now()
                + ", duration: " + durationMs
                + ", matcherSize: N/A"
                + ", parallelism: " + numStreamThreads;
        producer.send(new ProducerRecord<>(kafkaTopicPerformance, metricName, payload));
    }

    private static double elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000.0;
    }

    private static Path resolveLocalMatcherDbPath(String keyName) {
        return Paths.get("").toAbsolutePath().resolve(Paths.get(keyName).getFileName());
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

    public static String computeMD5Checksum(Path path) throws IOException {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm is unavailable.", e);
        }

        try (InputStream is = Files.newInputStream(path)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                md.update(buffer, 0, bytesRead);
            }
        }

        byte[] digest = md.digest();
        StringBuilder sb = new StringBuilder(digest.length * 2);
        for (byte b : digest) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
