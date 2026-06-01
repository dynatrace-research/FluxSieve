/*
 * Copyright (c) 2026 Dynatrace LLC. All rights reserved.
 * Licensed under the terms in the repository LICENSE file.
 */

package com.dynatrace.research.matchAndEnrichRecords;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gliwka.hyperscan.wrapper.*;
import com.gliwka.hyperscan.wrapper.Scanner;
import io.smallrye.config.SmallRyeConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.serialization.Serdes;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class KStreamsMatchingHyperscanEnrich5Dbs {

    private static final int DB_COUNT = 5;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private KafkaStreams kafkaStreamsApp = null;
    private boolean isReady = false;
    private static final Logger LOGGER = LoggerFactory.getLogger(KStreamsMatchingHyperscanEnrich5Dbs.class);
    private final List<Database> databases = new ArrayList<>(DB_COUNT);
    private final List<Scanner> scanners = new ArrayList<>(DB_COUNT);

    public KStreamsMatchingHyperscanEnrich5Dbs() throws CompileErrorException, IOException {
        final SmallRyeConfig config = ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KStreamsDynamicRulesMatchingHyperscan");

        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, config.getValue("config.commit.interval.ms", String.class)); // 1000 ms

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getValue("kafka.bootstrap.servers", String.class));

        final String kafkaTopicInput = config.getValue("kafka.topic.input", String.class);
        final String kafkaTopicOutput = config.getValue("kafka.topic.output", String.class);

        final int numStreamThreads = config.getValue("config.parallelism", int.class);

        if (numStreamThreads > 1) {
            LOGGER.info("Parallel execution with {} stream threads/tasks.", numStreamThreads);
            props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numStreamThreads);
        }

        final int rulesPerDb = parseAndValidateRulesPerDb(config.getValue("matcher.version", String.class));
        int[] rulesByDb = new int[DB_COUNT];
        Arrays.fill(rulesByDb, rulesPerDb);

        LOGGER.info("rulesDb1: {} --rulesDb2: {} --rulesDb3: {} --rulesDb4: {} --rulesDb5: {}",
            rulesByDb[0], rulesByDb[1], rulesByDb[2], rulesByDb[3], rulesByDb[4]);
        int currentStartId = 0;

        for (int dbIndex = 1; dbIndex <= DB_COUNT; dbIndex++) {
            int ruleCount = rulesByDb[dbIndex - 1];
            String filePath = null;
            if (ruleCount > 0) {
                filePath = resolveExpressionFilePath(dbIndex);
                LOGGER.info("file{}: {}", dbIndex, filePath);
            }

            LinkedList<Expression> expressions = loadExpressions(filePath, ruleCount, currentStartId);
            Database db = Database.compile(expressions);
            Scanner scanner = new Scanner();
            scanner.allocScratch(db);

            databases.add(db);
            scanners.add(scanner);
            currentStartId += ruleCount;
        }
        final StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream(kafkaTopicInput, Consumed.with(Serdes.ByteArray(), Serdes.String()))
                .flatMapValues(value -> {
                    List<byte[]> enrichedRecords = new ArrayList<>(1);

                    try {
                        // Parse JSON
                        Map<String, Object> jsonMap = MAPPER.readValue(value, new TypeReference<Map<String, Object>>() {});
                        Set<String> matchedRuleIds = new HashSet<>();

                        for (int fieldIndex = 1; fieldIndex <= DB_COUNT; fieldIndex++) {
                            final int contentIndex = fieldIndex;
                            String content = (String) jsonMap.getOrDefault("content" + contentIndex, "");
                            Scanner scanner = scanners.get(contentIndex - 1);
                            Database db = databases.get(contentIndex - 1);

                            scanner.scan(db, content, (expression, fromByteIdx, toByteIdxExclusive) -> {
                                matchedRuleIds.add(String.valueOf(expression.getId()));
                                LOGGER.trace("Matched content{} rule: {} .", contentIndex, expression.getId());
                                return true;
                            });
                        }

                        // Enrich JSON with matched rules
                        if (matchedRuleIds.isEmpty()) {
                            jsonMap.put("matchedRule_0", false);
                        } else {
                            for (String ruleId : matchedRuleIds) {
                                jsonMap.put("matchedRule_" + ruleId, true);
                            }
                        }

                        // Serialize enriched record
                        byte[] enriched = MAPPER.writeValueAsBytes(jsonMap);
                        enrichedRecords.add(enriched);

                    } catch (Exception e) {
                        LOGGER.error("Failed to parse or enrich JSON: {}", value, e);
                        enrichedRecords.add(value.getBytes(StandardCharsets.UTF_8)); // fallback
                    }

                    return enrichedRecords;
                })
                .to(kafkaTopicOutput, Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));

        final Topology topology = builder.build();
        this.kafkaStreamsApp = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                stop();
            } catch (Exception e) {
                LOGGER.error("Error while stopping app in shutdown hook", e);
            }
        }, "kstreams-hyperscan-shutdown"));
    }

    public static void main(String[] args) throws CompileErrorException, IOException {

        KStreamsMatchingHyperscanEnrich5Dbs KStreamsDynamicRulesMatching = new KStreamsMatchingHyperscanEnrich5Dbs();
        KStreamsDynamicRulesMatching.start();

    }

    /**
     * Checks if Kafka is ready
     * @return True if ... or false if ...
     */
    public boolean isReady() {
        return isReady; // TODO get this information from Kafka Streams
    }

    public void start() {
        this.kafkaStreamsApp.start();
        this.isReady = true;
    }

    public synchronized void stop() {
        if (this.kafkaStreamsApp != null) {
            this.kafkaStreamsApp.close();
        }
        closeHyperscanResources();
        this.isReady = false;
    }

    private static String resolveExpressionFilePath(int dbIndex) {
        String fileName = "random_strings_custom" + dbIndex + ".txt";
        Path moduleRelative = Path.of("src", "test", "resources", fileName);
        if (Files.exists(moduleRelative)) {
            return moduleRelative.toString();
        }

        Path repoRelative = Path.of("kstreams", "src", "test", "resources", fileName);
        if (Files.exists(repoRelative)) {
            return repoRelative.toString();
        }

        throw new IllegalStateException("Rules file not found for dbIndex=" + dbIndex
                + ". Tried: " + moduleRelative + " and " + repoRelative);
    }

    private static LinkedList<Expression> loadExpressions(String filePath, int numberOfRules, int startId) throws IOException {
        if (numberOfRules < 0) {
            throw new IllegalArgumentException("numberOfRules must be >= 0, got " + numberOfRules);
        }
        if (numberOfRules == 0) {
            return new LinkedList<>();
        }
        if (filePath == null || filePath.isBlank()) {
            throw new IllegalArgumentException("filePath must be set when numberOfRules > 0");
        }

        LOGGER.info("Loading {} rules from ID {} to {}", numberOfRules, startId, startId + numberOfRules - 1);

        LinkedList<Expression> expressions = new LinkedList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            int id = startId;
            while ((line = reader.readLine()) != null && id < startId + numberOfRules) {
                expressions.add(new Expression(
                        line,
                        EnumSet.of(ExpressionFlag.UTF8, ExpressionFlag.SOM_LEFTMOST, ExpressionFlag.CASELESS),
                        id++
                ));
            }
        }

        if (expressions.size() < numberOfRules) {
            throw new IllegalStateException("Requested " + numberOfRules + " rules from " + filePath
                    + " starting at ID " + startId + " but loaded only " + expressions.size());
        }

        LOGGER.info("Loaded {} expressions from file {}", expressions.size(), filePath);
        return expressions;
    }

    private static int parseAndValidateRulesPerDb(String rawValue) {
        final int parsed;
        try {
            parsed = Integer.parseInt(rawValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("matcher.version must be an integer, got: " + rawValue, e);
        }

        if (parsed < 0) {
            throw new IllegalArgumentException("matcher.version must be >= 0, got: " + parsed);
        }
        return parsed;
    }

    private void closeHyperscanResources() {
        for (Scanner scanner : scanners) {
            try {
                scanner.close();
            } catch (Exception e) {
                LOGGER.warn("Failed to close Hyperscan scanner", e);
            }
        }
        scanners.clear();

        for (Database db : databases) {
            try {
                db.close();
            } catch (Exception e) {
                LOGGER.warn("Failed to close Hyperscan database", e);
            }
        }
        databases.clear();
    }
}