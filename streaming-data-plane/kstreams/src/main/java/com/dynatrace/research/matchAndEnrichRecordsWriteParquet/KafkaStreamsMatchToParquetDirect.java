/*
 * Copyright (c) 2026 Dynatrace LLC. All rights reserved.
 * Licensed under the terms in the repository LICENSE file.
 */

package com.dynatrace.research.matchAndEnrichRecordsWriteParquet;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gliwka.hyperscan.wrapper.*;
import com.gliwka.hyperscan.wrapper.Scanner;
import io.smallrye.config.SmallRyeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
/**
 * PoC note: this pipeline is intended for best-effort ingestion.
 * It does not guarantee exactly-once processing or strict no-loss delivery semantics.
 */
public class KafkaStreamsMatchToParquetDirect {

    private static final String ENV_PARQUET_DIR = "PARQUET_DIR";
    private static final String ENV_FILE_FORMAT = "FILE_FORMAT";
    private static final String ENV_PARQUET_COMPRESSION = "PARQUET_COMPRESSION";
    private static final String ENV_BATCH_MAX_ROWS = "BATCH_MAX_ROWS";
    private static final String ENV_BATCH_MAX_SECONDS = "BATCH_MAX_SECONDS";
    private static final String ENV_FILE_MAX_ROWS = "FILE_MAX_ROWS";
    private static final String ENV_FILE_MAX_SECONDS = "FILE_MAX_SECONDS";

    private static final String CFG_KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    private static final String CFG_CONSUMER_GROUP_ID = "consumer.group.id";
    private static final String CFG_KAFKA_TOPIC_INPUT = "kafka.topic.input";
    private static final String CFG_MATCHER_RULES_DB1 = "matcher.rules.db1";
    private static final String CFG_MATCHER_RULES_DB2 = "matcher.rules.db2";

    private static final String DEFAULT_STREAM_BASE_DIR = "./data_streaming_parquet_sink_enriched";
    private static final String DEFAULT_FILE_FORMAT = "parquet";
    private static final String DEFAULT_PARQUET_COMPRESSION = "zstd";
    private static final String DEFAULT_BATCH_MAX_ROWS = "1000";
    private static final String DEFAULT_BATCH_MAX_SECONDS = "1";
    private static final String DEFAULT_FILE_MAX_ROWS = "2000";
    private static final String DEFAULT_FILE_MAX_SECONDS = "5";
    private static final String DEFAULT_CONSUMER_GROUP_ID = "kafka-match-parquet-direct";
    private static final int DEFAULT_MATCHER_RULES_DB1 = 800;
    private static final int DEFAULT_MATCHER_RULES_DB2 = 200;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsMatchToParquetDirect.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String STREAM_BASE_DIR = getEnv(ENV_PARQUET_DIR, DEFAULT_STREAM_BASE_DIR);
    private static final String FILE_FORMAT = getEnv(ENV_FILE_FORMAT, DEFAULT_FILE_FORMAT).toLowerCase();
    private static final String PARQUET_COMPRESSION = getEnv(ENV_PARQUET_COMPRESSION, DEFAULT_PARQUET_COMPRESSION);

    private static final int BATCH_MAX_ROWS = Integer.parseInt(getEnv(ENV_BATCH_MAX_ROWS, DEFAULT_BATCH_MAX_ROWS));
    private static final int BATCH_MAX_SECONDS = Integer.parseInt(getEnv(ENV_BATCH_MAX_SECONDS, DEFAULT_BATCH_MAX_SECONDS));

    private static final int FILE_MAX_ROWS = Integer.parseInt(getEnv(ENV_FILE_MAX_ROWS, DEFAULT_FILE_MAX_ROWS));
    private static final int FILE_MAX_SECONDS = Integer.parseInt(getEnv(ENV_FILE_MAX_SECONDS, DEFAULT_FILE_MAX_SECONDS));

    private static final String OUT_DIR = STREAM_BASE_DIR + File.separator + FILE_FORMAT + File.separator + "sink";

    private static final DateTimeFormatter TS_FILENAME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS").withZone(ZoneOffset.UTC);

    private static final String INSTANCE_ID = Optional.ofNullable(System.getenv("INSTANCE_ID"))
            .filter(s -> !s.isBlank()).orElseGet(() -> UUID.randomUUID().toString());
    private static final long PID = ProcessHandle.current().pid();
    private static final AtomicLong FILE_SEQ = new AtomicLong(0);

    private static final Schema ENRICHED_SCHEMA = createEnrichedSchema();

    private volatile boolean running = true;
    private volatile boolean shuttingDown = false;

    private ParquetWriter<GenericRecord> writer;
    private String currentFile;
    private long openAt;
    private int rowsWritten;
    private long lastFlushTs = System.currentTimeMillis();

    private final Database db1;
    private final Database db2;
    private final Scanner scanner1;
    private final Scanner scanner2;

    public KafkaStreamsMatchToParquetDirect() throws Exception {
        final SmallRyeConfig cfg = ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);

        int rulesDb1 = cfg.getOptionalValue(CFG_MATCHER_RULES_DB1, Integer.class).orElse(DEFAULT_MATCHER_RULES_DB1);
        int rulesDb2 = cfg.getOptionalValue(CFG_MATCHER_RULES_DB2, Integer.class).orElse(DEFAULT_MATCHER_RULES_DB2);
        int currentStartId = 0;

        String filePath1 = "kstreams/src/test/resources/random_strings_custom-twofields1.txt";
        String filePath2 = "kstreams/src/test/resources/random_strings_custom-twofields2.txt";

        EnumSet<ExpressionFlag> hsFlags = EnumSet.of(ExpressionFlag.UTF8, ExpressionFlag.CASELESS);
        LinkedList<Expression> expressionsDb1 = loadExpressions(filePath1, rulesDb1, currentStartId, hsFlags);
        currentStartId += rulesDb1;
        LinkedList<Expression> expressionsDb2 = loadExpressions(filePath2, rulesDb2, currentStartId, hsFlags);

        db1 = Database.compile(expressionsDb1);
        db2 = Database.compile(expressionsDb2);

        scanner1 = new Scanner();
        scanner1.allocScratch(db1);
        scanner2 = new Scanner();
        scanner2.allocScratch(db2);

        LOGGER.info("Hyperscan databases compiled: db1={} rules, db2={} rules", rulesDb1, rulesDb2);
    }

    public static void main(String[] args) {
        new File(OUT_DIR).mkdirs();

        final SmallRyeConfig cfg = ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);
        final String bootstrap = cfg.getValue(CFG_KAFKA_BOOTSTRAP_SERVERS, String.class);
        final String groupId = cfg.getOptionalValue(CFG_CONSUMER_GROUP_ID, String.class).orElse(DEFAULT_CONSUMER_GROUP_ID);
        final String inputTopic = cfg.getValue(CFG_KAFKA_TOPIC_INPUT, String.class);

        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(Math.max(BATCH_MAX_ROWS, 1000)));
        consProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(Math.max(300000, BATCH_MAX_SECONDS * 1000 * 5)));

        KafkaStreamsMatchToParquetDirect processor = null;
        try {
            processor = new KafkaStreamsMatchToParquetDirect();
        } catch (Exception e) {
            LOGGER.error("Failed to initialize Hyperscan", e);
            System.exit(1);
        }

        final KafkaStreamsMatchToParquetDirect finalProcessor = processor;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                LOGGER.info("Shutdown hook triggered: requesting graceful stop.");
                finalProcessor.shuttingDown = true;
                finalProcessor.running = false;

                Thread.sleep(300);

                if (finalProcessor.writer != null) {
                    LOGGER.warn("Writer still open in shutdown hook - forcing close");
                    try {
                        finalProcessor.forceCloseCurrentFile();
                    } catch (Exception e) {
                        LOGGER.error("Failed to close writer in shutdown hook", e);
                    }
                }

                try {
                    finalProcessor.scanner1.close();
                    finalProcessor.scanner2.close();
                    finalProcessor.db1.close();
                    finalProcessor.db2.close();
                } catch (Exception e) {
                    LOGGER.error("Failed to close Hyperscan resources", e);
                }
            } catch (Throwable t) {
                LOGGER.error("Shutdown hook error", t);
            }
        }));

        final Duration pollTimeout = Duration.ofMillis(200);

        KafkaConsumer<byte[], String> consumer = null;
        try {
            consumer = new KafkaConsumer<>(consProps);
            consumer.subscribe(Collections.singletonList(inputTopic));

            List<ConsumerRecord<byte[], String>> buffer = new ArrayList<>(BATCH_MAX_ROWS);
            long lastFlush = System.currentTimeMillis();

            while (finalProcessor.running) {
                if (finalProcessor.shuttingDown && finalProcessor.writer != null) {
                    LOGGER.info("Shutdown detected mid-loop, closing writer immediately");
                    finalProcessor.forceCloseCurrentFile();
                    break;
                }

                ConsumerRecords<byte[], String> records = consumer.poll(pollTimeout);
                if (!records.isEmpty()) {
                    for (ConsumerRecord<byte[], String> r : records) {
                        buffer.add(r);
                    }
                }

                boolean timeExceeded = (System.currentTimeMillis() - lastFlush) >= BATCH_MAX_SECONDS * 1000L;
                boolean sizeExceeded = buffer.size() >= BATCH_MAX_ROWS;

                if (timeExceeded || sizeExceeded) {
                    if (!buffer.isEmpty()) {
                        try {
                            boolean processed = finalProcessor.writeBatch(buffer);
                            if (processed) {
                                consumer.commitSync();
                                buffer.clear();
                            } else {
                                LOGGER.info("Batch not processed due to shutdown state; skipping commit");
                            }
                            lastFlush = System.currentTimeMillis();
                        } catch (Exception e) {
                            LOGGER.error("Error writing batch, will retry later (no commit)", e);
                        }
                    } else {
                        lastFlush = System.currentTimeMillis();
                    }
                }
            }

            try {
                LOGGER.info("Main loop exited, performing final drain");
                ConsumerRecords<byte[], String> finalRecords = consumer.poll(Duration.ofMillis(100));
                if (!finalRecords.isEmpty()) {
                    for (ConsumerRecord<byte[], String> r : finalRecords) {
                        buffer.add(r);
                    }
                }
                if (!buffer.isEmpty()) {
                    LOGGER.info("Writing final batch of {} records", buffer.size());
                    boolean processed = finalProcessor.writeBatch(buffer);
                    if (processed) {
                        consumer.commitSync();
                        LOGGER.info("Final batch committed successfully");
                    } else {
                        LOGGER.info("Final batch was not processed due to shutdown state; skipping commit");
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("Final drain failed; data will be re-delivered next run", e);
            }

        } catch (Throwable t) {
            LOGGER.error("Fatal error in KafkaStreamsMatchToParquetDirect", t);
            throw new RuntimeException(t);
        } finally {
            try {
                LOGGER.info("Finally block: ensuring writer is closed");
                finalProcessor.forceCloseCurrentFile();
            } catch (Exception e) {
                LOGGER.error("Failed to close writer in finally block", e);
            }

            if (consumer != null) {
                try {
                    LOGGER.info("Closing Kafka consumer");
                    consumer.close(Duration.ofSeconds(5));
                } catch (Exception e) {
                    LOGGER.error("Failed to close consumer", e);
                }
            }

            LOGGER.info("KafkaStreamsMatchToParquetDirect shutdown complete");
        }
    }

    private boolean writeBatch(List<ConsumerRecord<byte[], String>> batch) throws Exception {
        if (batch == null || batch.isEmpty()) {
            return false;
        }

        if (!shuttingDown && (writer == null || rotateNeeded())) {
            openNewWriter();
        } else if (writer == null && shuttingDown) {
            LOGGER.info("Skipping new file open due to shutdown in progress; batch size={}", batch.size());
            return false;
        }

        long batchIntervalMs = System.currentTimeMillis() - lastFlushTs;
        long writeStart = System.currentTimeMillis();

        int writtenThisBatch = 0;
        for (ConsumerRecord<byte[], String> rec : batch) {
            try {
                Dto dto = MAPPER.readValue(rec.value(), Dto.class);

                final MatchResult result = new MatchResult();
                scanner1.scan(db1, dto.content1 == null ? "" : dto.content1,
                        (expression, from, toExclusive) -> {
                            result.set(expression.getId());
                            return true;
                        });
                scanner2.scan(db2, dto.content2 == null ? "" : dto.content2,
                        (expression, from, toExclusive) -> {
                            result.set(expression.getId());
                            return true;
                        });

                Enriched enriched = Enriched.from(dto, result);
                GenericRecord gr = buildEnrichedRecord(enriched);

                writer.write(gr);
                rowsWritten++;
                writtenThisBatch++;
            } catch (Exception e) {
                LOGGER.error("Failed to process/write record at offset {}: {}", rec.offset(), e.getMessage());
                throw e;
            }
        }

        long writeDurationMs = System.currentTimeMillis() - writeStart;
        LOGGER.info("parquet_sink batch_interval_ms={} write_duration_ms={} batch_rows={} file_rows_cumulative={} file={}",
                batchIntervalMs, writeDurationMs, writtenThisBatch, rowsWritten, currentFile);

        lastFlushTs = System.currentTimeMillis();
        return true;
    }

    private boolean rotateNeeded() {
        long sinceOpen = System.currentTimeMillis() - openAt;
        boolean timeRotate = sinceOpen >= FILE_MAX_SECONDS * 1000L;
        boolean sizeRotate = rowsWritten >= FILE_MAX_ROWS;

        if (sinceOpen >= FILE_MAX_SECONDS * 2000L && !shuttingDown) {
            LOGGER.warn("Emergency rotation triggered - file open for {}ms (max={}ms)",
                    sinceOpen, FILE_MAX_SECONDS * 1000L);
        }

        return timeRotate || sizeRotate;
    }

    private void openNewWriter() throws Exception {
        closeWriter();

        String ts = TS_FILENAME_FORMATTER.format(Instant.now());
        long seq = FILE_SEQ.incrementAndGet();
        currentFile = OUT_DIR + File.separator +
                String.format("sink_enriched_%s_%s_%d_%d.parquet", ts, INSTANCE_ID, PID, seq);

        writer = AvroParquetWriter.<GenericRecord>builder(new Path(currentFile))
                .withSchema(ENRICHED_SCHEMA)
                .withCompressionCodec(getCompressionCodec(PARQUET_COMPRESSION))
                .withRowGroupSize(8 * 1024 * 1024)
                .withPageSize(1 * 1024 * 1024)
                .build();

        openAt = System.currentTimeMillis();
        rowsWritten = 0;

        LOGGER.info("parquet_sink opened_new_file file={} at={} schema={}", currentFile, ts, ENRICHED_SCHEMA.getName());
    }

    private void closeWriter() throws Exception {
        if (writer != null) {
            String fileBeingClosed = currentFile;
            int finalRows = rowsWritten;

            try {
                LOGGER.info("Closing Parquet writer for file={} rows={}", fileBeingClosed, finalRows);
                writer.close();

                long size = -1L;
                try {
                    if (fileBeingClosed != null) {
                        File f = new File(fileBeingClosed);
                        if (f.exists()) {
                            size = f.length();
                        } else {
                            LOGGER.warn("File does not exist after close: {}", fileBeingClosed);
                        }
                    }
                } catch (Throwable t) {
                    LOGGER.warn("Could not get file size: {}", t.getMessage());
                }

                LOGGER.info("parquet_sink closed_file file={} final_rows={} size_bytes={}",
                        fileBeingClosed, finalRows, size);

            } catch (Exception e) {
                LOGGER.error("Error while closing Parquet writer for file={}", fileBeingClosed, e);
                throw e;
            } finally {
                writer = null;
                currentFile = null;
                rowsWritten = 0;
                openAt = 0L;
            }
        }
    }

    public synchronized void forceCloseCurrentFile() throws Exception {
        if (writer != null) {
            LOGGER.info("Force closing current Parquet file: file={} rows={}", currentFile, rowsWritten);
            closeWriter();
        } else {
            LOGGER.debug("forceCloseCurrentFile called but no writer is open");
        }
    }

    private static CompressionCodecName getCompressionCodec(String compression) {
        switch (compression.toLowerCase()) {
            case "none":
            case "uncompressed":
                return CompressionCodecName.UNCOMPRESSED;
            case "snappy":
                return CompressionCodecName.SNAPPY;
            case "gzip":
                return CompressionCodecName.GZIP;
            case "lzo":
                return CompressionCodecName.LZO;
            case "brotli":
                return CompressionCodecName.BROTLI;
            case "lz4":
                return CompressionCodecName.LZ4;
            case "zstd":
                return CompressionCodecName.ZSTD;
            default:
                LOGGER.warn("Unknown compression codec '{}', defaulting to ZSTD", compression);
                return CompressionCodecName.ZSTD;
        }
    }

    private static Schema createEnrichedSchema() {
        String schemaJson = """
        {
          "type": "record",
          "name": "KafkaRecordEnriched",
          "fields": [
            {"name":"timestamp","type":"long"},
            {"name":"status","type":"string"},
            {"name":"eventType","type":"string"},
            {"name":"content1","type":"string"},
            {"name":"content2","type":"string"},
            {"name":"matchedRuleIds","type":{"type":"array","items":"int"}}
          ]
        }
        """;
        return new Schema.Parser().parse(schemaJson);
    }

    private static GenericRecord buildEnrichedRecord(Enriched e) {
        GenericRecord record = new GenericData.Record(ENRICHED_SCHEMA);
        record.put("timestamp", e.timestamp);
        record.put("status", e.status == null ? "" : e.status);
        record.put("eventType", e.eventType == null ? "" : e.eventType);
        record.put("content1", e.content1 == null ? "" : e.content1);
        record.put("content2", e.content2 == null ? "" : e.content2);
        List<Integer> ids = new ArrayList<>(e.matchedRuleIds == null ? 0 : e.matchedRuleIds.length);
        if (e.matchedRuleIds != null) {
            for (int id : e.matchedRuleIds) {
                ids.add(id);
            }
        }
        record.put("matchedRuleIds", ids);
        return record;
    }

    private static LinkedList<Expression> loadExpressions(String filePath, int numberOfRules, int startId, EnumSet<ExpressionFlag> flags) throws IOException {
        File rulesFile = resolveRulesFile(filePath);
        LinkedList<Expression> expressions = new LinkedList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(rulesFile))) {
            String line;
            int id = startId;
            while ((line = reader.readLine()) != null && id < startId + numberOfRules) {
                expressions.add(new Expression(line, flags, id++));
            }
        }
        return expressions;
    }

    private static File resolveRulesFile(String filePath) throws FileNotFoundException {
        File directPath = new File(filePath);
        if (directPath.exists()) {
            return directPath;
        }

        String normalized = filePath.replace('\\', '/');
        if (normalized.startsWith("kstreams/")) {
            File trimmedPath = new File(normalized.substring("kstreams/".length()));
            if (trimmedPath.exists()) {
                return trimmedPath;
            }
        }

        throw new FileNotFoundException("Rules file not found: " + filePath);
    }

    private static String getEnv(String key, String defaultValue) {
        String v = System.getenv(key);
        return v != null && !v.isBlank() ? v : defaultValue;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static final class Dto {
        public long timestamp;
        public String status;
        public String eventType;
        public String content1;
        public String content2;
    }

    static final class MatchResult {
        private final java.util.BitSet bitset = new java.util.BitSet();

        void set(int id) {
            bitset.set(id);
        }

        int[] toIds() {
            int cardinality = bitset.cardinality();
            int[] ids = new int[cardinality];
            int idx = 0, curr = bitset.nextSetBit(0);
            while (curr >= 0) {
                ids[idx++] = curr;
                curr = bitset.nextSetBit(curr + 1);
            }
            return ids;
        }
    }

    static final class Enriched {
        public long timestamp;
        public String status;
        public String eventType;
        public String content1;
        public String content2;
        public int[] matchedRuleIds;

        static Enriched from(Dto d, MatchResult r) {
            Enriched e = new Enriched();
            e.timestamp = d.timestamp;
            e.status = d.status;
            e.eventType = d.eventType;
            e.content1 = d.content1;
            e.content2 = d.content2;
            e.matchedRuleIds = r.toIds();
            return e;
        }
    }
}
