/*
 * Copyright (c) 2026 Dynatrace LLC. All rights reserved.
 * Licensed under the terms in the repository LICENSE file.
 */

package com.dynatrace.research.matchAndEnrichRecordsWriteParquet;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.io.File;
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
public class KafkaStreamsBaselineWriteParquetDirect {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsBaselineWriteParquetDirect.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String STREAM_BASE_DIR = getEnv("PARQUET_DIR", "./data_streaming_parquet_sink3");
    private static final String FILE_FORMAT = getEnv("FILE_FORMAT", "parquet").toLowerCase();
    private static final String PARQUET_COMPRESSION = getEnv("PARQUET_COMPRESSION", "zstd");

    private static final int BATCH_MAX_ROWS = Integer.parseInt(getEnv("BATCH_MAX_ROWS", "1000"));
    private static final int BATCH_MAX_SECONDS = Integer.parseInt(getEnv("BATCH_MAX_SECONDS", "1"));

    private static final int FILE_MAX_ROWS = Integer.parseInt(getEnv("FILE_MAX_ROWS", "2000"));
    private static final int FILE_MAX_SECONDS = Integer.parseInt(getEnv("FILE_MAX_SECONDS", "5"));

    private static final String OUT_DIR = STREAM_BASE_DIR + File.separator + FILE_FORMAT + File.separator + "sink";

    private static final DateTimeFormatter TS_FILENAME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS").withZone(ZoneOffset.UTC);

    private static final String INSTANCE_ID = Optional.ofNullable(System.getenv("INSTANCE_ID"))
            .filter(s -> !s.isBlank()).orElseGet(() -> UUID.randomUUID().toString());
    private static final long PID = ProcessHandle.current().pid();
    private static final AtomicLong FILE_SEQ = new AtomicLong(0);

    private static final Schema BASELINE_SCHEMA = createBaselineSchema();

    private volatile boolean running = true;
    private volatile boolean shuttingDown = false;

    private ParquetWriter<GenericRecord> writer;
    private String currentFile;
    private long openAt;
    private int rowsWritten;

    private long lastFlushTs = System.currentTimeMillis();

    public static void main(String[] args) {
        new File(OUT_DIR).mkdirs();

        final SmallRyeConfig cfg = ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);
        final String bootstrap = cfg.getValue("kafka.bootstrap.servers", String.class);
        final String groupId = cfg.getOptionalValue("consumer.group.id", String.class).orElse("kafka-to-parquet-direct");
        final String inputTopic = cfg.getValue("kafka.topic.input", String.class);

        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "200");
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(Math.max(BATCH_MAX_ROWS, 1000)));
        consProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(Math.max(300000, BATCH_MAX_SECONDS * 1000 * 5)));

        KafkaStreamsBaselineWriteParquetDirect processor = new KafkaStreamsBaselineWriteParquetDirect();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                LOGGER.info("Shutdown hook triggered: requesting graceful stop.");
                processor.shuttingDown = true;
                processor.running = false;

                Thread.sleep(300);

                if (processor.writer != null) {
                    LOGGER.warn("Writer still open in shutdown hook - forcing close");
                    try {
                        processor.forceCloseCurrentFile();
                    } catch (Exception e) {
                        LOGGER.error("Failed to close writer in shutdown hook", e);
                    }
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

            while (processor.running) {
                if (processor.shuttingDown && processor.writer != null) {
                    LOGGER.info("Shutdown detected mid-loop, closing writer immediately");
                    processor.forceCloseCurrentFile();
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
                            processor.writeBatch(buffer);
                            consumer.commitSync();
                        } catch (Exception e) {
                            LOGGER.error("Error writing batch, will retry later (no commit)", e);
                        } finally {
                            buffer.clear();
                            lastFlush = System.currentTimeMillis();
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
                    processor.writeBatch(buffer);
                    consumer.commitSync();
                    LOGGER.info("Final batch committed successfully");
                }
            } catch (Exception e) {
                LOGGER.warn("Final drain failed; data will be re-delivered next run", e);
            }

        } catch (Throwable t) {
            LOGGER.error("Fatal error in Kafka to Parquet processor", t);
            throw new RuntimeException(t);
        } finally {
            try {
                LOGGER.info("Finally block: ensuring writer is closed");
                processor.forceCloseCurrentFile();
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

            LOGGER.info("KafkaStreamsBaselineWriteParquetDirect shutdown complete");
        }
    }

    private void writeBatch(List<ConsumerRecord<byte[], String>> batch) throws Exception {
        if (batch == null || batch.isEmpty()) {
            return;
        }

        if (!shuttingDown && (writer == null || rotateNeeded())) {
            openNewWriter();
        } else if (writer == null && shuttingDown) {
            LOGGER.info("Skipping new file open due to shutdown in progress; batch size={}", batch.size());
            return;
        }

        long batchIntervalMs = System.currentTimeMillis() - lastFlushTs;
        long writeStart = System.currentTimeMillis();

        int writtenThisBatch = 0;
        for (ConsumerRecord<byte[], String> rec : batch) {
            try {
                MAPPER.readTree(rec.value());
                GenericRecord gr = buildBaseline(rec.value());
                writer.write(gr);
                rowsWritten++;
                writtenThisBatch++;
            } catch (Exception e) {
                LOGGER.warn("Failed to write record at offset {}: {}", rec.offset(), e.getMessage());
            }
        }

        long writeDurationMs = System.currentTimeMillis() - writeStart;
        LOGGER.info("parquet_sink batch_interval_ms={} write_duration_ms={} batch_rows={} file_rows_cumulative={} file={}",
                batchIntervalMs, writeDurationMs, writtenThisBatch, rowsWritten, currentFile);

        lastFlushTs = System.currentTimeMillis();
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
                String.format("sink_%s_%s_%d_%d.parquet", ts, INSTANCE_ID, PID, seq);

        writer = AvroParquetWriter.<GenericRecord>builder(new Path(currentFile))
                .withSchema(BASELINE_SCHEMA)
                .withCompressionCodec(getCompressionCodec(PARQUET_COMPRESSION))
                .withRowGroupSize(8 * 1024 * 1024)
                .withPageSize(1 * 1024 * 1024)
                .build();

        openAt = System.currentTimeMillis();
        rowsWritten = 0;

        LOGGER.info("parquet_sink opened_new_file file={} at={} schema={}", currentFile, ts, BASELINE_SCHEMA.getName());
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

    private static Schema createBaselineSchema() {
        String schemaJson = """
        {
          "type": "record",
          "name": "KafkaRecordBaseline",
          "fields": [
            {"name":"timestamp","type":"long"},
            {"name":"status","type":"string"},
            {"name":"eventType","type":"string"},
            {"name":"content1","type":"string"},
            {"name":"content2","type":"string"}
          ]
        }
        """;
        return new Schema.Parser().parse(schemaJson);
    }

    private static GenericRecord buildBaseline(String json) throws Exception {
        Dto dto = MAPPER.readValue(json, Dto.class);
        GenericRecord record = new GenericData.Record(BASELINE_SCHEMA);
        record.put("timestamp", dto.timestamp);
        record.put("status", dto.status == null ? "" : dto.status);
        record.put("eventType", dto.eventType == null ? "" : dto.eventType);
        record.put("content1", dto.content1 == null ? "" : dto.content1);
        record.put("content2", dto.content2 == null ? "" : dto.content2);
        return record;
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
}
