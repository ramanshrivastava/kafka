package minikafka.common.record;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A batch of records for efficient transmission and storage.
 *
 * Design rationale:
 * - Batching is THE key to Kafka's performance
 * - Amortize network/disk overhead across multiple records
 * - Enable compression at batch level (more effective than per-record)
 *
 * Historical context:
 * - Kafka 0.7 (2011): Original batching concept
 * - Kafka 0.8.1 (2013): New producer with sophisticated batching
 * - Kafka 0.11 (2017): Batch-level CRC (not per-record)
 *
 * Why batching matters:
 * - Network: 1 RPC for N records vs N RPCs
 * - Disk: 1 write for N records (sequential)
 * - Compression: Compress batch is more effective
 *
 * Example impact:
 * - Without batching: 1000 records = 1000 network calls = ~1 second
 * - With batching (100/batch): 1000 records = 10 network calls = ~10ms
 * - 100x speedup!
 *
 * Apache Kafka reference:
 * /home/user/kafka/clients/src/main/java/org/apache/kafka/common/record/RecordBatch.java
 * /home/user/kafka/clients/src/main/java/org/apache/kafka/common/record/MemoryRecords.java
 *
 * Differences from Apache Kafka:
 * - We use simple ArrayList<Record> (Kafka uses more complex structure)
 * - No compression yet (Phase 1.4)
 * - No CRC yet (Phase 1.3)
 * - Simpler header (just baseOffset, recordCount)
 *
 * @see Record for individual message format
 */
public class RecordBatch {

    // Batch metadata
    private final long baseOffset;       // Offset of first record in batch
    private final int recordCount;       // Number of records in batch
    private final List<Record> records;  // The actual records

    // Future: Add these in later phases
    // - CRC32 checksum (Phase 1.3)
    // - Compression type (Phase 1.4)
    // - Producer ID, epoch (for exactly-once, Phase 2+)

    /**
     * Batch format (simplified):
     *
     * +------------+-------------+----------+--------+
     * | BaseOffset | RecordCount | Record1  | Record2 | ... | RecordN |
     * | 8B         | 4B          | variable | variable | ... |variable |
     * +------------+-------------+----------+--------+
     *
     * Real Kafka has much more in header:
     * - CRC, magic byte, attributes, timestamp, producer info, etc.
     */

    /**
     * Create a record batch.
     *
     * @param baseOffset Offset of the first record
     * @param records List of records (will be copied)
     */
    public RecordBatch(long baseOffset, List<Record> records) {
        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException("RecordBatch must contain at least one record");
        }

        this.baseOffset = baseOffset;
        this.recordCount = records.size();
        this.records = new ArrayList<>(records); // Defensive copy

        // Validate: offsets should be sequential starting from baseOffset
        // (This is a simplified check; real Kafka has more complex validation)
        for (int i = 0; i < records.size(); i++) {
            long expectedOffset = baseOffset + i;
            long actualOffset = records.get(i).offset();
            if (actualOffset != expectedOffset) {
                throw new IllegalArgumentException(
                    String.format("Invalid offset at index %d: expected %d, got %d",
                        i, expectedOffset, actualOffset)
                );
            }
        }
    }

    // Getters

    public long baseOffset() {
        return baseOffset;
    }

    public int recordCount() {
        return recordCount;
    }

    /**
     * Get all records in this batch.
     * Returns unmodifiable list to prevent external modification.
     */
    public List<Record> records() {
        return Collections.unmodifiableList(records);
    }

    /**
     * Get the last offset in this batch.
     */
    public long lastOffset() {
        return baseOffset + recordCount - 1;
    }

    /**
     * Calculate the serialized size of this batch in bytes.
     *
     * Header size: 8 (baseOffset) + 4 (recordCount) = 12 bytes
     * Body size: Sum of all record sizes
     */
    public int sizeInBytes() {
        int size = 12; // Header
        for (Record record : records) {
            size += record.sizeInBytes();
        }
        return size;
    }

    /**
     * Serialize this batch to a ByteBuffer.
     *
     * Wire format:
     * +--------------+--------------+----------+
     * | Base Offset  | Record Count | Records  |
     * | 8B           | 4B           | variable |
     * +--------------+--------------+----------+
     */
    public ByteBuffer serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(sizeInBytes());

        // Header
        buffer.putLong(baseOffset);
        buffer.putInt(recordCount);

        // Body: all records
        for (Record record : records) {
            ByteBuffer recordBuffer = record.serialize();
            buffer.put(recordBuffer);
        }

        buffer.flip();
        return buffer;
    }

    /**
     * Deserialize a record batch from a ByteBuffer.
     */
    public static RecordBatch deserialize(ByteBuffer buffer) {
        if (buffer.remaining() < 12) {
            throw new IllegalArgumentException("Buffer too small for RecordBatch");
        }

        // Read header
        long baseOffset = buffer.getLong();
        int recordCount = buffer.getInt();

        if (recordCount <= 0) {
            throw new IllegalArgumentException("Invalid record count: " + recordCount);
        }

        // Read all records
        List<Record> records = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            Record record = Record.deserialize(buffer);
            records.add(record);
        }

        return new RecordBatch(baseOffset, records);
    }

    /**
     * Builder for creating batches incrementally.
     *
     * Usage:
     * <pre>
     * RecordBatch batch = RecordBatch.builder(baseOffset)
     *     .append(record1)
     *     .append(record2)
     *     .build();
     * </pre>
     */
    public static class Builder {
        private final long baseOffset;
        private final List<Record> records = new ArrayList<>();

        private Builder(long baseOffset) {
            this.baseOffset = baseOffset;
        }

        /**
         * Append a record to this batch.
         * Record offset will be auto-assigned as baseOffset + index.
         */
        public Builder append(long timestamp, byte[] key, byte[] value) {
            long offset = baseOffset + records.size();
            records.add(new Record(offset, timestamp, key, value));
            return this;
        }

        /**
         * Append a record with current timestamp.
         */
        public Builder append(byte[] key, byte[] value) {
            return append(System.currentTimeMillis(), key, value);
        }

        /**
         * Append an existing record.
         * NOTE: The record's offset must match baseOffset + current count!
         */
        public Builder appendRecord(Record record) {
            long expectedOffset = baseOffset + records.size();
            if (record.offset() != expectedOffset) {
                throw new IllegalArgumentException(
                    String.format("Record offset %d doesn't match expected %d",
                        record.offset(), expectedOffset)
                );
            }
            records.add(record);
            return this;
        }

        /**
         * Get current count of records in builder.
         */
        public int count() {
            return records.size();
        }

        /**
         * Build the final batch.
         */
        public RecordBatch build() {
            if (records.isEmpty()) {
                throw new IllegalStateException("Cannot build empty batch");
            }
            return new RecordBatch(baseOffset, records);
        }
    }

    /**
     * Create a builder for this batch.
     */
    public static Builder builder(long baseOffset) {
        return new Builder(baseOffset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecordBatch batch = (RecordBatch) o;
        return baseOffset == batch.baseOffset &&
               recordCount == batch.recordCount &&
               records.equals(batch.records);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseOffset, recordCount, records);
    }

    @Override
    public String toString() {
        return String.format("RecordBatch{baseOffset=%d, recordCount=%d, lastOffset=%d, sizeInBytes=%d}",
            baseOffset, recordCount, lastOffset(), sizeInBytes());
    }

    /**
     * Statistics about this batch (useful for debugging/monitoring).
     */
    public static class Stats {
        public final int recordCount;
        public final int totalBytes;
        public final int avgBytesPerRecord;
        public final long minTimestamp;
        public final long maxTimestamp;

        private Stats(RecordBatch batch) {
            this.recordCount = batch.recordCount;
            this.totalBytes = batch.sizeInBytes();
            this.avgBytesPerRecord = totalBytes / recordCount;

            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            for (Record record : batch.records) {
                min = Math.min(min, record.timestamp());
                max = Math.max(max, record.timestamp());
            }
            this.minTimestamp = min;
            this.maxTimestamp = max;
        }

        @Override
        public String toString() {
            return String.format(
                "Stats{records=%d, bytes=%d, avg=%d bytes/record, timespan=%dms}",
                recordCount, totalBytes, avgBytesPerRecord, maxTimestamp - minTimestamp
            );
        }
    }

    /**
     * Get statistics about this batch.
     */
    public Stats stats() {
        return new Stats(this);
    }
}
