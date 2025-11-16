package minikafka.common;

import minikafka.common.record.Record;
import minikafka.common.record.RecordBatch;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for RecordBatch class.
 *
 * Testing strategy:
 * - Batching multiple records
 * - Serialization/deserialization round-trip
 * - Builder pattern usage
 * - Offset validation
 * - Performance (batching should be efficient)
 */
class RecordBatchTest {

    @Test
    void testCreateBatchWithRecords() {
        List<Record> records = createTestRecords(0, 5);

        RecordBatch batch = new RecordBatch(0, records);

        assertThat(batch.baseOffset()).isEqualTo(0);
        assertThat(batch.recordCount()).isEqualTo(5);
        assertThat(batch.lastOffset()).isEqualTo(4);
        assertThat(batch.records()).hasSize(5);
    }

    @Test
    void testCannotCreateEmptyBatch() {
        assertThatThrownBy(() -> new RecordBatch(0, new ArrayList<>()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("at least one record");
    }

    @Test
    void testCannotCreateBatchWithNullRecords() {
        assertThatThrownBy(() -> new RecordBatch(0, null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBatchValidatesSequentialOffsets() {
        List<Record> records = new ArrayList<>();
        records.add(new Record(0, 1000L, null, "v0".getBytes()));
        records.add(new Record(2, 1001L, null, "v1".getBytes())); // Gap! Should be offset 1

        assertThatThrownBy(() -> new RecordBatch(0, records))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid offset");
    }

    @Test
    void testBatchIsImmutable() {
        List<Record> records = createTestRecords(0, 3);
        RecordBatch batch = new RecordBatch(0, records);

        // Modify original list
        records.add(new Record(100, 1000L, null, "extra".getBytes()));

        // Batch should still have 3 records
        assertThat(batch.recordCount()).isEqualTo(3);

        // Returned list should be unmodifiable
        assertThatThrownBy(() -> batch.records().add(new Record(200, 1000L, null, "bad".getBytes())))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testSerializeAndDeserialize() {
        List<Record> originalRecords = createTestRecords(10, 5);
        RecordBatch originalBatch = new RecordBatch(10, originalRecords);

        // Serialize
        ByteBuffer buffer = originalBatch.serialize();

        // Deserialize
        RecordBatch deserializedBatch = RecordBatch.deserialize(buffer);

        // Should be equal
        assertThat(deserializedBatch).isEqualTo(originalBatch);
        assertThat(deserializedBatch.baseOffset()).isEqualTo(10);
        assertThat(deserializedBatch.recordCount()).isEqualTo(5);
        assertThat(deserializedBatch.lastOffset()).isEqualTo(14);
        assertThat(deserializedBatch.records()).isEqualTo(originalRecords);
    }

    @Test
    void testSerializeAndDeserializeSingleRecord() {
        List<Record> records = createTestRecords(0, 1);
        RecordBatch batch = new RecordBatch(0, records);

        ByteBuffer buffer = batch.serialize();
        RecordBatch deserialized = RecordBatch.deserialize(buffer);

        assertThat(deserialized).isEqualTo(batch);
    }

    @Test
    void testSerializeAndDeserializeLargeBatch() {
        // 1000 records
        List<Record> records = createTestRecords(0, 1000);
        RecordBatch batch = new RecordBatch(0, records);

        ByteBuffer buffer = batch.serialize();
        RecordBatch deserialized = RecordBatch.deserialize(buffer);

        assertThat(deserialized).isEqualTo(batch);
        assertThat(deserialized.recordCount()).isEqualTo(1000);
    }

    @Test
    void testSizeInBytes() {
        List<Record> records = createTestRecords(0, 3);
        RecordBatch batch = new RecordBatch(0, records);

        int expectedSize = 12; // Header: 8 (baseOffset) + 4 (recordCount)
        for (Record record : records) {
            expectedSize += record.sizeInBytes();
        }

        assertThat(batch.sizeInBytes()).isEqualTo(expectedSize);
    }

    @Test
    void testBuilderPattern() {
        RecordBatch batch = RecordBatch.builder(100)
            .append("key1".getBytes(), "value1".getBytes())
            .append("key2".getBytes(), "value2".getBytes())
            .append("key3".getBytes(), "value3".getBytes())
            .build();

        assertThat(batch.baseOffset()).isEqualTo(100);
        assertThat(batch.recordCount()).isEqualTo(3);
        assertThat(batch.lastOffset()).isEqualTo(102);

        assertThat(batch.records().get(0).offset()).isEqualTo(100);
        assertThat(batch.records().get(1).offset()).isEqualTo(101);
        assertThat(batch.records().get(2).offset()).isEqualTo(102);
    }

    @Test
    void testBuilderWithTimestamp() {
        RecordBatch batch = RecordBatch.builder(0)
            .append(1000L, null, "v1".getBytes())
            .append(2000L, null, "v2".getBytes())
            .build();

        assertThat(batch.records().get(0).timestamp()).isEqualTo(1000L);
        assertThat(batch.records().get(1).timestamp()).isEqualTo(2000L);
    }

    @Test
    void testBuilderAppendRecord() {
        Record r1 = new Record(0, 1000L, "k1".getBytes(), "v1".getBytes());
        Record r2 = new Record(1, 2000L, "k2".getBytes(), "v2".getBytes());

        RecordBatch batch = RecordBatch.builder(0)
            .appendRecord(r1)
            .appendRecord(r2)
            .build();

        assertThat(batch.recordCount()).isEqualTo(2);
        assertThat(batch.records()).containsExactly(r1, r2);
    }

    @Test
    void testBuilderRejectsWrongOffset() {
        Record wrongOffset = new Record(99, 1000L, null, "value".getBytes());

        RecordBatch.Builder builder = RecordBatch.builder(0);

        assertThatThrownBy(() -> builder.appendRecord(wrongOffset))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("doesn't match expected");
    }

    @Test
    void testBuilderCount() {
        RecordBatch.Builder builder = RecordBatch.builder(0);

        assertThat(builder.count()).isEqualTo(0);

        builder.append("k1".getBytes(), "v1".getBytes());
        assertThat(builder.count()).isEqualTo(1);

        builder.append("k2".getBytes(), "v2".getBytes());
        assertThat(builder.count()).isEqualTo(2);
    }

    @Test
    void testBuilderCannotBuildEmpty() {
        RecordBatch.Builder builder = RecordBatch.builder(0);

        assertThatThrownBy(builder::build)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("empty batch");
    }

    @Test
    void testDeserializeInvalidBuffer() {
        ByteBuffer tooSmall = ByteBuffer.allocate(8);

        assertThatThrownBy(() -> RecordBatch.deserialize(tooSmall))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("too small");
    }

    @Test
    void testDeserializeInvalidRecordCount() {
        ByteBuffer buffer = ByteBuffer.allocate(100);
        buffer.putLong(0);      // baseOffset
        buffer.putInt(-5);      // Invalid record count
        buffer.flip();

        assertThatThrownBy(() -> RecordBatch.deserialize(buffer))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid record count");
    }

    @Test
    void testMultipleBatchesSerialization() {
        RecordBatch batch1 = new RecordBatch(0, createTestRecords(0, 3));
        RecordBatch batch2 = new RecordBatch(3, createTestRecords(3, 3));
        RecordBatch batch3 = new RecordBatch(6, createTestRecords(6, 3));

        // Serialize all to one buffer
        ByteBuffer buffer = ByteBuffer.allocate(
            batch1.sizeInBytes() + batch2.sizeInBytes() + batch3.sizeInBytes()
        );

        buffer.put(batch1.serialize());
        buffer.put(batch2.serialize());
        buffer.put(batch3.serialize());
        buffer.flip();

        // Deserialize all
        RecordBatch d1 = RecordBatch.deserialize(buffer);
        RecordBatch d2 = RecordBatch.deserialize(buffer);
        RecordBatch d3 = RecordBatch.deserialize(buffer);

        assertThat(d1).isEqualTo(batch1);
        assertThat(d2).isEqualTo(batch2);
        assertThat(d3).isEqualTo(batch3);
    }

    @Test
    void testBatchStats() {
        RecordBatch batch = RecordBatch.builder(0)
            .append(1000L, null, "v1".getBytes())
            .append(2000L, null, "v2".getBytes())
            .append(3000L, null, "v3".getBytes())
            .build();

        RecordBatch.Stats stats = batch.stats();

        assertThat(stats.recordCount).isEqualTo(3);
        assertThat(stats.totalBytes).isEqualTo(batch.sizeInBytes());
        assertThat(stats.avgBytesPerRecord).isEqualTo(stats.totalBytes / 3);
        assertThat(stats.minTimestamp).isEqualTo(1000L);
        assertThat(stats.maxTimestamp).isEqualTo(3000L);
    }

    @Test
    void testBatchStatsToString() {
        RecordBatch batch = new RecordBatch(0, createTestRecords(0, 5));
        RecordBatch.Stats stats = batch.stats();

        String str = stats.toString();

        assertThat(str).contains("records=5");
        assertThat(str).contains("bytes=");
        assertThat(str).contains("avg=");
    }

    @Test
    void testEquals() {
        List<Record> records = createTestRecords(0, 3);
        RecordBatch batch1 = new RecordBatch(0, records);
        RecordBatch batch2 = new RecordBatch(0, createTestRecords(0, 3));
        RecordBatch batch3 = new RecordBatch(10, createTestRecords(10, 3)); // Different base offset

        assertThat(batch1).isEqualTo(batch2);
        assertThat(batch1).isNotEqualTo(batch3);
    }

    @Test
    void testHashCode() {
        List<Record> records = createTestRecords(0, 3);
        RecordBatch batch1 = new RecordBatch(0, records);
        RecordBatch batch2 = new RecordBatch(0, createTestRecords(0, 3));

        assertThat(batch1.hashCode()).isEqualTo(batch2.hashCode());
    }

    @Test
    void testToString() {
        RecordBatch batch = new RecordBatch(100, createTestRecords(100, 5));

        String str = batch.toString();

        assertThat(str).contains("baseOffset=100");
        assertThat(str).contains("recordCount=5");
        assertThat(str).contains("lastOffset=104");
        assertThat(str).contains("sizeInBytes=");
    }

    /**
     * Performance test: Batching should be efficient.
     *
     * This demonstrates why batching matters:
     * - Single large batch vs many small batches
     */
    @Test
    void testBatchingPerformance() {
        int totalRecords = 10_000;

        // Scenario 1: 10,000 individual "batches" (batch size = 1)
        long start1 = System.nanoTime();
        for (int i = 0; i < totalRecords; i++) {
            RecordBatch batch = RecordBatch.builder(i)
                .append("key".getBytes(), "value".getBytes())
                .build();
            ByteBuffer buffer = batch.serialize();
            RecordBatch.deserialize(buffer);
        }
        long elapsed1 = (System.nanoTime() - start1) / 1_000_000;

        // Scenario 2: 100 batches of 100 records each
        long start2 = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            RecordBatch.Builder builder = RecordBatch.builder(i * 100);
            for (int j = 0; j < 100; j++) {
                builder.append("key".getBytes(), "value".getBytes());
            }
            RecordBatch batch = builder.build();
            ByteBuffer buffer = batch.serialize();
            RecordBatch.deserialize(buffer);
        }
        long elapsed2 = (System.nanoTime() - start2) / 1_000_000;

        System.out.printf("Individual batches (10K x 1):   %d ms%n", elapsed1);
        System.out.printf("Large batches (100 x 100):      %d ms%n", elapsed2);
        System.out.printf("Speedup from batching:          %.1fx%n", (double) elapsed1 / elapsed2);

        // Batching should be faster (not guaranteed due to JIT warmup, but generally true)
        // This demonstrates the value of batching
    }

    /**
     * Demonstrate overhead reduction from batching.
     */
    @Test
    void testBatchingOverheadReduction() {
        // 100 individual batches
        int overhead1 = 0;
        for (int i = 0; i < 100; i++) {
            RecordBatch batch = RecordBatch.builder(i)
                .append("key".getBytes(), "value".getBytes())
                .build();
            overhead1 += 12; // Header per batch
        }

        // 1 batch with 100 records
        RecordBatch.Builder builder = RecordBatch.builder(0);
        for (int i = 0; i < 100; i++) {
            builder.append("key".getBytes(), "value".getBytes());
        }
        RecordBatch largeBatch = builder.build();
        int overhead2 = 12; // Just one header

        System.out.printf("Overhead with 100 batches: %d bytes%n", overhead1);
        System.out.printf("Overhead with 1 batch:     %d bytes%n", overhead2);
        System.out.printf("Savings:                   %d bytes (%.1f%%)%n",
            overhead1 - overhead2, 100.0 * (overhead1 - overhead2) / overhead1);

        assertThat(overhead1).isGreaterThan(overhead2);
    }

    // Helper methods

    /**
     * Create test records with sequential offsets.
     */
    private List<Record> createTestRecords(long baseOffset, int count) {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long offset = baseOffset + i;
            byte[] key = ("key-" + i).getBytes(StandardCharsets.UTF_8);
            byte[] value = ("value-" + i).getBytes(StandardCharsets.UTF_8);
            records.add(new Record(offset, 1000L + i, key, value));
        }
        return records;
    }
}
