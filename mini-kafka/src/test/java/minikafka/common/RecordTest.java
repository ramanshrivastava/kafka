package minikafka.common;

import minikafka.common.record.Record;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for Record class.
 *
 * Testing strategy:
 * - Serialization/deserialization round-trip
 * - Edge cases (null key, empty value, large values)
 * - Size calculations
 * - Immutability guarantees
 */
class RecordTest {

    @Test
    void testCreateRecordWithKeyAndValue() {
        byte[] key = "user-123".getBytes(StandardCharsets.UTF_8);
        byte[] value = "login".getBytes(StandardCharsets.UTF_8);

        Record record = new Record(0, 1234567890L, key, value);

        assertThat(record.offset()).isEqualTo(0);
        assertThat(record.timestamp()).isEqualTo(1234567890L);
        assertThat(record.key()).isEqualTo(key);
        assertThat(record.value()).isEqualTo(value);
        assertThat(record.hasKey()).isTrue();
    }

    @Test
    void testCreateRecordWithNullKey() {
        byte[] value = "some-value".getBytes(StandardCharsets.UTF_8);

        Record record = new Record(10, 1000L, null, value);

        assertThat(record.offset()).isEqualTo(10);
        assertThat(record.key()).isNull();
        assertThat(record.hasKey()).isFalse();
        assertThat(record.value()).isEqualTo(value);
    }

    @Test
    void testCannotCreateRecordWithNullValue() {
        assertThatThrownBy(() -> new Record(0, 1000L, null, null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("value cannot be null");
    }

    @Test
    void testRecordImmutability() {
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        Record record = new Record(0, 1000L, key, value);

        // Modify original arrays
        key[0] = 'X';
        value[0] = 'X';

        // Record should still have original values (defensive copy)
        assertThat(record.key()[0]).isEqualTo((byte) 'k');
        assertThat(record.value()[0]).isEqualTo((byte) 'v');

        // Modify returned arrays
        byte[] returnedKey = record.key();
        byte[] returnedValue = record.value();
        returnedKey[0] = 'Y';
        returnedValue[0] = 'Y';

        // Record should still have original values
        assertThat(record.key()[0]).isEqualTo((byte) 'k');
        assertThat(record.value()[0]).isEqualTo((byte) 'v');
    }

    @Test
    void testSizeInBytes() {
        // Record with key and value
        byte[] key = "123".getBytes(StandardCharsets.UTF_8);  // 3 bytes
        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);  // 5 bytes

        Record record = new Record(0, 1000L, key, value);

        // Expected: 8 (offset) + 8 (timestamp) + 4 (key len) + 3 (key) + 4 (value len) + 5 (value) = 32
        assertThat(record.sizeInBytes()).isEqualTo(32);
    }

    @Test
    void testSizeInBytesWithNullKey() {
        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);  // 5 bytes

        Record record = new Record(0, 1000L, null, value);

        // Expected: 8 (offset) + 8 (timestamp) + 4 (key len = -1) + 0 (no key) + 4 (value len) + 5 (value) = 29
        assertThat(record.sizeInBytes()).isEqualTo(29);
    }

    @Test
    void testSerializeAndDeserialize() {
        byte[] key = "user-123".getBytes(StandardCharsets.UTF_8);
        byte[] value = "login-event".getBytes(StandardCharsets.UTF_8);

        Record original = new Record(42, 1234567890L, key, value);

        // Serialize
        ByteBuffer buffer = original.serialize();

        // Deserialize
        Record deserialized = Record.deserialize(buffer);

        // Should be equal
        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.offset()).isEqualTo(42);
        assertThat(deserialized.timestamp()).isEqualTo(1234567890L);
        assertThat(deserialized.key()).isEqualTo(key);
        assertThat(deserialized.value()).isEqualTo(value);
    }

    @Test
    void testSerializeAndDeserializeWithNullKey() {
        byte[] value = "some-value".getBytes(StandardCharsets.UTF_8);

        Record original = new Record(100, 9999L, null, value);

        ByteBuffer buffer = original.serialize();
        Record deserialized = Record.deserialize(buffer);

        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.key()).isNull();
        assertThat(deserialized.value()).isEqualTo(value);
    }

    @Test
    void testSerializeAndDeserializeEmptyValue() {
        byte[] emptyValue = new byte[0];

        Record original = new Record(0, 1000L, null, emptyValue);

        ByteBuffer buffer = original.serialize();
        Record deserialized = Record.deserialize(buffer);

        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.value()).isEmpty();
    }

    @Test
    void testSerializeAndDeserializeLargeValue() {
        // 10KB value
        byte[] largeValue = new byte[10 * 1024];
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) (i % 256);
        }

        Record original = new Record(0, 1000L, null, largeValue);

        ByteBuffer buffer = original.serialize();
        Record deserialized = Record.deserialize(buffer);

        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.value()).isEqualTo(largeValue);
    }

    @Test
    void testDeserializeInvalidBuffer() {
        // Buffer too small
        ByteBuffer tooSmall = ByteBuffer.allocate(10);

        assertThatThrownBy(() -> Record.deserialize(tooSmall))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("too small");
    }

    @Test
    void testDeserializeInvalidKeyLength() {
        ByteBuffer buffer = ByteBuffer.allocate(100);
        buffer.putLong(0);      // offset
        buffer.putLong(1000);   // timestamp
        buffer.putInt(1000);    // key length (too large)
        buffer.flip();

        assertThatThrownBy(() -> Record.deserialize(buffer))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testMultipleRecordsSerialization() {
        // Test that we can serialize multiple records in sequence
        Record r1 = new Record(0, 1000L, "k1".getBytes(), "v1".getBytes());
        Record r2 = new Record(1, 2000L, "k2".getBytes(), "v2".getBytes());
        Record r3 = new Record(2, 3000L, null, "v3".getBytes());

        // Serialize all to one buffer
        ByteBuffer buffer = ByteBuffer.allocate(
            r1.sizeInBytes() + r2.sizeInBytes() + r3.sizeInBytes()
        );

        buffer.put(r1.serialize());
        buffer.put(r2.serialize());
        buffer.put(r3.serialize());
        buffer.flip();

        // Deserialize all
        Record d1 = Record.deserialize(buffer);
        Record d2 = Record.deserialize(buffer);
        Record d3 = Record.deserialize(buffer);

        assertThat(d1).isEqualTo(r1);
        assertThat(d2).isEqualTo(r2);
        assertThat(d3).isEqualTo(r3);
    }

    @Test
    void testEquals() {
        Record r1 = new Record(0, 1000L, "key".getBytes(), "value".getBytes());
        Record r2 = new Record(0, 1000L, "key".getBytes(), "value".getBytes());
        Record r3 = new Record(1, 1000L, "key".getBytes(), "value".getBytes()); // different offset

        assertThat(r1).isEqualTo(r2);
        assertThat(r1).isNotEqualTo(r3);
        assertThat(r1).isNotEqualTo(null);
        assertThat(r1).isNotEqualTo("string");
    }

    @Test
    void testHashCode() {
        Record r1 = new Record(0, 1000L, "key".getBytes(), "value".getBytes());
        Record r2 = new Record(0, 1000L, "key".getBytes(), "value".getBytes());

        assertThat(r1.hashCode()).isEqualTo(r2.hashCode());
    }

    @Test
    void testToString() {
        Record record = new Record(42, 1234567890L, "key".getBytes(), "value".getBytes());

        String str = record.toString();

        assertThat(str).contains("offset=42");
        assertThat(str).contains("timestamp=1234567890");
        assertThat(str).contains("key=byte[3]");
        assertThat(str).contains("valueSize=5");
    }

    @Test
    void testToStringWithNullKey() {
        Record record = new Record(0, 1000L, null, "value".getBytes());

        String str = record.toString();

        assertThat(str).contains("key=null");
    }

    /**
     * Performance test: Serialization should be fast.
     * This is not a strict benchmark, just a sanity check.
     */
    @Test
    void testSerializationPerformance() {
        byte[] key = "user-12345".getBytes(StandardCharsets.UTF_8);
        byte[] value = "event-data-here".getBytes(StandardCharsets.UTF_8);

        long start = System.nanoTime();

        for (int i = 0; i < 10_000; i++) {
            Record record = new Record(i, System.currentTimeMillis(), key, value);
            ByteBuffer buffer = record.serialize();
            Record deserialized = Record.deserialize(buffer);
        }

        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        // Should be able to do 10K serialization round-trips in under 500ms
        // (This is very conservative; actual should be much faster)
        assertThat(elapsedMs).isLessThan(500);

        System.out.printf("Serialized/deserialized 10,000 records in %d ms%n", elapsedMs);
    }
}
