package minikafka.common.record;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * A single message record in Mini-Kafka.
 *
 * Design decisions:
 * - Immutable: Records cannot be modified after creation (like Kafka)
 * - Simple structure: key, value, timestamp, offset
 * - No headers initially (added in Phase 1.2)
 *
 * Historical context:
 * - Kafka 0.7 (2011): Original simple message format
 * - Kafka 0.10 (2016): Added timestamps
 * - Kafka 0.11 (2017): Added headers
 *
 * Apache Kafka reference:
 * /home/user/kafka/clients/src/main/java/org/apache/kafka/common/record/Record.java
 *
 * Differences from Apache Kafka:
 * - We use simple byte[] for key/value (Kafka uses ByteBuffer for zero-copy)
 * - No compression at individual record level (handled at batch level)
 * - No headers yet (keeping it simple for Phase 1.1)
 *
 * @see RecordBatch for batching multiple records
 */
public class Record {

    // Record fields
    private final long offset;          // Position in partition log
    private final long timestamp;       // When record was created
    private final byte[] key;           // Optional key for partitioning/compaction
    private final byte[] value;         // Actual message payload

    /**
     * Size calculation (for serialization):
     * - offset: 8 bytes (long)
     * - timestamp: 8 bytes (long)
     * - key length: 4 bytes (int) + key.length
     * - value length: 4 bytes (int) + value.length
     * Total: 24 + key.length + value.length
     */

    /**
     * Create a new record.
     *
     * @param offset Position in the partition log (assigned by broker)
     * @param timestamp Timestamp in milliseconds since epoch
     * @param key Optional key (can be null)
     * @param value Message value (required)
     */
    public Record(long offset, long timestamp, byte[] key, byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("Record value cannot be null");
        }

        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key; // null is allowed for key
        this.value = value;
    }

    /**
     * Create a record with system timestamp.
     */
    public Record(long offset, byte[] key, byte[] value) {
        this(offset, System.currentTimeMillis(), key, value);
    }

    // Getters

    public long offset() {
        return offset;
    }

    public long timestamp() {
        return timestamp;
    }

    public byte[] key() {
        // Return copy to maintain immutability
        return key == null ? null : Arrays.copyOf(key, key.length);
    }

    public byte[] value() {
        // Return copy to maintain immutability
        return Arrays.copyOf(value, value.length);
    }

    public boolean hasKey() {
        return key != null;
    }

    /**
     * Calculate the serialized size of this record in bytes.
     *
     * Format:
     * - offset: 8 bytes
     * - timestamp: 8 bytes
     * - key length: 4 bytes
     * - key: variable
     * - value length: 4 bytes
     * - value: variable
     */
    public int sizeInBytes() {
        int size = 8 + 8 + 4 + 4; // offset + timestamp + key length + value length
        size += key == null ? 0 : key.length;
        size += value.length;
        return size;
    }

    /**
     * Serialize this record to a ByteBuffer.
     *
     * Wire format:
     * +--------+------------+-----------+-----+--------------+-------+
     * | Offset | Timestamp  | KeyLength | Key | ValueLength  | Value |
     * | 8B     | 8B         | 4B        | ... | 4B           | ...   |
     * +--------+------------+-----------+-----+--------------+-------+
     *
     * This is a simplified version of Kafka's format.
     * Real Kafka uses varints, compression, and more complex encoding.
     */
    public ByteBuffer serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(sizeInBytes());

        buffer.putLong(offset);
        buffer.putLong(timestamp);

        // Key (length-prefixed, -1 for null)
        if (key == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(key.length);
            buffer.put(key);
        }

        // Value (length-prefixed)
        buffer.putInt(value.length);
        buffer.put(value);

        buffer.flip();
        return buffer;
    }

    /**
     * Deserialize a record from a ByteBuffer.
     *
     * @throws IllegalArgumentException if buffer doesn't contain valid record
     */
    public static Record deserialize(ByteBuffer buffer) {
        if (buffer.remaining() < 24) { // Minimum size
            throw new IllegalArgumentException("Buffer too small for Record");
        }

        long offset = buffer.getLong();
        long timestamp = buffer.getLong();

        // Read key
        int keyLength = buffer.getInt();
        byte[] key = null;
        if (keyLength >= 0) {
            if (buffer.remaining() < keyLength) {
                throw new IllegalArgumentException("Invalid key length");
            }
            key = new byte[keyLength];
            buffer.get(key);
        }

        // Read value
        int valueLength = buffer.getInt();
        if (valueLength < 0 || buffer.remaining() < valueLength) {
            throw new IllegalArgumentException("Invalid value length");
        }
        byte[] value = new byte[valueLength];
        buffer.get(value);

        return new Record(offset, timestamp, key, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return offset == record.offset &&
               timestamp == record.timestamp &&
               Arrays.equals(key, record.key) &&
               Arrays.equals(value, record.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(offset, timestamp);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return String.format("Record{offset=%d, timestamp=%d, key=%s, valueSize=%d}",
            offset, timestamp,
            key == null ? "null" : "byte[" + key.length + "]",
            value.length);
    }
}
