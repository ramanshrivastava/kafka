# ADR-001: Append-Only Log Storage for Message Persistence

**Status**: Accepted
**Date**: 2025-01-15
**Commit**: Phase 2.1 (Log segment implementation)
**Kafka Reference**: `/home/user/kafka/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java`
**Related KIPs**: N/A (foundational design from 2011)
**Historical**: Kafka 0.7.0 (2011) - Original design

---

## Context

### The Problem
We need to persist messages sent to Kafka topics in a way that:
1. Supports high write throughput (100K+ messages/second)
2. Allows efficient reads from arbitrary offsets
3. Enables message replay (consumers can re-read old messages)
4. Simple and reliable (minimal complexity)

### Why This Matters
Traditional message queues (like RabbitMQ, ActiveMQ) delete messages after consumption. This makes:
- Re-processing impossible (can't replay)
- Debugging hard (messages are gone)
- Multiple consumers complex (need separate queues)

### Kafka's Original Use Case (LinkedIn 2011)
LinkedIn needed to process billions of events (page views, searches, etc.) for analytics and monitoring. They needed:
- **High throughput**: Handle peak loads
- **Replay**: Reprocess data when algorithms change
- **Multi-subscriber**: Same data to multiple systems

---

## Decision

**We will use append-only log files for message storage, organized into segments.**

### Structure
```
Topic: "events", Partition: 0

/data/events-0/
├── 00000000000000000000.log      ← First segment (base offset 0)
├── 00000000000000000000.index    ← Offset index for segment
├── 00000000000001234567.log      ← Second segment (base offset 1234567)
├── 00000000000001234567.index
└── 00000000000002345678.log      ← Active segment (currently appending)
    └── 00000000000002345678.index
```

### Core Concepts

**1. Append-Only**
```java
// Writing is always at the end
class LogSegment {
    FileChannel channel;
    long nextOffset;

    void append(RecordBatch batch) {
        // Always write at end (append)
        channel.position(channel.size());
        channel.write(batch.serialize());
        nextOffset += batch.recordCount();
    }
}
```

**2. Segment Rolling**
When a segment reaches a size limit (e.g., 1GB), create a new one:
```
Segment 0: [offset 0 - 999999]     (1GB, full)
Segment 1: [offset 1000000 - ...]  (active)
```

**3. Offset-Based Addressing**
Each message gets a unique, monotonically increasing offset within a partition:
```
Partition 0:
  offset 0: {"event": "login", "user": 123}
  offset 1: {"event": "purchase", "user": 456}
  offset 2: {"event": "logout", "user": 123}
  ...
```

**4. Retention**
Keep messages for a configured time (e.g., 7 days) then delete oldest segments:
```java
void deleteOldSegments() {
    long now = System.currentTimeMillis();
    for (LogSegment segment : segments) {
        if (now - segment.lastModifiedTime() > retentionMs) {
            segment.delete();
        }
    }
}
```

---

## Rationale

### Why Append-Only Logs?

#### 1. Sequential Writes are FAST
**Hard drive sequential vs random writes:**
- Sequential write: ~300 MB/sec (HDD), ~1 GB/sec (SSD)
- Random write: ~1 MB/sec (HDD), ~100 MB/sec (SSD)

**100-1000x performance difference!**

Append-only = always sequential writes = maximum throughput.

#### 2. Simple and Reliable
No complex data structures:
- No B-trees (like databases)
- No in-place updates (no corruption risk)
- No fragmentation
- Crash recovery is simple (just replay last segment)

#### 3. OS Page Cache Leveraging
Modern OS keeps recently accessed files in memory (page cache).
Kafka doesn't manage memory for caching - just relies on OS:
```
Write path:  App → Kafka → OS Page Cache → Disk (async)
Read path:   App ← Kafka ← OS Page Cache (if recent)
```

This is called "zero-copy" optimization in production Kafka.

#### 4. Enables Time Travel
Since messages aren't deleted immediately:
```
Consumer A: Reading from offset 1000 (latest)
Consumer B: Reading from offset 0 (reprocessing all data)
```

Both consumers can read independently at their own pace.

---

### Alternatives Considered

#### Alternative 1: Traditional Database (e.g., MySQL, PostgreSQL)

**Approach**:
```sql
CREATE TABLE messages (
    id BIGINT PRIMARY KEY,
    topic VARCHAR(100),
    partition INT,
    offset BIGINT,
    key BLOB,
    value BLOB,
    timestamp BIGINT
);

INSERT INTO messages VALUES (...);
```

**Pros**:
- ✅ Familiar technology
- ✅ Rich query capabilities (SQL)
- ✅ ACID transactions

**Cons**:
- ❌ Write throughput limited (~10K writes/sec with indexes)
- ❌ Random I/O for indexes (slow)
- ❌ Complex (query planner, transaction log, locks, etc.)
- ❌ Overkill (don't need SQL for append-only log)

**Why Rejected**: Too slow for high-throughput message streaming. Databases optimize for complex queries; we just need fast append and sequential read.

---

#### Alternative 2: In-Memory Queue (like Redis)

**Approach**:
```
Store all messages in memory
Fast reads/writes
Persist periodically to disk
```

**Pros**:
- ✅ Very fast (no disk I/O for reads)
- ✅ Simple

**Cons**:
- ❌ Limited by RAM size (expensive to scale)
- ❌ Data loss risk (if crash before persist)
- ❌ Can't store large history (retention limited by memory)

**Why Rejected**: Can't handle the scale (billions of messages). RAM too expensive for long retention.

---

#### Alternative 3: Log-Structured Merge Tree (LSM - like Cassandra, RocksDB)

**Approach**:
```
Write to memory (memtable)
Periodically flush to disk (SSTable)
Compact SSTables in background
```

**Pros**:
- ✅ Good write throughput (sequential writes)
- ✅ Supports updates/deletes (via tombstones)
- ✅ Proven technology

**Cons**:
- ❌ More complex than pure append-only
- ❌ Compaction overhead
- ❌ Don't need updates/deletes (messages are immutable)

**Why Rejected**: Append-only log is simpler and sufficient. LSM is overkill when messages are immutable.

---

## Kafka Comparison

### What Apache Kafka Does

**File**: `/home/user/kafka/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java`

Kafka's `LogSegment` (simplified):
```java
public class LogSegment {
    private final FileRecords log;           // Actual message file
    private final OffsetIndex offsetIndex;   // Offset → file position
    private final TimeIndex timeIndex;       // Timestamp → offset
    private final long baseOffset;           // First offset in segment

    public void append(long largestOffset,
                      long largestTimestamp,
                      long shallowOffsetOfMaxTimestamp,
                      MemoryRecords records) {
        // 1. Validate
        // 2. Append to log file
        // 3. Update indexes
        // 4. Update metadata
    }

    public FetchDataInfo read(long startOffset,
                             int maxSize,
                             long maxPosition,
                             boolean minOneMessage) {
        // 1. Lookup in index
        // 2. Read from file
        // 3. Return records
    }
}
```

**Additional features in real Kafka**:
1. **Time-based index** (find messages by timestamp)
2. **Transaction index** (for exactly-once semantics)
3. **Producer epoch tracking** (for idempotence)
4. **Compression** (batches can be compressed)
5. **Memory-mapped files** (for faster access)
6. **Zero-copy** (sendfile syscall to avoid kernel→user copy)

### What Mini-Kafka Does

**Simplified version**:
```java
public class SimpleLogSegment {
    private FileChannel fileChannel;
    private long baseOffset;
    private OffsetIndex index;

    public void append(RecordBatch batch) {
        long position = fileChannel.size();
        fileChannel.write(batch.toByteBuffer());

        // Update index (every 4KB)
        if (position % 4096 == 0) {
            index.append(batch.baseOffset(), position);
        }
    }

    public RecordBatch read(long offset) {
        // 1. Find position via index
        long position = index.lookup(offset);

        // 2. Read from file
        fileChannel.position(position);
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        fileChannel.read(buffer);

        // 3. Parse and return
        return RecordBatch.parse(buffer);
    }
}
```

**Differences**:

| Aspect | Mini-Kafka | Apache Kafka | Reason for Difference |
|--------|-----------|--------------|----------------------|
| Lines of code | ~400 | ~2000 | We skip edge cases, optimizations |
| Indexes | Offset only | Offset + Time + Txn | Learning focus, simplicity |
| I/O | Direct FileChannel | Memory-mapped + zero-copy | Performance vs simplicity |
| Compression | Optional | Multiple codecs | MVP simplification |
| Error handling | Basic | Extensive (corruption recovery) | Production vs learning |

---

## Historical Evolution

### Kafka 0.7.0 (2011): Original Design
**Context**: LinkedIn engineers (Jay Kreps, Neha Narkhede, Jun Rao) wanted a system that could:
- Handle LinkedIn's scale (billions of events/day)
- Replace complex lambda architecture (batch + stream)
- Learn from mistakes of earlier systems (Flume, Scribe)

**Key insight from Jay Kreps**:
> "Every database has a transaction log. What if we made that log the database itself?"

**Original implementation**:
- Simple log files
- Offset-based indexing
- No replication yet (!)
- ~500 lines of Scala code

**File**: Original commit https://github.com/apache/kafka/commit/fca5d4675f (historic)

### Kafka 0.10.0 (2016): Time-Based Indexing
**Why**: Users wanted to query "all messages from 3 days ago"
**Added**: TimeIndex alongside OffsetIndex

**File**: `/home/user/kafka/storage/src/main/java/org/apache/kafka/storage/internals/log/TimeIndex.java`

### Kafka 1.1.0 (2018): Memory-Mapped Files
**Why**: Reduce file I/O overhead
**Change**: Use mmap() for index files

### Kafka 2.1.0 (2018): Log Compaction Improvements
**Why**: Support changelog topics (keep only latest value per key)

---

## Trade-offs

### Benefits ✅

1. **Extreme Write Throughput**
   - Sequential writes → disk bandwidth saturation
   - Mini-Kafka: ~50K msg/sec
   - Real Kafka: ~1M msg/sec

2. **Simple and Reliable**
   - Fewer moving parts than database
   - Easy to debug (just files!)
   - Crash recovery: replay last segment

3. **Enables Replay**
   - Consumers can re-read old data
   - Critical for stream processing
   - Multiple consumers at different offsets

4. **OS Page Cache Efficiency**
   - Let OS manage caching
   - "Free" performance boost
   - No GC pressure (data in kernel memory)

5. **Predictable Performance**
   - No compaction pauses (unlike LSM trees)
   - No garbage collection spikes
   - Latency: O(log N) for index lookup + O(1) for read

### Limitations ❌

1. **No Updates or Deletes**
   - Messages are immutable
   - Can't edit published messages
   - Workaround: Log compaction (keeps latest value per key)

2. **Retention-Based Cleanup Only**
   - Can't delete specific messages
   - Keep data for time period (e.g., 7 days)
   - Wastes disk space if only need some messages

3. **Ordering Per Partition Only**
   - Total ordering requires single partition
   - Single partition = limited parallelism
   - Trade-off: Scale vs strict ordering

4. **Disk Space Requirements**
   - Must store all data until retention expires
   - High-throughput = lots of disk
   - Mitigated by compression, tiered storage (modern Kafka)

5. **Read Amplification for Old Data**
   - Reading old offset requires disk seek
   - OS page cache won't help (data cold)
   - Trade-off: Recent data fast, old data slower

---

## Learning Outcomes

After implementing this, you should understand:

### Conceptual

1. **Why sequential I/O matters**
   - Disk bandwidth vs latency
   - How to design for sequential access
   - Why append-only is fast

2. **OS page cache leveraging**
   - How OS caches file reads
   - When to use memory-mapped files
   - Why "zero GC" is possible

3. **Log-structured storage**
   - Append-only logs vs update-in-place
   - Segment management
   - Index design for sparse lookups

4. **Time-space trade-offs**
   - Keep all data vs delete after consume
   - Disk space for replayability
   - Retention policies

### Practical

1. **File I/O in Java**
   - FileChannel API
   - ByteBuffer usage
   - File locking and flushing

2. **Index design**
   - Sparse vs dense indexes
   - Binary search on sorted index
   - Memory vs accuracy trade-off

3. **Segment rolling**
   - When to create new segments
   - How to name segments (base offset)
   - Atomic operations (create, rename)

4. **Offset management**
   - Monotonic offset assignment
   - Offset → file position mapping
   - Handling wraparound (very large offsets)

### Comparative

1. **Append-only vs B-tree**
   - Write: append-only 100x faster
   - Random reads: B-tree faster
   - Use case fit: message log vs database

2. **Log-structured storage vs LSM**
   - LSM handles updates via compaction
   - Append-only simpler for immutable data
   - When to use each

3. **Page cache vs application cache**
   - OS page cache "free" and huge (GBs)
   - Application cache requires GC (Java heap)
   - Kafka's choice: rely on OS

---

## References

### Apache Kafka Source Code

1. **Log Segment**:
   - `/home/user/kafka/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java`
   - Key methods: `append()`, `read()`, `recover()`

2. **Offset Index**:
   - `/home/user/kafka/storage/src/main/java/org/apache/kafka/storage/internals/log/OffsetIndex.java`
   - Sparse index with binary search

3. **Unified Log** (manages all segments):
   - `/home/user/kafka/storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java`
   - Handles: append, read, roll, delete, recovery

### Design Documents

1. **Original Paper**:
   - "Kafka: a Distributed Messaging System for Log Processing" (NetDB 2011)
   - http://notes.stephenholiday.com/Kafka.pdf

2. **Jay Kreps' Blog Post**:
   - "The Log: What every software engineer should know about real-time data's unifying abstraction"
   - https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying

3. **Kafka Documentation**:
   - https://kafka.apache.org/documentation/#design
   - Section: "4.4 Log"

### Academic Papers

1. **Log-Structured File Systems** (Rosenblum & Ousterhout, 1992)
   - Original idea of treating disk as a log
   - Inspired modern log-structured storage

2. **The Design and Implementation of a Log-Structured File System** (TOCS 1992)
   - Academic foundation for append-only designs

### Related Commits

1. **Original log implementation**:
   - https://github.com/apache/kafka/commit/fca5d4675f (historic)

2. **Time index addition** (Kafka 0.10.0):
   - KIP-33: Add time index
   - https://cwiki.apache.org/confluence/display/KAFKA/KIP-33+-+Add+a+time+based+log+index

---

## Exercises

### Exercise 1: Implement Basic Log Segment
**Task**: Implement `LogSegment.append()` and `LogSegment.read()`
**Difficulty**: ⭐⭐☆☆☆
**Estimated Time**: 2 hours

**Requirements**:
```java
class LogSegment {
    void append(RecordBatch batch);  // Write to end of file
    RecordBatch read(long offset);   // Read specific offset
}
```

**Success criteria**:
- Write 10,000 records
- Read record at offset 5000
- Verify data correctness

**Hints**:
- Use `FileChannel` for I/O
- Use `ByteBuffer` for serialization
- Don't forget to flush!

---

### Exercise 2: Build Sparse Offset Index
**Task**: Implement an offset index that stores 1 entry per 4KB
**Difficulty**: ⭐⭐⭐☆☆
**Estimated Time**: 3 hours

**Requirements**:
```java
class OffsetIndex {
    void append(long offset, int position);  // Add index entry
    int lookup(long offset);                 // Binary search
}
```

**Success criteria**:
- Index grows much slower than log file
- Lookup is O(log N) where N = index entries
- Can find any offset (not just indexed ones)

**Hints**:
- Store pairs: (offset, filePosition)
- On lookup, find closest offset <= target
- Then scan forward in log file

---

### Exercise 3: Benchmark Sequential vs Random Writes
**Task**: Measure the performance difference
**Difficulty**: ⭐⭐⭐☆☆
**Estimated Time**: 2 hours

**Experiment**:
1. Write 1M records sequentially (append)
2. Write 1M records randomly (seek then write)
3. Measure throughput for each

**Expected results**:
- Sequential: ~100K records/sec
- Random: ~1K records/sec
- **100x difference!**

**Questions to answer**:
- Why is sequential so much faster?
- How does this justify Kafka's design?
- When would random writes be necessary?

---

### Exercise 4: Implement Segment Rolling
**Task**: Auto-create new segment when current reaches 1GB
**Difficulty**: ⭐⭐⭐☆☆
**Estimated Time**: 2 hours

**Requirements**:
```java
class Log {
    void append(RecordBatch batch) {
        if (activeSegment.size() >= MAX_SEGMENT_SIZE) {
            rollSegment();
        }
        activeSegment.append(batch);
    }

    void rollSegment() {
        // 1. Close current segment
        // 2. Create new segment with next base offset
        // 3. Set as active
    }
}
```

**Success criteria**:
- Write 2GB of data
- See multiple segment files created
- Can read across segment boundaries

---

### Exercise 5: Debug a Corruption Bug
**Task**: We've introduced a bug where log becomes corrupted after crash
**Difficulty**: ⭐⭐⭐⭐☆
**Estimated Time**: 3 hours

**Bug scenario**:
1. Write records to log
2. Kill process (simulate crash)
3. Restart and try to read
4. **Last few records are corrupted!**

**Your task**: Find the bug and fix it

**Hints**:
- What happens to unflushed data?
- How does FileChannel.write() work?
- When is data actually on disk?

**Solution**: Need to call `fileChannel.force(true)` to fsync!

---

## Conclusion

Append-only log storage is the foundation of Kafka's design. It enables:
- **High throughput** (sequential writes)
- **Simplicity** (just files!)
- **Replayability** (time travel)
- **Scalability** (horizontal partitioning)

This single design decision cascades through the entire system:
- Pull-based consumers (can choose offset)
- Retention-based cleanup (delete old segments)
- Offset-based addressing (monotonic position)

**Key Takeaway**: Sometimes the simplest approach (append to a file) is the best. Don't over-engineer with complex data structures when a log file suffices.

---

**Next**: ADR-002 will cover **Partitioning Strategy** - how Kafka scales horizontally by splitting topics into partitions.
