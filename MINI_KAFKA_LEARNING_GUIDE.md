# Mini Kafka Learning Guide
**Based on Apache Kafka Codebase Analysis**
*Historically-Grounded, Reasoning-Based Implementation*

---

## 📊 Quick Reference

### What's in Apache Kafka
- **Total Code**: ~192,000 lines (Java + Scala)
- **Files**: ~6,000 source files
- **Main Components**: 6 core systems
- **Message Protocol**: Binary protocol with 60+ request types
- **Storage**: Log-based append-only storage
- **Replication**: Leader/follower with ISR (In-Sync Replicas)

### What Your Mini Version Should Have
- **Target Size**: 10,000-15,000 lines of Java
- **Core APIs**: Producer, Consumer, Broker
- **Request Types**: ~10 essential (vs 60+ in Kafka)
- **Storage**: Simple log segments (no compaction initially)
- **Replication**: Basic leader-follower (no ISR initially)
- **Memory**: Simple buffer management (no zero-copy initially)

---

## 🏗️ Architecture Overview

```
Producer Client
       ↓
  SERIALIZATION     (500-1,000 lines)
       ↓
  Wire Protocol
       ↓
  NETWORK LAYER     (1,500-2,000 lines)
  ├─ Request Parser
  ├─ Response Builder
  └─ Connection Manager
       ↓
  BROKER/REQUEST HANDLER (2,000-3,000 lines)
  ├─ Request Router
  ├─ Partition Manager
  └─ Replica Manager
       ↓
  LOG STORAGE       (2,500-3,500 lines)
  ├─ Log Segments
  ├─ Index Files
  └─ Offset Management
       ↓
  REPLICATION       (1,500-2,500 lines)
  ├─ Leader Election
  ├─ Follower Sync
  └─ ISR Management
       ↓
  Consumer Client
```

---

## 📋 Component Design Details

### 1. MESSAGE SERIALIZATION (500-1,000 lines)
**Purpose**: Convert data structures ↔ binary wire format

**Historical Context**: Kafka 0.7.0 (2011) - Original binary protocol

**What to implement**:
- Message format (key, value, headers, timestamp)
- Record batch structure
- Compression (optional: GZIP, Snappy)
- CRC checksums

**Kafka Reference**:
- `/home/user/kafka/clients/src/main/java/org/apache/kafka/common/record/`
- Key files: `Record.java`, `RecordBatch.java`, `MemoryRecords.java`

**Simplified approach**:
- Fixed message format (no versioning initially)
- Simple length-prefixed encoding
- Optional compression
- Basic CRC32 checksum

**Example structure**:
```java
class SimpleRecord {
    long offset;
    long timestamp;
    byte[] key;
    byte[] value;
    Header[] headers;
}

class RecordBatch {
    long baseOffset;
    int batchLength;
    int recordCount;
    SimpleRecord[] records;
}
```

**Learning Outcomes**:
- Binary protocol design
- Efficient serialization techniques
- Why Kafka uses batching
- Trade-offs: efficiency vs complexity

---

### 2. NETWORK PROTOCOL (1,500-2,000 lines)
**Purpose**: Handle client-broker communication over TCP

**Historical Context**:
- Kafka 0.7 (2011): Original protocol
- Kafka 0.10 (2016): Protocol versioning
- Kafka 2.0 (2018): Request/response headers standardized

**Request types to implement** (~10 vs 60+ in Kafka):
1. **Produce** - Send messages to topics
2. **Fetch** - Retrieve messages
3. **Metadata** - Discover topics/partitions/leaders
4. **OffsetFetch** - Get consumer offsets
5. **OffsetCommit** - Commit consumer offsets
6. **CreateTopics** - Create new topics
7. **FindCoordinator** - Locate group coordinator
8. **JoinGroup** - Join consumer group
9. **SyncGroup** - Synchronize group state
10. **Heartbeat** - Keep consumer group membership alive

**Kafka Reference**:
- `/home/user/kafka/clients/src/main/resources/common/message/`
- `/home/user/kafka/clients/src/main/java/org/apache/kafka/common/requests/`

**Protocol structure**:
```java
// Request header
class RequestHeader {
    short apiKey;        // Which API (Produce=0, Fetch=1, etc.)
    short apiVersion;    // Protocol version
    int correlationId;   // Match request to response
    String clientId;     // Client identifier
}

// Response header
class ResponseHeader {
    int correlationId;   // Matches request
}

// Example: ProduceRequest
class ProduceRequest {
    short requiredAcks;  // 0, 1, or -1 (all ISR)
    int timeout;
    Map<String, TopicData> topics;
}
```

**Learning Outcomes**:
- Binary protocol design patterns
- Request/response correlation
- Protocol versioning strategies
- Backward/forward compatibility

---

### 3. LOG STORAGE (2,500-3,500 lines)
**Purpose**: Persist messages in append-only log segments

**Historical Context**:
- Kafka 0.7 (2011): Original log design inspired by commit logs
- Kafka 0.10 (2016): Time-based indexing
- Kafka 2.1 (2018): Log compaction improvements

**Core concepts**:
```
Topic: "orders"
├─ Partition 0
│  ├─ 00000000000000000000.log    (segment file)
│  ├─ 00000000000000000000.index  (offset index)
│  ├─ 00000000000000000000.timeindex (time index)
│  ├─ 00000000000000123456.log
│  ├─ 00000000000000123456.index
│  └─ ...
└─ Partition 1
   └─ ...
```

**Kafka Reference**:
- `/home/user/kafka/storage/src/main/java/org/apache/kafka/storage/internals/log/`
- Key files: `LogSegment.java`, `OffsetIndex.java`, `TimeIndex.java`
- Core module: `/home/user/kafka/core/src/main/scala/kafka/log/`

**Components to build**:

**A. Log Segment** (800-1,200 lines)
```java
class LogSegment {
    File logFile;           // Actual message data
    File indexFile;         // Offset → position mapping
    long baseOffset;        // First offset in segment
    long size;              // Current size in bytes

    void append(RecordBatch batch);
    RecordBatch read(long offset);
    void flush();
    void close();
}
```

**B. Offset Index** (400-600 lines)
```java
class OffsetIndex {
    // Sparse index: offset → file position
    // Not every offset, but sampled (e.g., every 4KB)

    void append(long offset, int position);
    int lookup(long offset);  // Find position for offset
}
```

**C. Log Manager** (1,000-1,500 lines)
```java
class Log {
    List<LogSegment> segments;
    long nextOffset;

    void append(RecordBatch batch);
    FetchResult read(long offset, int maxBytes);
    void roll();  // Create new segment
    void delete(long beforeOffset);  // Retention
}
```

**Simplified for MVP**:
- Fixed segment size (e.g., 1GB)
- Time-based retention only (skip size-based)
- No log compaction initially
- Simple offset index (skip time index initially)

**Learning Outcomes**:
- Why append-only logs are efficient
- Index design for fast lookups
- Segment rolling strategies
- Retention policies
- Compare to databases (B-trees vs append logs)

---

### 4. BROKER & REQUEST HANDLER (2,000-3,000 lines)
**Purpose**: Core server that processes requests and coordinates storage

**Historical Context**:
- Kafka 0.7 (2011): Single-threaded request handler
- Kafka 0.8 (2012): Multi-threaded request handler
- Kafka 2.8 (2021): KRaft mode (removes ZooKeeper)

**Kafka Reference**:
- `/home/user/kafka/core/src/main/scala/kafka/server/`
- Key files: `KafkaServer.scala`, `KafkaApis.scala`, `ReplicaManager.scala`

**Architecture**:
```java
class KafkaBroker {
    SocketServer socketServer;        // Accept connections
    RequestHandler[] handlers;        // Process requests
    ReplicaManager replicaManager;    // Manage partitions
    LogManager logManager;            // Manage logs
    MetadataCache metadataCache;      // Cluster state

    void startup();
    void shutdown();
}

class RequestHandler implements Runnable {
    void handle(ProduceRequest req);
    void handle(FetchRequest req);
    void handle(MetadataRequest req);
    // ... other request types
}
```

**Request flow**:
```
Client Request
     ↓
SocketServer (acceptor thread)
     ↓
Request Queue
     ↓
RequestHandler thread pool
     ↓
Route to appropriate handler
     ↓
ReplicaManager/LogManager
     ↓
Response Queue
     ↓
SocketServer (sender thread)
     ↓
Client Response
```

**Key operations**:

**A. Produce Request** (300-500 lines)
```java
void handleProduce(ProduceRequest request) {
    for (TopicPartition tp : request.partitions()) {
        // 1. Validate request
        // 2. Check if leader for partition
        // 3. Append to local log
        // 4. Replicate to followers (if acks=-1)
        // 5. Return response

        LogAppendResult result = replicaManager.appendRecords(
            tp, request.records(tp), request.acks()
        );
    }
}
```

**B. Fetch Request** (400-600 lines)
```java
void handleFetch(FetchRequest request) {
    for (TopicPartition tp : request.partitions()) {
        // 1. Check if replica for partition
        // 2. Read from log at requested offset
        // 3. Return records up to maxBytes

        FetchResult result = replicaManager.fetchRecords(
            tp, request.fetchOffset(tp), request.maxBytes()
        );
    }
}
```

**C. Metadata Request** (200-300 lines)
```java
void handleMetadata(MetadataRequest request) {
    // Return topic/partition/leader information
    for (String topic : request.topics()) {
        TopicMetadata metadata = metadataCache.getTopicMetadata(topic);
        // Include partition count, leader, replicas, ISR
    }
}
```

**Simplified for MVP**:
- Single-threaded request handler initially
- Skip ZooKeeper/KRaft (hardcode cluster config)
- No dynamic topic creation (pre-configure)
- Simple in-memory metadata

**Learning Outcomes**:
- Request routing patterns
- Thread pool design
- Synchronous vs asynchronous processing
- Acks levels (0, 1, -1) and durability guarantees

---

### 5. REPLICATION (1,500-2,500 lines)
**Purpose**: Ensure fault tolerance through data replication

**Historical Context**:
- Kafka 0.7 (2011): No replication!
- Kafka 0.8 (2012): Replication added (major milestone)
- Kafka 0.9 (2015): Improved replica fetcher

**Core concepts**:
```
Topic: "events", Partition: 0, Replication Factor: 3

Broker 1 (Leader)     Broker 2 (Follower)   Broker 3 (Follower)
├─ offset 0           ├─ offset 0           ├─ offset 0
├─ offset 1           ├─ offset 1           ├─ offset 1
├─ offset 2           ├─ offset 2           └─ (catching up...)
└─ offset 3 (LEO)     └─ offset 3 (LEO)

ISR = {1, 2}  (In-Sync Replicas - caught up within threshold)
```

**Kafka Reference**:
- `/home/user/kafka/core/src/main/scala/kafka/cluster/Partition.scala`
- `/home/user/kafka/core/src/main/scala/kafka/server/ReplicaManager.scala`
- `/home/user/kafka/core/src/main/scala/kafka/server/ReplicaFetcherThread.scala`

**Components**:

**A. Partition Leader** (600-1,000 lines)
```java
class PartitionLeader {
    Log localLog;
    Map<Integer, FollowerState> followers;  // brokerId → state
    Set<Integer> isr;  // In-Sync Replica set

    long append(RecordBatch batch) {
        long offset = localLog.append(batch);

        // Update high watermark when ISR catches up
        updateHighWatermark();

        return offset;
    }

    void updateHighWatermark() {
        // HW = minimum LEO across all ISR members
        long minLEO = followers.values().stream()
            .filter(f -> isr.contains(f.brokerId))
            .mapToLong(f -> f.logEndOffset)
            .min().orElse(localLog.logEndOffset());

        highWatermark = minLEO;
    }
}
```

**B. Replica Fetcher** (500-800 lines)
```java
class ReplicaFetcher implements Runnable {
    int leaderId;
    TopicPartition partition;
    long fetchOffset;

    void run() {
        while (running) {
            // 1. Fetch from leader
            FetchResponse response = fetchFromLeader(fetchOffset);

            // 2. Append to local log
            for (RecordBatch batch : response.records()) {
                localLog.append(batch);
            }

            // 3. Update fetch offset
            fetchOffset = localLog.logEndOffset();

            Thread.sleep(fetchWaitTime);
        }
    }
}
```

**C. Leader Election** (400-600 lines)
```java
class LeaderElector {
    void electLeader(TopicPartition partition) {
        // 1. Choose new leader from ISR
        // 2. If ISR is empty, choose from all replicas (unclean election)
        // 3. Update metadata
        // 4. Notify brokers

        List<Integer> isr = getISR(partition);
        int newLeader = isr.isEmpty() ?
            chooseFromAllReplicas(partition) :
            isr.get(0);

        updateMetadata(partition, newLeader);
    }
}
```

**Key algorithms**:
- **High Watermark (HW)**: Highest offset replicated to all ISR members
- **Log End Offset (LEO)**: Highest offset in a replica's log
- **ISR Management**: Add/remove replicas based on lag

**Simplified for MVP**:
- Static replication factor (no dynamic changes)
- Simple leader election (first ISR member)
- No unclean leader election initially
- Fixed ISR lag threshold

**Learning Outcomes**:
- Distributed consensus basics
- Leader/follower patterns
- Consistency vs availability trade-offs
- Why Kafka chose high watermark approach
- Compare to Raft/Paxos

---

### 6. PRODUCER CLIENT (1,000-1,500 lines)
**Purpose**: Client API for sending messages to Kafka

**Historical Context**:
- Kafka 0.7 (2011): Original synchronous producer
- Kafka 0.8.1 (2013): New async producer
- Kafka 0.9 (2015): New producer API (current design)

**Kafka Reference**:
- `/home/user/kafka/clients/src/main/java/org/apache/kafka/clients/producer/`
- Key files: `KafkaProducer.java`, `RecordAccumulator.java`, `Sender.java`

**Architecture**:
```java
class KafkaProducer {
    RecordAccumulator accumulator;  // Buffer records
    Sender sender;                  // Background I/O thread
    Partitioner partitioner;        // Choose partition

    Future<RecordMetadata> send(ProducerRecord record) {
        // 1. Serialize key/value
        // 2. Determine partition
        // 3. Add to accumulator
        // 4. Wake up sender thread

        byte[] serializedKey = keySerializer.serialize(record.key());
        byte[] serializedValue = valueSerializer.serialize(record.value());

        int partition = partitioner.partition(
            record.topic(), record.key(), serializedKey,
            record.value(), serializedValue, metadata
        );

        RecordAppendResult result = accumulator.append(
            new TopicPartition(record.topic(), partition),
            record.timestamp(),
            serializedKey,
            serializedValue,
            record.headers()
        );

        if (result.batchIsFull || result.newBatchCreated) {
            sender.wakeup();
        }

        return result.future;
    }
}

class Sender implements Runnable {
    void run() {
        while (running) {
            // 1. Get ready batches from accumulator
            // 2. Group by broker
            // 3. Send produce requests
            // 4. Handle responses

            Map<Integer, List<ProducerBatch>> batches =
                accumulator.drain(metadata);

            for (Map.Entry<Integer, List<ProducerBatch>> entry : batches) {
                sendProduceRequest(entry.getKey(), entry.getValue());
            }
        }
    }
}
```

**Key features**:
- **Batching**: Combine multiple records for efficiency
- **Compression**: Compress batches before sending
- **Async**: Non-blocking send with futures/callbacks
- **Retries**: Automatic retry on transient failures
- **Ordering**: Per-partition ordering guarantees

**Simplified for MVP**:
- Simple round-robin partitioner
- Fixed batch size/linger time
- Basic retry logic (no idempotence)
- No transactions

**Learning Outcomes**:
- Producer batching strategies
- Async I/O patterns
- Partitioning strategies
- Delivery guarantees (at-most-once, at-least-once, exactly-once)

---

### 7. CONSUMER CLIENT (1,500-2,000 lines)
**Purpose**: Client API for reading messages from Kafka

**Historical Context**:
- Kafka 0.7 (2011): Simple consumer (manual offset management)
- Kafka 0.8.2 (2014): High-level consumer with ZooKeeper
- Kafka 0.9 (2015): New consumer API with consumer groups

**Kafka Reference**:
- `/home/user/kafka/clients/src/main/java/org/apache/kafka/clients/consumer/`
- Key files: `KafkaConsumer.java`, `ConsumerCoordinator.java`, `Fetcher.java`

**Architecture**:
```java
class KafkaConsumer {
    ConsumerCoordinator coordinator;  // Group coordination
    Fetcher fetcher;                  // Fetch records
    SubscriptionState subscriptions;  // Track assigned partitions

    void subscribe(List<String> topics) {
        subscriptions.subscribe(topics);
        coordinator.requestRejoin();  // Join group
    }

    ConsumerRecords poll(Duration timeout) {
        // 1. Join group if needed
        coordinator.ensureActiveGroup();

        // 2. Fetch records
        Map<TopicPartition, List<ConsumerRecord>> records =
            fetcher.fetchedRecords();

        return new ConsumerRecords(records);
    }

    void commitSync() {
        coordinator.commitOffsetsSync(
            subscriptions.allConsumed()
        );
    }
}

class ConsumerCoordinator {
    void ensureActiveGroup() {
        if (!joinedGroup) {
            // 1. Find coordinator
            // 2. Send JoinGroup request
            // 3. Wait for partition assignment
            // 4. Send SyncGroup request
            // 5. Update subscriptions

            joinGroup();
        }

        sendHeartbeat();  // Keep membership alive
    }
}
```

**Consumer group protocol**:
```
Consumer Group: "analytics-group"
Topic: "events" (3 partitions)

Consumer 1 (Leader)          Consumer 2              Consumer 3
├─ Partition 0               ├─ Partition 1          ├─ Partition 2
└─ Offset: 1000             └─ Offset: 2000         └─ Offset: 500

Coordinator (Broker)
├─ Tracks group membership
├─ Monitors heartbeats
└─ Triggers rebalance on changes
```

**Rebalance protocol**:
1. **Join Phase**: All consumers send JoinGroup request
2. **Leader Selection**: Coordinator picks leader consumer
3. **Assignment**: Leader assigns partitions to consumers
4. **Sync Phase**: Leader sends assignment to coordinator
5. **Completion**: All consumers receive their assignment

**Simplified for MVP**:
- Simple range partition assignment
- Manual offset commits (skip auto-commit)
- No rebalance listeners
- Fixed session timeout

**Learning Outcomes**:
- Consumer groups and load balancing
- Rebalance protocol design
- Offset management strategies
- At-least-once vs at-most-once consumption
- Compare to other messaging systems (RabbitMQ, etc.)

---

## 📅 Phased Implementation Timeline

### Phase 1: Message Format & Serialization (Est. 3-4 commits, ~800 lines)
**Historical Context**: Kafka 0.7.0 (2011) - Original message format

| Commit | Feature | ADR | Kafka Ref | KIP/History | Lines | Learning Focus |
|--------|---------|-----|-----------|-------------|-------|----------------|
| 1.1 | Project setup + simple record format | ADR-001 | clients/.../record/Record.java | Original design (2011) | 200 | Binary serialization basics |
| 1.2 | Record batch structure | ADR-002 | clients/.../record/RecordBatch.java | Batching rationale | 300 | Batching for efficiency |
| 1.3 | CRC checksums & validation | ADR-003 | clients/.../record/Checksums.java | Data integrity | 200 | Error detection |
| 1.4 | Compression (GZIP) | ADR-004 | clients/.../compression/ | KIP-31 (compression) | 200 | Compression trade-offs |

**Checkpoint 1**: Serialize/deserialize a batch of 1000 records

---

### Phase 2: Log Storage (Est. 5 commits, ~2,500 lines)
**Historical Context**: Kafka 0.7 (2011) - Log-based storage design

| Commit | Feature | ADR | Kafka Ref | KIP/History | Lines | Learning Focus |
|--------|---------|-----|-----------|-------------|-------|----------------|
| 2.1 | Log segment file format | ADR-005 | storage/.../log/LogSegment.java | Append-only logs | 400 | Log-structured storage |
| 2.2 | Offset index implementation | ADR-006 | storage/.../log/OffsetIndex.java | Sparse indexing | 400 | Index design |
| 2.3 | Log manager & append | ADR-007 | storage/.../log/UnifiedLog.java | Log management | 600 | Segment rolling |
| 2.4 | Read/fetch from log | ADR-008 | storage/.../log/UnifiedLog.java | Fetch optimization | 500 | Efficient reads |
| 2.5 | Time-based retention | ADR-009 | storage/.../log/LogCleaner.scala | Retention policies | 400 | Cleanup strategies |

**Checkpoint 2**: Write 1M messages, read from specific offset

---

### Phase 3: Network Protocol (Est. 4 commits, ~1,500 lines)
**Historical Context**: Kafka 0.8 (2012) - Protocol standardization

| Commit | Feature | ADR | Kafka Ref | KIP/History | Lines | Learning Focus |
|--------|---------|-----|-----------|-------------|-------|----------------|
| 3.1 | Request/response headers | ADR-010 | clients/.../requests/RequestHeader.java | Protocol structure | 300 | Binary protocols |
| 3.2 | Produce request/response | ADR-011 | clients/.../requests/ProduceRequest.java | Produce API | 400 | Request design |
| 3.3 | Fetch request/response | ADR-012 | clients/.../requests/FetchRequest.java | Fetch API | 400 | Pull model |
| 3.4 | Metadata request/response | ADR-013 | clients/.../requests/MetadataRequest.java | Cluster discovery | 300 | Metadata propagation |

**Checkpoint 3**: Send produce/fetch requests over network

---

### Phase 4: Broker Core (Est. 5 commits, ~2,500 lines)
**Historical Context**: Kafka 0.8 (2012) - Multi-threaded broker

| Commit | Feature | ADR | Kafka Ref | KIP/History | Lines | Learning Focus |
|--------|---------|-----|-----------|-------------|-------|----------------|
| 4.1 | Socket server & connection handling | ADR-014 | core/.../network/SocketServer.scala | NIO networking | 600 | Java NIO |
| 4.2 | Request routing & dispatch | ADR-015 | core/.../server/KafkaApis.scala | Request handling | 500 | Request routing |
| 4.3 | Produce request handler | ADR-016 | core/.../server/KafkaApis.scala | Write path | 500 | Acks semantics |
| 4.4 | Fetch request handler | ADR-017 | core/.../server/KafkaApis.scala | Read path | 400 | Consumer fetch |
| 4.5 | Metadata cache & responses | ADR-018 | core/.../server/MetadataCache.scala | Metadata management | 400 | Caching strategies |

**Checkpoint 4**: Run a working broker, produce and consume

---

### Phase 5: Replication (Est. 4 commits, ~2,000 lines)
**Historical Context**: Kafka 0.8 (2012) - Replication introduced (HUGE milestone)

| Commit | Feature | ADR | Kafka Ref | KIP/History | Lines | Learning Focus |
|--------|---------|-----|-----------|-------------|-------|----------------|
| 5.1 | Replica manager & partition state | ADR-019 | core/.../server/ReplicaManager.scala | Partition management | 600 | Partition replicas |
| 5.2 | Replica fetcher thread | ADR-020 | core/.../server/ReplicaFetcherThread.scala | Follower replication | 500 | Pull-based replication |
| 5.3 | High watermark & ISR | ADR-021 | core/.../cluster/Partition.scala | Consistency model | 500 | HW vs LEO |
| 5.4 | Leader election | ADR-022 | core/.../controller/PartitionLeaderSelector.scala | Fault tolerance | 400 | Leader selection |

**Checkpoint 5**: Kill leader, verify failover works

---

### Phase 6: Producer Client (Est. 3 commits, ~1,200 lines)
**Historical Context**: Kafka 0.9 (2015) - New producer API

| Commit | Feature | ADR | Kafka Ref | KIP/History | Lines | Learning Focus |
|--------|---------|-----|-----------|-------------|-------|----------------|
| 6.1 | Producer API & record accumulator | ADR-023 | clients/.../producer/KafkaProducer.java | Producer design | 500 | Batching |
| 6.2 | Sender thread & I/O | ADR-024 | clients/.../producer/internals/Sender.java | Async I/O | 400 | Background threads |
| 6.3 | Partitioning & callbacks | ADR-025 | clients/.../producer/Partitioner.java | Partition selection | 300 | Load balancing |

**Checkpoint 6**: Produce 1M messages with batching

---

### Phase 7: Consumer Client (Est. 4 commits, ~1,500 lines)
**Historical Context**: Kafka 0.9 (2015) - New consumer API with groups

| Commit | Feature | ADR | Kafka Ref | KIP/History | Lines | Learning Focus |
|--------|---------|-----|-----------|-------------|-------|----------------|
| 7.1 | Consumer API & subscription | ADR-026 | clients/.../consumer/KafkaConsumer.java | Consumer design | 400 | Poll model |
| 7.2 | Fetcher & fetch manager | ADR-027 | clients/.../consumer/internals/Fetcher.java | Fetch logic | 400 | Prefetching |
| 7.3 | Consumer coordinator & group protocol | ADR-028 | clients/.../consumer/internals/ConsumerCoordinator.java | Consumer groups | 500 | Rebalance protocol |
| 7.4 | Offset commit & management | ADR-029 | clients/.../consumer/internals/ConsumerCoordinator.java | Offset tracking | 300 | Offset storage |

**Checkpoint 7**: Run consumer group with 3 consumers

---

## 📝 Architecture Decision Record (ADR) Template

```markdown
# ADR-XXX: [Decision Title]

**Status**: Accepted
**Date**: 2025-XX-XX
**Commit**: [hash]
**Kafka Reference**: [file:line]
**Related KIPs**: [KIP numbers]

## Context

What problem are we solving in mini-Kafka?
What constraints exist?
What did real Kafka face when solving this?

## Decision

What approach did we choose?

### Code Example
```java
// Our mini-kafka implementation
[code snippet]
```

## Rationale

### Why This Approach?
1. Reason 1
2. Reason 2
3. Simplifications from Kafka

### Alternatives Considered
- **Alternative A**: [why rejected]
- **Alternative B**: [why rejected]

## Kafka Comparison

### What Apache Kafka Does
[Explanation + file references from /home/user/kafka]

**Key differences**:
| Aspect | Mini-Kafka | Apache Kafka | Why Different? |
|--------|-----------|--------------|----------------|
| Implementation | Simple version | Full version | Learning focus |
| Lines of code | ~200 | ~2000 | We skip edge cases |

### Historical Evolution
- **Kafka 0.7 (2011)**: [original approach]
- **Kafka 0.8 (2012)**: [major change - replication]
- **Kafka 0.9 (2015)**: [new clients]
- **Kafka 2.0 (2018)**: [modern approach]

## Trade-offs

### Benefits
✅ Benefit 1
✅ Benefit 2

### Limitations
❌ Limitation 1
❌ Limitation 2

## Learning Outcomes

After this commit, you should understand:
1. [Concept 1]
2. [Concept 2]
3. [Concept 3]

## References

- **Kafka Source**: `/home/user/kafka/[file]:[lines]`
- **KIP**: https://cwiki.apache.org/confluence/display/KAFKA/KIP-XXX
- **Blog Post**: [if applicable]
- **Commit**: https://github.com/apache/kafka/commit/[hash]
- **Paper**: [academic paper if relevant]

## Exercises

1. **Extension**: Add support for [feature]
2. **Debugging**: We've introduced a bug - find it
3. **Performance**: Benchmark this vs [alternative]
```

---

## 🎯 Learning Checkpoint Template

```markdown
# Phase X Checkpoint: [Phase Name]

## Self-Assessment Quiz

### Conceptual Understanding

**Question 1**: Why does Kafka use a pull model for consumers instead of push?
- **Answer**: [Pull allows consumers to control rate, simplifies broker, enables replay]
- **Reference**: ADR-027, Kafka documentation on consumer design

**Question 2**: What is the high watermark and why is it important?
- **Answer**: [HW = highest offset replicated to all ISR; guarantees consistency]
- **Reference**: ADR-021, Kafka replication protocol

[More questions...]

### Code Comprehension

**Question 1**: What does this code do?
```java
long hw = followers.stream()
    .filter(f -> isr.contains(f.id))
    .mapToLong(f -> f.leo)
    .min().orElse(localLEO);
```
**Answer**: Calculates high watermark as minimum LEO across ISR members

## Hands-On Exercises

### Exercise 1: Extend the Feature
**Task**: Add support for multiple compression codecs (Snappy, LZ4)
**Difficulty**: ⭐⭐☆☆☆
**Estimated Time**: 45 minutes
**Learning Goal**: Understand compression trade-offs

**Hints**:
- Look at `/home/user/kafka/clients/src/main/java/org/apache/kafka/common/record/CompressionType.java`
- You'll need to modify `RecordBatch` serialization
- Benchmark compression ratio vs CPU usage

### Exercise 2: Debug the Code
**Task**: We've introduced a replication bug where followers fall out of ISR incorrectly
**Difficulty**: ⭐⭐⭐☆☆
**Bug Description**: Followers are removed from ISR even when caught up

**Debugging Steps**:
1. Add logging to ISR update logic
2. Check LEO vs HW calculations
3. Verify fetch offset tracking

### Exercise 3: Performance Analysis
**Task**: Why is batching critical for Kafka performance?
**Difficulty**: ⭐⭐⭐⭐☆

**Steps**:
1. Benchmark: Produce 1M messages with batch.size=1
2. Benchmark: Produce 1M messages with batch.size=16384
3. Measure: throughput, latency, network packets
4. Explain: Why is batching 100x faster?

## Comparative Analysis

### Mini-Kafka vs Apache Kafka

| Aspect | Mini-Kafka | Apache Kafka | Why Different? |
|--------|-----------|--------------|----------------|
| Lines of code | 2,500 | 15,000 | We skip edge cases, optimizations |
| Request types | 10 | 60+ | We implement only core APIs |
| Features | Basic replication | ISR, unclean election, preferred leader | Learning focus |
| Performance | 10K msg/sec | 1M+ msg/sec | No zero-copy, batching optimizations |

### Side-by-Side Code Comparison

**Mini-Kafka** (our implementation):
```java
// Simple partition assignment
Map<String, List<Integer>> assign(List<String> members, Map<String, Integer> partitions) {
    int idx = 0;
    for (String topic : partitions.keySet()) {
        for (int p = 0; p < partitions.get(topic); p++) {
            assignments.get(members.get(idx++ % members.size())).add(p);
        }
    }
}
```

**Apache Kafka** (real implementation):
```java
// From /home/user/kafka/clients/.../RangeAssignor.java (line 50+)
// Handles: unequal partitions, consumer metadata, rack awareness,
// sticky assignment, cooperative rebalancing... (200+ lines)
```

**Why the difference?**
We prioritize learning core concepts over production edge cases.

## Performance Benchmark

Run `make benchmark-phase-X`:

**Expected Results**:
```
Produce throughput: ~50K msg/sec
Consume throughput: ~80K msg/sec
End-to-end latency (p99): ~50ms
```

**Bottlenecks**:
- No batching optimization → low throughput
- Synchronous disk writes → high latency
- Single-threaded handler → low concurrency

**Kafka's Solutions**:
- Batching: 100x throughput improvement
- Zero-copy: Bypass kernel buffer copy
- OS page cache: Leverage filesystem cache
- Request pipelining: Handle multiple in-flight requests

## Next Steps

Before Phase X+1, ensure you can:

- [ ] Explain [key concept] to someone else
- [ ] Modify the code to add [feature]
- [ ] Identify the performance bottleneck
- [ ] Read equivalent Kafka code comfortably
- [ ] Understand trade-offs made in design

---

## 🔧 Interactive Learning Tools

### Tool 1: Message Inspector
```bash
./tools/inspect-log.sh /data/topic-0/00000000000000000000.log

# Output:
# Offset: 0, Key: user-123, Value: {"event": "login"}, Timestamp: 1234567890
# Offset: 1, Key: user-456, Value: {"event": "purchase"}, Timestamp: 1234567891
```

### Tool 2: Replication Visualizer
```bash
./tools/show-replication.sh topic-name partition-0

# Output:
# Leader (Broker 1):   LEO=1000, HW=950
# Follower (Broker 2): LEO=950,  Lag=50   [IN ISR]
# Follower (Broker 3): LEO=800,  Lag=200  [OUT OF ISR]
```

### Tool 3: Protocol Debugger
```bash
./tools/protocol-trace.sh

# Shows wire protocol bytes + decoded structure:
# [Request]  ApiKey=0 (Produce), CorrelationId=5, ClientId=mini-producer
# [Response] CorrelationId=5, Partition=0, Offset=12345
```

---

## 📚 Historical Timeline

### Kafka Evolution Mapped to Mini-Kafka Commits

| Commit | Our Feature | Kafka Version | Year | Historical Context |
|--------|-------------|---------------|------|-------------------|
| 1.1 | Message serialization | 0.7.0 | 2011 | LinkedIn's initial release |
| 2.1 | Log segments | 0.7.0 | 2011 | Inspired by commit logs |
| 5.1 | Replication | 0.8.0 | 2012 | **Huge milestone** - fault tolerance |
| 6.1 | New producer | 0.9.0 | 2015 | Complete rewrite for performance |
| 7.3 | Consumer groups | 0.9.0 | 2015 | Removed ZooKeeper dependency for consumers |

### Key Design Decisions in Kafka History

**2011 - Why append-only logs?**
- LinkedIn needed high throughput for activity tracking
- Traditional messaging queues (ActiveMQ, RabbitMQ) couldn't scale
- Solution: Treat messages like database commit logs
- Result: 100x throughput improvement

**2012 - Why pull-based consumers?**
- Push model: broker controls rate → consumer overload
- Pull model: consumer controls rate → better backpressure
- Trade-off: Slightly higher latency, but better scalability

**2012 - Why replication added?**
- Original Kafka had no replication → data loss on failure
- Added leader/follower model inspired by databases
- ISR concept: Balance consistency and availability

**2015 - Why new producer/consumer?**
- Old APIs: synchronous, inefficient, hard to use
- New APIs: async, batching, cleaner abstractions
- Breaking change but huge improvement

---

## 🎓 Key Learning Outcomes

### After completing Mini-Kafka, you will understand:

#### Distributed Systems Concepts
- ✅ Replication strategies (leader/follower vs quorum)
- ✅ Consistency models (high watermark, ISR)
- ✅ Partition tolerance and availability trade-offs
- ✅ Leader election algorithms
- ✅ Distributed consensus basics (compare to Raft/Paxos)

#### Systems Programming
- ✅ Binary protocol design
- ✅ Efficient serialization
- ✅ Log-structured storage
- ✅ Memory-mapped files
- ✅ Zero-copy techniques (reading Kafka code)
- ✅ NIO networking in Java

#### Software Architecture
- ✅ Client-server architecture
- ✅ Request-response patterns
- ✅ Background thread patterns
- ✅ Batching for performance
- ✅ API design principles

#### Kafka-Specific
- ✅ Why Kafka is fast (append-only, batching, zero-copy, page cache)
- ✅ Exactly-once semantics (reading code)
- ✅ Consumer groups and rebalancing
- ✅ Offset management strategies
- ✅ When to use Kafka vs other systems

---

## 📖 References & Resources

### Kafka Source Code (/home/user/kafka)

**Core Components**:
- **Log Storage**: `/home/user/kafka/storage/src/main/java/org/apache/kafka/storage/internals/log/`
- **Network**: `/home/user/kafka/clients/src/main/java/org/apache/kafka/common/network/`
- **Broker**: `/home/user/kafka/core/src/main/scala/kafka/server/`
- **Producer**: `/home/user/kafka/clients/src/main/java/org/apache/kafka/clients/producer/`
- **Consumer**: `/home/user/kafka/clients/src/main/java/org/apache/kafka/clients/consumer/`
- **Replication**: `/home/user/kafka/core/src/main/scala/kafka/cluster/Partition.scala`

### KIPs (Kafka Improvement Proposals)
- **KIP-4**: Command line and centralized admin operations
- **KIP-98**: Exactly once delivery and transactional messaging
- **KIP-101**: Consumer group rebalance protocol improvements
- **KIP-392**: Allow consumers to fetch from closest replica

### Academic Papers
- **"Kafka: a Distributed Messaging System for Log Processing"** (2011) - Original paper
- **"The Log: What every software engineer should know"** - Jay Kreps (2013)

### Blog Posts
- **Kafka Architecture** - Confluent documentation
- **How Kafka's Storage Internals Work** - Kafka documentation
- **Kafka Replication** - Kafka documentation

---

## 🚀 Getting Started

### Prerequisites
- Java 17+
- Gradle 7+
- Basic understanding of networking, threads, I/O

### Phase 1: Setup

```bash
# Create project structure
mkdir mini-kafka
cd mini-kafka

# Initialize Gradle
gradle init --type java-application

# Project structure
mini-kafka/
├── src/
│   ├── main/java/mini/kafka/
│   │   ├── common/       # Record, RecordBatch, serialization
│   │   ├── storage/      # Log, LogSegment, Index
│   │   ├── protocol/     # Requests, Responses
│   │   ├── server/       # Broker, RequestHandler
│   │   ├── producer/     # Producer client
│   │   └── consumer/     # Consumer client
│   └── test/java/mini/kafka/
├── docs/
│   ├── adrs/             # Architecture decisions
│   ├── comparisons/      # Mini vs Real Kafka
│   └── checkpoints/      # Learning checkpoints
├── tools/                # Debugging/visualization tools
└── examples/             # Example usage
```

### First Commit: Message Format

**Goal**: Implement basic Record and RecordBatch

**Files to create**:
1. `Record.java` - Single message
2. `RecordBatch.java` - Batch of messages
3. `Serializer.java` - Binary encoding/decoding

**Test**: Serialize 1000 records, deserialize, verify correctness

**Learning**: Understand binary protocols, efficient serialization

---

## 🎯 Success Criteria

### MVP (Minimum Viable Product)
- [ ] Single broker can store messages in log
- [ ] Producer can send messages
- [ ] Consumer can read messages
- [ ] Basic replication (leader + 2 followers)
- [ ] Consumer group with partition assignment
- [ ] 10K+ messages/sec throughput

### Stretch Goals (Phase 2+)
- [ ] Log compaction
- [ ] Transactions
- [ ] Exactly-once semantics
- [ ] Zero-copy optimization
- [ ] Multiple brokers with controller
- [ ] Rack awareness

---

## 💡 Design Principles

### 1. **Simplicity over Completeness**
Build a working subset, not a complete clone. Focus on core concepts.

### 2. **Learning over Performance**
Prioritize understandable code over optimized code. Add optimizations later with clear comments explaining trade-offs.

### 3. **Reasoning-Based**
Every design decision should have a documented rationale comparing alternatives.

### 4. **Historical Context**
Understand *why* Kafka evolved the way it did, not just *what* it does.

### 5. **Hands-On**
Theory + Practice. Read code, write code, debug code, benchmark code.

---

## 🔥 Common Pitfalls to Avoid

### 1. **Trying to Implement Everything**
❌ Don't try to build all 60+ request types
✅ Focus on 10 core requests

### 2. **Optimizing Too Early**
❌ Don't start with zero-copy and memory-mapped files
✅ Get it working first, then optimize with measurements

### 3. **Skipping Documentation**
❌ Don't just write code
✅ Write ADRs explaining your decisions

### 4. **Ignoring History**
❌ Don't just copy current Kafka
✅ Understand how it evolved and why

### 5. **Not Testing**
❌ Don't skip tests
✅ Write tests for each component, integration tests for end-to-end

---

## 📝 Commit Message Format

```
[Phase X.Y] Title - Historical Context

Brief description of what this commit implements.

Kafka Reference: /home/user/kafka/[file]:[lines]
KIP Reference: KIP-XXX (Title)
Historical Note: This mirrors Kafka X.Y's [feature] introduction

Design Decisions:
- Decision 1: [rationale]
- Decision 2: [rationale]

Trade-offs:
- We simplified [X] because [Y]
- We kept [A] to preserve learning value of [B]

Learning Outcomes:
1. Understand [concept]
2. See how [feature] works in distributed systems
3. Compare [approach A] vs [approach B]

See docs/adrs/ADR-XXX.md for full decision record.
Testing: [what was tested]
```

---

## 🎉 Ready to Start?

This guide provides a comprehensive roadmap to learning Kafka by building it from scratch.

**Estimated Timeline**:
- **Phase 1-2** (Serialization + Storage): 1-2 weeks
- **Phase 3-4** (Protocol + Broker): 2-3 weeks
- **Phase 5** (Replication): 1-2 weeks
- **Phase 6-7** (Clients): 1-2 weeks

**Total**: 6-10 weeks of deep learning

**What You'll Gain**:
- Deep understanding of distributed systems
- Kafka internals mastery
- Systems programming skills
- Ability to debug production Kafka issues
- Foundation for building distributed systems

Let's start building! 🚀
