# Mini-Kafka Project Structure

**A complete reference for organizing the mini-kafka implementation**

---

## Directory Structure

```
mini-kafka/
├── README.md                          # Project overview & quickstart
├── LEARNING_GUIDE.md                  # Main learning documentation (link to this repo)
├── build.gradle                       # Gradle build configuration
├── settings.gradle
│
├── docs/                              # 📚 Documentation & Learning Materials
│   ├── adrs/                          # Architecture Decision Records
│   │   ├── ADR-001-append-only-log-storage.md
│   │   ├── ADR-002-partitioning-strategy.md
│   │   ├── ADR-003-binary-protocol-design.md
│   │   ├── ADR-004-replication-isr-model.md
│   │   └── ...
│   │
│   ├── comparisons/                   # Mini-Kafka vs Apache Kafka
│   │   ├── log-storage-comparison.md
│   │   ├── network-protocol-comparison.md
│   │   ├── replication-comparison.md
│   │   └── performance-comparison.md
│   │
│   ├── checkpoints/                   # Learning checkpoints per phase
│   │   ├── phase1-serialization-checkpoint.md
│   │   ├── phase2-log-storage-checkpoint.md
│   │   ├── phase3-network-protocol-checkpoint.md
│   │   └── ...
│   │
│   ├── diagrams/                      # Architecture diagrams
│   │   ├── execution-pipeline.svg
│   │   ├── replication-flow.svg
│   │   ├── consumer-group-protocol.svg
│   │   └── log-segment-structure.svg
│   │
│   └── references/                    # External references
│       ├── kafka-source-map.md        # Map to Apache Kafka source
│       ├── kips-referenced.md         # Relevant KIPs
│       └── papers.md                  # Academic papers
│
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── minikafka/
│   │   │       │
│   │   │       ├── common/                    # 📦 Phase 1: Serialization (800 lines)
│   │   │       │   ├── record/
│   │   │       │   │   ├── Record.java                 # Single message
│   │   │       │   │   ├── RecordBatch.java            # Batch of records
│   │   │       │   │   ├── Header.java                 # Message headers
│   │   │       │   │   └── Compression.java            # GZIP compression
│   │   │       │   │
│   │   │       │   ├── serialization/
│   │   │       │   │   ├── Serializer.java             # Encode/decode
│   │   │       │   │   ├── ByteBufferUtils.java
│   │   │       │   │   └── CRC32Checksum.java          # Data integrity
│   │   │       │   │
│   │   │       │   ├── TopicPartition.java             # (topic, partition) tuple
│   │   │       │   └── PartitionInfo.java              # Metadata about partition
│   │   │       │
│   │   │       ├── storage/                   # 💾 Phase 2: Log Storage (2,500 lines)
│   │   │       │   ├── log/
│   │   │       │   │   ├── Log.java                    # Unified log (all segments)
│   │   │       │   │   ├── LogSegment.java             # Single segment file
│   │   │       │   │   ├── OffsetIndex.java            # Offset → position index
│   │   │       │   │   ├── FileRecords.java            # File-backed records
│   │   │       │   │   └── LogConfig.java              # Configuration (retention, etc.)
│   │   │       │   │
│   │   │       │   ├── LogManager.java                 # Manages all logs
│   │   │       │   ├── LogCleaner.java                 # Retention enforcement
│   │   │       │   └── OffsetPosition.java             # (offset, position) pair
│   │   │       │
│   │   │       ├── protocol/                  # 🌐 Phase 3: Network Protocol (1,500 lines)
│   │   │       │   ├── ApiKeys.java                    # Request type enum
│   │   │       │   ├── RequestHeader.java              # All request headers
│   │   │       │   ├── ResponseHeader.java             # All response headers
│   │   │       │   │
│   │   │       │   ├── requests/
│   │   │       │   │   ├── ProduceRequest.java
│   │   │       │   │   ├── ProduceResponse.java
│   │   │       │   │   ├── FetchRequest.java
│   │   │       │   │   ├── FetchResponse.java
│   │   │       │   │   ├── MetadataRequest.java
│   │   │       │   │   ├── MetadataResponse.java
│   │   │       │   │   ├── OffsetCommitRequest.java
│   │   │       │   │   ├── OffsetCommitResponse.java
│   │   │       │   │   ├── FindCoordinatorRequest.java
│   │   │       │   │   └── ...
│   │   │       │   │
│   │   │       │   └── Errors.java                     # Error codes
│   │   │       │
│   │   │       ├── network/                   # 🔌 Phase 3: Network Layer (part of protocol)
│   │   │       │   ├── SocketServer.java               # Accept connections
│   │   │       │   ├── RequestChannel.java             # Request queue
│   │   │       │   ├── Processor.java                  # NIO event loop
│   │   │       │   ├── NetworkReceive.java
│   │   │       │   └── NetworkSend.java
│   │   │       │
│   │   │       ├── server/                    # 🖥️ Phase 4: Broker (2,500 lines)
│   │   │       │   ├── KafkaBroker.java                # Main server class
│   │   │       │   ├── KafkaConfig.java                # Broker configuration
│   │   │       │   ├── RequestHandler.java             # Process requests
│   │   │       │   ├── KafkaApis.java                  # API handlers
│   │   │       │   │
│   │   │       │   ├── ReplicaManager.java             # Manage replicas
│   │   │       │   ├── PartitionManager.java           # Manage partitions
│   │   │       │   │
│   │   │       │   ├── metadata/
│   │   │       │   │   ├── MetadataCache.java          # Cluster metadata
│   │   │       │   │   ├── TopicMetadata.java
│   │   │       │   │   └── BrokerMetadata.java
│   │   │       │   │
│   │   │       │   └── coordinator/
│   │   │       │       ├── GroupCoordinator.java       # Consumer group coordination
│   │   │       │       └── OffsetCoordinator.java      # Offset management
│   │   │       │
│   │   │       ├── replication/               # 🔄 Phase 5: Replication (2,000 lines)
│   │   │       │   ├── Partition.java                  # Partition with replicas
│   │   │       │   ├── Replica.java                    # Single replica
│   │   │       │   ├── ReplicaFetcher.java             # Follower fetches from leader
│   │   │       │   ├── LeaderElector.java              # Elect new leader
│   │   │       │   ├── ISRManager.java                 # In-Sync Replica set management
│   │   │       │   └── HighWatermark.java              # HW calculation
│   │   │       │
│   │   │       ├── producer/                  # 📤 Phase 6: Producer Client (1,200 lines)
│   │   │       │   ├── KafkaProducer.java              # Main producer API
│   │   │       │   ├── ProducerConfig.java             # Configuration
│   │   │       │   ├── ProducerRecord.java             # Record to send
│   │   │       │   ├── RecordMetadata.java             # Send result
│   │   │       │   │
│   │   │       │   ├── internals/
│   │   │       │   │   ├── RecordAccumulator.java      # Batch records
│   │   │       │   │   ├── ProducerBatch.java          # Single batch
│   │   │       │   │   ├── Sender.java                 # Background I/O thread
│   │   │       │   │   ├── Partitioner.java            # Choose partition
│   │   │       │   │   └── BufferPool.java             # Memory management
│   │   │       │   │
│   │   │       │   └── Callback.java                   # Async callback
│   │   │       │
│   │   │       ├── consumer/                  # 📥 Phase 7: Consumer Client (1,500 lines)
│   │   │       │   ├── KafkaConsumer.java              # Main consumer API
│   │   │       │   ├── ConsumerConfig.java             # Configuration
│   │   │       │   ├── ConsumerRecord.java             # Consumed record
│   │   │       │   ├── ConsumerRecords.java            # Batch of records
│   │   │       │   │
│   │   │       │   ├── internals/
│   │   │       │   │   ├── ConsumerCoordinator.java    # Group coordination
│   │   │       │   │   ├── Fetcher.java                # Fetch records
│   │   │       │   │   ├── SubscriptionState.java      # Track subscriptions
│   │   │       │   │   ├── PartitionAssignor.java      # Assign partitions
│   │   │       │   │   │   ├── RangeAssignor.java
│   │   │       │   │   │   └── RoundRobinAssignor.java
│   │   │       │   │   └── OffsetManager.java          # Track offsets
│   │   │       │   │
│   │   │       │   └── ConsumerRebalanceListener.java  # Rebalance callbacks
│   │   │       │
│   │   │       └── utils/                     # 🛠️ Utilities
│   │   │           ├── Time.java                       # Time abstraction
│   │   │           ├── Scheduler.java                  # Task scheduling
│   │   │           ├── ThreadUtils.java
│   │   │           └── IOUtils.java
│   │   │
│   │   └── resources/
│   │       ├── log4j2.xml                     # Logging configuration
│   │       └── mini-kafka.properties          # Default config
│   │
│   └── test/
│       ├── java/
│       │   └── minikafka/
│       │       ├── common/                    # Unit tests for Phase 1
│       │       │   ├── RecordTest.java
│       │       │   ├── RecordBatchTest.java
│       │       │   └── SerializationTest.java
│       │       │
│       │       ├── storage/                   # Unit tests for Phase 2
│       │       │   ├── LogSegmentTest.java
│       │       │   ├── OffsetIndexTest.java
│       │       │   └── LogTest.java
│       │       │
│       │       ├── protocol/                  # Unit tests for Phase 3
│       │       │   ├── ProduceRequestTest.java
│       │       │   └── FetchRequestTest.java
│       │       │
│       │       ├── server/                    # Unit tests for Phase 4
│       │       │   ├── ReplicaManagerTest.java
│       │       │   └── KafkaApisTest.java
│       │       │
│       │       ├── replication/               # Unit tests for Phase 5
│       │       │   ├── PartitionTest.java
│       │       │   └── ISRManagerTest.java
│       │       │
│       │       ├── producer/                  # Unit tests for Phase 6
│       │       │   ├── KafkaProducerTest.java
│       │       │   └── RecordAccumulatorTest.java
│       │       │
│       │       ├── consumer/                  # Unit tests for Phase 7
│       │       │   ├── KafkaConsumerTest.java
│       │       │   └── FetcherTest.java
│       │       │
│       │       └── integration/               # Integration tests
│       │           ├── ProducerConsumerIntegrationTest.java
│       │           ├── ReplicationIntegrationTest.java
│       │           └── ConsumerGroupIntegrationTest.java
│       │
│       └── resources/
│           └── test-log4j2.xml
│
├── tools/                             # 🔧 Interactive Learning Tools
│   ├── log-inspector/
│   │   ├── InspectLog.java                    # View log file contents
│   │   └── README.md
│   │
│   ├── protocol-debugger/
│   │   ├── ProtocolTrace.java                 # Trace wire protocol
│   │   └── README.md
│   │
│   ├── replication-visualizer/
│   │   ├── ShowReplication.java               # Visualize ISR state
│   │   └── README.md
│   │
│   └── benchmark/
│       ├── ThroughputBenchmark.java           # Measure throughput
│       ├── LatencyBenchmark.java              # Measure latency
│       └── README.md
│
├── examples/                          # 📖 Example Usage
│   ├── SimpleProducer.java                    # Basic producer example
│   ├── SimpleConsumer.java                    # Basic consumer example
│   ├── ConsumerGroup.java                     # Consumer group example
│   ├── TransactionalProducer.java             # (Phase 2+)
│   └── README.md
│
└── scripts/                           # 🚀 Helper Scripts
    ├── start-broker.sh                        # Start a broker
    ├── create-topic.sh                        # Create topic
    ├── produce-messages.sh                    # Send test messages
    ├── consume-messages.sh                    # Read messages
    └── run-benchmark.sh                       # Run benchmarks
```

---

## File Organization Principles

### 1. **Phased Structure**
Each package corresponds to an implementation phase:
- `common/` → Phase 1
- `storage/` → Phase 2
- `protocol/` + `network/` → Phase 3
- `server/` → Phase 4
- `replication/` → Phase 5
- `producer/` → Phase 6
- `consumer/` → Phase 7

### 2. **Mirroring Apache Kafka**
Where possible, mirror Kafka's structure:
```
Apache Kafka: /home/user/kafka/clients/src/main/java/org/apache/kafka/clients/producer/
Mini-Kafka:   mini-kafka/src/main/java/minikafka/producer/
```

This makes it easy to compare implementations.

### 3. **Documentation Co-located**
Each phase has:
- ADR documenting design decisions
- Checkpoint for self-assessment
- Comparison with Apache Kafka

### 4. **Tools for Learning**
Interactive tools to visualize and debug:
- Log inspector: See what's in log files
- Protocol debugger: Trace network communication
- Replication visualizer: Understand ISR state

---

## Package Descriptions

### `minikafka.common` (Phase 1)
**Purpose**: Message serialization and core data structures

**Key Classes**:
- `Record`: Single message (key, value, timestamp, headers)
- `RecordBatch`: Batch of records (for efficiency)
- `Serializer`: Encode/decode binary format
- `TopicPartition`: (topic, partition) tuple

**Lines**: ~800
**Kafka Ref**: `/home/user/kafka/clients/src/main/java/org/apache/kafka/common/record/`

---

### `minikafka.storage` (Phase 2)
**Purpose**: Persistent log storage

**Key Classes**:
- `Log`: Manages all segments for a partition
- `LogSegment`: Single segment file (e.g., 1GB)
- `OffsetIndex`: Sparse index for fast lookups
- `LogManager`: Manages logs for all partitions

**Lines**: ~2,500
**Kafka Ref**: `/home/user/kafka/storage/src/main/java/org/apache/kafka/storage/internals/log/`

---

### `minikafka.protocol` (Phase 3)
**Purpose**: Binary network protocol

**Key Classes**:
- `ApiKeys`: Enum of request types (Produce, Fetch, etc.)
- `ProduceRequest/Response`: Send messages
- `FetchRequest/Response`: Retrieve messages
- `MetadataRequest/Response`: Cluster info

**Lines**: ~1,000
**Kafka Ref**: `/home/user/kafka/clients/src/main/java/org/apache/kafka/common/requests/`

---

### `minikafka.network` (Phase 3)
**Purpose**: TCP connection management

**Key Classes**:
- `SocketServer`: Accept and manage connections
- `Processor`: NIO event loop
- `RequestChannel`: Queue requests to handlers

**Lines**: ~500
**Kafka Ref**: `/home/user/kafka/core/src/main/scala/kafka/network/`

---

### `minikafka.server` (Phase 4)
**Purpose**: Broker core logic

**Key Classes**:
- `KafkaBroker`: Main server class
- `KafkaApis`: Route and handle requests
- `ReplicaManager`: Manage partition replicas
- `MetadataCache`: Cluster metadata

**Lines**: ~2,500
**Kafka Ref**: `/home/user/kafka/core/src/main/scala/kafka/server/`

---

### `minikafka.replication` (Phase 5)
**Purpose**: Fault tolerance via replication

**Key Classes**:
- `Partition`: Partition with leader/followers
- `ReplicaFetcher`: Follower pulls from leader
- `ISRManager`: In-Sync Replica set
- `LeaderElector`: Elect new leader on failure

**Lines**: ~2,000
**Kafka Ref**: `/home/user/kafka/core/src/main/scala/kafka/cluster/Partition.scala`

---

### `minikafka.producer` (Phase 6)
**Purpose**: Client library for sending messages

**Key Classes**:
- `KafkaProducer`: Main API
- `RecordAccumulator`: Batch records
- `Sender`: Background I/O thread
- `Partitioner`: Choose partition for record

**Lines**: ~1,200
**Kafka Ref**: `/home/user/kafka/clients/src/main/java/org/apache/kafka/clients/producer/`

---

### `minikafka.consumer` (Phase 7)
**Purpose**: Client library for reading messages

**Key Classes**:
- `KafkaConsumer`: Main API
- `ConsumerCoordinator`: Group management
- `Fetcher`: Fetch records from broker
- `PartitionAssignor`: Assign partitions to consumers

**Lines**: ~1,500
**Kafka Ref**: `/home/user/kafka/clients/src/main/java/org/apache/kafka/clients/consumer/`

---

## Documentation Structure

### Architecture Decision Records (ADRs)

**Purpose**: Document *why* we made each design decision

**Template**: See `/home/user/kafka/docs/adrs/ADR-001-append-only-log-storage.md`

**Content**:
1. Context (what problem?)
2. Decision (what we chose)
3. Rationale (why this approach?)
4. Alternatives (what we rejected and why)
5. Kafka comparison (how does real Kafka do it?)
6. Trade-offs (benefits and limitations)
7. Learning outcomes (what you should understand)
8. Exercises (hands-on practice)

**One ADR per major decision**:
- ADR-001: Append-only log storage
- ADR-002: Partitioning strategy
- ADR-003: Binary protocol design
- ADR-004: ISR replication model
- ADR-005: Pull-based consumer
- ...

---

### Learning Checkpoints

**Purpose**: Self-assessment after each phase

**Template**:
```markdown
# Phase X Checkpoint

## Quiz
[Conceptual questions]

## Exercises
[Hands-on coding tasks]

## Comparison
[Mini-Kafka vs Apache Kafka]

## Performance
[Benchmark results]

## Next Steps
[Prerequisites for next phase]
```

**One checkpoint per phase** (7 total)

---

### Comparisons

**Purpose**: Side-by-side comparison of mini vs real Kafka

**Example**:
```markdown
# Log Storage Comparison

## Mini-Kafka Implementation (400 lines)
```java
class SimpleLogSegment {
    FileChannel channel;
    OffsetIndex index;

    void append(RecordBatch batch) { ... }
}
```

## Apache Kafka Implementation (2,000 lines)
```java
class LogSegment {
    FileRecords log;
    OffsetIndex offsetIndex;
    TimeIndex timeIndex;
    TransactionIndex txnIndex;

    // Much more complex...
}
```

## Key Differences
| Feature | Mini | Kafka | Why |
|---------|------|-------|-----|
| Indexes | Offset only | 3 types | Simplicity |
| ... | ... | ... | ... |
```

---

## Testing Strategy

### Unit Tests
**Purpose**: Test individual components in isolation

**Coverage**:
- Each major class has tests
- Test happy path + edge cases
- Mock dependencies

**Example**:
```java
@Test
public void testLogSegmentAppend() {
    LogSegment segment = new LogSegment(baseOffset);
    RecordBatch batch = createTestBatch(10);

    segment.append(batch);

    assertEquals(10, segment.recordCount());
}
```

---

### Integration Tests
**Purpose**: Test components working together

**Examples**:
- `ProducerConsumerIntegrationTest`: End-to-end flow
- `ReplicationIntegrationTest`: Leader/follower sync
- `ConsumerGroupIntegrationTest`: Rebalance protocol

**Pattern**:
```java
@Test
public void testProduceAndConsume() {
    // 1. Start broker
    broker.start();

    // 2. Produce messages
    producer.send(records);

    // 3. Consume messages
    List<Record> consumed = consumer.poll();

    // 4. Verify
    assertEquals(records, consumed);
}
```

---

### Benchmarks
**Purpose**: Measure performance and understand bottlenecks

**Metrics**:
- Throughput (messages/second)
- Latency (p50, p99, p999)
- CPU usage
- Disk I/O

**Tools** (in `tools/benchmark/`):
- `ThroughputBenchmark`: Max throughput
- `LatencyBenchmark`: Latency distribution
- `EndToEndBenchmark`: Producer → Broker → Consumer

---

## Interactive Tools

### 1. Log Inspector
**Purpose**: View contents of log files

**Usage**:
```bash
java -cp mini-kafka.jar minikafka.tools.InspectLog \
  /data/events-0/00000000000000000000.log

# Output:
# Offset: 0, Key: user-123, Value: {"event": "login"}
# Offset: 1, Key: user-456, Value: {"event": "purchase"}
# ...
```

**Implementation**: Parse log file and print records

---

### 2. Protocol Debugger
**Purpose**: Trace network protocol (request/response)

**Usage**:
```bash
java -cp mini-kafka.jar minikafka.tools.ProtocolTrace \
  --host localhost --port 9092

# Output (live capture):
# [→ Request]  Produce (apiKey=0), correlationId=5
#   Topic: events, Partition: 0, Records: 100
# [← Response] Produce, correlationId=5
#   Partition: 0, Offset: 12345
```

**Implementation**: Network sniffer + protocol parser

---

### 3. Replication Visualizer
**Purpose**: Show replication state (leader, followers, ISR)

**Usage**:
```bash
java -cp mini-kafka.jar minikafka.tools.ShowReplication \
  --topic events --partition 0

# Output:
# Leader (Broker 1):    LEO=10000, HW=9500
# Follower (Broker 2):  LEO=9500,  Lag=500   [IN ISR]
# Follower (Broker 3):  LEO=8000,  Lag=2000  [OUT OF ISR]
#
# ISR: {1, 2}
# High Watermark: 9500
```

**Implementation**: Query broker metadata + pretty print

---

## Build Configuration

### `build.gradle`

```gradle
plugins {
    id 'java'
    id 'application'
}

group = 'minikafka'
version = '0.1.0'
sourceCompatibility = '17'

repositories {
    mavenCentral()
}

dependencies {
    // Minimal dependencies (keep it simple!)
    implementation 'org.slf4j:slf4j-api:2.0.9'
    implementation 'org.apache.logging.log4j:log4j-slf4j2-impl:2.20.0'

    // Testing
    testImplementation 'org.junit.jupiter:junit-jupiter:5.10.0'
    testImplementation 'org.mockito:mockito-core:5.5.0'
}

test {
    useJUnitPlatform()
}

// Main application entry point
application {
    mainClass = 'minikafka.server.KafkaBroker'
}

// Create executable JAR
jar {
    manifest {
        attributes 'Main-Class': 'minikafka.server.KafkaBroker'
    }
}
```

---

## Example Files

### `examples/SimpleProducer.java`

```java
package minikafka.examples;

import minikafka.producer.KafkaProducer;
import minikafka.producer.ProducerRecord;

public class SimpleProducer {
    public static void main(String[] args) {
        KafkaProducer producer = new KafkaProducer(config);

        for (int i = 0; i < 1000; i++) {
            ProducerRecord record = new ProducerRecord(
                "events",  // topic
                "key-" + i,
                "value-" + i
            );

            producer.send(record, (metadata, error) -> {
                if (error == null) {
                    System.out.println("Sent: offset=" + metadata.offset());
                } else {
                    error.printStackTrace();
                }
            });
        }

        producer.close();
    }
}
```

---

### `examples/SimpleConsumer.java`

```java
package minikafka.examples;

import minikafka.consumer.KafkaConsumer;
import minikafka.consumer.ConsumerRecords;

public class SimpleConsumer {
    public static void main(String[] args) {
        KafkaConsumer consumer = new KafkaConsumer(config);

        consumer.subscribe(Arrays.asList("events"));

        while (true) {
            ConsumerRecords records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord record : records) {
                System.out.println(
                    "Consumed: offset=" + record.offset() +
                    ", key=" + record.key() +
                    ", value=" + record.value()
                );
            }

            consumer.commitSync();
        }
    }
}
```

---

## Git Workflow

### Branch Strategy
```
main
  └─ dev
      ├─ phase-1-serialization
      ├─ phase-2-log-storage
      ├─ phase-3-network-protocol
      └─ ...
```

**Work on feature branches, merge to dev, periodically merge dev → main**

---

### Commit Message Format

```
[Phase X.Y] Title - Historical Context

Brief description.

Kafka Ref: /home/user/kafka/[file]:[lines]
ADR: docs/adrs/ADR-XXX.md

Changes:
- Added [feature]
- Implemented [functionality]

Testing: [what was tested]
```

**Example**:
```
[Phase 2.1] Implement Log Segment - Kafka 0.7 (2011)

Implement basic log segment with append and read operations.

Kafka Ref: /home/user/kafka/storage/.../LogSegment.java:50-200
ADR: docs/adrs/ADR-001-append-only-log-storage.md

Changes:
- Added LogSegment class with FileChannel-based I/O
- Implemented append() for sequential writes
- Implemented read() with offset lookup

Testing: Unit tests for 10K record append/read
```

---

## Progressive Complexity

### Phase 1: Simplest Possible
```java
// Just get it working
class Record {
    byte[] key;
    byte[] value;
}
```

### Phase 2: Add Features
```java
// Add more fields
class Record {
    long offset;
    long timestamp;
    byte[] key;
    byte[] value;
    Header[] headers;
}
```

### Phase 3: Optimize
```java
// Optimize for performance
class Record {
    // Use ByteBuffer to avoid allocations
    ByteBuffer serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(size());
        // Zero-copy serialization
        ...
    }
}
```

**Principle**: Start simple, add complexity incrementally with clear rationale.

---

## Summary

This structure provides:

✅ **Clear organization** by phase
✅ **Mirrors Apache Kafka** structure for easy comparison
✅ **Rich documentation** (ADRs, checkpoints, comparisons)
✅ **Interactive tools** for visualization
✅ **Progressive complexity** (start simple, add features)
✅ **Comprehensive testing** (unit, integration, benchmarks)

**Result**: A learning-optimized codebase that teaches Kafka internals through hands-on implementation.

---

## Getting Started

```bash
# 1. Create project
mkdir mini-kafka
cd mini-kafka
gradle init --type java-application

# 2. Set up structure
mkdir -p src/main/java/minikafka/{common,storage,protocol,network,server,replication,producer,consumer}
mkdir -p docs/{adrs,comparisons,checkpoints,diagrams,references}
mkdir -p tools/{log-inspector,protocol-debugger,replication-visualizer,benchmark}
mkdir -p examples

# 3. Start Phase 1
# Implement Record and RecordBatch
# Write ADR-001
# Write tests
# Commit!

# 4. Continue through phases...
```

**Next**: Start implementing Phase 1 (Message Serialization)!
