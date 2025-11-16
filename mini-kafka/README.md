# Mini-Kafka

A simplified implementation of Apache Kafka for learning purposes.

## About

This project implements core Kafka concepts from scratch to understand:
- How distributed message systems work
- Why Kafka's design choices were made
- Distributed systems patterns (replication, consensus, etc.)

**This is a learning project, not for production use.**

## Documentation

- **Learning Framework**: See `/KAFKA_LEARNING_FRAMEWORK_README.md`
- **Implementation Guide**: See `/MINI_KAFKA_LEARNING_GUIDE.md`
- **Historical Context**: See `/HISTORICAL_TIMELINE.md`
- **Architecture Decisions**: See `/docs/adrs/`

## Current Status

- ✅ Phase 1: Message Serialization (In Progress)
- ⏳ Phase 2: Log Storage
- ⏳ Phase 3: Network Protocol
- ⏳ Phase 4: Broker Core
- ⏳ Phase 5: Replication
- ⏳ Phase 6: Producer Client
- ⏳ Phase 7: Consumer Client

## Building

```bash
./gradlew build
```

## Testing

```bash
./gradlew test
```

## Running

```bash
# Start broker
./gradlew run

# Run producer example
./gradlew runProducer

# Run consumer example
./gradlew runConsumer
```

## Learning Approach

Each commit represents an atomic, well-reasoned implementation step:
1. Read the corresponding ADR (Architecture Decision Record)
2. Understand the historical context (what Kafka version introduced this)
3. Implement the feature
4. Write comprehensive tests
5. Compare with Apache Kafka source code

## Apache Kafka Reference

This implementation references Apache Kafka source code at:
`/home/user/kafka/` (Apache Kafka 3.x)

## Target Metrics

| Metric | Mini-Kafka Goal | Apache Kafka |
|--------|----------------|--------------|
| Code Size | ~12,000 lines | ~192,000 lines |
| Throughput | ~50K msg/sec | ~1M+ msg/sec |
| Partitions | 100s | Millions |

**Focus**: Correctness and understanding over performance.

## License

Educational use only. Based on Apache Kafka architecture.
