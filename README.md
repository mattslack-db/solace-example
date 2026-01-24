# Solace PubSub+ Example

A complete example demonstrating how to produce and consume messages with Solace PubSub+ using Python and PySpark.

## Overview

This repository contains:

- **Solace Producer** - Python module to publish synthetic JSON messages to Solace
- **Solace Consumer** - PySpark Structured Streaming consumer using the Solace Spark Connector
- **Demo Notebooks** - Jupyter notebooks for interactive exploration
- **Setup Scripts** - Utilities to configure Solace queues and download the connector JAR

## Prerequisites

- Python 3.10+
- Solace PubSub+ broker (local Docker or cloud)
- Java 8+ (for PySpark)

### Local Development with Docker

Start a local Solace broker:

```bash
docker run -d -p 8081:8080 -p 55554:55555 -p 8008:8008 \
  --shm-size=2g \
  --name solace \
  solace/solace-pubsub-standard:latest
```

## Installation

```bash
# Create virtual environment
uv venv
source .venv/bin/activate

# Install the package
uv pip install -e ".[dev]"

# Download Solace Spark Connector JAR (for consumer)
bash scripts/download_connector.sh
```

## Quick Start

### 1. Setup Queue

Create a queue on Solace to receive messages:

```bash
python scripts/setup_solace_queue.py --queue spark-consumer-queue --topic "synthetic/>"
```

### 2. Produce Messages

**Command Line:**

```bash
# Produce 100 messages
python -m solace_producer -n 100

# With custom options
python -m solace_producer -n 1000 --topic "events/user" --delay 10
```

**Python API:**

```python
from solace_producer import SolaceProducer

with SolaceProducer(topic="synthetic/events") as producer:
    producer.produce_messages(count=100)
```

### 3. Consume Messages (PySpark)

**Command Line:**

```bash
python -m solace_consumer --connector-jar ./jars/pubsubplus-connector-spark-3.1.6.jar
```

**In Databricks/Spark:**

```python
df = spark.readStream \
    .format("solace") \
    .option("host", "tcp://localhost:55554") \
    .option("vpn", "default") \
    .option("username", "default") \
    .option("password", "default") \
    .option("queue", "spark-consumer-queue") \
    .option("batchSize", 100) \
    .load()

# Write to Delta Lake
df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/solace") \
    .toTable("solace_events")
```

## Project Structure

```
solace-example/
├── solace_producer/           # Producer module
│   ├── __init__.py
│   ├── producer.py            # SolaceProducer class
│   └── __main__.py            # CLI entry point
├── solace_consumer/           # PySpark consumer module
│   ├── __init__.py
│   ├── consumer.py            # SolaceSparkConsumer class
│   └── __main__.py            # CLI entry point
├── scripts/
│   ├── setup_solace_queue.py  # Create queues via SEMP API
│   └── download_connector.sh  # Download Spark connector JAR
├── tests/
│   ├── test_producer.py       # Producer unit tests
│   ├── test_consumer.py       # Consumer unit tests
│   ├── test_setup_queue.py    # Setup script tests
│   └── test_integration.py    # Integration tests (requires Solace)
├── solace_producer_demo.ipynb # Producer demo notebook
├── solace_consumer_demo.ipynb # Consumer demo notebook
├── pyproject.toml             # Package configuration
└── README.md
```

## Message Schema

The producer generates synthetic JSON messages with this structure:

```json
{
  "event_id": "uuid",
  "timestamp": "2024-01-15T10:30:00+00:00",
  "event_type": "user_login|purchase|page_view|click|signup",
  "user": {
    "user_id": "uuid",
    "username": "string",
    "email": "email",
    "ip_address": "ipv4"
  },
  "device": {
    "type": "mobile|desktop|tablet",
    "os": "iOS|Android|Windows|macOS|Linux",
    "browser": "Chrome|Firefox|Safari|Edge"
  },
  "location": {
    "country": "US",
    "city": "New York",
    "latitude": 40.7128,
    "longitude": -74.0060
  },
  "metadata": {
    "session_id": "uuid",
    "page_url": "https://example.com/page",
    "referrer": "https://google.com",
    "value": 123.45
  }
}
```

## Configuration

### Producer Options

| Option | Default | Description |
|--------|---------|-------------|
| `--host` | `tcp://localhost:55554` | Solace broker URL |
| `--vpn` | `default` | Message VPN name |
| `--username` | `default` | Client username |
| `--password` | `default` | Client password |
| `--topic` | `synthetic/events` | Topic to publish to |
| `-n, --count` | `10` | Number of messages |
| `--delay` | `0` | Delay between messages (ms) |

### Consumer Options

| Option | Default | Description |
|--------|---------|-------------|
| `--host` | `tcp://localhost:55554` | Solace broker URL |
| `--vpn` | `default` | Message VPN name |
| `--queue` | `spark-consumer-queue` | Queue to consume from |
| `--batch-size` | `100` | Messages per micro-batch |
| `--partitions` | `1` | Consumer count (0=auto) |
| `--output` | `console` | Output: `console` or `parquet` |

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run only unit tests (no Solace required)
pytest tests/ -v --ignore=tests/test_integration.py

# Run integration tests (requires running Solace)
pytest tests/test_integration.py -v
```

## Databricks Deployment

1. Install the Solace Spark Connector JAR on your cluster:
   - Maven coordinates: `com.solacecoe.connectors:pubsubplus-connector-spark:3.1.6`

2. Upload the notebooks to your workspace:
   - `solace_producer_demo.ipynb`
   - `solace_consumer_demo.ipynb`

3. Update connection settings for your Solace Cloud or broker instance.

4. Store credentials securely using Databricks secrets:
   ```python
   SOLACE_PASSWORD = dbutils.secrets.get(scope="solace", key="password")
   ```

## Links

- [Solace PubSub+ Documentation](https://docs.solace.com/)
- [Solace Spark Connector Guide](https://docs.solace.com/API/API-Developer-Guide-Python/Python-API-Messaging-Service.htm)
- [Solace Python API](https://docs.solace.com/API-Developer-Online-Ref-Documentation/python/)

## License

MIT
