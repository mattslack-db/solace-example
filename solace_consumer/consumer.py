"""PySpark Solace Consumer using the Solace Spark Connector.

This module provides a consumer that reads messages from Solace PubSub+
using the pubsubplus-connector-spark library.

Prerequisites:
    1. Download the Solace Spark Connector JAR from Maven Central
    2. Create a queue on Solace and subscribe it to your topic
    3. Configure the queue's 'Maximum Delivered Unacknowledged Messages per Flow'
       to 2x your batch size for optimal throughput
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import col
from typing import Optional


class SolaceSparkConsumer:
    """Consumer that reads messages from Solace using Spark Structured Streaming."""

    def __init__(
        self,
        app_name: str = "SolaceSparkConsumer",
        host: str = "tcp://localhost:55554",
        vpn: str = "default",
        username: str = "default",
        password: str = "default",
        queue: str = "spark-consumer-queue",
        batch_size: int = 100,
        partitions: int = 1,
        include_headers: bool = True,
        connect_retries: int = 3,
        reconnect_retries: int = 3,
        spark_master: Optional[str] = None,
        connector_jar: Optional[str] = None,
    ):
        """
        Initialize the Solace Spark Consumer.

        Args:
            app_name: Spark application name
            host: Solace broker host URL
            vpn: Message VPN name
            username: Client username
            password: Client password
            queue: Queue name to consume from (must exist and be subscribed to topic)
            batch_size: Number of messages per micro-batch
            partitions: Number of consumers (0 = auto-scale to worker nodes)
            include_headers: Include message headers in output
            connect_retries: Connection retry attempts
            reconnect_retries: Reconnection retry attempts
            spark_master: Spark master URL (e.g., "local[*]" for local mode)
            connector_jar: Path to Solace Spark Connector JAR (optional if on classpath)
        """
        self.app_name = app_name
        self.host = host
        self.vpn = vpn
        self.username = username
        self.password = password
        self.queue = queue
        self.batch_size = batch_size
        self.partitions = partitions
        self.include_headers = include_headers
        self.connect_retries = connect_retries
        self.reconnect_retries = reconnect_retries
        self.spark_master = spark_master
        self.connector_jar = connector_jar
        self._spark: Optional[SparkSession] = None

    def get_spark_session(self) -> SparkSession:
        """Get or create a Spark session configured for Solace."""
        if self._spark is not None:
            return self._spark

        builder = SparkSession.builder.appName(self.app_name)

        if self.spark_master:
            builder = builder.master(self.spark_master)

        # Add the Solace connector JAR if specified
        if self.connector_jar:
            builder = builder.config("spark.jars", self.connector_jar)

        self._spark = builder.getOrCreate()
        return self._spark

    def create_streaming_dataframe(self) -> DataFrame:
        """
        Create a streaming DataFrame that reads from Solace.

        Returns:
            Streaming DataFrame with Solace message schema:
                - Id: String (message ID)
                - Payload: Binary (message payload)
                - PartitionKey: String (partition key if present)
                - Topic: String (topic message was published to)
                - TimeStamp: Timestamp (sender timestamp or receive time)
                - Headers: Map<String, Binary> (if includeHeaders=true)
        """
        spark = self.get_spark_session()

        df = (
            spark.readStream
            .format("solace")
            .option("host", self.host)
            .option("vpn", self.vpn)
            .option("username", self.username)
            .option("password", self.password)
            .option("queue", self.queue)
            .option("batchSize", self.batch_size)
            .option("partitions", self.partitions)
            .option("includeHeaders", str(self.include_headers).lower())
            .option("connectRetries", self.connect_retries)
            .option("reconnectRetries", self.reconnect_retries)
            .load()
        )

        return df

    def write_to_console(
        self,
        df: Optional[DataFrame] = None,
        stream_name: str = "solace-console-stream",
        truncate: bool = False,
    ) -> StreamingQuery:
        """
        Write streaming data to console for debugging.

        Args:
            df: Streaming DataFrame (creates new one if None)
            stream_name: Query name
            truncate: Whether to truncate output

        Returns:
            StreamingQuery handle
        """
        if df is None:
            df = self.create_streaming_dataframe()

        # Parse payload as string for console output
        df_parsed = df.withColumn("payload_str", col("Payload").cast("string"))

        query = (
            df_parsed.writeStream
            .format("console")
            .outputMode("append")
            .queryName(stream_name)
            .option("truncate", str(truncate).lower())
            .start()
        )

        return query

    def write_to_parquet(
        self,
        output_path: str,
        checkpoint_path: str,
        df: Optional[DataFrame] = None,
        stream_name: str = "solace-parquet-stream",
    ) -> StreamingQuery:
        """
        Write streaming data to Parquet files.

        Args:
            output_path: Path for Parquet output files
            checkpoint_path: Path for checkpoint directory
            df: Streaming DataFrame (creates new one if None)
            stream_name: Query name

        Returns:
            StreamingQuery handle
        """
        if df is None:
            df = self.create_streaming_dataframe()

        query = (
            df.writeStream
            .format("parquet")
            .outputMode("append")
            .queryName(stream_name)
            .option("checkpointLocation", checkpoint_path)
            .option("path", output_path)
            .start()
        )

        return query

    def write_to_delta(
        self,
        table_name: str,
        checkpoint_path: str,
        df: Optional[DataFrame] = None,
        stream_name: str = "solace-delta-stream",
    ) -> StreamingQuery:
        """
        Write streaming data to Delta Lake table (Databricks).

        Args:
            table_name: Delta table name
            checkpoint_path: Path for checkpoint directory
            df: Streaming DataFrame (creates new one if None)
            stream_name: Query name

        Returns:
            StreamingQuery handle
        """
        if df is None:
            df = self.create_streaming_dataframe()

        query = (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .queryName(stream_name)
            .option("checkpointLocation", checkpoint_path)
            .toTable(table_name)
        )

        return query

    def process_with_foreach_batch(
        self,
        process_func,
        checkpoint_path: str,
        df: Optional[DataFrame] = None,
        stream_name: str = "solace-foreach-stream",
    ) -> StreamingQuery:
        """
        Process streaming data with a custom function per micro-batch.

        Args:
            process_func: Function(df, batch_id) to process each micro-batch
            checkpoint_path: Path for checkpoint directory
            df: Streaming DataFrame (creates new one if None)
            stream_name: Query name

        Returns:
            StreamingQuery handle
        """
        if df is None:
            df = self.create_streaming_dataframe()

        query = (
            df.writeStream
            .foreachBatch(process_func)
            .outputMode("append")
            .queryName(stream_name)
            .option("checkpointLocation", checkpoint_path)
            .start()
        )

        return query

    def stop(self) -> None:
        """Stop the Spark session."""
        if self._spark:
            self._spark.stop()
            self._spark = None
