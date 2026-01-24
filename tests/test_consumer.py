"""Tests for the Solace consumer module."""

import pytest
from unittest.mock import MagicMock, patch

from solace_consumer.consumer import SolaceSparkConsumer


class TestSolaceSparkConsumer:
    """Tests for SolaceSparkConsumer class."""

    def test_init_default_values(self):
        """Test consumer initializes with default values."""
        consumer = SolaceSparkConsumer()
        
        assert consumer.app_name == "SolaceSparkConsumer"
        assert consumer.host == "tcp://localhost:55554"
        assert consumer.vpn == "default"
        assert consumer.username == "default"
        assert consumer.password == "default"
        assert consumer.queue == "spark-consumer-queue"
        assert consumer.batch_size == 100
        assert consumer.partitions == 1
        assert consumer.include_headers is True
        assert consumer.connect_retries == 3
        assert consumer.reconnect_retries == 3

    def test_init_custom_values(self):
        """Test consumer initializes with custom values."""
        consumer = SolaceSparkConsumer(
            app_name="CustomApp",
            host="tcp://broker:55555",
            vpn="custom-vpn",
            username="user1",
            password="pass1",
            queue="custom-queue",
            batch_size=500,
            partitions=4,
            include_headers=False,
            connect_retries=5,
            reconnect_retries=10,
            spark_master="local[4]",
            connector_jar="/path/to/connector.jar",
        )
        
        assert consumer.app_name == "CustomApp"
        assert consumer.host == "tcp://broker:55555"
        assert consumer.vpn == "custom-vpn"
        assert consumer.username == "user1"
        assert consumer.password == "pass1"
        assert consumer.queue == "custom-queue"
        assert consumer.batch_size == 500
        assert consumer.partitions == 4
        assert consumer.include_headers is False
        assert consumer.connect_retries == 5
        assert consumer.reconnect_retries == 10
        assert consumer.spark_master == "local[4]"
        assert consumer.connector_jar == "/path/to/connector.jar"

    def test_spark_session_not_created_on_init(self):
        """Test Spark session is not created during initialization."""
        consumer = SolaceSparkConsumer()
        assert consumer._spark is None

    @patch("solace_consumer.consumer.SparkSession")
    def test_get_spark_session_creates_session(self, mock_spark_session):
        """Test get_spark_session creates a SparkSession."""
        mock_builder = MagicMock()
        mock_session = MagicMock()
        
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_session
        
        consumer = SolaceSparkConsumer(
            app_name="TestApp",
            spark_master="local[*]",
        )
        session = consumer.get_spark_session()
        
        mock_builder.appName.assert_called_with("TestApp")
        mock_builder.master.assert_called_with("local[*]")
        assert session == mock_session

    @patch("solace_consumer.consumer.SparkSession")
    def test_get_spark_session_with_connector_jar(self, mock_spark_session):
        """Test get_spark_session configures connector JAR."""
        mock_builder = MagicMock()
        mock_session = MagicMock()
        
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_session
        
        consumer = SolaceSparkConsumer(
            spark_master="local[*]",
            connector_jar="/path/to/solace-connector.jar",
        )
        consumer.get_spark_session()
        
        mock_builder.config.assert_called_with("spark.jars", "/path/to/solace-connector.jar")

    @patch("solace_consumer.consumer.SparkSession")
    def test_get_spark_session_caches_session(self, mock_spark_session):
        """Test get_spark_session returns cached session on subsequent calls."""
        mock_builder = MagicMock()
        mock_session = MagicMock()
        
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_session
        
        consumer = SolaceSparkConsumer()
        session1 = consumer.get_spark_session()
        session2 = consumer.get_spark_session()
        
        assert session1 is session2
        # getOrCreate should only be called once
        assert mock_builder.getOrCreate.call_count == 1

    @patch("solace_consumer.consumer.SparkSession")
    def test_create_streaming_dataframe_options(self, mock_spark_session):
        """Test create_streaming_dataframe sets correct options."""
        mock_builder = MagicMock()
        mock_session = MagicMock()
        mock_read_stream = MagicMock()
        mock_df = MagicMock()
        
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_session
        mock_session.readStream = mock_read_stream
        mock_read_stream.format.return_value = mock_read_stream
        mock_read_stream.option.return_value = mock_read_stream
        mock_read_stream.load.return_value = mock_df
        
        consumer = SolaceSparkConsumer(
            host="tcp://test:55555",
            vpn="test-vpn",
            username="testuser",
            password="testpass",
            queue="test-queue",
            batch_size=200,
            partitions=2,
            include_headers=True,
            connect_retries=5,
            reconnect_retries=10,
        )
        df = consumer.create_streaming_dataframe()
        
        mock_read_stream.format.assert_called_with("solace")
        
        # Check all options were set
        option_calls = {call[0][0]: call[0][1] for call in mock_read_stream.option.call_args_list}
        assert option_calls["host"] == "tcp://test:55555"
        assert option_calls["vpn"] == "test-vpn"
        assert option_calls["username"] == "testuser"
        assert option_calls["password"] == "testpass"
        assert option_calls["queue"] == "test-queue"
        assert option_calls["batchSize"] == 200
        assert option_calls["partitions"] == 2
        assert option_calls["includeHeaders"] == "true"
        assert option_calls["connectRetries"] == 5
        assert option_calls["reconnectRetries"] == 10

    @patch("solace_consumer.consumer.SparkSession")
    def test_stop_stops_session(self, mock_spark_session):
        """Test stop() stops the Spark session."""
        mock_builder = MagicMock()
        mock_session = MagicMock()
        
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_session
        
        consumer = SolaceSparkConsumer()
        consumer.get_spark_session()  # Create session
        consumer.stop()
        
        mock_session.stop.assert_called_once()
        assert consumer._spark is None

    def test_stop_without_session(self):
        """Test stop() does nothing if no session exists."""
        consumer = SolaceSparkConsumer()
        # Should not raise
        consumer.stop()
        assert consumer._spark is None


class TestSolaceSparkConsumerCLI:
    """Tests for consumer CLI argument parsing."""

    def test_cli_main_callable(self):
        """Test CLI main function is callable."""
        from solace_consumer.__main__ import main
        assert callable(main)
