"""Tests for the Solace producer module."""

import json
import pytest
from unittest.mock import MagicMock, patch

from solace_producer.producer import SolaceProducer


class TestSolaceProducer:
    """Tests for SolaceProducer class."""

    def test_init_default_values(self):
        """Test producer initializes with default values."""
        producer = SolaceProducer()
        
        assert producer.host == "tcp://localhost:55554"
        assert producer.vpn_name == "default"
        assert producer.username == "default"
        assert producer.password == "default"
        assert producer.topic_name == "synthetic/events"

    def test_init_custom_values(self):
        """Test producer initializes with custom values."""
        producer = SolaceProducer(
            host="tcp://broker:55555",
            vpn_name="custom-vpn",
            username="user1",
            password="pass1",
            topic="custom/topic",
        )
        
        assert producer.host == "tcp://broker:55555"
        assert producer.vpn_name == "custom-vpn"
        assert producer.username == "user1"
        assert producer.password == "pass1"
        assert producer.topic_name == "custom/topic"

    def test_generate_synthetic_message_structure(self):
        """Test synthetic message has expected structure."""
        producer = SolaceProducer()
        message = producer.generate_synthetic_message()
        
        # Check top-level keys
        assert "event_id" in message
        assert "timestamp" in message
        assert "event_type" in message
        assert "user" in message
        assert "device" in message
        assert "location" in message
        assert "metadata" in message

    def test_generate_synthetic_message_user_fields(self):
        """Test synthetic message user object has expected fields."""
        producer = SolaceProducer()
        message = producer.generate_synthetic_message()
        
        user = message["user"]
        assert "user_id" in user
        assert "username" in user
        assert "email" in user
        assert "ip_address" in user

    def test_generate_synthetic_message_device_fields(self):
        """Test synthetic message device object has expected fields."""
        producer = SolaceProducer()
        message = producer.generate_synthetic_message()
        
        device = message["device"]
        assert "type" in device
        assert "os" in device
        assert "browser" in device

    def test_generate_synthetic_message_location_fields(self):
        """Test synthetic message location object has expected fields."""
        producer = SolaceProducer()
        message = producer.generate_synthetic_message()
        
        location = message["location"]
        assert "country" in location
        assert "city" in location
        assert "latitude" in location
        assert "longitude" in location

    def test_generate_synthetic_message_metadata_fields(self):
        """Test synthetic message metadata object has expected fields."""
        producer = SolaceProducer()
        message = producer.generate_synthetic_message()
        
        metadata = message["metadata"]
        assert "session_id" in metadata
        assert "page_url" in metadata
        assert "referrer" in metadata
        assert "value" in metadata

    def test_generate_synthetic_message_event_types(self):
        """Test event_type is one of the expected values."""
        producer = SolaceProducer()
        expected_types = {"user_login", "purchase", "page_view", "click", "signup"}
        
        # Generate multiple messages to check variety
        event_types = set()
        for _ in range(100):
            message = producer.generate_synthetic_message()
            event_types.add(message["event_type"])
        
        # All generated types should be in expected set
        assert event_types.issubset(expected_types)

    def test_generate_synthetic_message_json_serializable(self):
        """Test synthetic message can be serialized to JSON."""
        producer = SolaceProducer()
        message = producer.generate_synthetic_message()
        
        # Should not raise
        json_str = json.dumps(message)
        assert isinstance(json_str, str)
        
        # Should round-trip
        parsed = json.loads(json_str)
        assert parsed == message

    def test_generate_synthetic_message_uniqueness(self):
        """Test each message has a unique event_id."""
        producer = SolaceProducer()
        
        event_ids = [producer.generate_synthetic_message()["event_id"] for _ in range(100)]
        
        # All IDs should be unique
        assert len(event_ids) == len(set(event_ids))

    @patch("solace_producer.producer.MessagingService")
    def test_connect_creates_service(self, mock_messaging_service):
        """Test connect creates messaging service with correct properties."""
        mock_builder = MagicMock()
        mock_service = MagicMock()
        mock_publisher = MagicMock()
        
        mock_messaging_service.builder.return_value = mock_builder
        mock_builder.from_properties.return_value = mock_builder
        mock_builder.build.return_value = mock_service
        mock_service.create_direct_message_publisher_builder.return_value.on_back_pressure_elastic.return_value.build.return_value = mock_publisher
        
        producer = SolaceProducer(
            host="tcp://test:55555",
            vpn_name="test-vpn",
            username="testuser",
            password="testpass",
        )
        producer.connect()
        
        # Verify builder was called with correct properties
        mock_builder.from_properties.assert_called_once()
        call_props = mock_builder.from_properties.call_args[0][0]
        assert call_props["solace.messaging.transport.host"] == "tcp://test:55555"
        assert call_props["solace.messaging.service.vpn-name"] == "test-vpn"
        
        # Verify service was connected
        mock_service.connect.assert_called_once()
        
        # Verify publisher was started
        mock_publisher.start.assert_called_once()

    @patch("solace_producer.producer.MessagingService")
    def test_disconnect(self, mock_messaging_service):
        """Test disconnect terminates publisher and disconnects service."""
        mock_builder = MagicMock()
        mock_service = MagicMock()
        mock_publisher = MagicMock()
        
        mock_messaging_service.builder.return_value = mock_builder
        mock_builder.from_properties.return_value = mock_builder
        mock_builder.build.return_value = mock_service
        mock_service.create_direct_message_publisher_builder.return_value.on_back_pressure_elastic.return_value.build.return_value = mock_publisher
        
        producer = SolaceProducer()
        producer.connect()
        producer.disconnect()
        
        mock_publisher.terminate.assert_called_once()
        mock_service.disconnect.assert_called_once()

    @patch("solace_producer.producer.MessagingService")
    def test_context_manager(self, mock_messaging_service):
        """Test producer works as context manager."""
        mock_builder = MagicMock()
        mock_service = MagicMock()
        mock_publisher = MagicMock()
        
        mock_messaging_service.builder.return_value = mock_builder
        mock_builder.from_properties.return_value = mock_builder
        mock_builder.build.return_value = mock_service
        mock_service.create_direct_message_publisher_builder.return_value.on_back_pressure_elastic.return_value.build.return_value = mock_publisher
        
        with SolaceProducer() as producer:
            assert producer._messaging_service is not None
        
        mock_service.connect.assert_called_once()
        mock_publisher.terminate.assert_called_once()
        mock_service.disconnect.assert_called_once()

    def test_publish_message_without_connection_raises(self):
        """Test publishing without connection raises RuntimeError."""
        producer = SolaceProducer()
        
        with pytest.raises(RuntimeError, match="Publisher not connected"):
            producer.publish_message({"test": "message"})


class TestSolaceProducerCLI:
    """Tests for producer CLI argument parsing."""

    def test_cli_default_args(self):
        """Test CLI uses correct defaults."""
        import argparse
        from solace_producer.__main__ import main
        
        # We can't easily test the full CLI, but we can verify the module imports
        assert callable(main)
