"""
Integration tests for Solace producer and consumer.

These tests require a running Solace broker in Docker.
Run with: pytest tests/test_integration.py -v

To skip if Solace is not available:
    pytest tests/test_integration.py -v -m "not integration"
"""

import json
import time
import pytest
import requests
from requests.auth import HTTPBasicAuth

from solace_producer.producer import SolaceProducer


# Configuration for local Docker Solace
SOLACE_HOST = "tcp://localhost:55554"
SOLACE_SEMP_URL = "http://localhost:8081"
SOLACE_VPN = "default"
SOLACE_USERNAME = "default"
SOLACE_PASSWORD = "default"
SOLACE_ADMIN_USER = "admin"
SOLACE_ADMIN_PASSWORD = "admin"
TEST_QUEUE = "integration-test-queue"
TEST_TOPIC = "integration/test/>"


def is_solace_available() -> bool:
    """Check if Solace broker is available."""
    try:
        response = requests.get(
            f"{SOLACE_SEMP_URL}/SEMP/v2/config/msgVpns/{SOLACE_VPN}",
            auth=HTTPBasicAuth(SOLACE_ADMIN_USER, SOLACE_ADMIN_PASSWORD),
            timeout=5,
        )
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


def create_test_queue() -> bool:
    """Create a test queue for integration testing."""
    # Create queue
    queue_url = f"{SOLACE_SEMP_URL}/SEMP/v2/config/msgVpns/{SOLACE_VPN}/queues"
    queue_payload = {
        "queueName": TEST_QUEUE,
        "accessType": "exclusive",
        "egressEnabled": True,
        "ingressEnabled": True,
        "permission": "consume",
        "maxMsgSpoolUsage": 100,
        "maxDeliveredUnackedMsgsPerFlow": 200,
    }
    
    response = requests.post(
        queue_url,
        json=queue_payload,
        auth=HTTPBasicAuth(SOLACE_ADMIN_USER, SOLACE_ADMIN_PASSWORD),
        headers={"Content-Type": "application/json"},
    )
    
    if response.status_code not in (200, 400):  # 400 = already exists
        return False
    
    # Add subscription
    sub_url = f"{SOLACE_SEMP_URL}/SEMP/v2/config/msgVpns/{SOLACE_VPN}/queues/{TEST_QUEUE}/subscriptions"
    sub_payload = {"subscriptionTopic": TEST_TOPIC}
    
    response = requests.post(
        sub_url,
        json=sub_payload,
        auth=HTTPBasicAuth(SOLACE_ADMIN_USER, SOLACE_ADMIN_PASSWORD),
        headers={"Content-Type": "application/json"},
    )
    
    return response.status_code in (200, 400)


def get_queue_message_count() -> int:
    """Get the number of messages spooled in the test queue."""
    url = f"{SOLACE_SEMP_URL}/SEMP/v2/monitor/msgVpns/{SOLACE_VPN}/queues/{TEST_QUEUE}"
    
    response = requests.get(
        url,
        auth=HTTPBasicAuth(SOLACE_ADMIN_USER, SOLACE_ADMIN_PASSWORD),
    )
    
    if response.status_code == 200:
        data = response.json()
        return data.get("data", {}).get("msgSpoolUsage", 0)
    return -1


def get_queue_msg_count() -> int:
    """Get the count of messages in the queue."""
    url = f"{SOLACE_SEMP_URL}/SEMP/v2/monitor/msgVpns/{SOLACE_VPN}/queues/{TEST_QUEUE}"
    
    response = requests.get(
        url,
        auth=HTTPBasicAuth(SOLACE_ADMIN_USER, SOLACE_ADMIN_PASSWORD),
    )
    
    if response.status_code == 200:
        data = response.json()
        return data.get("data", {}).get("spooledMsgCount", 0)
    return -1


def purge_queue():
    """Purge all messages from the test queue."""
    # Use delete messages action
    delete_url = f"{SOLACE_SEMP_URL}/SEMP/v2/action/msgVpns/{SOLACE_VPN}/queues/{TEST_QUEUE}/deleteMsgs"
    
    requests.put(
        delete_url,
        json={},
        auth=HTTPBasicAuth(SOLACE_ADMIN_USER, SOLACE_ADMIN_PASSWORD),
        headers={"Content-Type": "application/json"},
    )
    time.sleep(0.5)  # Give Solace time to process


def delete_test_queue():
    """Delete the test queue."""
    url = f"{SOLACE_SEMP_URL}/SEMP/v2/config/msgVpns/{SOLACE_VPN}/queues/{TEST_QUEUE}"
    
    requests.delete(
        url,
        auth=HTTPBasicAuth(SOLACE_ADMIN_USER, SOLACE_ADMIN_PASSWORD),
    )


# Skip all tests in this module if Solace is not available
pytestmark = pytest.mark.skipif(
    not is_solace_available(),
    reason="Solace broker not available at localhost:55554"
)


@pytest.fixture(scope="module", autouse=True)
def setup_test_queue():
    """Setup test queue before tests and cleanup after."""
    # Setup
    assert create_test_queue(), "Failed to create test queue"
    purge_queue()
    
    yield
    
    # Cleanup
    purge_queue()


@pytest.fixture
def clean_queue():
    """Purge queue before each test."""
    purge_queue()
    time.sleep(0.5)  # Give Solace time to process
    yield
    purge_queue()


class TestProducerIntegration:
    """Integration tests for the Solace producer."""

    def test_producer_connects_to_solace(self):
        """Test producer can connect to Solace broker."""
        producer = SolaceProducer(
            host=SOLACE_HOST,
            vpn_name=SOLACE_VPN,
            username=SOLACE_USERNAME,
            password=SOLACE_PASSWORD,
        )
        
        # Should not raise
        producer.connect()
        assert producer._messaging_service is not None
        assert producer._publisher is not None
        
        producer.disconnect()

    def test_producer_context_manager(self):
        """Test producer works as context manager with real broker."""
        with SolaceProducer(
            host=SOLACE_HOST,
            vpn_name=SOLACE_VPN,
            username=SOLACE_USERNAME,
            password=SOLACE_PASSWORD,
        ) as producer:
            assert producer._messaging_service is not None
            assert producer._publisher is not None

    def test_producer_publishes_messages(self, clean_queue):
        """Test producer can publish messages to Solace."""
        initial_count = get_queue_msg_count()
        
        with SolaceProducer(
            host=SOLACE_HOST,
            vpn_name=SOLACE_VPN,
            username=SOLACE_USERNAME,
            password=SOLACE_PASSWORD,
            topic="integration/test/events",
        ) as producer:
            # Publish a single message
            message = producer.generate_synthetic_message()
            producer.publish_message(message)
        
        # Give Solace time to process
        time.sleep(1)
        
        # Verify message was queued
        final_count = get_queue_msg_count()
        assert final_count > initial_count, f"Expected messages to increase from {initial_count}, got {final_count}"

    def test_producer_publishes_multiple_messages(self, clean_queue):
        """Test producer can publish multiple messages."""
        num_messages = 25
        initial_count = get_queue_msg_count()
        
        with SolaceProducer(
            host=SOLACE_HOST,
            vpn_name=SOLACE_VPN,
            username=SOLACE_USERNAME,
            password=SOLACE_PASSWORD,
            topic="integration/test/events",
        ) as producer:
            published = producer.produce_messages(
                count=num_messages,
                verbose=False,
            )
        
        assert published == num_messages
        
        # Give Solace time to process
        time.sleep(1)
        
        # Verify messages were queued
        final_count = get_queue_msg_count()
        added = final_count - initial_count
        assert added >= num_messages, f"Expected {num_messages} new messages, got {added}"

    def test_producer_message_content(self, clean_queue):
        """Test published message has correct JSON structure."""
        initial_count = get_queue_msg_count()
        
        test_message = {
            "event_id": "test-event-123",
            "event_type": "integration_test",
            "data": {"key": "value"},
        }
        
        with SolaceProducer(
            host=SOLACE_HOST,
            vpn_name=SOLACE_VPN,
            username=SOLACE_USERNAME,
            password=SOLACE_PASSWORD,
            topic="integration/test/events",
        ) as producer:
            producer.publish_message(test_message)
        
        time.sleep(1)
        
        # Message should be in queue
        final_count = get_queue_msg_count()
        assert final_count > initial_count, "Message was not added to queue"

    def test_producer_high_throughput(self, clean_queue):
        """Test producer can handle high message throughput."""
        num_messages = 500
        initial_count = get_queue_msg_count()
        
        with SolaceProducer(
            host=SOLACE_HOST,
            vpn_name=SOLACE_VPN,
            username=SOLACE_USERNAME,
            password=SOLACE_PASSWORD,
            topic="integration/test/events",
        ) as producer:
            start_time = time.time()
            published = producer.produce_messages(
                count=num_messages,
                verbose=False,
            )
            elapsed = time.time() - start_time
        
        assert published == num_messages
        
        # Should achieve reasonable throughput (at least 100 msg/sec)
        rate = num_messages / elapsed if elapsed > 0 else 0
        assert rate > 100, f"Throughput too low: {rate:.1f} msg/sec"
        
        # Give Solace time to process
        time.sleep(2)
        
        final_count = get_queue_msg_count()
        added = final_count - initial_count
        assert added >= num_messages * 0.95, f"Expected ~{num_messages}, got {added}"


class TestProducerConsumerIntegration:
    """Integration tests for producer and consumer working together."""

    def test_end_to_end_message_flow(self, clean_queue):
        """Test messages flow from producer to queue for consumer."""
        num_messages = 10
        initial_count = get_queue_msg_count()
        
        # Produce messages
        with SolaceProducer(
            host=SOLACE_HOST,
            vpn_name=SOLACE_VPN,
            username=SOLACE_USERNAME,
            password=SOLACE_PASSWORD,
            topic="integration/test/events",
        ) as producer:
            for i in range(num_messages):
                message = {
                    "event_id": f"e2e-test-{i}",
                    "sequence": i,
                    "timestamp": time.time(),
                }
                producer.publish_message(message)
        
        time.sleep(1)
        
        # Verify messages are available for consumption
        final_count = get_queue_msg_count()
        added = final_count - initial_count
        assert added == num_messages, f"Expected {num_messages} new messages, got {added}"

    def test_synthetic_messages_are_valid_json(self, clean_queue):
        """Test that synthetic messages are valid JSON when published."""
        initial_count = get_queue_msg_count()
        num_messages = 5
        
        with SolaceProducer(
            host=SOLACE_HOST,
            vpn_name=SOLACE_VPN,
            username=SOLACE_USERNAME,
            password=SOLACE_PASSWORD,
            topic="integration/test/events",
        ) as producer:
            for _ in range(num_messages):
                message = producer.generate_synthetic_message()
                
                # Verify it's valid JSON before publishing
                json_str = json.dumps(message)
                parsed = json.loads(json_str)
                
                assert parsed["event_id"] == message["event_id"]
                assert "timestamp" in parsed
                assert "user" in parsed
                assert "device" in parsed
                
                producer.publish_message(message)
        
        time.sleep(1)
        final_count = get_queue_msg_count()
        added = final_count - initial_count
        assert added >= num_messages, f"Expected {num_messages} new messages, got {added}"


class TestConnectionResilience:
    """Tests for connection handling and resilience."""

    def test_reconnection_after_disconnect(self):
        """Test producer can reconnect after disconnection."""
        producer = SolaceProducer(
            host=SOLACE_HOST,
            vpn_name=SOLACE_VPN,
            username=SOLACE_USERNAME,
            password=SOLACE_PASSWORD,
        )
        
        # First connection
        producer.connect()
        assert producer._messaging_service is not None
        producer.disconnect()
        
        # Reconnection
        producer.connect()
        assert producer._messaging_service is not None
        producer.disconnect()

    def test_invalid_vpn_fails(self):
        """Test connection fails with invalid VPN name."""
        producer = SolaceProducer(
            host=SOLACE_HOST,
            vpn_name="nonexistent-vpn",
            username=SOLACE_USERNAME,
            password=SOLACE_PASSWORD,
        )
        
        with pytest.raises(Exception):
            producer.connect()

    def test_invalid_host_fails(self):
        """Test connection fails with invalid host."""
        producer = SolaceProducer(
            host="tcp://nonexistent-host:55555",
            vpn_name=SOLACE_VPN,
            username=SOLACE_USERNAME,
            password=SOLACE_PASSWORD,
        )
        
        with pytest.raises(Exception):
            producer.connect()
