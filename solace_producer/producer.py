"""Solace producer for publishing synthetic JSON messages."""

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from faker import Faker
from solace.messaging.messaging_service import MessagingService
from solace.messaging.config.transport_security_strategy import TLS
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener
from solace.messaging.resources.topic import Topic


class SolaceProducer:
    """Producer that publishes synthetic JSON messages to Solace PubSub+."""

    def __init__(
        self,
        host: str = "tcp://localhost:55554",
        vpn_name: str = "default",
        username: str = "default",
        password: str = "default",
        topic: str = "synthetic/events",
    ):
        """
        Initialize the Solace producer.

        Args:
            host: Solace broker host URL (e.g., tcp://localhost:55555)
            vpn_name: Message VPN name
            username: Client username
            password: Client password
            topic: Default topic to publish messages to
        """
        self.host = host
        self.vpn_name = vpn_name
        self.username = username
        self.password = password
        self.topic_name = topic
        self.faker = Faker()
        self._messaging_service = None
        self._publisher = None

    def connect(self) -> None:
        """Connect to the Solace broker and start the publisher."""
        broker_props = {
            "solace.messaging.transport.host": self.host,
            "solace.messaging.service.vpn-name": self.vpn_name,
            "solace.messaging.authentication.scheme.basic.username": self.username,
            "solace.messaging.authentication.scheme.basic.password": self.password,
        }

        self._messaging_service = (
            MessagingService.builder()
            .from_properties(broker_props)
            .build()
        )
        self._messaging_service.connect()
        print(f"Connected to Solace broker at {self.host}")

        self._publisher = (
            self._messaging_service
            .create_direct_message_publisher_builder()
            .on_back_pressure_elastic()
            .build()
        )
        self._publisher.start()
        print("Publisher started")

    def disconnect(self) -> None:
        """Disconnect from the Solace broker."""
        if self._publisher:
            self._publisher.terminate()
            print("Publisher terminated")
        if self._messaging_service:
            self._messaging_service.disconnect()
            print("Disconnected from Solace broker")

    def generate_synthetic_message(self) -> dict[str, Any]:
        """
        Generate a synthetic JSON message with realistic fake data.

        Returns:
            Dictionary containing synthetic event data
        """
        return {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": self.faker.random_element(
                ["user_login", "purchase", "page_view", "click", "signup"]
            ),
            "user": {
                "user_id": self.faker.uuid4(),
                "username": self.faker.user_name(),
                "email": self.faker.email(),
                "ip_address": self.faker.ipv4(),
            },
            "device": {
                "type": self.faker.random_element(["mobile", "desktop", "tablet"]),
                "os": self.faker.random_element(["iOS", "Android", "Windows", "macOS", "Linux"]),
                "browser": self.faker.random_element(["Chrome", "Firefox", "Safari", "Edge"]),
            },
            "location": {
                "country": self.faker.country_code(),
                "city": self.faker.city(),
                "latitude": float(self.faker.latitude()),
                "longitude": float(self.faker.longitude()),
            },
            "metadata": {
                "session_id": self.faker.uuid4(),
                "page_url": self.faker.url(),
                "referrer": self.faker.url() if self.faker.boolean(chance_of_getting_true=70) else None,
                "value": round(self.faker.pyfloat(min_value=0, max_value=1000), 2),
            },
        }

    def publish_message(self, message: dict[str, Any], topic: str | None = None) -> None:
        """
        Publish a message to the Solace broker.

        Args:
            message: Dictionary to publish as JSON
            topic: Optional topic override
        """
        if not self._publisher:
            raise RuntimeError("Publisher not connected. Call connect() first.")

        topic_destination = Topic.of(topic or self.topic_name)
        message_body = json.dumps(message)

        outbound_msg = (
            self._messaging_service
            .message_builder()
            .with_application_message_id(message.get("event_id", str(uuid.uuid4())))
            .build(message_body)
        )

        self._publisher.publish(outbound_msg, topic_destination)

    def produce_messages(
        self,
        count: int,
        topic: str | None = None,
        delay_ms: float = 0,
        verbose: bool = True,
    ) -> int:
        """
        Produce a specified number of synthetic messages.

        Args:
            count: Number of messages to produce
            topic: Optional topic override
            delay_ms: Delay between messages in milliseconds
            verbose: Print progress information

        Returns:
            Number of messages successfully published
        """
        published = 0
        start_time = time.time()

        for i in range(count):
            message = self.generate_synthetic_message()
            self.publish_message(message, topic)
            published += 1

            if verbose and (i + 1) % 100 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                print(f"Published {i + 1}/{count} messages ({rate:.1f} msg/sec)")

            if delay_ms > 0:
                time.sleep(delay_ms / 1000)

        elapsed = time.time() - start_time
        if verbose:
            rate = published / elapsed if elapsed > 0 else 0
            print(f"\nCompleted: {published} messages in {elapsed:.2f}s ({rate:.1f} msg/sec)")

        return published

    def __enter__(self) -> "SolaceProducer":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.disconnect()
