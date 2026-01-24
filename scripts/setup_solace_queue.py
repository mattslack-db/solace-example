#!/usr/bin/env python3
"""
Setup script to create a queue on Solace PubSub+ broker via SEMP API.

This script creates a queue and subscribes it to a topic so the
Spark consumer can read messages.
"""

import argparse
import requests
from requests.auth import HTTPBasicAuth


def create_queue(
    semp_url: str,
    vpn: str,
    queue_name: str,
    admin_user: str,
    admin_password: str,
) -> bool:
    """Create a queue on Solace broker."""
    url = f"{semp_url}/SEMP/v2/config/msgVpns/{vpn}/queues"

    payload = {
        "queueName": queue_name,
        "accessType": "exclusive",
        "egressEnabled": True,
        "ingressEnabled": True,
        "permission": "consume",
        "maxMsgSpoolUsage": 1500,  # MB
        "maxDeliveredUnackedMsgsPerFlow": 200,  # 2x typical batch size
    }

    try:
        response = requests.post(
            url,
            json=payload,
            auth=HTTPBasicAuth(admin_user, admin_password),
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            print(f"✓ Queue '{queue_name}' created successfully")
            return True
        elif response.status_code == 400 and "ALREADY_EXISTS" in response.text:
            print(f"✓ Queue '{queue_name}' already exists")
            return True
        else:
            print(f"✗ Failed to create queue: {response.status_code}")
            print(f"  Response: {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"✗ Connection error: {e}")
        return False


def add_subscription(
    semp_url: str,
    vpn: str,
    queue_name: str,
    topic: str,
    admin_user: str,
    admin_password: str,
) -> bool:
    """Add a topic subscription to a queue."""
    url = f"{semp_url}/SEMP/v2/config/msgVpns/{vpn}/queues/{queue_name}/subscriptions"

    payload = {
        "subscriptionTopic": topic,
    }

    try:
        response = requests.post(
            url,
            json=payload,
            auth=HTTPBasicAuth(admin_user, admin_password),
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            print(f"✓ Subscription '{topic}' added to queue '{queue_name}'")
            return True
        elif response.status_code == 400 and "ALREADY_EXISTS" in response.text:
            print(f"✓ Subscription '{topic}' already exists on queue '{queue_name}'")
            return True
        else:
            print(f"✗ Failed to add subscription: {response.status_code}")
            print(f"  Response: {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"✗ Connection error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Setup Solace queue for Spark consumer",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--semp-url",
        type=str,
        default="http://localhost:8081",
        help="Solace SEMP management URL",
    )
    parser.add_argument(
        "--vpn",
        type=str,
        default="default",
        help="Message VPN name",
    )
    parser.add_argument(
        "--queue",
        type=str,
        default="spark-consumer-queue",
        help="Queue name to create",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="synthetic/>",
        help="Topic subscription (use > for wildcard)",
    )
    parser.add_argument(
        "--admin-user",
        type=str,
        default="admin",
        help="SEMP admin username",
    )
    parser.add_argument(
        "--admin-password",
        type=str,
        default="admin",
        help="SEMP admin password",
    )

    args = parser.parse_args()

    print(f"\nSetting up Solace queue for Spark consumer")
    print(f"  SEMP URL: {args.semp_url}")
    print(f"  VPN: {args.vpn}")
    print(f"  Queue: {args.queue}")
    print(f"  Topic: {args.topic}")
    print()

    # Create queue
    if not create_queue(
        args.semp_url,
        args.vpn,
        args.queue,
        args.admin_user,
        args.admin_password,
    ):
        return 1

    # Add topic subscription
    if not add_subscription(
        args.semp_url,
        args.vpn,
        args.queue,
        args.topic,
        args.admin_user,
        args.admin_password,
    ):
        return 1

    print(f"\n✓ Queue setup complete!")
    print(f"\nYou can now run the Spark consumer:")
    print(f"  python -m solace_consumer --queue {args.queue}")

    return 0


if __name__ == "__main__":
    exit(main())
