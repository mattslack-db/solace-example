"""Command-line interface for the Solace producer."""

import argparse
import sys

from .producer import SolaceProducer


def main() -> int:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Generate and publish synthetic JSON messages to Solace PubSub+",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-n", "--count",
        type=int,
        default=10,
        help="Number of messages to produce",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="tcp://localhost:55554",
        help="Solace broker host URL",
    )
    parser.add_argument(
        "--vpn",
        type=str,
        default="default",
        help="Message VPN name",
    )
    parser.add_argument(
        "--username",
        type=str,
        default="default",
        help="Client username",
    )
    parser.add_argument(
        "--password",
        type=str,
        default="default",
        help="Client password",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="synthetic/events",
        help="Topic to publish messages to",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0,
        help="Delay between messages in milliseconds",
    )
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Suppress progress output",
    )

    args = parser.parse_args()

    try:
        with SolaceProducer(
            host=args.host,
            vpn_name=args.vpn,
            username=args.username,
            password=args.password,
            topic=args.topic,
        ) as producer:
            producer.produce_messages(
                count=args.count,
                delay_ms=args.delay,
                verbose=not args.quiet,
            )
        return 0

    except KeyboardInterrupt:
        print("\nInterrupted by user")
        return 130

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
