"""Command-line interface for the PySpark Solace Consumer."""

import argparse
import sys
from pathlib import Path

from .consumer import SolaceSparkConsumer


def main() -> int:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Consume messages from Solace PubSub+ using PySpark Structured Streaming",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Connection options
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
        "--queue",
        type=str,
        default="spark-consumer-queue",
        help="Queue name to consume from",
    )

    # Processing options
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of messages per micro-batch",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=1,
        help="Number of consumers (0 = auto-scale)",
    )

    # Output options
    parser.add_argument(
        "--output",
        type=str,
        choices=["console", "parquet"],
        default="console",
        help="Output destination",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default="./solace_output",
        help="Output path for parquet files",
    )
    parser.add_argument(
        "--checkpoint-path",
        type=str,
        default="./solace_checkpoints",
        help="Checkpoint directory path",
    )
    parser.add_argument(
        "--stream-name",
        type=str,
        default="solace-spark-stream",
        help="Name for the streaming query",
    )

    # Spark options
    parser.add_argument(
        "--master",
        type=str,
        default="local[*]",
        help="Spark master URL",
    )
    parser.add_argument(
        "--connector-jar",
        type=str,
        default=None,
        help="Path to Solace Spark Connector JAR",
    )

    # Runtime options
    parser.add_argument(
        "--timeout",
        type=int,
        default=None,
        help="Timeout in seconds (None = run forever)",
    )

    args = parser.parse_args()

    try:
        consumer = SolaceSparkConsumer(
            app_name="SolaceSparkConsumer",
            host=args.host,
            vpn=args.vpn,
            username=args.username,
            password=args.password,
            queue=args.queue,
            batch_size=args.batch_size,
            partitions=args.partitions,
            spark_master=args.master,
            connector_jar=args.connector_jar,
        )

        print(f"Starting Solace Spark Consumer...")
        print(f"  Host: {args.host}")
        print(f"  VPN: {args.vpn}")
        print(f"  Queue: {args.queue}")
        print(f"  Output: {args.output}")

        if args.output == "console":
            query = consumer.write_to_console(stream_name=args.stream_name)
        else:
            output_path = str(Path(args.output_path).absolute())
            checkpoint_path = str(Path(args.checkpoint_path).absolute())
            print(f"  Output Path: {output_path}")
            print(f"  Checkpoint Path: {checkpoint_path}")
            query = consumer.write_to_parquet(
                output_path=output_path,
                checkpoint_path=checkpoint_path,
                stream_name=args.stream_name,
            )

        print("\nStreaming started. Press Ctrl+C to stop.\n")

        if args.timeout:
            query.awaitTermination(args.timeout * 1000)
        else:
            query.awaitTermination()

        return 0

    except KeyboardInterrupt:
        print("\nInterrupted by user")
        return 130

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1

    finally:
        if "consumer" in locals():
            consumer.stop()


if __name__ == "__main__":
    sys.exit(main())
