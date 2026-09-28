#!/usr/bin/env python3
"""911 Call Ingestion Service - Ingests emergency calls and publishes to Kafka."""
import argparse
import json
import sys
import time
from datetime import UTC, datetime
from typing import Dict, Any

# No need to modify sys.path - we're now inside the package

from sentinel_dispatch.utils.config import load_config
from sentinel_dispatch.utils.kafka_utils import KafkaProducer, ensure_topic_exists
from sentinel_dispatch.utils.logging_config import configure_logging, get_logger

# Configure logging
configure_logging()
logger = get_logger(__name__)


def parse_call_file(file_path: str) -> list[Dict[str, Any]]:
    """
    Parse a JSON file containing emergency calls.

    Args:
        file_path: Path to JSON file with calls

    Returns:
        List of call dictionaries
    """
    with open(file_path, "r") as f:
        data = json.load(f)
        # Handle both single call and list of calls
        if isinstance(data, dict):
            return [data]
        return data


def format_call_message(call: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format a call dictionary to match the expected schema.

    Args:
        call: Raw call data

    Returns:
        Formatted call message
    """
    # Ensure required fields exist
    message = {
        "callId": call.get("callId") or call.get("id", f"call-{int(time.time())}"),
        "timestamp": call.get("timestamp") or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "callerLocation": call.get("callerLocation") or call.get("location", {}),
        "transcript": call.get("transcript") or call.get("text", ""),
        "metadata": call.get("metadata", {}),
    }

    # Ensure metadata has required fields
    if "phoneNumber" not in message["metadata"]:
        message["metadata"]["phoneNumber"] = call.get("phoneNumber", "unknown")
    if "callDuration" not in message["metadata"]:
        message["metadata"]["callDuration"] = call.get("callDuration", 0)

    return message


def ingest_calls_from_file(producer: KafkaProducer, file_path: str, topic: str = "emergency-calls") -> None:
    """
    Ingest calls from a JSON file and publish to Kafka.

    Args:
        producer: Kafka producer instance
        file_path: Path to JSON file with calls
        topic: Kafka topic name
    """
    logger.info("sentinel_dispatch.services.ingest_calls.ingest_calls_from_file.read_file", file_path=file_path)
    calls = parse_call_file(file_path)

    logger.info("sentinel_dispatch.services.ingest_calls.ingest_calls_from_file.publishing", num_calls=len(calls), topic=topic)
    for call in calls:
        try:
            message = format_call_message(call)
            call_id = message["callId"]
            producer.publish(topic, key=call_id, value=message)
            logger.info("sentinel_dispatch.services.ingest_calls.ingest_calls_from_file.published", call_id=call_id)
        except Exception as e:
            logger.error("sentinel_dispatch.services.ingest_calls.ingest_calls_from_file.error", exc_info=True, error=str(e))

    # Flush to ensure all messages are sent
    producer.flush()
    logger.info("sentinel_dispatch.services.ingest_calls.ingest_calls_from_file.completed", num_calls=len(calls))


def ingest_single_call(producer: KafkaProducer, call: Dict[str, Any], topic: str = "emergency-calls") -> None:
    """
    Ingest a single call and publish to Kafka.

    Args:
        producer: Kafka producer instance
        call: Call dictionary
        topic: Kafka topic name
    """
    try:
        message = format_call_message(call)
        call_id = message["callId"]
        producer.publish(topic, key=call_id, value=message)
        producer.flush()
        logger.info("sentinel_dispatch.services.ingest_calls.ingest_single_call.published", call_id=call_id)
    except Exception as e:
        logger.error("sentinel_dispatch.services.ingest_calls.ingest_single_call.error", exc_info=True, error=str(e))
        raise


def main():
    """Main entry point for call ingestion service."""
    parser = argparse.ArgumentParser(description="Ingest 911 calls and publish to Kafka")
    parser.add_argument(
        "--file",
        type=str,
        help="Path to JSON file containing calls to ingest",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="emergency-calls",
        help="Kafka topic name (default: emergency-calls)",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Watch file for changes and ingest new calls (not implemented yet)",
    )

    args = parser.parse_args()

    try:
        # Load configuration (mode from SENTINEL_MODE env var)
        config = load_config()
        kafka_config = config.kafka
        bootstrap_servers = kafka_config.get("bootstrap_servers", "localhost:9092")

        # Ensure topic exists
        logger.info("sentinel_dispatch.services.ingest_calls.main.ensuring_topic", topic=args.topic)
        ensure_topic_exists(bootstrap_servers, args.topic)

        # Create producer
        producer = KafkaProducer(bootstrap_servers)

        if args.file:
            # Ingest from file
            ingest_calls_from_file(producer, args.file, args.topic)
        else:
            logger.error("sentinel_dispatch.services.ingest_calls.main.no_input")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("sentinel_dispatch.services.ingest_calls.main.interrupted")
        sys.exit(0)
    except Exception as e:
        logger.error("sentinel_dispatch.services.ingest_calls.main.fatal_error", exc_info=True, error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()

