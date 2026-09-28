"""Kafka utilities for Sentinel Dispatch."""
import json
from typing import Any, Dict, Optional

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.cimpl import KafkaException

from sentinel_dispatch.utils.logging_config import get_logger

logger = get_logger(__name__)


class KafkaProducer:
    """Kafka producer wrapper for publishing messages."""

    def __init__(self, bootstrap_servers: str, **kwargs):
        """
        Initialize Kafka producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers (e.g., "localhost:9092")
            **kwargs: Additional producer configuration
        """
        config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "sentinel-dispatch-producer",
            **kwargs,
        }
        self.producer = Producer(config)
        logger.info("sentinel_dispatch.kafka_utils.KafkaProducer.__init__.initialized", bootstrap_servers=bootstrap_servers)

    def publish(self, topic: str, key: Optional[str], value: Dict[str, Any]) -> None:
        """
        Publish a message to a Kafka topic.

        Args:
            topic: Topic name
            key: Message key (optional)
            value: Message value (dict that will be JSON serialized)
        """
        try:
            # Serialize value to JSON
            value_json = json.dumps(value).encode("utf-8")
            key_bytes = key.encode("utf-8") if key else None

            # Publish message
            self.producer.produce(
                topic,
                key=key_bytes,
                value=value_json,
                callback=self._delivery_callback,
            )

            # Trigger delivery callbacks
            self.producer.poll(0)

            call_id = value.get('callId')
            fire_id = value.get('fireId')
            logger.debug("sentinel_dispatch.kafka_utils.KafkaProducer.publish.published", topic=topic, call_id=call_id, fire_id=fire_id)

        except Exception as e:
            logger.error("sentinel_dispatch.kafka_utils.KafkaProducer.publish.error", topic=topic, error=str(e), exc_info=True)
            raise

    def flush(self, timeout: float = 10.0) -> None:
        """
        Flush pending messages.

        Args:
            timeout: Maximum time to wait for messages to be delivered
        """
        self.producer.flush(timeout)

    @staticmethod
    def _delivery_callback(err, msg):
        """Callback for message delivery confirmation."""
        if err:
            logger.error("sentinel_dispatch.kafka_utils.KafkaProducer._delivery_callback.failed", error=str(err))
        else:
            logger.debug("sentinel_dispatch.kafka_utils.KafkaProducer._delivery_callback.delivered", topic=msg.topic(), partition=msg.partition())


def ensure_topic_exists(
    bootstrap_servers: str, topic: str, num_partitions: int = 1, replication_factor: int = 1
) -> None:
    """
    Ensure a Kafka topic exists, creating it if necessary.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        topic: Topic name
        num_partitions: Number of partitions
        replication_factor: Replication factor
    """
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

    # Check if topic exists
    metadata = admin_client.list_topics(timeout=10)
    if topic in metadata.topics:
        logger.info("sentinel_dispatch.kafka_utils.ensure_topic_exists.exists", topic=topic)
        return

    # Create topic
    topic_list = [NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)]
    futures = admin_client.create_topics(topic_list)

    # Wait for topic creation
    for topic_name, future in futures.items():
        try:
            future.result()
            logger.info("sentinel_dispatch.kafka_utils.ensure_topic_exists.created", topic=topic_name)
        except KafkaException as e:
            logger.error("sentinel_dispatch.kafka_utils.ensure_topic_exists.error", topic=topic_name, error=str(e))
            raise

