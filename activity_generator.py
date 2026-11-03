
import logging
import json
import random
import uuid

from datetime import datetime, timezone
from kafka import KafkaProducer

logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = [
    "kafka1:19092",
    "kafka2:19093",
    "kafka3:19094"
]

KAFKA_TOPIC = "user_activities"

EVENT_TYPES = [
    "login",
    "logout",
    "view_product",
    "search",
    "add_to_cart",
    "purchase"
]

DEVICES = ["mobile", "desktop", "tablet"]

USER_IDS = [f"user_{i:03d}" for i in range(1, 101)]

PRODUCT_IDS = [f"product_{i:03d}" for i in range(1, 51)]


def generate_activity() -> dict:
    """
    Generates one random user activity event.
    """

    event_type = random.choice(EVENT_TYPES)

    activity = {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(USER_IDS),
        "event_type": event_type,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "device": random.choice(DEVICES),
        "product_id": (
            random.choice(PRODUCT_IDS)
            if event_type in ["view_product", "add_to_cart", "purchase"]
            else None
        )
    }

    return activity


def create_kafka_producer():
    """
    Creates Kafka Producer.
    """

    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS
    )


def start_activity_streaming():
    """
    Generates one activity event and publishes it to Kafka.
    """

    producer = create_kafka_producer()

    try:
        activity = generate_activity()

        producer.send(
            KAFKA_TOPIC,
            json.dumps(activity).encode("utf-8")
        ).get(timeout=10)

        logger.info(
            "Published activity event: %s",
            activity["event_type"]
        )

    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    start_activity_streaming()
