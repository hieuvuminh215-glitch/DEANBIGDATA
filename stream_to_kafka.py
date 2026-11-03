"""
Gets the random user API data and writes the data to a Kafka topic every 10 seconds
"""
import logging
import requests
import json
from kafka import KafkaProducer

logger = logging.getLogger(__name__)
API_URL = "https://randomuser.me/api/?results=1"
BOOTSTRAP_SERVERS = ["kafka1:19092", "kafka2:19093", "kafka3:19094"]


def create_response_dict(url: str = API_URL) -> dict:
    """
    Creates the results JSON from the random user API call
    """
    response = requests.get(url, timeout=(3.05, 10))
    response.raise_for_status()
    data = response.json()
    results = data.get("results")
    if not results:
        raise ValueError("The random user API response contains no results")

    return results[0]


def create_final_json(results: dict) -> dict:
    """
    Creates the final JSON to be sent to Kafka topic only with necessary keys
    """
    login = results.get("login", {})
    profile_id = login.get("uuid")
    if not profile_id:
        raise ValueError("The random user API result is missing login.uuid")

    kafka_data = {
        "profile_id": str(profile_id),
    }

    kafka_data["full_name"] = f"{results['name']['title']}. {results['name']['first']} {results['name']['last']}"
    kafka_data["gender"] = results["gender"]
    kafka_data["location"] = f"{results['location']['street']['number']}, {results['location']['street']['name']}"
    kafka_data["city"] = results['location']['city']
    kafka_data["country"] = results['location']['country']
    kafka_data["postcode"] = str(results['location']['postcode'])
    kafka_data["latitude"] = float(results['location']['coordinates']['latitude'])
    kafka_data["longitude"] = float(results['location']['coordinates']['longitude'])
    kafka_data["email"] = results["email"]

    return kafka_data


def create_kafka_producer():
    """
    Creates the Kafka producer object
    """
    return KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)


def start_streaming():
    """
    Writes one API result to the Kafka topic random_names.

    Airflow schedules this task every 10 seconds.
    """
    producer = create_kafka_producer()
    try:
        kafka_data = create_final_json(create_response_dict())
        producer.send(
            "random_names",
            json.dumps(kafka_data).encode("utf-8")
        ).get(timeout=10)
        logger.info("Published a random user to Kafka topic random_names")
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    start_streaming()
