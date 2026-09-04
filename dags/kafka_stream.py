import logging
import os
import time
import uuid
from datetime import datetime, timezone

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("producer_logger")


def parse_iso_to_millis(iso_str):
    """Convert an ISO-8601 timestamp string to epoch milliseconds."""
    return int(datetime.fromisoformat(iso_str.replace("Z", "+00:00")).timestamp() * 1000)


KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "users_created")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
PIPELINE_DURATION = int(os.environ.get("PIPELINE_DURATION", "60"))

# Shared Avro contract (registered by producer, decoded by consumer).
SCHEMA_PATH = "/opt/airflow/dags/schemas/user.avsc"

SPARK_JOB = "/opt/airflow/sparkjobs/spark_stream.py"
SPARK_PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
    "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0",
    "org.apache.spark:spark-avro_2.13:4.2.0",
]
CHECKPOINT_LOCATION = "/opt/airflow/logs/spark-checkpoints/users"


def load_avro_schema():
    """Read the Avro schema JSON from the shared schema file."""
    with open(SCHEMA_PATH) as f:
        return f.read()


def get_data():
    """Fetch one random user record from the RandomUser API."""
    response = requests.get(
        "https://randomuser.me/api/",
        timeout=10,
    )
    response.raise_for_status()

    return response.json()["results"][0]


def format_data(user):
    """Map the API payload onto our Avro-schema-shaped record."""
    location = user["location"]

    return {
        "id": str(uuid.uuid4()),
        "first_name": user["name"]["first"],
        "last_name": user["name"]["last"],
        "gender": user["gender"],
        "address": (
            f"{location['street']['number']} "
            f"{location['street']['name']}, "
            f"{location['city']}, "
            f"{location['state']}, "
            f"{location['country']}"
        ),
        "post_code": str(location["postcode"]),
        "email": user["email"],
        "username": user["login"]["username"],
        "dob": parse_iso_to_millis(user["dob"]["date"]),
        "registered_date": parse_iso_to_millis(user["registered"]["date"]),
        "phone": user["phone"],
        "picture": user["picture"]["medium"],
    }


def delivery_report(err, msg):
    """Log the outcome of an async message delivery."""
    if err is not None:
        logger.error(
            "Message delivery failed: %s",
            err,
        )
    else:
        logger.info(
            "Message delivered to %s [%s] at offset %s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def stream_data():
    """Publish users to Kafka as Avro for PIPELINE_DURATION seconds.

    The Avro serializer encodes each record with the schema registered in the
    schema registry and embeds the confluent wire header (magic byte + schema
    id) that the Spark consumer strips before decoding.
    """
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(
        schema_registry_client,
        load_avro_schema(),
        to_dict=lambda obj, ctx: obj,
    )

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "delivery.timeout.ms": 30000,
        }
    )

    start_time = time.time()

    while time.time() - start_time < PIPELINE_DURATION:
        try:
            user = get_data()
            user_data = format_data(user)

            producer.produce(
                topic=KAFKA_TOPIC,
                value=avro_serializer(
                    user_data,
                    SerializationContext(KAFKA_TOPIC, MessageField.VALUE),
                ),
                callback=delivery_report,
            )

            producer.poll(0)

            logger.info(
                "User produced: %s",
                user_data["id"],
            )

        except requests.RequestException as e:
            logger.error(
                "Failed to fetch user data: %s",
                e,
            )

        except Exception as e:
            logger.exception("Unexpected error while streaming data: %s", e)

        time.sleep(1)

    producer.flush()
    logger.info("Producer finished after %s seconds.", PIPELINE_DURATION)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1, tzinfo=timezone.utc),
}


# Single pipeline DAG: producer and consumer run in parallel on one trigger.
with DAG(
    dag_id="user_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["kafka", "spark", "mongodb", "users"],
) as dag:
    producer_task = PythonOperator(
        task_id="stream_data_from_api",
        python_callable=stream_data,
    )

    consumer_task = SparkSubmitOperator(
        task_id="spark_consumer",
        application=SPARK_JOB,
        conn_id="spark_default",
        name="spark_consumer",
        packages=",".join(SPARK_PACKAGES),
        env_vars={
            "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS,
            "KAFKA_TOPIC": KAFKA_TOPIC,
            "MONGO_URI": os.environ.get(
                "MONGO_URI",
                "mongodb://admin:admin@mongodb1:27017,mongodb2:27017,mongodb3:27017/?replicaSet=rs0&authSource=admin",
            ),
            "MONGO_DATABASE": os.environ.get("MONGO_DATABASE", "users_db"),
            "MONGO_COLLECTION": os.environ.get("MONGO_COLLECTION", "users"),
            "SCHEMA_REGISTRY_URL": SCHEMA_REGISTRY_URL,
            "CHECKPOINT_LOCATION": CHECKPOINT_LOCATION,
        },
    )
