import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp, expr, floor, lower, months_between

KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092"
)
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb://mongodb1:27017,mongodb2:27017,mongodb3:27017/?replicaSet=rs0",
)
MONGO_DATABASE = os.environ.get("MONGO_DATABASE", "users_db")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "users")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "users_created")
CHECKPOINT_LOCATION = os.environ.get("CHECKPOINT_LOCATION", "/opt/spark/checkpoints/users")

# The producer registers this schema and the consumer decodes with it, so both
# sides share one data contract.
SCHEMA_PATH = "/opt/airflow/dags/schemas/user.avsc"


def load_avro_schema():
    """Read the Avro schema JSON from the shared schema file."""
    with open(SCHEMA_PATH) as f:
        return f.read()


def create_spark_connection():
    """Create (or reuse) the SparkSession, the entry point to Spark."""
    try:
        spark = (
            SparkSession.builder
            .appName("SparkDataStreaming")
            .getOrCreate()
        )
        logging.info("Spark connection created successfully.")
        return spark
    except Exception as e:
        logging.error(f"Could not create Spark connection: {e}")
        raise


def read_from_kafka(spark_conn, topic):
    """Define a streaming read of the Kafka topic (no data loaded yet)."""
    return (
        spark_conn.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_avro_stream(kafka_df, avro_schema):
    """Decode Confluent-wire-format Avro values into columns.

    The serializer prefix is [magic byte][4-byte schema id]; the schema id is
    only needed by the registry, so it is stripped before decoding.
    """
    return (
        kafka_df
        .withColumn("avro_value", expr("substring(value, 6)"))
        .select(from_avro(col("avro_value"), avro_schema).alias("data"))
        .select("data.*")
    )


def transform_users(df):
    """Enrich the decoded records before they are persisted."""
    return (
        df.withColumn("email", lower(col("email")))
        .withColumn("username", lower(col("username")))
        .withColumn(
            "age",
            floor(months_between(current_timestamp(), col("dob")) / 12),
        )
        .withColumn("processed_at", current_timestamp())
    )


def write_to_mongodb(batch_df, _):
    """Write a micro-batch of transformed users to MongoDB."""
    (
        batch_df.write
        .format("mongodb")
        .mode("append")
        .option("connection.uri", MONGO_URI)
        .option("database", MONGO_DATABASE)
        .option("collection", MONGO_COLLECTION)
        .option("write.concern.w", "majority")
        .save()
    )


def run_streaming_pipeline(spark_conn, topic):
    """Wire up read -> decode -> transform -> write and run the query."""
    kafka_df = read_from_kafka(spark_conn, topic)
    structured_df = transform_users(
        parse_avro_stream(kafka_df, load_avro_schema())
    )

    query = (
        structured_df.writeStream
        .foreachBatch(write_to_mongodb)
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    logging.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    spark_conn = create_spark_connection()
    run_streaming_pipeline(spark_conn, KAFKA_TOPIC)
    spark_conn.stop()