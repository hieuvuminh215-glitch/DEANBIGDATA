
import logging
import time

from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import (
    StructType, StructField,
    FloatType, StringType
)
from pyspark.sql.functions import (
    coalesce, col, from_json, lit, to_timestamp, trim
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s"
)

logger = logging.getLogger("spark_structured_streaming")

KAFKA_SERVERS = (
    "kafka1:19092,kafka2:19093,kafka3:19094"
)

KEYSPACE = "spark_streaming"
PROFILE_ID_TABLE = "random_names_by_id"
CHECKPOINT_ROOT = "/opt/spark/work-dir/checkpoints/cluster-v1"
SPARK_PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0",
    "com.datastax.oss:java-driver-core-shaded:4.13.0",
]
CASSANDRA_RETRY_ATTEMPTS = 3
STREAM_RESTART_DELAY_SECONDS = 10
CASSANDRA_TRANSIENT_ERRORS = (
    "connection refused",
    "could not reach any contact point",
    "allnodesfailedexception",
    "connectioninitexception",
    "timed out",
    "timeout",
    "closedchannel",
)


def make_checkpoint_path(topic: str) -> str:
    return f"{CHECKPOINT_ROOT}/{topic}"


def create_spark_session():
    try:
        spark = (
            SparkSession.builder
            .appName("UserProfileAndActivityStreaming")
            .config("spark.jars.packages", ",".join(SPARK_PACKAGES))
            .config("spark.cassandra.connection.host", "cassandra")
            .config("spark.cassandra.connection.port", "9042")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")
        logger.info(
            "Spark session created successfully with packages: %s",
            ", ".join(SPARK_PACKAGES),
        )
        logger.info(
            "Cassandra endpoint configured as %s:%s",
            spark.sparkContext.getConf().get("spark.cassandra.connection.host"),
            spark.sparkContext.getConf().get("spark.cassandra.connection.port"),
        )

        return spark

    except Exception:
        logger.exception("Could not create Spark session")
        raise


def write_cassandra_batch(batch_df, table, batch_id):
    for attempt in range(1, CASSANDRA_RETRY_ATTEMPTS + 1):
        try:
            (
                batch_df.write
                .format("org.apache.spark.sql.cassandra")
                .options(table=table, keyspace=KEYSPACE)
                .mode("append")
                .save()
            )
            return
        except Exception as error:
            message = str(error).lower()
            is_transient = any(
                marker in message for marker in CASSANDRA_TRANSIENT_ERRORS
            )
            if not is_transient or attempt == CASSANDRA_RETRY_ATTEMPTS:
                raise

            delay = 5 * attempt
            logger.warning(
                "Transient Cassandra write failure for table %s, batch %s "
                "(attempt %s/%s); retrying in %s seconds: %s",
                table,
                batch_id,
                attempt,
                CASSANDRA_RETRY_ATTEMPTS,
                delay,
                error,
            )
            time.sleep(delay)


def read_kafka_stream(spark, topic):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )


def create_profile_dataframe(df):
    schema = StructType([
        StructField("profile_id", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("location", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("postcode", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("email", StringType(), True)
    ])

    parsed_df = (
        df.selectExpr("CAST(value AS STRING) AS json_value")
        .withColumn("data", from_json(col("json_value"), schema))
        .select("json_value", "data.*")
    )

    profile_id_is_uuid = col("profile_id").rlike(
        "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
        "[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    )
    is_valid = (
        profile_id_is_uuid
        & (trim(col("full_name")) != "")
        & (trim(col("email")) != "")
        & col("latitude").between(-90, 90)
        & col("longitude").between(-180, 180)
    )
    return parsed_df.withColumn("_is_valid", coalesce(is_valid, lit(False)))


def create_activity_dataframe(df):
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("device", StringType(), True),
        StructField("product_id", StringType(), True)
    ])

    parsed_df = (
        df.selectExpr("CAST(value AS STRING) AS json_value")
        .withColumn("data", from_json(col("json_value"), schema))
        .select("json_value", "data.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
    )

    valid_event_types = (
        "login", "logout", "view_product", "search", "add_to_cart", "purchase"
    )
    valid_devices = ("mobile", "desktop", "tablet")
    product_events = ("view_product", "add_to_cart", "purchase")
    event_id_is_uuid = col("event_id").rlike(
        "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
        "[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    )
    has_product_when_required = (
        (~col("event_type").isin(*product_events))
        | (col("product_id").isNotNull() & (trim(col("product_id")) != ""))
    )
    is_valid = (
        event_id_is_uuid
        & (trim(col("user_id")) != "")
        & col("event_type").isin(*valid_event_types)
        & col("event_time").isNotNull()
        & col("device").isin(*valid_devices)
        & has_product_when_required
    )
    return parsed_df.withColumn("_is_valid", coalesce(is_valid, lit(False)))


def write_profile_batch(batch_df, batch_id):
    batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        total_count = batch_df.count()
        valid_df = batch_df.filter(col("_is_valid")).dropDuplicates(["profile_id"])
        valid_count = valid_df.count()
        rejected_count = total_count - batch_df.filter(col("_is_valid")).count()
        logger.info(
            "Profile batch %s: received=%s valid_unique_profiles=%s rejected=%s",
            batch_id,
            total_count,
            valid_count,
            rejected_count,
        )
        if valid_count:
            legacy_columns = [
                "full_name", "gender", "location", "city", "country",
                "postcode", "latitude", "longitude", "email",
            ]
            write_cassandra_batch(
                valid_df.select("profile_id", *legacy_columns),
                PROFILE_ID_TABLE,
                batch_id,
            )
            write_cassandra_batch(
                valid_df.dropDuplicates(["full_name"]).select(*legacy_columns),
                "random_names",
                batch_id,
            )
    except Exception:
        logger.exception("Failed to write profile batch %s to Cassandra", batch_id)
        raise
    finally:
        batch_df.unpersist()


def write_activity_batch(batch_df, batch_id):
    batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        total_count = batch_df.count()
        valid_df = batch_df.filter(col("_is_valid")).dropDuplicates(["event_id"])
        valid_count = valid_df.count()
        rejected_count = total_count - batch_df.filter(col("_is_valid")).count()
        logger.info(
            "Activity batch %s: received=%s valid_unique_events=%s rejected=%s",
            batch_id,
            total_count,
            valid_count,
            rejected_count,
        )
        if valid_count:
            write_cassandra_batch(
                valid_df.drop("_is_valid", "json_value"),
                "user_activities",
                batch_id,
            )
    except Exception:
        logger.exception("Failed to write activity batch %s to Cassandra", batch_id)
        raise
    finally:
        batch_df.unpersist()


def start_streaming(profile_df, activity_df):
    logger.info("Starting both streaming queries")

    profile_checkpoint = make_checkpoint_path("random_names")
    activity_checkpoint = make_checkpoint_path("user_activities")

    profile_query = (
        profile_df.writeStream
        .outputMode("append")
        .option("checkpointLocation", profile_checkpoint)
        .foreachBatch(write_profile_batch)
        .start()
    )

    activity_query = (
        activity_df.writeStream
        .outputMode("append")
        .option("checkpointLocation", activity_checkpoint)
        .foreachBatch(write_activity_batch)
        .start()
    )

    logger.info(
        "Both streaming queries started successfully with checkpoints: %s, %s",
        profile_checkpoint,
        activity_checkpoint,
    )

    try:
        profile_df.sparkSession.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Stopping Spark streaming queries due to interrupt")
        raise
    except Exception:
        logger.exception("A Spark streaming query terminated unexpectedly")
        raise
    finally:
        if profile_query.isActive:
            profile_query.stop()
        if activity_query.isActive:
            activity_query.stop()


def write_streaming_data():
    spark = create_spark_session()
    try:
        while True:
            try:
                profile_kafka_df = read_kafka_stream(spark, "random_names")
                activity_kafka_df = read_kafka_stream(spark, "user_activities")
                profile_df = create_profile_dataframe(profile_kafka_df)
                activity_df = create_activity_dataframe(activity_kafka_df)
                start_streaming(profile_df, activity_df)
            except Exception:
                logger.exception(
                    "A streaming query failed; restarting both queries from "
                    "their existing checkpoints in %s seconds",
                    STREAM_RESTART_DELAY_SECONDS,
                )
                time.sleep(STREAM_RESTART_DELAY_SECONDS)
    finally:
        spark.stop()


if __name__ == "__main__":
    write_streaming_data()
