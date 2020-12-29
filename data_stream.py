import json
import logging
import pyspark.sql.functions as psf
import time

from dateutil.parser import parse as parse_date
from pyspark.sql import SparkSession
from pyspark.sql.types import *

RADIO_CODE_JSON_FILEPATH = "radio_code.json"
TOPIC = "sf_crime"

schema = StructType([
    StructField("address", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("call_date_time", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("city", StringType(), True),
    StructField("common_location", StringType(), True),
    StructField("crime_id", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("state", StringType(), True)
])

radio_schema = StructType([
    StructField("disposition_code", StringType(), True),
    StructField("description", StringType(), True)
])


def init_spark():
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')
    return spark

def run_spark_job(spark):

    df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", TOPIC)
        .option("maxOffsetsPerTrigger", 200)
        .option("maxRatePerPartition", 10)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Show schema for the incoming resources for checks
    df.printSchema()

    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("DF")) \
        .select("DF.*")

    cols = [psf.col('crime_id'),
            psf.col('original_crime_type_name'),
            psf.to_timestamp(psf.col('call_date_time')).alias('call_timestamp'),
            psf.col('address'),
            psf.col('disposition')]

    distinct_table = service_table.select(*cols)
    agg_df = (
        distinct_table.withWatermark("call_timestamp", "60 minutes")
        .groupBy(psf.col('original_crime_type_name')).count()
    )

    query = (
        agg_df.orderBy(psf.desc("count"))
        .writeStream
        .trigger(processingTime="5 seconds")
        .queryName("service_table")
        .outputMode('complete')
        .format('console')
        .option("truncate", "false")
        .start()
    )

    agg_disp_df = (
        distinct_table.withWatermark("call_timestamp", "60 minutes")
        .groupBy(psf.window(distinct_table.call_timestamp, "60 minutes", "60 minutes"),
                 psf.col('original_crime_type_name'),
                 psf.col('disposition')).count()
    )

    radio_code_df = (
        spark
        .read
        .option("multiline", "true")
        .json(RADIO_CODE_JSON_FILEPATH, schema=radio_schema)
        .withColumnRenamed("disposition_code", "disposition"))

    join_query = (
        agg_disp_df
        .join(radio_code_df, "disposition")
        .select(psf.col("window"), psf.col("original_crime_type_name"), psf.col("description"), psf.col("count"))
        .orderBy(psf.desc("window"), psf.desc("count"))
        .writeStream
        .trigger(processingTime="5 seconds")
        .outputMode("Complete")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()
    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = init_spark()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
