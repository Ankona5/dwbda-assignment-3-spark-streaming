"""Assignment 3 - Spark Structured Streaming with fault tolerance.

Source      : new JSON files dropped into input/ (file source)
Processing  : drop duplicate events, then keep running totals per category
Sink        : console
Fault tolerance:
  * checkpointLocation  - Spark saves source offsets, aggregation state and
    dedup state to disk. After a crash or restart the query resumes exactly
    where it stopped (no data lost, no file processed twice).
  * withWatermark + dropDuplicates - bounded state, exactly-once style results
    even when the source re-sends events.

Usage:
  python stream_job.py          # runs until stopped (Ctrl+C)
  python stream_job.py --once   # processes everything new, then stops
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

CHECKPOINT_DIR = "checkpoint"

spark = SparkSession.builder.appName("FaultTolerantStream").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "4")   # small local demo
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("event_id", StringType()),
    StructField("category", StringType()),
    StructField("amount", DoubleType()),
    StructField("event_time", TimestampType()),
])

events = spark.readStream.schema(schema).json("input/")

deduped = (
    events.withWatermark("event_time", "1 hour")
    .dropDuplicates(["event_id"])
)

totals = deduped.groupBy("category").agg(
    F.count("*").alias("events"),
    F.round(F.sum("amount"), 2).alias("total_amount"),
)

writer = (
    totals.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", CHECKPOINT_DIR)   # <- fault tolerance
)
if "--once" in sys.argv:
    writer = writer.trigger(availableNow=True)
else:
    writer = writer.trigger(processingTime="5 seconds")

query = writer.start()
query.awaitTermination()
