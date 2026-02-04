import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql import functions as F
from pyspark import SparkConf

import xml.etree.ElementTree as ET
from pyspark.sql.types import StructType, StructField, StringType
import pandas as pd

# https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html
# https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html


conf = SparkConf()

conf.set("spark.metrics.conf", "../metrics.properties")
conf.set("spark.ui.port", "4040")

kv6_schema = StructType([
    StructField("msg_type", StringType(), True),
    StructField("dataownercode", StringType(), True),
    StructField("lineplanningnumber", StringType(), True),
    StructField("journeynumber", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("vehiclenumber", StringType(), True),
    StructField("rd_x", StringType(), True),
    StructField("rd_y", StringType(), True)
])

save_dir = os.path.join(os.path.dirname(__file__), "../data/lake")
os.makedirs(save_dir, exist_ok=True)

spark = SparkSession.builder \
    .appName("ndov_xml_consumer") \
    .master("local[*]") \
    .config(conf=conf) \
    .config("spark.ui.host", "0.0.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1") \
    .getOrCreate()

print(spark.sparkContext.getConf().get("spark.jars.packages"))

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ndov_xml") \
    .option("startingOffsets", "earliest") \
    .load()
    
    # startingOffsets: fix streaming query offset @ run time

df = df.selectExpr("CAST(value AS STRING) as xml_payload")
# xml_payload: clmn name where Kafka msg value is stored


def extract_kv6info(decompressed_str: str):
    ns = {
        "tmi8": "http://bison.connekt.nl/tmi8/kv6/msg",
        "tmi8c": "http://bison.connekt.nl/tmi8/kv6/core"
    }

    try:
        root = ET.fromstring(decompressed_str)
        kv6info = root.find(".//tmi8:KV6posinfo", ns)

        if kv6info is None:
            return {
                "msg_type": "UNKNOWN",
                "dataownercode": "",
                "lineplanningnumber": "",
                "journeynumber": "",
                "timestamp": "",
                "vehiclenumber": "",
                "rd_x": "",
                "rd_y": ""
            }

        for msg_type in ["INIT", "ONROUTE", "ONSTOP", "ARRIVAL", "DEPARTURE", "DELAY", "END"]:
            elem = kv6info.find(f"tmi8:{msg_type}", ns)
            if elem is not None:
                return {
                    "msg_type": msg_type,
                    "dataownercode": elem.findtext("tmi8:dataownercode", default="", namespaces=ns),
                    "lineplanningnumber": elem.findtext("tmi8:lineplanningnumber", default="", namespaces=ns),
                    "journeynumber": elem.findtext("tmi8:journeynumber", default="", namespaces=ns),
                    "timestamp": elem.findtext("tmi8:timestamp", default="", namespaces=ns),
                    "vehiclenumber": elem.findtext("tmi8:vehiclenumber", default="", namespaces=ns),
                    "rd_x": elem.findtext("tmi8:rd-x", default="", namespaces=ns),
                    "rd_y": elem.findtext("tmi8:rd-y", default="", namespaces=ns),
                }

        # If KV6posinfo exists but no known type found
        return {
            "msg_type": "UNKNOWN",
            "dataownercode": "",
            "lineplanningnumber": "",
            "journeynumber": "",
            "timestamp": "",
            "vehiclenumber": "",
            "rd_x": "",
            "rd_y": ""
        }

    except Exception as e:
        # Return error row instead of None
        return {
            "msg_type": f"ERROR: {e}",
            "dataownercode": "",
            "lineplanningnumber": "",
            "journeynumber": "",
            "timestamp": "",
            "vehiclenumber": "",
            "rd_x": "",
            "rd_y": ""
        }

    

@pandas_udf(kv6_schema)
def parse_xml_udf(xml_series: pd.Series) -> pd.DataFrame:
    rows = []
    for xml in xml_series:
        parsed = extract_kv6info(xml)
        if parsed:
            rows.append(parsed)
    return pd.DataFrame(rows)

parsed_df = df.select(parse_xml_udf("xml_payload").alias("data")).select("data.*")

# Cast timestamp
parsed_df = (
    parsed_df
    .withColumn("timestamp", F.expr("try_cast(timestamp as timestamp)"))
    .withColumn("lineplanningnumber", F.expr("try_cast(lineplanningnumber as int)"))
    .withColumn("journeynumber", F.expr("try_cast(journeynumber as int)"))
    .withColumn("vehiclenumber", F.expr("try_cast(vehiclenumber as int)"))
    .withColumn("rd_x", F.expr("try_cast(rd_x as int)"))
    .withColumn("rd_y", F.expr("try_cast(rd_y as int)"))
)


# Raw Parquet
'''
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", os.path.join(save_dir, "checkpoints")) \
    .option("path", save_dir) \
    .start()
    # .partitionBy("lineplanningnumber", "journeynumber") \
'''

# Delta Lake
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", os.path.join(save_dir, "checkpoints")) \
    .option("path", save_dir) \
    .start()
    


query.awaitTermination()
