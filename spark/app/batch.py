from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType
import math
import os
import time

# Data lake to data warehouse

# Continuously run this batch job every 5 min
# period=5
# while True:
#     time.sleep(period*60)

save_dir = os.path.join(os.path.dirname(__file__), "../data/warehouse")
os.makedirs(save_dir, exist_ok=True)

spark = SparkSession.builder.appName("distance_batch") \
    .getOrCreate()


df = spark.read.format("delta").load("../data/lake")

'''
# Define window for lag
w = Window.partitionBy("lineplanningnumber", "journeynumber", "vehiclenumber").orderBy("timestamp")

# Previous coordinates
df = df.withColumn("prev_rd_x", F.lag("rd_x").over(w))
df = df.withColumn("prev_rd_y", F.lag("rd_y").over(w))

# Distance function
def dist(x1, y1, x2, y2):
    if None in (x1, y1, x2, y2):
        return None
    try:
        x1, y1, x2, y2 = float(x1), float(y1), float(x2), float(y2)
        return round(math.sqrt((x2 - x1)**2 + (y2 - y1)**2), 2)
    except Exception:
        return None


dist_udf = F.udf(dist, FloatType())

df = df.withColumn("straight_line_distance", dist_udf("rd_x", "rd_y", "prev_rd_x", "prev_rd_y"))
'''

df.write.mode("overwrite").parquet(save_dir)
spark.stop()
