import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, date_trunc, row_number, split,
    regexp_extract, regexp_replace, when, 
    monotonically_increasing_id, broadcast
)
from pyspark.sql import functions as F
from pyspark.sql import Window
spark = SparkSession.builder.getOrCreate()

spark = SparkSession.builder.getOrCreate()
checkin_df.show(5)
checkin_df.printSchema()
