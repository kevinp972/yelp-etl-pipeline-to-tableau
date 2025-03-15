import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    expr,
    when,
    array,
    lit,
    size,
    posexplode_outer,
    explode,
    split,
    to_timestamp,
    trim,
    to_date
)
from pyspark.sql import functions as F
from pyspark.sql import Window

spark = SparkSession.builder.getOrCreate()

# business.json
df = spark.read.json("file:///home/anshadui/yelp_academic_dataset_business.json")

# Filter restaurants
restaurant_keywords = [
    "Restaurants",
    "Food",
    "Diner",
    "Fast Food",
    "Pizza",
    "Sushi",
    "Bistro",
    "Buffet",
    "Steakhouse",
    "Catering",
    "Delis",
]
restaurant_pattern = "|".join(restaurant_keywords)
restaurant_df = df.filter(
    (col("attributes.RestaurantsCounterService").cast("boolean") == True)
    | (col("attributes.RestaurantsDelivery").cast("boolean") == True)
    | (col("attributes.RestaurantsGoodForGroups").cast("boolean") == True)
    | (col("attributes.RestaurantsReservations").cast("boolean") == True)
    | (col("attributes.RestaurantsTableService").cast("boolean") == True)
    | (col("attributes.RestaurantsTakeOut").cast("boolean") == True)
    | (col("categories").rlike(restaurant_pattern))
)

# Filter restaurants by cuisine
cuisine_keywords = [
    "American",
    "Italian",
    "Japanese",
    "Mexican",
    "Chinese",
    "French",
    "Greek",
    "Korean",
    "Vietnamese",
    "Filipino",
    "Moroccan",
    "Mediterranean",
    "Indian",
    "Cajun/Creole",
    "Thai",
    "Spanish",
    "Lebanese",
    "Turkish",
    "Caribbean",
    "Brazilian",
    "Argentine",
    "Persian",
    "Iranian",
    "African",
    "Ethiopian",
]

restaurant_df = restaurant_df.withColumn(
    "cuisine_list",
    array(
        *[
            when(col("categories").rlike(f"\\b{cuisine}\\b"), lit(cuisine))
            for cuisine in cuisine_keywords
        ]
    ),
)
restaurant_df = restaurant_df.withColumn(
    "cuisine_list", expr("filter(cuisine_list, x -> x is not null)")
)
restaurant_df = restaurant_df.withColumn(
    "cuisine_list",
    when(size(col("cuisine_list")) == 0, None).otherwise(col("cuisine_list")),
)
restaurant_df_exploded = restaurant_df.select(
    "*", posexplode_outer(col("cuisine_list")).alias("pos", "cuisine_type")
).drop("cuisine_list", "pos")


# Filter open restaurants
restaurant_df_exploded = restaurant_df_exploded.filter(col("is_open") == 1)

# Select columns
selected_df = restaurant_df_exploded.select(
    col("business_id"),
    col("state"),
    col("city"),
    col("postal_code"),
    col("latitude"),
    col("longitude"),
    col("stars").alias("rating"),
    col("review_count"),
    col("cuisine_type"),
    col("name").alias("restaurant_name"),
)

selected_df.write.option("maxRecordsPerFile", 100000).mode("overwrite").parquet(
    "file:///home/anshadui/team17/spark/output/business_df.parquet"
)

# checkin.json
checkin_df = spark.read.json("file:///home/anshadui/yelp_academic_dataset_checkin.json")

checkin_df = (
    checkin_df.withColumn("date", explode(split(col("date"), ",")))
    .withColumn("date", trim(col("date")))
    .withColumn("check_in_time", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
    .drop("date")
)

checkin_df.write.option("maxRecordsPerFile", 100000).mode("overwrite").parquet(
    "file:///home/anshadui/team17/spark/output/checkin_df.parquet"
)


# Tip.json
tip_df = spark.read.json("file:///home/anshadui/yelp_academic_dataset_tip.json")

# rename columns
tip_df = tip_df.withColumnRenamed("text", "review_text").withColumnRenamed(
    "compliment_count", "review_popularity"
)
tip_df = tip_df.withColumn("date", to_date("date"))
tip_df = tip_df.drop("user_id")

tip_df.write.option("maxRecordsPerFile", 100000).mode("overwrite").parquet(
    "file:///home/anshadui/team17/spark/output/tip_df.parquet"
)
