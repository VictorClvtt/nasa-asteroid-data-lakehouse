# %%
print("Importing libraries, variables and initializing a Spark session...")
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from datetime import datetime

import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
sys.path.append(project_root)

from src.utils.variables import load_env_vars
from src.utils.bucket import df_to_bucket

_, access_key, secret_key, bucket_name, bucket_endpoint = load_env_vars()

today_str = datetime.today().strftime("%Y-%m-%d")

spark = SparkSession.builder \
    .appName("bronze_to_silver") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.698") \
    .config("spark.hadoop.fs.s3a.endpoint", bucket_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# %%
print("Reading data from the Bronze layer...")
# Ler o JSON
df = (
    spark.read
    .option("multiline", "true")
    .json(f"s3a://{bucket_name}/bronze/JSON/nasa-asteroid-data-{today_str}.json")
)

# %%
df_objects = df.select(
    F.explode(f"near_earth_objects.{today_str}")
)

# %%
print("\nFlattening JSON Semi-Structured Data...\n")

df_objects = (
    df_objects
    .select(
        # Campos simples
        F.col("col.id").cast("int").alias("id"),
        F.col("col.neo_reference_id").cast("int").alias("neo_reference_id"),
        F.col("col.name").cast("string").alias("name"),
        F.col("col.absolute_magnitude_h").cast("double").alias("absolute_magnitude_h"),
        F.col("col.is_potentially_hazardous_asteroid").cast("boolean").alias("is_hazardous"),
        F.col("col.is_sentry_object").cast("boolean").alias("is_sentry"),
        F.col("col.nasa_jpl_url").cast("string").alias("nasa_jpl_url"),
        F.col("col.links.self").cast("string").alias("link_self"),

        # Diâmetros
        F.col("col.estimated_diameter.feet.estimated_diameter_min")
            .cast("double").alias("diam_min_feet"),
        F.col("col.estimated_diameter.feet.estimated_diameter_max")
            .cast("double").alias("diam_max_feet"),

        F.col("col.estimated_diameter.kilometers.estimated_diameter_min")
            .cast("double").alias("diam_min_km"),
        F.col("col.estimated_diameter.kilometers.estimated_diameter_max")
            .cast("double").alias("diam_max_km"),

        F.col("col.estimated_diameter.meters.estimated_diameter_min")
            .cast("double").alias("diam_min_m"),
        F.col("col.estimated_diameter.meters.estimated_diameter_max")
            .cast("double").alias("diam_max_m"),

        F.col("col.estimated_diameter.miles.estimated_diameter_min")
            .cast("double").alias("diam_min_mi"),
        F.col("col.estimated_diameter.miles.estimated_diameter_max")
            .cast("double").alias("diam_max_mi"),

        # explode do close_approach_data
        F.explode("col.close_approach_data").alias("approach")
    )
    .select(
        "*",
        F.col("approach.close_approach_date")
            .cast("string").alias("approach_date"),
        F.col("approach.close_approach_date_full")
            .cast("string").alias("approach_date_full"),
        F.col("approach.epoch_date_close_approach")
            .cast("double").alias("approach_epoch"),
        F.col("approach.relative_velocity.kilometers_per_hour")
            .cast("double").alias("velocity_km_h"),
        F.col("approach.relative_velocity.kilometers_per_second")
            .cast("double").alias("velocity_km_s"),
        F.col("approach.relative_velocity.miles_per_hour")
            .cast("double").alias("velocity_mi_h"),
        F.col("approach.miss_distance.astronomical")
            .cast("double").alias("miss_au"),
        F.col("approach.miss_distance.kilometers")
            .cast("double").alias("miss_km"),
        F.col("approach.miss_distance.lunar")
            .cast("double").alias("miss_lunar"),
        F.col("approach.miss_distance.miles")
            .cast("double").alias("miss_mi"),
        F.col("approach.orbiting_body")
            .cast("string").alias("orbiting_body")
    )
    .drop("approach")
)

df_objects.printSchema()

# %%
print("Normalizing text columns and handling invalid placeholders...\n")
for col in df_objects.columns:
    df_objects = df_objects.withColumn(
        col,
        F.when(F.trim(F.col(col)) == "NULL", None)
        .when(F.trim(F.col(col)) == "Null", None)
        .when(F.trim(F.col(col)) == "", None)
        .otherwise(F.trim(F.col(col)))
    )

# %%
print("Checking Null Values...\n")

print(f"Row count: {df_objects.count()}")
for col in df_objects.columns:
    nulls = df_objects.filter(
        F.col(col).isNull()
    ).count()

    print(f"Null values on '{col}': {nulls}")


# %%
print("Checking Unique Values...\n")

for col in df_objects.columns:
    print(f"Unique values on '{col}': {df_objects.select(col).distinct().count()}")
    df_objects.select(col).distinct().show(truncate=False)

# %%
print("Recording processed data to the Silver layer...\n")

df_to_bucket(
    df=df_objects,
    path=f"s3a://{bucket_name}/silver/asteroids/",
    partition_by="approach_date",
    mode="overwrite"
)

print(f"✅ Data from {today_str} recorded successfully!")

# %%
