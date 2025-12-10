# %%
print("Importing libraries, variables and initializing a Spark session...")
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from datetime import datetime

import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
sys.path.append(project_root)

from include.src.utils.variables import load_env_vars
from include.src.utils.bucket import save_or_update_table

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
print("Reading data from the Silver layer...")
# Ler o Parquet
df = (
    spark.read
    .parquet(f"s3a://{bucket_name}/silver/asteroids/approach_date={today_str}")
)

# %%
print('🥈 Silver tables schema:\n')

print('📃 "asteroids" table schema:')
df.printSchema()

df.show()

# %%
print('🥇 Gold denormalized schemas:\n')

print('📃 "dim_approach_date" table schema:')
dim_approach_date = (
    df.select("approach_date_full")
    .dropna()
    .dropDuplicates()
    .withColumn(
        "parsed_ts",
        F.to_timestamp("approach_date_full", "yyyy-MMM-dd HH:mm")
    )
    .withColumn("approach_date", F.to_date("parsed_ts"))
    .withColumn("year", F.year("parsed_ts"))
    .withColumn("month", F.month("parsed_ts"))
    .withColumn("day", F.dayofmonth("parsed_ts"))
    .withColumn("hour", F.hour("parsed_ts"))
    .withColumn("minute", F.minute("parsed_ts"))
    .withColumn("week_of_year", F.weekofyear("parsed_ts"))
    .withColumn(
        "sk_approach_date",
        F.sha2(F.concat_ws("||", F.col("approach_date_full")), 256)
    )
)

dim_approach_date.printSchema()

print('📃 "dim_orbiting_body" table schema:')
dim_orbiting_body = (
    df.select("orbiting_body")
    .dropna()
    .dropDuplicates()
    .withColumn(
        "sk_orbiting_body",
        F.sha2(F.col("orbiting_body"), 256)
    )
)
dim_orbiting_body.printSchema()

print('📃 "dim_asteroid" table schema:')
dim_asteroid = (
    df.select(
        "id",
        "neo_reference_id",
        "name",
        "absolute_magnitude_h",
        "diam_min_feet",
        "diam_max_feet",
        "diam_min_km",
        "diam_max_km",
        "diam_min_m",
        "diam_max_m",
        "diam_min_mi",
        "diam_max_mi",
        "is_hazardous",
        "is_sentry",
        "nasa_jpl_url",
        "link_self"
    )
    .dropDuplicates(["id"])
    .withColumn("absolute_magnitude_h", F.col("absolute_magnitude_h").cast("double"))
    .withColumn("diam_min_feet", F.col("diam_min_feet").cast("double"))
    .withColumn("diam_max_feet", F.col("diam_max_feet").cast("double"))
    .withColumn("diam_min_km", F.col("diam_min_km").cast("double"))
    .withColumn("diam_max_km", F.col("diam_max_km").cast("double"))
    .withColumn("diam_min_m", F.col("diam_min_m").cast("double"))
    .withColumn("diam_max_m", F.col("diam_max_m").cast("double"))
    .withColumn("diam_min_mi", F.col("diam_min_mi").cast("double"))
    .withColumn("diam_max_mi", F.col("diam_max_mi").cast("double"))
    .withColumn("is_hazardous", F.col("is_hazardous").cast("boolean"))
    .withColumn("is_sentry", F.col("is_sentry").cast("boolean"))
    .withColumn(
        "sk_asteroid",
        F.sha2(F.col("id").cast("string"), 256)
    )
)
dim_asteroid.printSchema()


print('📦 "fact_asteroid_approach" table schema:')
fact_asteroid_approach = (
    df
    .select(
        # SKs geradas direto na fato (hashes)
        F.sha2(F.col("id").cast("string"), 256).alias("sk_asteroid"),
        F.sha2(F.col("orbiting_body"), 256).alias("sk_orbiting_body"),
        F.sha2(F.col("approach_date_full"), 256).alias("sk_approach_date"),

        # Medidas numéricas
        F.col("velocity_km_h").cast("double"),
        F.col("velocity_km_s").cast("double"),
        F.col("velocity_mi_h").cast("double"),

        F.col("miss_au").cast("double"),
        F.col("miss_km").cast("double"),
        F.col("miss_mi").cast("double"),
        F.col("miss_lunar").cast("double"),

        F.col("approach_epoch").cast("long")
    )
)
fact_asteroid_approach.printSchema()

# %%
print("Recording processed data to the Gold layer...\n")

save_or_update_table(dim_asteroid,
                    f"s3a://{bucket_name}/gold/dim_asteroid/",
                    dedup_cols=["id"])

save_or_update_table(dim_approach_date,
                    f"s3a://{bucket_name}/gold/dim_approach_date/",
                    dedup_cols=["approach_date_full"])

save_or_update_table(dim_orbiting_body,
                    f"s3a://{bucket_name}/gold/dim_orbiting_body/",
                    dedup_cols=["orbiting_body"])

save_or_update_table(fact_asteroid_approach,
                    f"s3a://{bucket_name}/gold/fact_asteroid_approach/",
                    dedup_cols=["sk_asteroid", "sk_approach_date"])

print(f"✅ Data from {today_str} recorded successfully!")
# %%
