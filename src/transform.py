from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

RAW_PATH = "data/raw"
PROCESSED_PATH = "data/processed"
FILE_PATTERN = r"([a-z]+)_(backfill|daily)\.json$"

def read_raw(spark):
    raw = spark.read.json(RAW_PATH)
    return (
        raw
        .withColumn("file_path", F.col("_metadata.file_path"))
        .withColumn("city", F.regexp_extract("file_path", FILE_PATTERN, 1))
        .withColumn("mode", F.regexp_extract("file_path", FILE_PATTERN, 2))
    )

def flatten(raw):
    return (
        raw
        .select(
            "city",
            "mode",
            "date",
            F.col("hourly.time").alias("time"),
            F.col("hourly.temperature_2m").alias("temp"),
            F.col("hourly.precipitation").alias("precip"),
        )
        .withColumn("h", F.explode(F.arrays_zip("time", "temp", "precip")))
        .select(
            "city",
            "mode",
            F.col("date").alias("extracted_on"),
            F.to_timestamp("h.time", "yyyy-MM-dd'T'HH:mm").alias("observed_at"),
            F.col("h.temp").alias("temperature_c"),
            F.col("h.precip").alias("precipitation_mm"),
        )
    )


def deduplicate(hourly):
    priority = F.when(F.col("mode") == "backfill", 0).otherwise(1)
    w = Window.partitionBy("city", "observed_at").orderBy(
        F.col("extracted_on").desc(), priority
    )
    return (
        hourly
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def check_quality(hourly):
    # Completeness: no nulls
    null_counts = hourly.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in hourly.columns]
    ).first().asDict()
    columns_with_nulls = {c: n for c, n in null_counts.items() if n > 0}
    if columns_with_nulls:
        raise ValueError(f"Null values found: {columns_with_nulls}")

    # Uniqueness: one row per city per hour
    total = hourly.count()
    distinct = hourly.select("city", "observed_at").distinct().count()
    if total != distinct:
        raise ValueError(f"{total - distinct} duplicate city+hour rows found")

    # Validity: values that are impossible in Morocco
    invalid = hourly.filter(
        (F.col("temperature_c") < -20)
        | (F.col("temperature_c") > 55)
        | (F.col("precipitation_mm") < 0)
    ).count()
    if invalid > 0:
        raise ValueError(f"{invalid} rows with unrealistic values")

    print(f"Quality checks passed on {total} rows")


def build_daily(hourly):
    w7 = Window.partitionBy("city").orderBy("day").rowsBetween(-6, 0)
    return (
        hourly
        .withColumn("day", F.to_date("observed_at"))
        .groupBy("city", "day")
        .agg(
            F.round(F.min("temperature_c"), 1).alias("temp_min_c"),
            F.round(F.max("temperature_c"), 1).alias("temp_max_c"),
            F.round(F.avg("temperature_c"), 1).alias("temp_avg_c"),
            F.round(F.sum("precipitation_mm"), 1).alias("precip_total_mm"),
            F.count("*").alias("hours_recorded"),
        )
        .withColumn("temp_avg_7d_c", F.round(F.avg("temp_avg_c").over(w7), 1))
        .withColumn("is_rainy_day", F.col("precip_total_mm") >= 1.0)
    )


def main():
    spark = SparkSession.builder.appName("weather-transform").getOrCreate()

    raw = read_raw(spark)
    hourly = deduplicate(flatten(raw))
    hourly.cache()

    check_quality(hourly)

    daily = build_daily(hourly)

    hourly.write.mode("overwrite").partitionBy("city").parquet(
        f"{PROCESSED_PATH}/weather_hourly"
    )
    daily.write.mode("overwrite").parquet(f"{PROCESSED_PATH}/weather_daily")

    print(f"weather_hourly: {hourly.count()} rows")
    print(f"weather_daily: {daily.count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()