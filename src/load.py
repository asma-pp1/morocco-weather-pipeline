import os

from pyspark.sql import SparkSession

PROCESSED_PATH = "data/processed"
MYSQL_PACKAGE = "com.mysql:mysql-connector-j:8.4.0"

def get_jdbc_options():
    host = os.environ.get("MYSQL_HOST", "host.docker.internal")
    port = os.environ.get("MYSQL_PORT", "3306")
    database = os.environ["MYSQL_DATABASE"]
    return {
        "url": f"jdbc:mysql://{host}:{port}/{database}?rewriteBatchedStatements=true",
        "user": os.environ["MYSQL_USER"],
        "password": os.environ["MYSQL_PASSWORD"],
        "driver": "com.mysql.cj.jdbc.Driver",
    }

def load_table(df, table, options):
    (
        df.write
        .format("jdbc")
        .options(**options)
        .option("dbtable", table)
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )
    print(f"{table}: {df.count()} rows loaded")


def main():
    spark = (
        SparkSession.builder
        .appName("weather-load")
        .config("spark.jars.packages", MYSQL_PACKAGE)
        .getOrCreate()
        )

    options = get_jdbc_options()

    daily = spark.read.parquet(f"{PROCESSED_PATH}/weather_daily")
    hourly = spark.read.parquet(f"{PROCESSED_PATH}/weather_hourly")

    load_table(daily, "weather_daily", options)
    load_table(hourly, "weather_hourly", options)

    spark.stop()


if __name__ == "__main__":
    main()