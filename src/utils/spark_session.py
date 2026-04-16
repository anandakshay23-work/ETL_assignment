"""
SparkSession factory with PostgreSQL JDBC driver pre-configured.
"""

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "ETL_Pipeline") -> SparkSession:
    """
    Create and return a SparkSession configured for PostgreSQL.

    Args:
        app_name: Application name shown in Spark UI

    Returns:
        Configured SparkSession
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", "/app/jars/postgresql-42.7.1.jar")
        .config("spark.driver.extraClassPath", "/app/jars/postgresql-42.7.1.jar")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    # Set log level to reduce Spark verbosity
    spark.sparkContext.setLogLevel("WARN")

    return spark
