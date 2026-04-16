"""
RAW Loader — Loads extracted data to RAW schema as-is (no transformations).
Adds metadata columns: load_timestamp, source_file, batch_id.
"""

from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.db_connector import DBConnector


class RawLoader:
    """Loads raw extracted data into the RAW schema tables."""

    def __init__(self, dwh_connector: DBConnector, logger=None):
        """
        Initialize RAW loader.

        Args:
            dwh_connector: Database connector for the DWH database
            logger: Logger instance
        """
        self.connector = dwh_connector
        self.logger = logger

    def load(self, df: DataFrame, table_name: str, batch_id: str) -> int:
        """
        Load a DataFrame to a RAW schema table via Spark JDBC.

        Args:
            df: PySpark DataFrame to load
            table_name: Full table name (e.g., "raw.xml_xbrl_data")
            batch_id: Pipeline batch identifier

        Returns:
            Number of rows loaded
        """
        if self.logger:
            self.logger.info(f"Loading to RAW: {table_name}")

        # Add metadata columns
        df_with_meta = (
            df
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("load_timestamp", F.current_timestamp())
        )

        # Ensure source_file column exists
        if "source_file" not in df_with_meta.columns:
            df_with_meta = df_with_meta.withColumn("source_file", F.lit("unknown"))

        row_count = df_with_meta.count()

        # Write to PostgreSQL via JDBC
        try:
            (
                df_with_meta.write
                .format("jdbc")
                .option("url", self.connector.jdbc_url)
                .option("dbtable", table_name)
                .option("user", self.connector.config["user"])
                .option("password", self.connector.config["password"])
                .option("driver", "org.postgresql.Driver")
                .mode("append")
                .save()
            )

            if self.logger:
                self.logger.info(
                    f"  ✅ Loaded {row_count} rows to {table_name}"
                )
        except Exception as e:
            if self.logger:
                self.logger.error(f"  ❌ Failed to load to {table_name}: {e}")
            raise

        return row_count

    def load_csv_metadata(self, metadata_df: DataFrame, batch_id: str) -> int:
        """Load CSV metadata (entity info) to raw.csv_metadata."""
        return self.load(metadata_df, "raw.csv_metadata", batch_id)
