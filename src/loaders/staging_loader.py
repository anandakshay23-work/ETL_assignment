"""
STAGING Loader — Loads quality-checked and mapped data to STAGING schema.
Unifies data from all sources into a common staging model.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

from src.utils.db_connector import DBConnector


class StagingLoader:
    """Loads transformed data into STAGING schema tables."""

    def __init__(self, dwh_connector: DBConnector, logger=None):
        """
        Initialize STAGING loader.

        Args:
            dwh_connector: Database connector for the DWH database
            logger: Logger instance
        """
        self.connector = dwh_connector
        self.logger = logger

    def load_financials(self, df: DataFrame, batch_id: str) -> int:
        """
        Load unified financial data to staging.stg_financials.

        Ensures the DataFrame has all required columns in the correct types.

        Args:
            df: Transformed DataFrame with financial data
            batch_id: Pipeline batch identifier

        Returns:
            Number of rows loaded
        """
        if self.logger:
            self.logger.info("Loading to STAGING: staging.stg_financials")

        # Standardize schema for staging
        stg_df = self._standardize_financials_schema(df, batch_id)

        row_count = stg_df.count()

        try:
            (
                stg_df.write
                .format("jdbc")
                .option("url", self.connector.jdbc_url)
                .option("dbtable", "staging.stg_financials")
                .option("user", self.connector.config["user"])
                .option("password", self.connector.config["password"])
                .option("driver", "org.postgresql.Driver")
                .mode("append")
                .save()
            )

            if self.logger:
                self.logger.info(f"  ✅ Loaded {row_count} rows to staging.stg_financials")
        except Exception as e:
            if self.logger:
                self.logger.error(f"  ❌ Failed to load staging financials: {e}")
            raise

        return row_count

    def load_companies(self, df: DataFrame, batch_id: str) -> int:
        """
        Load company dimension data to staging.stg_companies.

        Args:
            df: DataFrame with company_id and company_name
            batch_id: Pipeline batch identifier

        Returns:
            Number of rows loaded
        """
        if self.logger:
            self.logger.info("Loading to STAGING: staging.stg_companies")

        # Select only relevant columns
        cols_to_keep = []
        for col in ["company_id", "company_name"]:
            if col in df.columns:
                cols_to_keep.append(col)

        if not cols_to_keep:
            if self.logger:
                self.logger.warning("No company columns found in DataFrame. Skipping.")
            return 0

        company_df = (
            df.select(*cols_to_keep)
            .dropDuplicates(["company_id"])
            .filter(F.col("company_id").isNotNull())
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("source_file", F.lit(df.select("source_file").first()[0]) if "source_file" in df.columns else F.lit("unknown"))
            .withColumn("load_timestamp", F.current_timestamp())
        )

        row_count = company_df.count()

        try:
            (
                company_df.write
                .format("jdbc")
                .option("url", self.connector.jdbc_url)
                .option("dbtable", "staging.stg_companies")
                .option("user", self.connector.config["user"])
                .option("password", self.connector.config["password"])
                .option("driver", "org.postgresql.Driver")
                .mode("append")
                .save()
            )

            if self.logger:
                self.logger.info(f"  ✅ Loaded {row_count} companies to staging.stg_companies")
        except Exception as e:
            if self.logger:
                self.logger.error(f"  ❌ Failed to load staging companies: {e}")
            raise

        return row_count

    def load_reject_log(self, rejected_df: DataFrame, source_name: str, batch_id: str):
        """
        Load rejected records to staging.stg_reject_log.

        Args:
            rejected_df: DataFrame with rejected records and rejection reasons
            source_name: Source name for reference
            batch_id: Pipeline batch identifier
        """
        if rejected_df.count() == 0:
            return

        if self.logger:
            self.logger.info(f"Loading {rejected_df.count()} rejected records to stg_reject_log")

        # Build reject log entries
        reject_entries = (
            rejected_df
            .withColumn(
                "record_data",
                F.to_json(F.struct([F.col(c) for c in rejected_df.columns
                                     if c not in ("_dq_status", "_dq_reason")]))
            )
            .withColumn("rejection_reason", F.col("_dq_reason"))
            .withColumn("source_file", F.lit(source_name))
            .withColumn("severity", F.lit("ERROR"))
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("rejected_at", F.current_timestamp())
            .select("source_file", "record_data", "rejection_reason",
                    "severity", "batch_id", "rejected_at")
        )

        try:
            (
                reject_entries.write
                .format("jdbc")
                .option("url", self.connector.jdbc_url)
                .option("dbtable", "staging.stg_reject_log")
                .option("user", self.connector.config["user"])
                .option("password", self.connector.config["password"])
                .option("driver", "org.postgresql.Driver")
                .mode("append")
                .save()
            )
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to load reject log: {e}")

    def _standardize_financials_schema(self, df: DataFrame, batch_id: str) -> DataFrame:
        """
        Ensure DataFrame has all staging columns with correct types.
        Adds missing columns with null values.
        """
        result = df

        # Ensure all required columns exist
        column_defaults = {
            "company_id": ("string", None),
            "company_name": ("string", None),
            "period_type": ("string", None),
            "year": ("int", None),
            "revenue": ("double", None),
            "revenue_usd": ("double", None),
            "profit": ("double", None),
            "currency": ("string", None),
            "source_file": ("string", "unknown"),
        }

        for col_name, (col_type, default) in column_defaults.items():
            if col_name not in result.columns:
                if default is not None:
                    result = result.withColumn(col_name, F.lit(default))
                else:
                    if col_type == "double":
                        result = result.withColumn(col_name, F.lit(None).cast(DoubleType()))
                    elif col_type == "int":
                        result = result.withColumn(col_name, F.lit(None).cast(IntegerType()))
                    else:
                        result = result.withColumn(col_name, F.lit(None).cast("string"))

        # Cast types
        result = (
            result
            .withColumn("year", F.col("year").cast(IntegerType()))
            .withColumn("revenue", F.col("revenue").cast(DoubleType()))
            .withColumn("revenue_usd", F.col("revenue_usd").cast(DoubleType()))
            .withColumn("profit", F.col("profit").cast(DoubleType()))
        )

        # Add metadata
        result = (
            result
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("load_timestamp", F.current_timestamp())
        )

        # Select only staging columns in correct order
        return result.select(
            "company_id", "company_name", "period_type", "year",
            "revenue", "revenue_usd", "profit", "currency",
            "source_file", "batch_id", "load_timestamp"
        )
