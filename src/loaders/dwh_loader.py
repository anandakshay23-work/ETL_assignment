"""
DWH Loader — Loads data to the final Data Warehouse layer (Star Schema).
Handles fact_financials loading with surrogate key resolution and incremental load.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

from src.utils.db_connector import DBConnector
from src.transformers.scd_handler import SCDHandler


class DWHLoader:
    """
    Loads data to the DWH star schema.
    
    - dim_company: Managed by SCDHandler (SCD Type 2)
    - fact_financials: Loaded with foreign key to dim_company
    """

    def __init__(self, dwh_connector: DBConnector, spark: SparkSession, logger=None):
        """
        Initialize DWH loader.

        Args:
            dwh_connector: Database connector for the DWH database
            spark: Active SparkSession
            logger: Logger instance
        """
        self.connector = dwh_connector
        self.spark = spark
        self.logger = logger
        self.scd_handler = SCDHandler(dwh_connector, spark, logger)

    def load_dimension_company(self, staging_companies_df: DataFrame, batch_id: str) -> dict:
        """
        Load/update dim_company using SCD Type 2.

        Args:
            staging_companies_df: DataFrame with company_id, company_name
            batch_id: Pipeline batch identifier

        Returns:
            SCD stats dict
        """
        if self.logger:
            self.logger.info("Loading DWH: dwh.dim_company (SCD Type 2)")

        return self.scd_handler.apply_scd2_company(staging_companies_df, batch_id)

    def load_fact_financials(self, staging_financials_df: DataFrame, batch_id: str) -> int:
        """
        Load fact_financials with surrogate key resolution.
        
        Implements incremental load:
        - Checks existing records by company_id + period_type + source_file
        - Only inserts new/changed records

        Args:
            staging_financials_df: Staging financials DataFrame
            batch_id: Pipeline batch identifier

        Returns:
            Number of rows loaded
        """
        if self.logger:
            self.logger.info("Loading DWH: dwh.fact_financials")

        # Get current dim_company for surrogate key lookup
        dim_company_df = self.scd_handler.get_current_dim_company()

        # Join staging data with dim_company to get surrogate keys
        # Alias to avoid ambiguous column reference
        dim_selected = dim_company_df.select(
            F.col("dim_company_sk"),
            F.col("company_id").alias("_dim_company_id")
        )

        fact_df = (
            staging_financials_df
            .join(
                dim_selected,
                staging_financials_df["company_id"] == dim_selected["_dim_company_id"],
                "left"
            )
            .drop("_dim_company_id")
        )

        # Check for unresolved surrogate keys
        unresolved = fact_df.filter(F.col("dim_company_sk").isNull()).count()
        if unresolved > 0 and self.logger:
            self.logger.warning(
                f"  {unresolved} records have no matching dim_company entry. "
                f"These will be loaded with NULL dim_company_sk."
            )

        # Incremental Load: Check what's already in fact table
        try:
            existing_facts = (
                self.spark.read
                .format("jdbc")
                .option("url", self.connector.jdbc_url)
                .option("dbtable",
                        "(SELECT company_id, period_type, source_file "
                        "FROM dwh.fact_financials) AS existing_facts"
                        )
                .option("user", self.connector.config["user"])
                .option("password", self.connector.config["password"])
                .option("driver", "org.postgresql.Driver")
                .load()
            )

            # Anti-join: only keep records not already in fact table
            fact_df = fact_df.join(
                existing_facts,
                on=["company_id", "period_type", "source_file"],
                how="left_anti"
            )

            incremental_count = fact_df.count()
            if self.logger:
                self.logger.info(
                    f"  Incremental load: {incremental_count} new records to insert"
                )
        except Exception as e:
            if self.logger:
                self.logger.warning(
                    f"  Could not check existing facts (first run?): {e}. "
                    f"Loading all records."
                )

        # Prepare final fact columns
        fact_output = (
            fact_df
            .select(
                F.col("dim_company_sk").cast(IntegerType()),
                "company_id",
                "period_type",
                F.col("year").cast(IntegerType()),
                F.col("revenue").cast(DoubleType()),
                F.col("revenue_usd").cast(DoubleType()),
                F.col("profit").cast(DoubleType()),
                "currency",
                "source_file",
                F.lit(batch_id).alias("batch_id"),
                F.current_timestamp().alias("load_timestamp")
            )
        )

        row_count = fact_output.count()

        if row_count == 0:
            if self.logger:
                self.logger.info("  No new records to load to fact_financials (all exist).")
            return 0

        try:
            (
                fact_output.write
                .format("jdbc")
                .option("url", self.connector.jdbc_url)
                .option("dbtable", "dwh.fact_financials")
                .option("user", self.connector.config["user"])
                .option("password", self.connector.config["password"])
                .option("driver", "org.postgresql.Driver")
                .mode("append")
                .save()
            )

            if self.logger:
                self.logger.info(f"  ✅ Loaded {row_count} rows to dwh.fact_financials")
        except Exception as e:
            if self.logger:
                self.logger.error(f"  ❌ Failed to load fact_financials: {e}")
            raise

        return row_count
