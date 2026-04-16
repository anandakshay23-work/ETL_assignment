"""
SCD Type 2 Handler — Implements Slowly Changing Dimension Type 2.

Manages historical tracking for dim_company:
- New records: Inserted with is_current=True, valid_to=9999-12-31
- Changed records: Old version expired (is_current=False, valid_to=now),
                   new version inserted (is_current=True)
- Unchanged records: No action taken
"""

from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, TimestampType, IntegerType
)

from src.utils.db_connector import DBConnector


class SCDHandler:
    """
    Manages SCD Type 2 logic for dimension tables.
    
    SCD Type 2 tracks full history:
    - Each change creates a new row
    - Previous version is "expired" (valid_to=now, is_current=False)
    - New version gets is_current=True, valid_to=9999-12-31
    """

    def __init__(self, dwh_connector: DBConnector, spark: SparkSession, logger=None):
        """
        Initialize SCD handler.

        Args:
            dwh_connector: Database connector for the DWH database
            spark: Active SparkSession
            logger: Logger instance
        """
        self.dwh_connector = dwh_connector
        self.spark = spark
        self.logger = logger
        self.max_date = "9999-12-31 00:00:00"

    def apply_scd2_company(self, staging_companies_df: DataFrame, batch_id: str) -> dict:
        """
        Apply SCD Type 2 to dim_company.

        Args:
            staging_companies_df: DataFrame with company_id, company_name
            batch_id: Current pipeline batch ID

        Returns:
            Dict with stats: {new_inserts, updates, unchanged}
        """
        if self.logger:
            self.logger.info("Applying SCD Type 2 to dwh.dim_company...")

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        stats = {"new_inserts": 0, "updates": 0, "unchanged": 0}

        # Get current dimension records
        try:
            existing_records = self.dwh_connector.execute_query(
                "SELECT dim_company_sk, company_id, company_name "
                "FROM dwh.dim_company WHERE is_current = TRUE"
            )
        except Exception:
            existing_records = []

        existing_map = {r["company_id"]: r for r in existing_records}

        # Collect incoming company data (deduplicated)
        incoming = (
            staging_companies_df
            .select("company_id", "company_name")
            .dropDuplicates(["company_id"])
            .collect()
        )

        for row in incoming:
            company_id = row["company_id"]
            company_name = row["company_name"]

            if company_id is None:
                continue

            if company_id not in existing_map:
                # NEW RECORD — Insert
                self.dwh_connector.execute_command(
                    """
                    INSERT INTO dwh.dim_company 
                        (company_id, company_name, valid_from, valid_to, is_current, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, TRUE, %s, %s)
                    """,
                    (company_id, company_name, now, self.max_date, now, now)
                )
                stats["new_inserts"] += 1

            else:
                existing = existing_map[company_id]
                if existing["company_name"] != company_name and company_name is not None:
                    # CHANGED — Expire old record, insert new version
                    # Step 1: Expire the current record
                    self.dwh_connector.execute_command(
                        """
                        UPDATE dwh.dim_company 
                        SET valid_to = %s, is_current = FALSE, updated_at = %s
                        WHERE company_id = %s AND is_current = TRUE
                        """,
                        (now, now, company_id)
                    )

                    # Step 2: Insert new version
                    self.dwh_connector.execute_command(
                        """
                        INSERT INTO dwh.dim_company 
                            (company_id, company_name, valid_from, valid_to, is_current, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, TRUE, %s, %s)
                        """,
                        (company_id, company_name, now, self.max_date, now, now)
                    )
                    stats["updates"] += 1

                    if self.logger:
                        self.logger.info(
                            f"  SCD2 UPDATE: {company_id} — "
                            f"'{existing['company_name']}' → '{company_name}'"
                        )
                else:
                    stats["unchanged"] += 1

        if self.logger:
            self.logger.info(
                f"SCD Type 2 complete: "
                f"{stats['new_inserts']} new, "
                f"{stats['updates']} updated, "
                f"{stats['unchanged']} unchanged"
            )

        return stats

    def get_current_dim_company(self) -> DataFrame:
        """
        Read current dim_company records as a Spark DataFrame.
        Used for joining with fact data to get surrogate keys.

        Returns:
            DataFrame with dim_company_sk, company_id, company_name
        """
        df = (
            self.spark.read
            .format("jdbc")
            .option("url", self.dwh_connector.jdbc_url)
            .option("dbtable", "(SELECT dim_company_sk, company_id, company_name FROM dwh.dim_company WHERE is_current = TRUE) AS dim_company")
            .option("user", self.dwh_connector.config["user"])
            .option("password", self.dwh_connector.config["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )
        return df
