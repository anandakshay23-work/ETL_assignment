"""
Data Quality Engine — Validates, cleans, and segregates corrupted data.
Implements comprehensive quality checks with reject logging.
"""

import os
import json
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


class DataQualityEngine:
    """
    Validates data quality and segregates corrupted records.
    
    Quality Rules:
    1. Null/missing company_id (critical key)
    2. Invalid year values (non-numeric like "20x3", "20X3")
    3. Duplicate records (same company_id + period + source)
    4. Invalid numeric values (non-parseable revenue/profit)
    5. Suspicious data patterns
    """

    def __init__(self, reject_path: str, logger=None):
        """
        Initialize the quality engine.

        Args:
            reject_path: Directory to write reject files
            logger: Logger instance
        """
        self.reject_path = reject_path
        self.logger = logger
        self.quality_report = {
            "total_input": 0,
            "total_passed": 0,
            "total_rejected": 0,
            "rejection_reasons": {}
        }
        os.makedirs(reject_path, exist_ok=True)

    def validate(self, df: DataFrame, source_name: str) -> tuple:
        """
        Run all quality checks on a DataFrame.

        Args:
            df: Input DataFrame to validate
            source_name: Name of the data source (for logging/reject files)

        Returns:
            Tuple of (valid_df, rejected_df):
            - valid_df: Records that passed all quality checks
            - rejected_df: Records that failed with rejection reasons
        """
        if self.logger:
            self.logger.info(f"Running data quality checks on: {source_name}")

        input_count = df.count()
        self.quality_report["total_input"] += input_count

        # Add a quality flag column (start as valid)
        df = df.withColumn("_dq_status", F.lit("VALID"))
        df = df.withColumn("_dq_reason", F.lit(""))

        # Apply quality rules
        df = self._check_missing_company_id(df, source_name)
        df = self._check_invalid_year(df, source_name)
        df = self._check_invalid_profit(df, source_name)

        # Split into valid and rejected
        valid_df = df.filter(F.col("_dq_status") == "VALID")
        rejected_df = df.filter(F.col("_dq_status") == "REJECTED")

        # Drop internal quality columns from valid data
        valid_df = valid_df.drop("_dq_status", "_dq_reason")

        valid_count = valid_df.count()
        rejected_count = rejected_df.count()

        self.quality_report["total_passed"] += valid_count
        self.quality_report["total_rejected"] += rejected_count

        if self.logger:
            self.logger.info(
                f"Quality check results for {source_name}: "
                f"{valid_count} passed, {rejected_count} rejected "
                f"(out of {input_count})"
            )

        # Write reject file if there are rejections
        if rejected_count > 0:
            self._write_reject_file(rejected_df, source_name)

        return valid_df, rejected_df

    def _check_missing_company_id(self, df: DataFrame, source_name: str) -> DataFrame:
        """Flag records with null or empty company_id."""
        if "company_id" not in df.columns:
            return df

        mask = (F.col("company_id").isNull()) | (F.trim(F.col("company_id")) == "")

        flagged_count = df.filter(mask & (F.col("_dq_status") == "VALID")).count()
        if flagged_count > 0:
            df = df.withColumn(
                "_dq_status",
                F.when(mask, "REJECTED").otherwise(F.col("_dq_status"))
            )
            df = df.withColumn(
                "_dq_reason",
                F.when(mask, F.concat(F.col("_dq_reason"), F.lit("MISSING_COMPANY_ID; ")))
                .otherwise(F.col("_dq_reason"))
            )
            reason = "MISSING_COMPANY_ID"
            self.quality_report["rejection_reasons"][reason] = (
                self.quality_report["rejection_reasons"].get(reason, 0) + flagged_count
            )
            if self.logger:
                self.logger.warning(
                    f"  [{source_name}] {flagged_count} records with missing company_id"
                )

        return df

    def _check_invalid_year(self, df: DataFrame, source_name: str) -> DataFrame:
        """Flag records with non-numeric year values (e.g., '20x3', '20X3')."""
        if "year" not in df.columns:
            return df

        # Year must be a 4-digit number
        invalid_year_mask = (
            F.col("year").isNotNull() &
            (F.trim(F.col("year")) != "") &
            (~F.col("year").rlike(r"^\d{4}$"))
        )

        flagged_count = df.filter(invalid_year_mask & (F.col("_dq_status") == "VALID")).count()
        if flagged_count > 0:
            df = df.withColumn(
                "_dq_status",
                F.when(invalid_year_mask, "REJECTED").otherwise(F.col("_dq_status"))
            )
            df = df.withColumn(
                "_dq_reason",
                F.when(
                    invalid_year_mask,
                    F.concat(F.col("_dq_reason"), F.lit("INVALID_YEAR("), F.col("year"), F.lit("); "))
                ).otherwise(F.col("_dq_reason"))
            )
            reason = "INVALID_YEAR"
            self.quality_report["rejection_reasons"][reason] = (
                self.quality_report["rejection_reasons"].get(reason, 0) + flagged_count
            )
            if self.logger:
                self.logger.warning(
                    f"  [{source_name}] {flagged_count} records with invalid year"
                )

        return df

    def _check_invalid_profit(self, df: DataFrame, source_name: str) -> DataFrame:
        """Flag records where profit cannot be parsed to a number after split_comma."""
        if "profit" not in df.columns:
            return df

        # After split_comma is applied, check if result would be null
        # Profit should start with digits
        invalid_profit_mask = (
            F.col("profit").isNotNull() &
            (F.trim(F.col("profit")) != "") &
            (~F.col("profit").rlike(r"^\d"))
        )

        flagged_count = df.filter(invalid_profit_mask & (F.col("_dq_status") == "VALID")).count()
        if flagged_count > 0:
            if self.logger:
                self.logger.warning(
                    f"  [{source_name}] {flagged_count} records with potentially "
                    f"invalid profit format (will attempt split_comma recovery)"
                )

        return df

    def remove_duplicates(self, df: DataFrame, key_columns: list, source_name: str) -> DataFrame:
        """
        Remove duplicate records based on key columns.
        
        Args:
            df: Input DataFrame
            key_columns: Columns that define uniqueness
            source_name: Source name for logging
            
        Returns:
            Deduplicated DataFrame
        """
        existing_cols = [c for c in key_columns if c in df.columns]
        if not existing_cols:
            return df

        before_count = df.count()
        df_deduped = df.dropDuplicates(existing_cols)
        after_count = df_deduped.count()
        removed = before_count - after_count

        if removed > 0 and self.logger:
            self.logger.warning(
                f"  [{source_name}] Removed {removed} duplicate records "
                f"(key: {existing_cols})"
            )

        return df_deduped

    def _write_reject_file(self, rejected_df: DataFrame, source_name: str):
        """Write rejected records to a CSV file with rejection reasons."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        reject_file = os.path.join(
            self.reject_path, f"reject_{source_name}_{timestamp}.csv"
        )

        # Convert to Pandas for easy CSV write (small dataset)
        try:
            rejected_pd = rejected_df.toPandas()
            rejected_pd.to_csv(reject_file, index=False)
            if self.logger:
                self.logger.info(
                    f"  Reject file written: {reject_file} "
                    f"({len(rejected_pd)} records)"
                )
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to write reject file: {e}")

    def get_quality_report(self) -> dict:
        """Return the quality report summary."""
        return self.quality_report

    def log_quality_summary(self):
        """Log the overall quality report."""
        if self.logger:
            report = self.quality_report
            self.logger.info("=" * 60)
            self.logger.info("DATA QUALITY SUMMARY")
            self.logger.info(f"  Total records processed: {report['total_input']}")
            self.logger.info(f"  Records passed:          {report['total_passed']}")
            self.logger.info(f"  Records rejected:        {report['total_rejected']}")
            if report["rejection_reasons"]:
                self.logger.info("  Rejection breakdown:")
                for reason, count in report["rejection_reasons"].items():
                    self.logger.info(f"    - {reason}: {count}")
            self.logger.info("=" * 60)
