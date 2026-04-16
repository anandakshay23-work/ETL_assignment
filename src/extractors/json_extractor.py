"""
JSON Extractor — Flattens nested and multi-period JSON structures.
Handles deeply nested objects and CY/PY period splitting.
"""

import json
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType

from src.extractors.base_extractor import BaseExtractor


class JSONExtractor(BaseExtractor):
    """
    Extracts data from JSON files.
    
    Supports two JSON formats:
    1. Nested financials (samplejson1.json) — company.details.financials (deep nesting)
    2. Multi-period (samplejson2.json) — CY vs PY structure
    """

    def extract(self) -> DataFrame:
        """Route to appropriate parser based on source name."""
        if self.logger:
            self.logger.info(f"Extracting JSON: {self.source_name} from {self.source_path}")

        if self.source_name == "financials_json":
            df = self._extract_nested_financials()
        elif self.source_name == "period_json":
            df = self._extract_multi_period()
        else:
            raise ValueError(f"Unknown JSON source: {self.source_name}")

        self._log_extraction(df)
        return df

    def _extract_nested_financials(self) -> DataFrame:
        """
        Flatten deeply nested JSON structure.
        
        Input structure:
            [{"company": {"company_id": "C001", "details": {"year": 2023,
              "financials": {"revenue": 3813, "currency": "INR"}}}}]
        
        Output columns: company_id, year, revenue, currency, source_file
        """
        with open(self.source_path, "r") as f:
            data = json.load(f)

        rows = []
        for record in data:
            try:
                company = record.get("company", {})
                company_id = company.get("company_id")
                details = company.get("details", {})
                year = details.get("year")
                financials = details.get("financials", {})
                revenue = financials.get("revenue")
                currency = financials.get("currency")

                rows.append(Row(
                    company_id=str(company_id) if company_id else None,
                    year=str(year) if year else None,
                    revenue=str(revenue) if revenue else None,
                    currency=str(currency) if currency else None,
                    source_file="samplejson1.json"
                ))
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Failed to parse JSON record: {e} — {record}")

        if self.logger:
            self.logger.info(f"Nested JSON: {len(rows)} records flattened")

        schema = StructType([
            StructField("company_id", StringType(), True),
            StructField("year", StringType(), True),
            StructField("revenue", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("source_file", StringType(), True),
        ])

        return self.spark.createDataFrame(rows, schema)

    def _extract_multi_period(self) -> DataFrame:
        """
        Flatten multi-period JSON (CY vs PY) into separate rows.
        
        Input structure:
            [{"company_id": "C001", "financials": {
                "CY": {"revenue": 2094}, 
                "PY": {"revenue": 3476}}}]
        
        Output: Two rows per company (one CY, one PY)
        """
        with open(self.source_path, "r") as f:
            data = json.load(f)

        rows = []
        for record in data:
            try:
                company_id = record.get("company_id")
                financials = record.get("financials", {})

                for period in ["CY", "PY"]:
                    period_data = financials.get(period, {})
                    revenue = period_data.get("revenue")

                    rows.append(Row(
                        company_id=str(company_id) if company_id else None,
                        period=period,
                        revenue=str(revenue) if revenue else None,
                        source_file="samplejson2.json"
                    ))
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Failed to parse period JSON record: {e} — {record}")

        if self.logger:
            self.logger.info(
                f"Multi-period JSON: {len(rows)} records "
                f"({len(rows) // 2} companies × 2 periods)"
            )

        schema = StructType([
            StructField("company_id", StringType(), True),
            StructField("period", StringType(), True),
            StructField("revenue", StringType(), True),
            StructField("source_file", StringType(), True),
        ])

        return self.spark.createDataFrame(rows, schema)
