"""
Dynamic Mapping Engine — Applies database-driven transformations.

All transformation rules come from the mapping_xbrl table at runtime.
NO hardcoded mapping logic — this is a strict requirement.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
import re


class MappingEngine:
    """
    Applies dynamic transformations based on mapping rules fetched from the config DB.
    
    Supported transformations:
    - direct:           Pass-through (no change)
    - upper:            Convert to uppercase
    - currency_convert: Convert revenue to USD using rates from config
    - split_comma:      Extract numeric value from "692,extra" format
    - map_cy_py:        Standardize period values to CY/PY
    """

    def __init__(self, mapping_dict: dict, currency_rates: dict, logger=None):
        """
        Initialize with mapping rules and currency rates.

        Args:
            mapping_dict: Dict from ConfigManager.get_mapping_dict()
                          {source_field: {target_column, transformation, datatype}}
            currency_rates: Dict of currency-to-USD rates {USD: 1.0, EUR: 1.08, INR: 0.012}
            logger: Logger instance
        """
        self.mapping_dict = mapping_dict
        self.currency_rates = currency_rates
        self.logger = logger

        if self.logger:
            self.logger.info(
                f"MappingEngine initialized with {len(mapping_dict)} rules, "
                f"{len(currency_rates)} currency rates"
            )

    def apply_transformations(self, df: DataFrame, source_columns_map: dict = None) -> DataFrame:
        """
        Apply all mapping transformations to a DataFrame.

        Args:
            df: Input PySpark DataFrame
            source_columns_map: Optional mapping of {mapping_source_field: actual_df_column}
                               Use when DF column names don't match mapping source_fields exactly.

        Returns:
            Transformed DataFrame with columns renamed to target_column names
        """
        if self.logger:
            self.logger.info(f"Applying transformations. Input columns: {df.columns}")

        result_df = df

        for source_field, mapping in self.mapping_dict.items():
            target_col = mapping["target_column"]
            transformation = mapping["transformation"]
            datatype = mapping["datatype"]

            # Determine actual column name in the DF
            actual_col = source_field
            if source_columns_map and source_field in source_columns_map:
                actual_col = source_columns_map[source_field]

            # Skip if column doesn't exist in this DataFrame
            if actual_col not in result_df.columns:
                if self.logger:
                    self.logger.debug(
                        f"  Skipping mapping {source_field} → {target_col}: "
                        f"column '{actual_col}' not in DataFrame"
                    )
                continue

            if self.logger:
                self.logger.info(
                    f"  Applying: {actual_col} → {target_col} "
                    f"(transform={transformation}, type={datatype})"
                )

            result_df = self._apply_single_transformation(
                result_df, actual_col, target_col, transformation, datatype
            )

        return result_df

    def _apply_single_transformation(
        self, df: DataFrame, source_col: str, target_col: str,
        transformation: str, datatype: str
    ) -> DataFrame:
        """Apply a single transformation rule to one column."""

        if transformation == "direct":
            df = self._transform_direct(df, source_col, target_col, datatype)

        elif transformation == "upper":
            df = self._transform_upper(df, source_col, target_col)

        elif transformation == "currency_convert":
            df = self._transform_currency_convert(df, source_col, target_col)

        elif transformation == "split_comma":
            df = self._transform_split_comma(df, source_col, target_col)

        elif transformation == "map_cy_py":
            df = self._transform_map_cy_py(df, source_col, target_col)

        else:
            if self.logger:
                self.logger.warning(
                    f"Unknown transformation: {transformation}. "
                    f"Falling back to direct copy."
                )
            df = self._transform_direct(df, source_col, target_col, datatype)

        return df

    def _transform_direct(self, df: DataFrame, src: str, tgt: str, datatype: str) -> DataFrame:
        """Direct pass-through with optional type casting."""
        if datatype == "int":
            return df.withColumn(tgt, F.col(src).cast(IntegerType()))
        elif datatype == "numeric":
            return df.withColumn(tgt, F.col(src).cast(DoubleType()))
        else:
            if src != tgt:
                return df.withColumn(tgt, F.col(src))
            return df

    def _transform_upper(self, df: DataFrame, src: str, tgt: str) -> DataFrame:
        """Convert string column to uppercase."""
        return df.withColumn(tgt, F.upper(F.col(src)))

    def _transform_currency_convert(self, df: DataFrame, src: str, tgt: str) -> DataFrame:
        """
        Convert revenue values to USD using currency rates.
        
        If 'currency' column exists, use it for conversion.
        Otherwise, assume values are already in USD.
        """
        # First, ensure the source column is numeric
        df = df.withColumn(f"_{src}_clean", F.col(src).cast(DoubleType()))

        if "currency" in df.columns:
            # Build currency rate mapping as a CASE expression
            rate_expr = F.lit(1.0)  # Default rate
            for currency, rate in self.currency_rates.items():
                rate_expr = F.when(
                    F.upper(F.col("currency")) == currency,
                    F.lit(rate)
                ).otherwise(rate_expr)

            df = df.withColumn(
                tgt,
                F.round(F.col(f"_{src}_clean") * rate_expr, 2)
            )
        else:
            # No currency column — keep as-is
            df = df.withColumn(tgt, F.col(f"_{src}_clean"))

        # Clean up temp column
        df = df.drop(f"_{src}_clean")
        return df

    def _transform_split_comma(self, df: DataFrame, src: str, tgt: str) -> DataFrame:
        """
        Extract numeric value from corrupted format like "692,extra".
        
        Strategy: Split on comma, take the first part, cast to numeric.
        """
        df = df.withColumn(
            tgt,
            F.regexp_extract(F.col(src), r"^(\d+)", 1).cast(DoubleType())
        )
        return df

    def _transform_map_cy_py(self, df: DataFrame, src: str, tgt: str) -> DataFrame:
        """
        Standardize period values to CY (Current Year) or PY (Previous Year).
        
        Maps various representations:
        - CY, cy, Current Year, current → CY
        - PY, py, Previous Year, previous → PY
        """
        df = df.withColumn(
            tgt,
            F.when(F.upper(F.col(src)).isin("CY", "CURRENT YEAR", "CURRENT"), "CY")
            .when(F.upper(F.col(src)).isin("PY", "PREVIOUS YEAR", "PREVIOUS"), "PY")
            .otherwise(F.upper(F.col(src)))
        )
        return df
