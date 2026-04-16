"""
CSV Extractor — Handles complex/corrupted CSV with multi-table detection.
Detects multiple tables within a single CSV, infers schemas dynamically,
and extracts metadata from non-standard rows.
"""

import csv
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.extractors.base_extractor import BaseExtractor


class CSVExtractor(BaseExtractor):
    """
    Extracts data from complex CSV files containing:
    - Multiple tables separated by blank rows
    - Different column layouts per sub-table
    - Metadata rows at the bottom
    - Corrupted/malformed data
    """

    def extract(self) -> tuple:
        """
        Extract all data from the complex CSV.
        
        Returns:
            Tuple of (data_df, metadata_df):
            - data_df: Combined DataFrame from all sub-tables
            - metadata_df: DataFrame with entity metadata
        """
        if self.logger:
            self.logger.info(f"Extracting CSV: {self.source_name} from {self.source_path}")

        tables, metadata = self._detect_and_split_tables()
        data_df = self._build_data_dataframe(tables)
        metadata_df = self._build_metadata_dataframe(metadata)

        self._log_extraction(data_df)
        if self.logger:
            self.logger.info(f"CSV metadata: {metadata_df.count()} metadata entries extracted")

        return data_df, metadata_df

    def _detect_and_split_tables(self) -> tuple:
        """
        Scan CSV file, detect table boundaries, and split into sub-tables.
        
        Detection strategy:
        - Blank rows (all columns empty) = table separator
        - Rows starting with known header patterns = new table header
        - Rows with key-value patterns (field, value) = metadata
        
        Returns:
            Tuple of (tables_list, metadata_list)
        """
        with open(self.source_path, "r", newline="") as f:
            reader = csv.reader(f)
            all_rows = list(reader)

        tables = []
        metadata = []
        current_table = {"header": None, "rows": [], "table_index": 0}
        table_index = 0
        blank_count = 0

        for i, row in enumerate(all_rows):
            # Strip whitespace from all cells
            cleaned = [cell.strip() for cell in row]

            # Check if row is blank (all cells empty)
            if all(cell == "" for cell in cleaned):
                blank_count += 1
                if blank_count >= 1 and current_table["rows"]:
                    # Save the current table
                    tables.append(current_table)
                    current_table = {"header": None, "rows": [], "table_index": table_index + 1}
                    table_index += 1
                continue

            blank_count = 0

            # Check if this is a metadata row (key-value pattern in CSV footer)
            # Metadata rows have pattern: empty, field_name, field_value, empty, empty
            if self._is_metadata_row(cleaned):
                metadata.append(cleaned)
                continue

            # Check if this is a header row
            if self._is_header_row(cleaned):
                if current_table["rows"]:
                    # There were data rows before this header → save previous table
                    tables.append(current_table)
                    table_index += 1
                current_table = {
                    "header": cleaned,
                    "rows": [],
                    "table_index": table_index
                }
                continue

            # Data row
            if current_table["header"] is not None:
                current_table["rows"].append(cleaned)

        # Save last table if it has data
        if current_table["rows"]:
            tables.append(current_table)

        if self.logger:
            self.logger.info(
                f"CSV table detection: found {len(tables)} sub-tables, "
                f"{len(metadata)} metadata rows"
            )
            for t in tables:
                self.logger.info(
                    f"  Table {t['table_index']}: {len(t['rows'])} rows, "
                    f"header={t['header']}"
                )

        return tables, metadata

    def _is_header_row(self, row: list) -> bool:
        """Check if a row is a table header (contains known column names)."""
        known_headers = {"company_id", "profit", "year", "remarks"}
        row_lower = {cell.lower() for cell in row if cell}
        return len(row_lower & known_headers) >= 2

    def _is_metadata_row(self, row: list) -> bool:
        """Check if a row is a metadata entry (key-value in footer)."""
        known_meta = {"enity name", "entity name", "period start date", "period end date"}
        for cell in row:
            if cell.lower().strip() in known_meta:
                return True
        return False

    def _build_data_dataframe(self, tables: list) -> DataFrame:
        """
        Combine all sub-tables into a unified PySpark DataFrame.
        
        Handles different column layouts between tables:
        - Table 1: (empty), company_id, profit, year, remarks
        - Table 2: company_id, profit, year, remarks, (empty)
        """
        rows = []

        for table in tables:
            header = table["header"]
            table_idx = table["table_index"]

            # Find the positions of known columns
            col_positions = {}
            for i, col_name in enumerate(header):
                cleaned_name = col_name.strip().lower()
                if cleaned_name in ("company_id", "profit", "year", "remarks"):
                    col_positions[cleaned_name] = i

            for data_row in table["rows"]:
                try:
                    company_id = self._safe_get(data_row, col_positions.get("company_id"))
                    profit = self._safe_get(data_row, col_positions.get("profit"))
                    year = self._safe_get(data_row, col_positions.get("year"))
                    remarks = self._safe_get(data_row, col_positions.get("remarks"))

                    rows.append(Row(
                        company_id=company_id if company_id else None,
                        profit=profit if profit else None,
                        year=year if year else None,
                        remarks=remarks if remarks else None,
                        table_index=table_idx,
                        source_file="sample_100.csv"
                    ))
                except Exception as e:
                    if self.logger:
                        self.logger.warning(
                            f"Failed to parse CSV row in table {table_idx}: {e} — {data_row}"
                        )

        schema = StructType([
            StructField("company_id", StringType(), True),
            StructField("profit", StringType(), True),
            StructField("year", StringType(), True),
            StructField("remarks", StringType(), True),
            StructField("table_index", IntegerType(), True),
            StructField("source_file", StringType(), True),
        ])

        return self.spark.createDataFrame(rows, schema)

    def _build_metadata_dataframe(self, metadata: list) -> DataFrame:
        """Build DataFrame from metadata rows (entity name, period dates)."""
        rows = []
        for meta_row in metadata:
            # Find the field name and value
            non_empty = [cell for cell in meta_row if cell.strip()]
            if len(non_empty) >= 2:
                field_name = non_empty[0].strip()
                field_value = non_empty[1].strip()
                rows.append(Row(
                    field_name=field_name,
                    field_value=field_value,
                    source_file="sample_100.csv"
                ))

        schema = StructType([
            StructField("field_name", StringType(), True),
            StructField("field_value", StringType(), True),
            StructField("source_file", StringType(), True),
        ])

        return self.spark.createDataFrame(rows, schema)

    @staticmethod
    def _safe_get(row: list, index: int) -> str:
        """Safely get a value from a row by index."""
        if index is not None and 0 <= index < len(row):
            return row[index].strip() if row[index] else None
        return None
