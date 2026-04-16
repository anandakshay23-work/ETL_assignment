"""
Abstract base class for all data extractors.
Enforces consistent interface across XML, JSON, and CSV extractors.
"""

from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame


class BaseExtractor(ABC):
    """
    Base class for data source extractors.
    
    All extractors must implement the `extract()` method returning
    a PySpark DataFrame with extracted data.
    """

    def __init__(self, spark: SparkSession, source_config: dict, logger=None):
        """
        Initialize extractor with Spark session and source configuration.

        Args:
            spark: Active SparkSession
            source_config: Dict with name, type, path
            logger: Logger instance
        """
        self.spark = spark
        self.source_config = source_config
        self.source_name = source_config.get("name", "unknown")
        self.source_path = source_config.get("path", "")
        self.logger = logger

    @abstractmethod
    def extract(self) -> DataFrame:
        """
        Extract data from source and return as PySpark DataFrame.

        Returns:
            PySpark DataFrame with extracted data
        """
        pass

    def _log_extraction(self, df: DataFrame):
        """Log extraction summary statistics."""
        if self.logger:
            count = df.count()
            cols = len(df.columns)
            self.logger.info(
                f"Extracted from {self.source_name}: "
                f"{count} rows × {cols} columns — Schema: {df.columns}"
            )
