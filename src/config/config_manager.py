"""
Configuration Manager — Loads pipeline config and fetches DB-driven mappings at runtime.
STRICT RULE: No hardcoded mapping logic. All mappings fetched from database.
"""

import json
import os

from src.utils.db_connector import DBConnector


class ConfigManager:
    """
    Manages pipeline configuration from JSON file and database-driven mappings.
    
    Responsibilities:
    - Load pipeline_config.json
    - Connect to config DB and fetch mapping_xbrl table
    - Provide config accessors for all pipeline components
    """

    def __init__(self, config_path: str = None, logger=None):
        """
        Initialize ConfigManager.

        Args:
            config_path: Path to pipeline_config.json (or from env var CONFIG_PATH)
            logger: Logger instance
        """
        self.logger = logger
        self.config_path = config_path or os.environ.get(
            "CONFIG_PATH", "/app/config/pipeline_config.json"
        )
        self._config = None
        self._mappings = None
        self._config_db = None

    def load_config(self) -> dict:
        """Load and return the pipeline configuration from JSON file."""
        if self._config is None:
            if self.logger:
                self.logger.info(f"Loading pipeline config from: {self.config_path}")

            with open(self.config_path, "r") as f:
                self._config = json.load(f)

            if self.logger:
                self.logger.info(
                    f"Config loaded — {len(self._config.get('sources', []))} sources, "
                    f"{len(self._config.get('currency_rates', {}))} currency rates"
                )
        return self._config

    @property
    def config(self) -> dict:
        """Lazy-loaded config property."""
        if self._config is None:
            self.load_config()
        return self._config

    @property
    def mapping_db_config(self) -> dict:
        """Configuration for the mapping/config database."""
        return self.config["mapping_db"]

    @property
    def dwh_db_config(self) -> dict:
        """Configuration for the data warehouse database."""
        return self.config["dwh_db"]

    @property
    def sources(self) -> list:
        """List of data source configurations."""
        return self.config["sources"]

    @property
    def currency_rates(self) -> dict:
        """Currency conversion rates (to USD)."""
        return self.config["currency_rates"]

    @property
    def pipeline_config(self) -> dict:
        """Pipeline-level settings (paths, batch prefix, etc.)."""
        return self.config.get("pipeline", {})

    def get_config_db(self) -> DBConnector:
        """Get or create a connection to the config database."""
        if self._config_db is None:
            self._config_db = DBConnector(self.mapping_db_config, self.logger)
        return self._config_db

    def fetch_mappings(self) -> list:
        """
        Fetch mapping rules from the database at RUNTIME.
        
        This is the core requirement: mappings must be fetched dynamically
        from the mapping_xbrl table, NOT hardcoded.

        Returns:
            List of mapping dicts: [{source_field, target_column, transformation, datatype}, ...]
        """
        if self._mappings is None:
            if self.logger:
                self.logger.info("Fetching mapping rules from config database...")

            config_db = self.get_config_db()
            mapping_table = self.mapping_db_config.get("mapping_table", "mapping_xbrl")

            query = f"SELECT source_field, target_column, transformation, datatype FROM {mapping_table}"
            self._mappings = config_db.execute_query(query)

            if self.logger:
                self.logger.info(f"Fetched {len(self._mappings)} mapping rules from {mapping_table}")
                for m in self._mappings:
                    self.logger.debug(
                        f"  Mapping: {m['source_field']} → {m['target_column']} "
                        f"(transform={m['transformation']}, type={m['datatype']})"
                    )

        return self._mappings

    def get_mapping_dict(self) -> dict:
        """
        Return mappings as a dictionary keyed by source_field.

        Returns:
            Dict: {source_field: {target_column, transformation, datatype}}
        """
        mappings = self.fetch_mappings()
        return {
            m["source_field"]: {
                "target_column": m["target_column"],
                "transformation": m["transformation"],
                "datatype": m["datatype"]
            }
            for m in mappings
        }

    def close(self):
        """Close all database connections."""
        if self._config_db:
            self._config_db.close()
