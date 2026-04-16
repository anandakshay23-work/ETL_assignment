"""
Database connection utilities for PostgreSQL.
Provides both psycopg2 (direct SQL) and Spark JDBC connection helpers.
"""

import psycopg2
from psycopg2.extras import RealDictCursor


class DBConnector:
    """Manages PostgreSQL connections for both config and DWH databases."""

    def __init__(self, db_config: dict, logger=None):
        """
        Initialize with database configuration.

        Args:
            db_config: Dict with host, port, database, user, password
            logger: Logger instance
        """
        self.config = db_config
        self.logger = logger
        self._connection = None

    @property
    def jdbc_url(self) -> str:
        """Build JDBC URL for Spark DataFrame reads/writes."""
        return (
            f"jdbc:postgresql://{self.config['host']}:{self.config['port']}"
            f"/{self.config['database']}"
        )

    @property
    def jdbc_properties(self) -> dict:
        """JDBC connection properties for Spark."""
        return {
            "user": self.config["user"],
            "password": self.config["password"],
            "driver": "org.postgresql.Driver"
        }

    def get_connection(self):
        """Get or create a psycopg2 connection."""
        if self._connection is None or self._connection.closed:
            self._connection = psycopg2.connect(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["database"],
                user=self.config["user"],
                password=self.config["password"]
            )
            if self.logger:
                self.logger.info(
                    f"Connected to PostgreSQL: {self.config['database']}@{self.config['host']}"
                )
        return self._connection

    def execute_query(self, query: str, params=None) -> list:
        """
        Execute a SELECT query and return results as list of dicts.

        Args:
            query: SQL SELECT statement
            params: Optional query parameters

        Returns:
            List of dictionaries (column_name: value)
        """
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                results = cur.fetchall()
                if self.logger:
                    self.logger.debug(f"Query returned {len(results)} rows: {query[:100]}...")
                return [dict(row) for row in results]
        except Exception as e:
            if self.logger:
                self.logger.error(f"Query failed: {e}")
            raise

    def execute_command(self, command: str, params=None):
        """
        Execute a DML/DDL command (INSERT, UPDATE, DELETE, etc.).

        Args:
            command: SQL statement
            params: Optional query parameters
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(command, params)
            conn.commit()
            if self.logger:
                self.logger.debug(f"Command executed: {command[:100]}...")
        except Exception as e:
            conn.rollback()
            if self.logger:
                self.logger.error(f"Command failed: {e}")
            raise

    def truncate_table(self, table_name: str):
        """Truncate a table (for full reload scenarios)."""
        self.execute_command(f"TRUNCATE TABLE {table_name} CASCADE")
        if self.logger:
            self.logger.info(f"Truncated table: {table_name}")

    def close(self):
        """Close the database connection."""
        if self._connection and not self._connection.closed:
            self._connection.close()
            if self.logger:
                self.logger.info(f"Connection closed: {self.config['database']}")
