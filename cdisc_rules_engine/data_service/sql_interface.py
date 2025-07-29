import logging
from pathlib import Path
from typing import List, Any, Optional, Union, Dict, Tuple

from cdisc_rules_engine.data_service.database import DatabasePostgres, DatabaseConfigPostgres
from cdisc_rules_engine.data_service.sql_serialiser import SQLSerialiser
from cdisc_rules_engine.data_service.sql_compiler import SQLCompiler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresQLInterface:
    """Main interface for database operations"""

    def __init__(self, config: Optional[DatabaseConfigPostgres] = None):
        self.config = config or DatabaseConfigPostgres()
        self.db: Optional[DatabasePostgres] = None
        self.serialiser = SQLSerialiser()
        self.compiler = SQLCompiler()
        self._last_results: List[Any] = []

    def init_database(self):
        """Initialise the database connection"""
        try:
            self.db = DatabasePostgres(self.config)
            logger.info("Database initialised successfully")

            # Test connection
            with self.db.get_connection_and_cursor() as (_, cursor):
                cursor.execute("SELECT version();")
                version = cursor.fetchone()
                logger.info(f"Connected to PostgreSQL: {version}")

            # drop all previously generated analysis tables
            self.execute_sql_file(str(Path(__file__).parent / "schemas" / "drop_analysis_tables.sql"))

        except Exception as e:
            logger.error(f"Failed to initialise database: {e}")
            raise

    def execute_sql(self, query: str, params: Optional[Union[List, Tuple]] = None, commit: bool = True) -> int:
        """Execute a single SQL query"""
        if not self.db:
            raise RuntimeError("Database not initialised. Call init_database() first.")

        affected_rows = 0

        with self.db.get_connection_and_cursor() as (conn, cursor):
            try:
                cursor.execute(query, params)
                affected_rows = cursor.rowcount

                if query.strip().upper().startswith("SELECT"):
                    self._last_results = cursor.fetchall()

                if commit:
                    conn.commit()

                logger.info(f"Executed query successfully. Affected rows: {affected_rows}")

            except Exception as e:
                conn.rollback()
                logger.error(f"Query execution failed: {e}")
                raise

        return affected_rows

    def execute_many(
        self, queries: List[str], params_list: Optional[List[Union[List, Tuple]]] = None, commit: bool = True
    ) -> List[int]:
        """Execute multiple SQL queries"""
        if not self.db:
            raise RuntimeError("Database not initialised. Call init_database() first.")

        if params_list and len(queries) != len(params_list):
            raise ValueError("Number of queries must match number of parameter lists")

        affected_rows_list = []

        with self.db.get_connection_and_cursor() as (conn, cursor):
            try:
                for i, query in enumerate(queries):
                    params = params_list[i] if params_list else None
                    cursor.execute(query, params)
                    affected_rows_list.append(cursor.rowcount)

                if commit:
                    conn.commit()

                logger.info(f"Executed {len(queries)} queries successfully")

            except Exception as e:
                conn.rollback()
                logger.error(f"Batch execution failed: {e}")
                raise

        return affected_rows_list

    def fetch_one(self) -> Optional[Dict[str, Any]]:
        """Fetch one result from the last query"""
        if self._last_results:
            return self._last_results.pop(0)
        return None

    def fetch_all(self) -> List[Dict[str, Any]]:
        """Fetch all results from the last query"""
        results = self._last_results.copy()
        self._last_results.clear()
        return results

    def create_table_from_data(
        self, table_name: str, data: Union[Dict[str, Any], List[Dict[str, Any]]], primary_key: Optional[str] = None
    ) -> None:
        """Create a table from Python data structures"""
        sample = data if isinstance(data, dict) else data[0]

        create_stmt = self.serialiser.create_table_from_dict(table_name, sample, primary_key)

        self.execute_sql(create_stmt)
        logger.info(f"Table {table_name} created successfully")

    def insert_data(
        self, table_name: str, data: Union[Dict[str, list[str, int, float]], List[Dict[str, Any]]]
    ) -> Optional[int]:
        """Insert Python data into a table"""
        if isinstance(data, dict):
            query, values = self.serialiser.insert_dict(table_name, data)
            return self.execute_sql(query, values)
        else:
            if not self.db:
                raise RuntimeError("Database not initialised. Call init_database() first.")

            query, values_list = self.serialiser.insert_many_dicts(table_name, data)

            with self.db.get_connection_and_cursor() as (conn, cursor):
                try:
                    cursor.executemany(query, values_list)
                    affected_rows = cursor.rowcount
                    conn.commit()
                    logger.info(f"Inserted {affected_rows} rows into {table_name}")
                    return affected_rows
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Insert failed: {e}")
                    raise

    def compile_and_execute(self, statements: List[str], commit: bool = True) -> None:
        """Compile multiple statements and execute as a single query"""
        if not self.db:
            raise RuntimeError("Database not initialised. Call init_database() first.")

        compiled = self.compiler.compile_statements(statements)

        with self.db.get_connection_and_cursor() as (conn, cursor):
            try:
                cursor.execute(compiled)
                if commit:
                    conn.commit()
                logger.info("Compiled statements executed successfully")
            except Exception as e:
                conn.rollback()
                logger.error(f"Compiled execution failed: {e}")
                raise

    def execute_sql_file(self, sql_file_path: str) -> None:
        """
        Read a .sql file and execute its contents as a single statement block.
        """
        if not self.db:
            raise RuntimeError("Database not initialised. Call init_database() first.")
        try:
            with open(sql_file_path, "r", encoding="utf-8") as file:
                sql_content = file.read()
            self.compile_and_execute([sql_content])
        except Exception as e:
            logger.error(f"Failed to execute schema file: {e}")
            raise

    def close(self):
        """Close database connections"""
        if self.db:
            self.db.close_pool()
