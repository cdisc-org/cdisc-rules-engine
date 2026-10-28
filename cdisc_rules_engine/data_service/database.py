import logging
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from os import getenv
from typing import Optional

import psycopg2.pool
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

from cdisc_rules_engine.exceptions.custom_exceptions import DatabaseConnectionError
from cdisc_rules_engine.services import logger

load_dotenv()


@dataclass
class DatabaseConfigPostgres:
    host = getenv("DATABASE_HOST")
    port = int(getenv("DATABASE_PORT") or 5432)
    database = getenv("DATABASE_NAME")
    user = getenv("DATABASE_USER")
    password = getenv("DATABASE_PASSWORD")
    min_connections: int = 1
    max_connections: int = 10


@dataclass
class DatabaseConfigPGServer:
    min_connections: int = 1
    max_connections: int = 10


class DatabasePostgres:
    """Database connection management with connection pooling"""

    _pgserver_instance = None
    _pgserver_tempdir = None
    _pgserver_dburi = None

    def __init__(self, config: DatabaseConfigPostgres | DatabaseConfigPGServer = DatabaseConfigPostgres()):
        self.config = config
        self._pool: Optional[psycopg2.pool.SimpleConnectionPool] = None
        self._init_pool()

    def _init_pool(self):
        """Initialise connection pool"""
        try:
            if isinstance(self.config, DatabaseConfigPGServer):
                logging.getLogger("pgserver").setLevel(logging.WARNING)
                if DatabasePostgres._pgserver_instance is None:
                    self.setup_pg_server()
                else:
                    self.dburi = DatabasePostgres._pgserver_dburi
                self._pool = psycopg2.pool.SimpleConnectionPool(
                    self.config.min_connections,
                    self.config.max_connections,
                    self.dburi,
                )
            elif isinstance(self.config, DatabaseConfigPostgres):
                try:
                    self._pool = psycopg2.pool.SimpleConnectionPool(
                        self.config.min_connections,
                        self.config.max_connections,
                        host=self.config.host,
                        port=self.config.port,
                        database=self.config.database,
                        user=self.config.user,
                        password=self.config.password,
                    )
                except psycopg2.OperationalError as e:
                    raise DatabaseConnectionError(
                        f"Could not connect to Postgres database '{self.config.database}' at "
                        f"{self.config.host}:{self.config.port}: {str(e).strip()}\n"
                        "Check the DATABASE_* settings in your .env file, "
                        "or run with --in-memory-postgres to use a temporary in-memory instance."
                    ) from e
            logger.info("Database connection pool initialised successfully")
        except DatabaseConnectionError:
            raise
        except Exception as e:
            logger.error(f"Failed to initialise connection pool: {e}")
            raise

    def setup_pg_server(self):
        # Importing pgserver here so it is only loaded when an in-memory instance is requested
        try:
            import pgserver
        except ImportError as e:
            raise DatabaseConnectionError(
                "The in-memory Postgres instance requires the 'pgserver' package. "
                "Install it with `pip install -r requirements.txt`, or set the DATABASE_* settings "
                "in your .env file to use an existing Postgres database."
            ) from e

        DatabasePostgres._pgserver_tempdir = tempfile.mkdtemp()
        DatabasePostgres._pgserver_instance = pgserver.get_server(
            DatabasePostgres._pgserver_tempdir, cleanup_mode="delete"
        )
        DatabasePostgres._pgserver_dburi = DatabasePostgres._pgserver_instance.get_uri()
        self.dburi = DatabasePostgres._pgserver_dburi

    @contextmanager
    def get_connection_and_cursor(self, dict_cursor: bool = True):
        conn = self._pool.getconn()
        try:
            cursor_factory = RealDictCursor if dict_cursor else None
            cursor = conn.cursor(cursor_factory=cursor_factory)
            yield conn, cursor
            cursor.close()
        finally:
            self._pool.putconn(conn)

    def close_pool(self):
        """Close all connections in the pool"""
        if self._pool:
            self._pool.closeall()
            logger.info("Database connection pool closed")
