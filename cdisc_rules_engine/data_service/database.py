from contextlib import contextmanager
from dataclasses import dataclass
from os import getenv
from typing import Optional

import psycopg2.pool
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

import logging
from cdisc_rules_engine.services import logger

import pgserver
from tempfile import mkdtemp

load_dotenv()


@dataclass
class DatabaseConfigPostgres:
    host = getenv("DATABASE_HOST")
    port = (
        getenv("DATABASE_PORT") if not isinstance(getenv("DATABASE_PORT"), str) else int(getenv("DATABASE_PORT", 5432))
    )
    database = getenv("DATABASE_NAME")
    user = getenv("DATABASE_USER")
    password = getenv("DATABASE_PASSWORD")
    min_connections: int = 1
    max_connections: int = 10


@dataclass
class DatabaseConfigPGServer:
    logging.getLogger("pgserver").setLevel(logging.WARNING)
    temp_pg_data = mkdtemp()
    srv = pgserver.get_server(temp_pg_data, cleanup_mode="delete")
    dburi = srv.get_uri()
    min_connections: int = 1
    max_connections: int = 10


class DatabasePostgres:
    """Database connection management with connection pooling"""

    def __init__(self, config: DatabaseConfigPostgres | DatabaseConfigPGServer = DatabaseConfigPostgres()):
        self.config = config
        self._pool: Optional[psycopg2.pool.SimpleConnectionPool] = None
        self._init_pool()

    def _init_pool(self):
        """Initialise connection pool"""
        try:
            if isinstance(self.config, DatabaseConfigPGServer):
                self._pool = psycopg2.pool.SimpleConnectionPool(
                    self.config.min_connections,
                    self.config.max_connections,
                    self.config.dburi,
                )
            elif isinstance(self.config, DatabaseConfigPostgres):
                self._pool = psycopg2.pool.SimpleConnectionPool(
                    self.config.min_connections,
                    self.config.max_connections,
                    host=self.config.host,
                    port=self.config.port,
                    database=self.config.database,
                    user=self.config.user,
                    password=self.config.password,
                )
            logger.info("Database connection pool initialised successfully")
        except Exception as e:
            logger.error(f"Failed to initialise connection pool: {e}")
            raise

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
