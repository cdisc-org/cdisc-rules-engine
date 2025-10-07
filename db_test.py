from cdisc_rules_engine.data_service.database import (
    DatabaseConfigPostgres,
    DatabasePostgres,
)
from cdisc_rules_engine.services import logger

config = DatabaseConfigPostgres()
db = DatabasePostgres(config)

try:
    with db.get_connection_and_cursor() as (conn, cursor):
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        logger.info("✅ Database connection successful!")
        logger.info(f"PostgreSQL version: {version['version']}")
        # Test simple query
        cursor.execute("SELECT current_database(), current_user;")
        db_info = cursor.fetchone()
        logger.info(f"Connected to database: {db_info['current_database']} as user: {db_info['current_user']}")
except Exception as e:
    logger.error(f"❌ An error occurred while testing the database connection: {e}")
finally:
    db.close_pool()
