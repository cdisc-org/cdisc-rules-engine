import logging
from cdisc_rules_engine.data_service.database import DatabasePostgres, DatabaseConfigPostgres

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = DatabaseConfigPostgres()
db = DatabasePostgres(config)

try:
    if db.get_connection():
        logger.info("Database connection established successfully.")
    else:
        logger.error("Failed to establish database connection.")
except Exception as e:
    logger.error(f"An error occurred while connecting to the database: {e}")
