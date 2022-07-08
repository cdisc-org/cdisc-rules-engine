import os
from dotenv import load_dotenv


load_dotenv()


class ConfigService:

    _config_keys = []
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigService, cls).__new__(cls)
            # Put any initialization here.
            cls._config_keys = [
                "ENGINE_STORAGE_TYPE",
                "CACHE_TYPE",
                "REDIS_HOST_NAME",
                "REDIS_ACCESS_KEY",
                "CDISC_LIBRARY_API_KEY",
                "CDISC_LIBRARY_URL",
                "DICTIONARIES_TABLE_NAME",
                "WHODRUG_TERMS_TABLE_NAME",
                "MEDDRA_TERMS_TABLE_NAME"
                "DATA_SERVICE_TYPE"
            ]

        return cls._instance

    def getValue(self, key, default=None):
        if key in ConfigService._config_keys:
            return os.getenv(key)
        else:
            return default
