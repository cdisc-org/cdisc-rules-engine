import os

from dotenv import load_dotenv

from cdisc_rules_engine.interfaces import ConfigInterface

load_dotenv()


class ConfigService(ConfigInterface):

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
                "DATA_SERVICE_TYPE",
            ]

        return cls._instance

    def getValue(self, key, default=None):
        if key in ConfigService._config_keys:
            return os.getenv(key)
        else:
            return default
