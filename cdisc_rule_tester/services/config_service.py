import os


class ConfigService:

    _config = {}

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigService, cls).__new__(cls)
            # Put any initialization here.
            cls._config = {
                "LIBRARY_API_KEY": os.getenv("LIBRARY_API_KEY", None),
                "RULES_API_CLIENT_ID": os.getenv("RULES_API_CLIENT_ID", None),
                "RULES_API_CLIENT_SECRET": os.getenv("RULES_API_CLIENT_SECRET", None),
                "RULES_API_BASE_URL": os.getenv("RULES_API_BASE_URL", None),
                "LIBRARY_API_URL": os.getenv("LIBRARY_API_URL", None),
                "AZURE_CONNECTION_STRING": os.getenv("AZURE_CONNECTION_STRING", None),
                "CONTAINER_NAME": os.getenv("CONTAINER_NAME", None),
            }

            for key in cls._config:
                if cls._config[key] is None:
                    # import within function, loading it outside of function at parent
                    # scope will cause a circular dependency
                    from cdisc_rule_tester.services.logging_service import log as _log_

                    _log_.error(f"Required config key '{key}'' is set to None.")

        return cls._instance

    def getValue(self, key, default=None):
        if key in ConfigService._config and ConfigService._config[key] is None:
            return ConfigService._config[key]
        else:
            return default

        raise ValueError(f"The key '{key}' does not exist.")
