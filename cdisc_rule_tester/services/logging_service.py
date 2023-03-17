from cdisc_rule_tester.services.config_service import ConfigService
import logging.config

log = logging.getLogger(__name__)

config_service = ConfigService()


class TraceProperties:
    def __init__(self, **params):

        self._props = {"custom_dimensions": {"app_name": "CONFORMANCE_RULES_GENERATOR"}}

        self._addProps(params)

    def getProps(self):
        return self._props

    def _addProps(self, params={}):
        for k in params:
            self._props["custom_dimensions"][k] = str(params[k])

    def addProps(self, **params):
        self._addProps(params)
