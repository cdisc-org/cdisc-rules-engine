from business_rules.variables import BaseVariables, generic_rule_variable


class RecordVariable(BaseVariables):
    def __init__(self, record):
        self.record = record

    # common variables
    @generic_rule_variable(label="VALUE CHECK")
    def check_value(self, params):
        value = self.record.get(params.get("target"))
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        return value

    @generic_rule_variable(label="VALUE CHECK PREFIX")
    def check_value_prefix(self, params) -> str:
        """
        Returns prefix of a value that should be checked
        """
        value: str = self.record.get(params.get("target"))
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        prefix: str = params.get("prefix")
        if prefix:
            value = value[:prefix]
        return value

    @generic_rule_variable(label="VALUE CHECK SUFFIX")
    def check_value_suffix(self, params: dict) -> str:
        """
        Returns suffix of a value that should be checked
        """
        value: str = self.check_value(params)
        suffix: int = params.get("suffix")
        if suffix:
            # use last n chars of a string
            value = value[-suffix:]
        return value
