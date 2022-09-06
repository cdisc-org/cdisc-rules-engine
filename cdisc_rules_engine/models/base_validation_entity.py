from cdisc_rules_engine.interfaces import RepresentationInterface


class BaseValidationEntity(RepresentationInterface):
    def __init__(self):
        self.status = None
