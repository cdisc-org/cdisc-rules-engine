from abc import ABC

from cdisc_rules_engine.interfaces import RepresentationInterface


class BaseValidationEntity(RepresentationInterface, ABC):
    def __init__(self):
        self.status = None
