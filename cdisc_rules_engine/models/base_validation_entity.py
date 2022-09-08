from abc import ABC

from cdisc_rules_engine.interfaces import RepresentationInterface


class BaseValidationEntity(ABC, RepresentationInterface):
    def __init__(self):
        self.status = None
