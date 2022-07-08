from engine.models.representation_interface import RepresentationInterface


class BaseValidationEntity(RepresentationInterface):
    def __init__(self):
        self.status = None
