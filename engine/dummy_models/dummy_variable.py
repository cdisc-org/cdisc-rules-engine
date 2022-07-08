class DummyVariable:
    def __init__(self, variable_data):
        self.name = variable_data.get("name")
        self.label = variable_data.get("label")
        self.type = variable_data.get("type")
        self.length = variable_data.get("length")
