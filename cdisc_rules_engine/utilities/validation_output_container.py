class ValidationOutputContainer:
    def __init__(self):
        self.validation_output = {}

    def add_output(self, validation_id, message):
        if validation_id in self.validation_output:
            self.validation_output[validation_id] = self.validation_output[
                validation_id
            ] + [message]
        else:
            self.validation_output[validation_id] = [message]

    def get_validation_output(self, validation_id):
        return self.validation_output.get(validation_id, [])
