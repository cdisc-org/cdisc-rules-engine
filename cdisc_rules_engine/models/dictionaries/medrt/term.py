class MEDRTTerm:
    def __init__(self, code, id, **term_params):
        self.code = code
        self.id = id
        self.name = term_params.get("name")
        self.status = term_params.get("status")


class MEDRTConcept:
    def __init__(self, **params):
        self.name = params.get("name")
        self.code = params.get("code")
        self.id = params.get("id")
        self.status = params.get("status")
        self.synonyms = params.get("synonyms", [])
