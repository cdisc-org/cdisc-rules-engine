class SNOMEDTerm:
    def __init__(
        self,
        concept_id: str = None,
        active: bool = False,
        full_name: str = None,
        preferred_term: str = None,
    ):
        self.concept_id = concept_id
        self.active = active
        self.full_name = full_name
        self.preferred_term = preferred_term

    @classmethod
    def from_json(self, term_data: dict = {}) -> "SNOMEDTerm":
        term = SNOMEDTerm()
        term.concept_id = term_data.get("conceptId")
        term.active = term_data.get("active", False)
        term.full_name = term_data.get("fsn", {}).get("term")
        term.preferred_term = term_data.get("pt", {}).get("term")
        return term
