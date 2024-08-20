class LoincTerm:
    def __init__(self, loinc_num: str, component: str, **term_params):
        self.loinc_num = loinc_num
        self.component = component
        self.property = term_params.get("property")
        self.time_aspect = term_params.get("time_aspect")
        self.system = term_params.get("system")
        self.scale_type = term_params.get("scale_type")
        self.method_type = term_params.get("method_type")
        self.term_class = term_params.get("class")
