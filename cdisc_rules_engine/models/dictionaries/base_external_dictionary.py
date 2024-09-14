class ExternalDictionary:
    def __init__(self, terms: dict = {}, version: str = ""):
        self.terms = terms
        self._version = version

    @property
    def version(self):
        return self._version

    def __getitem__(self, key):
        return self.terms[key]

    def __contains__(self, key):
        return key in self.terms

    def __iter__(self):
        yield from self.terms

    def items(self):
        return self.terms.items()

    def get(self, key, default=None):
        return self.terms.get(key, default)

    def values(self):
        return self.terms.values()
