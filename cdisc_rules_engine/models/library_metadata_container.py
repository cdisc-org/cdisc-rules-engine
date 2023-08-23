class LibraryMetadataContainer:
    def __init__(
        self,
        standard_metadata={},
        model_metadata={},
        ct_package_metadata={},
        variable_codelist_map={},
        variables_metadata={},
        published_ct_packages=[],
    ):
        self._standard_metadata = standard_metadata
        self._model_metadata = model_metadata
        self._ct_package_metadata = ct_package_metadata
        self._variable_codelist_map = variable_codelist_map
        self._variables_metadata = variables_metadata
        self._published_ct_packages = published_ct_packages

    @property
    def standard_metadata(self):
        return self._standard_metadata

    @standard_metadata.setter
    def standard_metadata(self, value):
        self._standard_metadata = value

    @property
    def variable_codelist_map(self):
        return self._variable_codelist_map

    @variable_codelist_map.setter
    def variable_codelist_map(self, value):
        self._variable_codelist_map = value

    @property
    def variables_metadata(self):
        return self._variables_metadata

    @variables_metadata.setter
    def variables_metadata(self, value):
        self._variables_metadata = value

    @property
    def model_metadata(self):
        return self._model_metadata

    @model_metadata.setter
    def model_metadata(self, value):
        self._model_metadata = value

    @property
    def published_ct_packages(self):
        return self._published_ct_packages

    def get_ct_package_metadata(self, key):
        return self._ct_package_metadata.get(key)

    def get_all_ct_package_metadata(self):
        return list(self._ct_package_metadata.values())

    def set_ct_package_metadata(self, key, value):
        self._ct_package_metadata[key] = value
