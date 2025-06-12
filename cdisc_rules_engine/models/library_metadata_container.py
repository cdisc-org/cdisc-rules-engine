from os.path import join
from pickle import load
from typing import Iterable


class LibraryMetadataContainer:
    def __init__(
        self,
        standard_metadata={},
        model_metadata={},
        ct_package_metadata={},
        variable_codelist_map={},
        variables_metadata={},
        published_ct_packages=[],
        cache_path: str = "",
    ):
        self._standard_metadata = standard_metadata
        self._model_metadata = model_metadata
        self._ct_package_metadata = ct_package_metadata
        self._variable_codelist_map = variable_codelist_map
        self._variables_metadata = variables_metadata
        self._published_ct_packages = published_ct_packages
        self._cache_path = cache_path

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

    def _load_ct_package_data(self, package: str, version: str):
        ct_package_version = f"{package}-{version}"
        ct_package_data = self.get_ct_package_metadata(ct_package_version)
        if ct_package_data is None:
            file_name = f"{ct_package_version}.pkl"
            with open(join(self._cache_path, file_name), "rb") as f:
                ct_package_data = load(f)
                self.set_ct_package_metadata(ct_package_version, ct_package_data)
        return ct_package_data

    def build_ct_lists(self, package: str, versions: str | Iterable[str]):
        if isinstance(versions, str):
            versions = {versions}
        ct_lists = []
        for version in {*versions}:
            ct_package_data = self._load_ct_package_data(package, version)
            ct_lists.extend(
                [
                    {
                        "package": package,
                        "version": version,
                        "codelist_code": key,
                        "extensible": value.get("extensible"),
                    }
                    for key, value in ct_package_data.items()
                    if "terms" in value
                ]
            )
        return ct_lists

    def build_ct_terms(self, package: str, versions: str | Iterable[str]):
        if isinstance(versions, str):
            versions = {versions}
        ct_terms = []
        for version in {*versions}:
            ct_package_data = self._load_ct_package_data(package, version)
            ct_terms.extend(
                [
                    {
                        "package": package,
                        "version": version,
                        "codelist_code": codelist_code,
                        "term_code": term["conceptId"],
                        "term_value": term["submissionValue"],
                    }
                    for codelist_code, codelist in ct_package_data.items()
                    if "terms" in codelist
                    for term in codelist["terms"]
                ]
            )
        return ct_terms
