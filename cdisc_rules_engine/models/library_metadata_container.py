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

    def _load_ct_package_data(self, ct_package_type: str, version: str):
        ct_package_version = f"{ct_package_type}-{version}"
        ct_package_data = self.get_ct_package_metadata(ct_package_version)
        if ct_package_data is None:
            file_name = join(self._cache_path, f"{ct_package_version}.pkl")
            try:
                with open(file_name, "rb") as f:
                    ct_package_data = load(f)
            except FileNotFoundError:
                # ct_package_type and version may be coming from the source data.
                # Instead of raising an error, the rule should handle the missing package when appropriate.
                ct_package_data = {}
            self.set_ct_package_metadata(ct_package_version, ct_package_data)
        return ct_package_data

    def build_ct_lists(self, ct_package_type: str, versions: str | Iterable[str]):
        if isinstance(versions, str):
            versions = {versions}
        ct_lists = {
            "ct_package_type": [],
            "version": [],
            "codelist_code": [],
            "extensible": [],
        }
        for version in {*versions}:
            ct_package_data = self._load_ct_package_data(ct_package_type, version)
            for codelist_code, codelist in ct_package_data.items():
                if isinstance(codelist, dict) and "terms" in codelist:
                    ct_lists["ct_package_type"].append(ct_package_type)
                    ct_lists["version"].append(version)
                    ct_lists["codelist_code"].append(codelist_code)
                    ct_lists["extensible"].append(codelist.get("extensible"))
        return ct_lists

    def build_ct_terms(self, ct_package_type: str, versions: str | Iterable[str]):
        if isinstance(versions, str):
            versions = {versions}
        ct_terms = {
            "ct_package_type": [],
            "version": [],
            "codelist_code": [],
            "term_code": [],
            "term_value": [],
        }
        for version in {*versions}:
            ct_package_data = self._load_ct_package_data(ct_package_type, version)
            for codelist_code, codelist in ct_package_data.items():
                for term in (
                    codelist.get("terms", []) if isinstance(codelist, dict) else []
                ):
                    ct_terms["ct_package_type"].append(ct_package_type)
                    ct_terms["version"].append(version)
                    ct_terms["codelist_code"].append(codelist_code)
                    ct_terms["term_code"].append(term["conceptId"])
                    ct_terms["term_value"].append(term["submissionValue"])
        return ct_terms
