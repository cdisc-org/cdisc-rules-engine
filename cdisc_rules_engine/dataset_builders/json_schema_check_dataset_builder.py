import copy
import json
from copy import deepcopy
import re

from jsonschema import validators, exceptions
from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
from cdisc_rules_engine.models.dataset import DatasetInterface
from cdisc_rules_engine.utilities.utils import tag_source


class JsonSchemaCheckDatasetBuilder(BaseDatasetBuilder):
    dataset_template = {
        "json_path": [],
        "error_attribute": [],
        "error_value": [],
        "validator": [],
        "validator_value": [],
        "message": [],
        "dataset": [],
        "id": [],
        "_path": [],
    }

    def build(self, **kwargs) -> DatasetInterface:
        return self.get_dataset()

    def _get_cached_dataset(self) -> dict[str, list[str]]:
        cache_key: str = (
            f"json_schema_validation_result_{self.data_service.dataset_path}"
        )
        if cached := self.cache.get(cache_key):
            return cached

        schema = self.library_metadata.standard_schema_definition
        cls = validators.validator_for(schema)
        cls.check_schema(schema)
        validator = cls(schema)

        errtree = exceptions.ErrorTree(validator.iter_errors(self.data_service.json))

        errlist = copy.deepcopy(self.dataset_template)
        self.list_errors(errtree, errlist)

        self.cache.add(cache_key, errlist)

        return errlist

    def get_dataset(self, **kwargs) -> DatasetInterface:
        dataset = self._get_cached_dataset()
        records = [
            {key: dataset[key][i] for key in dataset}
            for i in range(len(next(iter(dataset.values()))))
        ]
        filtered = [
            row for row in records if row["dataset"] == self.dataset_metadata.name
        ]
        if filtered:
            result = self.dataset_implementation.from_records(filtered, **kwargs)
        else:
            empty_row = {key: "" for key in self.dataset_template.keys()}
            result = self.dataset_implementation.from_records([empty_row], **kwargs)
        return tag_source(result, self.dataset_metadata)

    def list_errors(self, tree: exceptions.ErrorTree, errlist: dict[str, list]):
        if tree.errors:
            for ve in tree.errors.values():
                self.process_error(error=ve, errlist=errlist)

        if len(tree._contents) > 0:
            for k, v in tree._contents.items():
                self.list_errors(
                    tree=v,
                    errlist=errlist,
                )

    def get_instance_by_path(self, instance: dict, path_list: list) -> dict:
        _inst = deepcopy(instance)
        for p in path_list:
            _inst = _inst[p]
        return _inst

    def get_parent_path(self, path_list: list):
        return list(path_list)[0 : (-1 - int(isinstance(path_list[-1], int)))]

    def parse_error(
        self,
        error: exceptions.ValidationError,
        errlist: dict[str, list],
        errpath: list,
    ):
        errctx = self.get_instance_by_path(self.data_service.json, errpath)
        errattr = (
            self.get_attributes_from_message(error.message)
            if error.validator in ["required", "additionalProperties"]
            else (
                "{}[{}]".format(error.absolute_path[-2], error.absolute_path[-1])
                if isinstance(error.absolute_path[-1], int)
                else error.absolute_path[-1]
            )
        )
        errlist["json_path"].append(error.json_path)
        errlist["error_attribute"].append(errattr)
        errlist["error_value"].append(json.dumps(error.instance))
        errlist["validator"].append(error.validator)
        errlist["validator_value"].append(str(error.validator_value))
        errlist["message"].append(
            error.message.replace(str(error.instance), f"[Value of {errattr}]")
            if len(str(error.instance)) > len(errattr) + 11
            and str(error.instance) in error.message
            else error.message
        )
        errlist["dataset"].append(errctx.get("instanceType", "") if errctx else "")
        errlist["id"].append(errctx.get("id", "") if errctx else "")
        errlist["_path"].append("/" + "/".join(map(str, errpath)))

    def list_context_errors(
        self,
        error: exceptions.ValidationError,
        errlist: dict[str, list],
        skip_subschemas: list = [],
    ):
        if error.context:
            for vec in error.context:
                if (
                    skip_subschemas == []
                    or list(vec.schema_path)[0] not in skip_subschemas
                ):
                    self.process_error(error=vec, errlist=errlist)

    def process_error(
        self, error: exceptions.ValidationError, errlist: dict[str, list]
    ):
        if error.validator == "anyOf":
            skip_ssi = []
            refs = [
                ss["$ref"].split("/")[-1]
                for ss in error.schema["anyOf"]
                if "$ref" in ss
            ]
            for vec in error.context:
                if (
                    list(vec.relative_path) == ["instanceType"]
                    and vec.validator == "const"
                    and vec.instance in refs
                ) or (
                    list(vec.relative_path) == []
                    and vec.validator == "type"
                    and vec.validator_value == "null"
                ):
                    skip_ssi.append(list(vec.schema_path)[0])
            self.list_context_errors(
                error=error, errlist=errlist, skip_subschemas=skip_ssi
            )
        else:
            self.parse_error(
                error=error,
                errlist=errlist,
                errpath=(
                    error.absolute_path
                    if error.validator in ["required", "additionalProperties"]
                    else self.get_parent_path(error.absolute_path)
                ),
            )
            self.list_context_errors(error=error, errlist=errlist)

    def get_attributes_from_message(self, message: str) -> list[str]:
        return re.findall(r"'([^, ]+)'", message)
