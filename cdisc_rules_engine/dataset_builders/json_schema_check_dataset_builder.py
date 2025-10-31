import copy
import json
from copy import deepcopy
from functools import lru_cache

from jsonschema import validators, exceptions
from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
from cdisc_rules_engine.models.dataset import DatasetInterface


class JsonSchemaCheckDatasetBuilder(BaseDatasetBuilder):
    output_vars = {"dataset": "instanceType", "row": "_path", "USUBJID": "id"}
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

    @lru_cache
    def _get_cached_dataset(self):
        schema = self.library_metadata.standard_schema_definition
        cls = validators.validator_for(schema)
        cls.check_schema(schema)
        validator = cls(schema)

        errtree = exceptions.ErrorTree(validator.iter_errors(self.data_service.json))

        errlist = copy.deepcopy(self.dataset_template)
        self.list_errors(errtree, errlist)

        return errlist

    def get_dataset(self) -> DatasetInterface:
        dataset = self._get_cached_dataset()
        records = [
            {key: dataset[key][i] for key in dataset}
            for i in range(len(next(iter(dataset.values()))))
        ]
        filtered = [
            row for row in records if row["dataset"] == self.dataset_metadata.name
        ]
        return (
            self.dataset_implementation.from_records(filtered)
            if filtered
            else self.dataset_implementation.from_dict(self.dataset_template)
        )

    def list_errors(self, tree: exceptions.ErrorTree, errlist: dict[str, list]):
        if tree.errors:
            for ve in tree.errors.values():
                self.parse_error(
                    error=ve,
                    errlist=errlist,
                    errctx=self.get_instance_by_path(
                        self.data_service.json, self.get_parent_path(ve.absolute_path)
                    ),
                )
                self.list_context_errors(error=ve, errlist=errlist)

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
        errctx: dict,
    ):
        errattr = (
            "{}[{}]".format(error.absolute_path[-2], error.absolute_path[-1])
            if isinstance(error.absolute_path[-1], int)
            else error.absolute_path[-1]
        )
        errlist["json_path"].append(error.json_path)
        errlist["error_attribute"].append(errattr)
        errlist["error_value"].append(json.dumps(error.instance))
        errlist["validator"].append(error.validator)
        errlist["validator_value"].append(str(error.validator_value))
        errlist["message"].append(
            error.message.replace(str(error.instance), f"[Value of {errattr}]")
            if len(str(error.instance)) > len(error.json_path) + 11
            and str(error.instance) in error.message
            else error.message
        )
        errlist["dataset"].append(errctx.get("instanceType", "") if errctx else "")
        errlist["id"].append(errctx.get("id", "") if errctx else "")
        errlist["_path"].append(errctx.get("_path", "") if errctx else "")

    def list_context_errors(
        self, error: exceptions.ValidationError, errlist: dict[str, list]
    ):
        if error.context:
            for vec in error.context:
                self.parse_error(
                    error=vec,
                    errlist=errlist,
                    errctx=self.get_instance_by_path(
                        self.data_service.json, self.get_parent_path(vec.absolute_path)
                    ),
                )
                self.list_context_errors(vec, errlist)
