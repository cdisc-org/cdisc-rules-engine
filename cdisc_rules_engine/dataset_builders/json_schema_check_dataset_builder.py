from copy import deepcopy

from jsonschema import validators, exceptions
from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder


class JsonSchemaCheckDatasetBuilder(BaseDatasetBuilder):
    output_vars = {"dataset": "instanceType", "row": "_path", "USUBJID": "id"}

    def get_dataset(self):
        """Return a dataset where each row represents a JSON Schema validation error.
        Columns: path, message, validator, validator_value, schema_path.
        Returns an empty dataset (with headers) if there are no errors.
        """
        schema = self.library_metadata.standard_schema_definition
        cls = validators.validator_for(schema)
        cls.check_schema(schema)
        validator = cls(schema)

        errtree = exceptions.ErrorTree(validator.iter_errors(self.data_service.json))

        errlist = {
            "json_path": [],
            "error_context": [],
            "error_attribute": [],
            "error_value": [],
            "validator": [],
            "validator_value": [],
            "message": [],
            "output_vars": [],
            "type": [],
            "context_level": [],
            "parent": [],
            "relative_path": [],
        }

        self.list_errors(errtree, errlist, self.data_service.json)

        # Build dataset with the detected implementation (Pandas / Dask)
        return self.dataset_implementation.from_dict(errlist)

    def list_errors(
        self, tree: exceptions.ErrorTree, errlist: dict[str, list], errctx: dict = None
    ):
        for ve in tree.errors.values():
            self.parse_error(ve, errlist, errctx, "_contents")
            self.list_context_errors(ve, errlist, errctx, 1)

        if len(tree._contents) > 0:
            for k, v in tree._contents.items():
                self.list_errors(
                    tree=v, errlist=errlist, errctx=self.get_context(errctx, [k])
                )

    def parse_error(
        self,
        error: exceptions.ValidationError,
        errlist: dict[str, list],
        errctx: dict,
        errtyp: str,
        ctxlvl: int = 0,
    ):
        errlist["json_path"].append(error.json_path)
        errlist["error_context"].append(errctx if errctx else "")
        errlist["error_attribute"].append(
            "{}[{}]".format(error.absolute_path[-2], error.absolute_path[-1])
            if isinstance(error.absolute_path[-1], int)
            else error.absolute_path[-1]
        )
        errlist["error_value"].append(error.instance)
        errlist["validator"].append(error.validator)
        errlist["validator_value"].append(str(error.validator_value))
        errlist["message"].append(
            error.message.replace(str(error.instance), f"[Value of {error.json_path}]")
            if len(str(error.instance)) > len(error.json_path) + 11
            and str(error.instance) in error.message
            else error.message
        )
        errlist["output_vars"].append(
            {ov: errctx.get(iv, None) for ov, iv in self.output_vars.items()}
        )
        errlist["type"].append(errtyp)
        errlist["context_level"].append(ctxlvl)
        errlist["parent"].append(error.parent.relative_path if error.parent else "")
        errlist["relative_path"].append(error.relative_path)

    def list_context_errors(
        self,
        error: exceptions.ValidationError,
        errlist: dict[str, list],
        errctx: dict,
        ctxlvl: int = 0,
    ):
        if error.context:
            for item in error.context:
                self.parse_error(
                    item,
                    errlist,
                    self.get_context(errctx, item.relative_path, allow_list=False),
                    "context",
                    ctxlvl,
                )
                self.list_context_errors(
                    item,
                    errlist,
                    self.get_context(errctx, item.relative_path, allow_list=False),
                    ctxlvl + 1,
                )

    def get_context(self, ctx: dict, key_list: list, allow_list: bool = True) -> dict:
        ctx_copy = deepcopy(ctx)
        for i, k in enumerate(key_list):
            if not isinstance(ctx_copy[k], dict | list):
                return ctx_copy
            if isinstance(ctx_copy[k], list):
                if not (allow_list or i < len(key_list) - 1):
                    return ctx_copy
            ctx_copy = ctx_copy[k]
        return ctx_copy
