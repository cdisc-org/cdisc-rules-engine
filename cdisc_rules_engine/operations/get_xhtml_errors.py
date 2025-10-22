import os

from lxml import etree

from cdisc_rules_engine.operations.base_operation import BaseOperation


class GetXhtmlErrors(BaseOperation):

    def _execute_operation(self):
        dataset = self.evaluation_dataset
        target = self.params.target
        if target not in dataset:
            raise KeyError(f"Column {target} not found in the dataset")

        return dataset[target].apply(self._ensure_dataset_is_valid_xhtml)

    def _ensure_dataset_is_valid_xhtml(self, value: str) -> list[str]:
        value: str = value.strip()
        if not value:
            return []

        with open(
            os.path.join(
                "resources",
                "schema",
                "xml",
                f"cdisc-{self.params.standard}-xhtml-1.0",
                f"{self.params.standard}-xhtml-1.0.xsd",
            )
        ) as schema_file:
            raw_schema: str = schema_file.read()

        schema = etree.XMLSchema(etree.XML(raw_schema.encode("utf-8")))
        parser = etree.XMLParser(recover=True, ns_clean=True)
        xhtml_to_validate = etree.XML(value.encode("utf-8"), parser)

        if not schema.validate(xhtml_to_validate):
            return [
                f"[{error.type_name}] at line {error.line}, col {error.column} — {error.message.strip()}"
                for error in schema.error_log
            ]

        return []
