from cdisc_rules_engine.operations.base_operation import BaseOperation


class Map(BaseOperation):
    def _execute_operation(self):
        map = self.evaluation_dataset.__class__.from_records(self.params.map)
        merge_columns = list(map.data.columns)
        merge_columns.remove("output")
        result = self.evaluation_dataset.merge(
            map.data,
            on=merge_columns,
            how="left",
        )
        return result["output"]
