from cdisc_rules_engine.operations.base_operation import BaseOperation


class Map(BaseOperation):
    def _execute_operation(self):
        map = self.evaluation_dataset.__class__.from_records(self.params.map)
        merge_columns = list(map.data.columns)
        merge_columns.remove("output")
        if not merge_columns and len(map) == 1:  # Direct mapping/assignment
            result = self.evaluation_dataset.copy()
            result["output"] = map.data["output"][0]
        else:
            result = self.evaluation_dataset.merge(
                map.data,
                on=merge_columns,
                how="left",
            )
        return result["output"]
