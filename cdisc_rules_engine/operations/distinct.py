import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation


def _check_column_exists_in_dataset(row, target_col_name, referenced_datasets):
    col_name = row[target_col_name]
    referenced_domain = row.get("RDOMAIN")
    if referenced_domain not in referenced_datasets:
        return None
    referenced_dataset = referenced_datasets[referenced_domain]
    columns = referenced_dataset.columns
    if col_name in columns:
        return col_name
    return None


class Distinct(BaseOperation):
    def _execute_operation(self):
        result = self.params.dataframe
        if self.params.filter:
            result = self._filter_data(result)
        value_is_reference = getattr(self.params, "value_is_reference", False)
        if not self.params.grouping:
            if value_is_reference:
                target = self.params.target
                referenced_datasets = self._get_referenced_datasets()
                data = result.apply(
                    lambda row: _check_column_exists_in_dataset(
                        row, target, referenced_datasets
                    ),
                    axis=1,
                )
                data = data.dropna().unique()
            else:
                data = result[self.params.target].unique()
            if len(data) > 0 and isinstance(data[0], bytes):
                data = data.astype(str)
            result = set(data)
        else:
            grouped = result.groupby(
                self.params.grouping, as_index=False, group_keys=False
            )
            if value_is_reference:
                target = self.params.target
                operation_id = self.params.operation_id
                referenced_datasets = self._get_referenced_datasets()

                def get_existing_column_names(group):
                    values = group.apply(
                        lambda row: _check_column_exists_in_dataset(
                            row, target, referenced_datasets
                        ),
                        axis=1,
                    )
                    return pd.Series({operation_id: set(values.dropna().unique())})

                result = grouped.apply(get_existing_column_names).reset_index()
            elif isinstance(result.data, pd.DataFrame):
                result = grouped.data[self.params.target].agg(
                    self._unique_values_for_column
                )
            else:
                result = (
                    grouped.data[self.params.target]
                    .unique()
                    .rename({self.params.target: self.params.operation_id})
                )
                result = result.apply(set).to_frame().reset_index()
        return result

    def _get_referenced_datasets(self):
        referenced_datasets = {}
        for dataset_meta in self.data_service.data:
            dataset = self.data_service.get_dataset(dataset_meta.filename)
            referenced_datasets[dataset_meta.name] = dataset
        return referenced_datasets

    def _unique_values_for_column(self, column):
        return pd.Series({self.params.operation_id: set(column.unique())})
