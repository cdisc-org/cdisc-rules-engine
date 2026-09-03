import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation


def _check_column_exists_in_dataset(
    row, target_col_name, referenced_domain_col, referenced_datasets
):
    col_name = row[target_col_name]
    referenced_domain = row.get(referenced_domain_col)
    if referenced_domain not in referenced_datasets:
        return None
    referenced_dataset = referenced_datasets[referenced_domain]
    columns = referenced_dataset.columns
    if col_name in columns:
        return col_name
    return None


def _apply_dropna_list(x):
    return sorted(x.dropna())


class Distinct(BaseOperation):
    def _execute_operation(self):
        result = self.params.dataframe
        if self.params.filter:
            result = self._filter_data(result)
        value_is_reference = getattr(self.params, "value_is_reference", False)
        referenced_domain_col = getattr(
            self.params, "referenced_domain_variable", "RDOMAIN"
        )
        if not self.params.grouping:
            if value_is_reference:
                target = self.params.target
                referenced_datasets = self._get_referenced_datasets()
                data = result.apply(
                    lambda row: _check_column_exists_in_dataset(
                        row, target, referenced_domain_col, referenced_datasets
                    ),
                    axis=1,
                )
                data = data.dropna().unique()
            else:
                data = result[self.params.target].dropna().unique()
            if len(data) > 0 and isinstance(data[0], bytes):
                data = data.astype(str)
            result = sorted(data)
        else:
            if value_is_reference:
                target = self.params.target
                operation_id = self.params.operation_id
                referenced_datasets = self._get_referenced_datasets()

                if len(result.data) == 0:
                    result = self._build_empty_grouped_result(result, operation_id)
                else:
                    grouped = result.groupby(
                        self.params.grouping, as_index=False, group_keys=False
                    )

                    def get_existing_column_names(group):
                        values = group.apply(
                            lambda row: _check_column_exists_in_dataset(
                                row,
                                target,
                                referenced_domain_col,
                                referenced_datasets,
                            ),
                            axis=1,
                        )
                        return pd.Series(
                            {operation_id: sorted(values.dropna().unique())}
                        )

                    result = grouped.apply(get_existing_column_names).reset_index()
            else:
                if len(result.data) == 0:
                    result = self._build_empty_grouped_result(
                        result, self.params.target
                    )
                else:
                    result = (
                        result.drop_duplicates(
                            subset=self.params.grouping + [self.params.target]
                        )
                        .groupby(self.params.grouping, as_index=False, group_keys=False)
                        .data[self.params.target]
                        .apply(_apply_dropna_list)
                        .reset_index()
                    )
        return result

    def _build_empty_grouped_result(self, result, value_column_name):
        empty_df = (
            result.data[self.params.grouping].drop_duplicates().reset_index(drop=True)
        )
        empty_df[value_column_name] = pd.Series(dtype=object)
        return empty_df

    def _get_referenced_datasets(self):
        referenced_datasets = {}
        for dataset_metadata in self.data_service.get_datasets():
            dataset = self.data_service.get_dataset(dataset_name=dataset_metadata.name)
            referenced_datasets[dataset_metadata.name] = dataset
        return referenced_datasets

    def _unique_values_for_column(self, column):
        return list(column.unique())
