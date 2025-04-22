from cdisc_rules_engine.dataset_builders.values_dataset_builder import (
    ValuesDatasetBuilder,
)
from cdisc_rules_engine.models.dataset import DatasetInterface


class ContentsDefineVLMDatasetBuilder(ValuesDatasetBuilder):
    def build(self):
        """
        Returns a long dataset where each value in each row of the original dataset is
        a row in the new dataset.
        The define xml vlm corresponding to each row's variable is
        attached to each row.
        Only variable values that have VLM attached are produced.
        Columns available in the dataset are:
        "row_number",
        "variable_name",
        "variable_value",
        "variable_value_length",
        "define_variable_name",
        "define_vlm_name",
        "define_vlm_label",
        "define_vlm_data_type",
        "define_vlm_role",
        "define_vlm_mandatory",
        ...,
        """
        # get dataset contents and convert it from wide to long
        data_contents_df: DatasetInterface = self.data_service.get_dataset(
            dataset_name=self.dataset_path
        )
        self.add_row_number(data_contents_df)
        data_contents_long_df: DatasetInterface = ValuesDatasetBuilder.build(self)

        # get Define XML VLM for domain
        vlm_df: DatasetInterface = self.dataset_implementation.from_records(
            self.get_define_xml_value_level_metadata()
        )

        # merge dataset contents with define variable metadata
        # LUT columns: row_number, define_variable_name, define_vlm_name
        lookup_table: DatasetInterface = self.dataset_implementation().concat(
            vlm_df.apply(
                self.apply_filters,
                meta=self.dataset_implementation,
                axis=1,
                data_contents_df=data_contents_df,
            ).tolist()
        )

        lookup_table.rename(
            columns={"define_variable_name": "variable_name"}, inplace=True
        )
        data_contents_with_lut: DatasetInterface = data_contents_long_df.merge(
            lookup_table.data,
            how="inner",
            on=["row_number", "variable_name"],
        )
        vlm_df = vlm_df.drop(labels=["filter"], axis=1)
        data_contents_with_vlm: DatasetInterface = data_contents_with_lut.merge(
            vlm_df.data,
            how="inner",
            on="define_vlm_name",
        )
        data_contents_with_vlm["variable_value_length"] = data_contents_with_vlm.data[
            ["variable_value", "define_vlm_data_type"]
        ].apply(
            lambda row: self.calculate_variable_value_length(
                row["variable_value"], row["define_vlm_data_type"]
            ),
            axis=1,
        )
        return data_contents_with_vlm

    @staticmethod
    def apply_filters(
        vlm_row: dict, data_contents_df: DatasetInterface
    ) -> DatasetInterface:
        filter_results = data_contents_df.apply(
            lambda data_contents_row: vlm_row["filter"](data_contents_row), axis=1
        )

        lut_subset: DatasetInterface = data_contents_df.__class__.cartesian_product(
            data_contents_df.data[filter_results]["row_number"].to_frame(),
            vlm_row.to_frame().T[["define_variable_name", "define_vlm_name"]],
        )
        return lut_subset
