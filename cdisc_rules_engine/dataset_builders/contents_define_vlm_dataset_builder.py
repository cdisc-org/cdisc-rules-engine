from cdisc_rules_engine.dataset_builders.values_dataset_builder import (
    ValuesDatasetBuilder,
)
import pandas as pd


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
        "define_variable_name",
        "define_vlm_name",
        "define_vlm_label",
        "define_vlm_data_type",
        "define_vlm_role",
        ...,
        """
        # get dataset contents and convert it from wide to long
        data_contents_df: pd.DataFrame = self.data_service.get_dataset(
            dataset_name=self.dataset_path
        )
        self.add_row_number(data_contents_df)
        data_contents_long_df: pd.DataFrame = ValuesDatasetBuilder.build(self)
        # get Define XML VLM for domain
        vlm_df: pd.DataFrame = pd.DataFrame(self.get_define_xml_value_level_metadata())
        # merge dataset contents with define variable metadata
        # LUT columns: row_number, define_variable_name, define_vlm_name
        lookup_table: pd.DataFrame = pd.concat(
            vlm_df.apply(
                self.apply_filters,
                axis=1,
                data_contents_df=data_contents_df,
            ).tolist()
        )
        lookup_table.rename(
            columns={"define_variable_name": "variable_name"}, inplace=True
        )
        data_contents_with_lut: pd.DataFrame = pd.merge(
            data_contents_long_df,
            lookup_table,
            how="inner",
            on=["row_number", "variable_name"],
        )
        vlm_df.drop(labels=["filter"], axis=1, inplace=True)
        data_contents_with_vlm: pd.DataFrame = pd.merge(
            data_contents_with_lut,
            vlm_df,
            how="inner",
            on="define_vlm_name",
        )
        return data_contents_with_vlm

    @staticmethod
    def apply_filters(vlm_row: dict, data_contents_df: pd.DataFrame) -> pd.DataFrame:
        filter_results: pd.Series = data_contents_df.apply(
            lambda data_contents_row: vlm_row["filter"](data_contents_row), axis=1
        )
        lut_subset: pd.DataFrame = pd.merge(
            data_contents_df[filter_results]["row_number"],
            vlm_row.to_frame().T[["define_variable_name", "define_vlm_name"]],
            how="cross",
        )
        return lut_subset
