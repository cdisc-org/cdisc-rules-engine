from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
from typing import List
from cdisc_rules_engine.models.dataset import DatasetInterface
import pandas as pd


class VariablesMetadataWithDefineAndLibraryDatasetBuilder(BaseDatasetBuilder):
    def build(self):
        """
        Returns the variable metadata from a given file.
        Returns a dataframe with the following columns:
        variable_name
        variable_order_number
        variable_label
        variable_size
        variable_data_type
        variable_is_empty
        variable_has_empty_values
        define_variable_name,
        define_variable_label,
        define_variable_data_type,
        define_variable_role,
        define_variable_size,
        define_variable_code,
        define_variable_format,
        define_variable_allowed_terms,
        define_variable_origin_type,
        define_variable_is_collected,
        define_variable_has_no_data,
        define_variable_order_number,
        define_variable_has_codelist,
        define_variable_codelist_coded_values,
        define_variable_codelist_coded_codes,
        define_variable_mandatory,
        define_vlm_present,
        define_vlm_item_count,
        define_vlm_ccodes,
        define_vlm_has_codelist_any,
        define_vlm_has_codelist_all,
        define_vlm_ccode_missing_any,
        define_vlm_ccode_matches_library_any,
        define_vlm_ccode_matches_library_all,
        library_variable_name,
        library_variable_label,
        library_variable_data_type,
        library_variable_role,
        library_variable_core,
        library_variable_has_codelist,
        library_variable_ccode,
        library_variable_order_number
        """
        variable_metadata: List[dict] = self.get_define_xml_variables_metadata()
        content_metadata: DatasetInterface = self.data_service.get_variables_metadata(
            dataset_name=self.dataset_metadata.name
        )
        define_metadata: DatasetInterface = self.dataset_implementation.from_records(
            variable_metadata
        )
        library_metadata: DatasetInterface = self.get_library_variables_metadata()
        column_name_mapping = {
            "library_variable_ordinal": "library_variable_order_number",
            "library_variable_simpleDatatype": "library_variable_data_type",
        }
        if hasattr(library_metadata, "data"):
            library_data = library_metadata.data
        else:
            library_data = library_metadata._data
        library_data = library_data.rename(columns=column_name_mapping)
        dataset_contents = self.get_dataset_contents()

        # First merge: content metadata with define metadata
        merged_data = content_metadata.merge(
            define_metadata.data,
            left_on="variable_name",
            right_on="define_variable_name",
            how="left",
        )
        # Second merge: add library metadata
        final_dataframe = merged_data.merge(
            library_data[
                [
                    "library_variable_name",
                    "library_variable_label",
                    "library_variable_data_type",
                    "library_variable_role",
                    "library_variable_core",
                    "library_variable_has_codelist",
                    "library_variable_ccode",
                    "library_variable_order_number",
                ]
            ],
            how="left",
            left_on="variable_name",
            right_on="library_variable_name",
        ).fillna("")

        final_dataframe[["variable_has_empty_values", "variable_is_empty"]] = (
            final_dataframe.apply(
                lambda row: self.get_variable_null_stats(
                    row["variable_name"], dataset_contents
                ),
                axis=1,
                result_type="expand",
            )
        )

        # Third merge: add VLM summary columns
        define_vlm_records: List[dict] = self.get_define_xml_value_level_metadata()
        define_vlm_dataset = self.dataset_implementation.from_records(define_vlm_records)
        define_vlm_df = define_vlm_dataset.data
        has_vlm = not define_vlm_df.empty

        required_vlm_cols = [
            "define_variable_name",
            "define_vlm_ccode",
            "define_vlm_has_codelist",
        ]

        # Normalize VLM columns so groupby is safe
        define_vlm_df = define_vlm_df.reindex(columns=required_vlm_cols)
        define_vlm_df["define_vlm_ccode"] = define_vlm_df["define_vlm_ccode"].fillna("")
        define_vlm_df["define_vlm_has_codelist"] = (
            define_vlm_df["define_vlm_has_codelist"].fillna(False).astype(bool)
        )

        if has_vlm:
            vlm_summary = (
                define_vlm_df.groupby("define_variable_name", as_index=False)
                .agg(
                    define_vlm_item_count=("define_vlm_ccode", "count"),
                    define_vlm_ccodes=(
                        "define_vlm_ccode",
                        lambda x: sorted(set(v for v in x if v != ""))
                    ),
                    define_vlm_has_codelist_any=("define_vlm_has_codelist", "any"),
                    define_vlm_has_codelist_all=("define_vlm_has_codelist", "all"),
                    define_vlm_ccode_missing_any=(
                        "define_vlm_ccode",
                        lambda x: (x == "").any()
                    ),
                )
            )
            vlm_summary["define_vlm_present"] = True
        else:
            vlm_summary = pd.DataFrame(columns=[
                "define_variable_name",
                "define_vlm_item_count",
                "define_vlm_ccodes",
                "define_vlm_has_codelist_any",
                "define_vlm_has_codelist_all",
                "define_vlm_ccode_missing_any",
                "define_vlm_present",
            ])

        vlm_summary = vlm_summary.rename(columns={"define_variable_name": "variable_name"})
        final_dataframe = final_dataframe.merge(
            vlm_summary,
            how="left",
            on="variable_name",
        )
        final_dataframe.drop(columns=["define_variable_name_y"], errors="ignore", inplace=True)

        final_dataframe["define_vlm_present"] = (
            final_dataframe["define_vlm_present"].fillna(False)
        )
        final_dataframe["define_vlm_item_count"] = (
            final_dataframe["define_vlm_item_count"].fillna(0).astype(int)
        )
        final_dataframe["define_vlm_ccodes"] = final_dataframe["define_vlm_ccodes"].apply(
            lambda x: x if isinstance(x, list) else []
        )
        for col in ["define_vlm_has_codelist_any", "define_vlm_has_codelist_all",
                    "define_vlm_ccode_missing_any"]:
            final_dataframe[col] = final_dataframe[col].fillna(False)

        final_dataframe["define_vlm_ccode_matches_library_any"] = final_dataframe.apply(
            lambda row: row["library_variable_ccode"] in row["define_vlm_ccodes"]
            if row["define_vlm_ccodes"] else False,
            axis=1,
        )
        final_dataframe["define_vlm_ccode_matches_library_all"] = final_dataframe.apply(
            lambda row: (
                bool(row["define_vlm_ccodes"])
                and all(c == row["library_variable_ccode"] for c in row["define_vlm_ccodes"])
            ),
            axis=1,
        )

        return final_dataframe

    def get_variable_null_stats(
        self, variable: str, content: DatasetInterface
    ) -> tuple[bool, bool]:
        if variable not in content:
            return True, True
        series = content[variable].mask(content[variable] == "")
        return series.isnull().any(), series.isnull().all()
