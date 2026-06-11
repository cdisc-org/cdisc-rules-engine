from .base_sql_operator import BaseSqlOperator


WHODRUG_RELEASE_DATES = {}
MEDDRA_RELEASE_DATES = {
    "29.0": "2026-03-01",
    "28.1": "2025-09-01",
    "28.0": "2025-03-01",
    "27.1": "2024-09-01",
    "27.0": "2024-03-01",
    "26.1": "2023-09-01",
    "26.0": "2023-03-01",
    "25.1": "2022-09-01",
    "25.0": "2022-03-01",
    "24.1": "2021-09-01",
    "24.0": "2021-03-01",
    "23.1": "2020-09-01",
}
MEDRT_RELEASE_DATES = {}
LOINC_RELEASE_DATES = {}
SNOMED_RELEASE_DATES = {}
UNII_RELEASE_DATES = {}

EXDICT_MAPPINGS = {
    "meddra": MEDDRA_RELEASE_DATES,
    "whodrug": WHODRUG_RELEASE_DATES,
    "medrt": MEDRT_RELEASE_DATES,
    "loinc": LOINC_RELEASE_DATES,
    "snomed": SNOMED_RELEASE_DATES,
    "unii": UNII_RELEASE_DATES,
}


class IsLatestAvailableExDictVersionOperator(BaseSqlOperator):
    def execute_operator(self, other_value):

        target = self.replace_prefix(other_value.get("target"))
        version = other_value.get("version")
        value_is_literal = other_value.get("value_is_literal", False)

        version_sql = None

        if not value_is_literal:
            if version in self.operation_variables:
                version_sql = self._process_constant_operation_variable(version)
            elif self._exists(version):
                version_sql = self.replace_prefix(version.lower())
        else:
            version_sql = self._handle_string_constant(version, lowercase=False)

        external_dictionary_type = other_value.get("external_dictionary_type")

        version_mapping = EXDICT_MAPPINGS.get(external_dictionary_type.lower())
        if not version_mapping:
            raise Exception(f"Unsupported external dictionary type: {external_dictionary_type}")

        target_date_sql = f"CAST({self._column_sql(target, alias=False)} AS TIMESTAMP)"

        sorted_versions = sorted(version_mapping.items(), key=lambda x: x[1], reverse=True)

        effective_version_sql = "CASE"
        for ver, release_date in sorted_versions:
            effective_version_sql += f" WHEN {target_date_sql} >= CAST('{release_date}' AS TIMESTAMP) THEN '{ver}'"
        effective_version_sql += " ELSE NULL END"

        def sql():
            return f"({effective_version_sql}) = {version_sql}"

        return self._do_check_operator(sql)
