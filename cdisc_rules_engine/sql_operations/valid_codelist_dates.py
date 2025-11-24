from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from cdisc_rules_engine.constants.standard_package_mappings import STANDARD_PACKAGE_MAPPINGS


class SqlValidCodelistDates(SqlBaseOperation):
    """
    Returns the valid terminology package dates for a given standard.
    The package dates are obtained from the standard library metadata,
    and filtered according to either the standard or a custom list of
    ct_package_types from the rule.

    Ex:
        Given a list of terminology packages:
            [sdtmct-2023-10-26, sdtmct-2023-12-13,
            adamct-2023-12-13, cdashct-2023-05-19]
        and standard: sdtmig
        the operation will return ["2023-10-26", "2023-12-13"]

    The default standard can be overridden using the optional
    `ct_package_types` operation parameter.
    Ex:
        Given the same list of terminology packages
        and `ct_package_types`: ["SDTM", "CDASH"]
        the operation will return:
            ["2023-05-19", "2023-10-26", "2023-12-13"]

    """

    def _execute_operation(self):

        ct_packages = self.params.standards_context.get_ct_packages()

        if not ct_packages:
            sorted_ct_packages = []
        sorted_ct_packages = sorted(
            list(
                set(
                    self._parse_date_from_ct_package(package)
                    for package in ct_packages
                    if self._is_applicable_ct_package(package)
                )
            )
        )

        query = self._format_variable_list_to_query(vars=sorted_ct_packages, unique=True, ordered=True)

        return SqlOperationResult(query=query, type="collection", subtype="Char")

    def _is_applicable_ct_package(self, package: str) -> bool:
        package_type = package.split("-", 1)[0]

        # Runs ct_package_types through the mapping if they exist, else defaults to
        # params.standards_context.standard. This is different to the old engine, which
        # didn't run the ct_package_types through the mapping, but I think it's needed
        applicable_package_types = (
            set().union(*(STANDARD_PACKAGE_MAPPINGS.get(cpt.lower(), set()) for cpt in self.params.ct_package_types))
            if self.params.ct_package_types
            else STANDARD_PACKAGE_MAPPINGS.get(self.params.standards_context.standard.lower(), set())
        )
        return package_type in applicable_package_types

    def _parse_date_from_ct_package(self, package: str) -> str:
        return package.split("-", 1)[-1]
