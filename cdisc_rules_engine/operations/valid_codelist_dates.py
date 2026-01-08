from cdisc_rules_engine.operations.base_operation import BaseOperation


class ValidCodelistDates(BaseOperation):
    """
    Returns the valid terminology package dates for a given standard
    Ex:
        Given a list of terminology packages:
            [sdtmct-2023-10-26, sdtmct-2023-12-13,
             adamct-2023-12-13, cdashct-2023-05-19]
        and standard: sdtmig
        the operation will return ["2023-10-26", "2023-12-13"]

    The default standard (obtained from the validation parameter) can
    be overridden using the optional `ct_package_types` operation
    parameter.
    Ex:
        Given the same list of terminology packages
        and `ct_package_types`: ["SDTM", "CDASH"]
        the operation will return:
            ["2023-05-19", "2023-10-26", "2023-12-13"]

    """

    def _execute_operation(self):
        # get metadata
        ct_packages = self.library_metadata.published_ct_packages
        if not ct_packages:
            return []
        return sorted(
            list(
                set(
                    self._parse_date_from_ct_package(package)
                    for package in ct_packages
                    if self._is_applicable_ct_package(package)
                )
            )
        )

    def _is_applicable_ct_package(self, package: str) -> bool:
        standard_to_package_type_mapping = {
            "sdtmig": {"sdtmct"},
            "sendig": {"sendct"},
            "cdashig": {"cdashct"},
            "adamig": {"sdtmct", "adamct"},
            "usdm": {"ddfct", "sdtmct"},
        }
        package_type = package.split("-", 1)[0]
        applicable_package_types = (
            set(self.params.ct_package_types)
            if self.params.ct_package_types
            else standard_to_package_type_mapping.get(
                self.params.standard.lower(), set()
            )
        )
        return package_type in applicable_package_types

    def _parse_date_from_ct_package(self, package: str) -> str:
        return package.split("-", 1)[-1]
