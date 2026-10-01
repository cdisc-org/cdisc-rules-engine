from typing import Optional

import pandas as pd

from cdisc_rules_engine.models.dataset import DaskDataset
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.services import logger

# this dict mirror the SQL operator's _COLUMN_MAP where they overlap.
_CODELIST_FIELDS = {
    "Codelist Code": "conceptId",
    "Codelist CCODE": "conceptId",
    "Codelist Value": "submissionValue",
    "Codelist Name": "name",
    "Extensible": "extensible",
}
_TERM_FIELDS = {
    "Term CCODE": "conceptId",
    "Term Value": "submissionValue",
    "Term Submission Value": "submissionValue",
    "Term Preferred Term": "preferredTerm",
    "Definition": "definition",
    "Synonyms": "synonyms",
}


def _ct_prefix(standard: Optional[str], substandard: Optional[str]) -> str:
    std = (standard or "").lower()
    if "tig" in std:
        std = (substandard or "").lower()
    if "adam" in std:
        return "adamct"
    if "send" in std:
        return "sendct"
    return "sdtmct"


def _get_ct_package(row, ct_target, ct_version, standard, substandard):
    version = row[ct_version]
    if pd.isna(version) or str(version).strip() == "":
        return ""
    target_val = str(row[ct_target]).strip() if pd.notna(row[ct_target]) else ""
    if target_val in ("CDISC", "CDISC CT"):
        return f"{_ct_prefix(standard, substandard)}-{version}"
    return f"{target_val}-{version}"


class CodeListAttributes(BaseOperation):
    """
    Fetches codelist attribute values (e.g. Term CCODEs) from CT packages.

    Row-specific: when `version` names a column in the dataset (and `name`
    names the reference column), each row gets the values from its own package.

    Static: otherwise, packages come from the run's CT packages (-ct) or
    from `version` used as a literal version/list of versions and every
    row gets the same set.

    ct_conditions filters codelists/terms in both
    """

    def _execute_operation(self):
        ct_attribute = self.params.ct_attribute
        if ct_attribute not in _CODELIST_FIELDS and ct_attribute not in _TERM_FIELDS:
            raise ValueError(f"Unsupported ct_attribute: {ct_attribute}")
        codelist_conds, term_conds = self._split_conditions(
            getattr(self.params, "ct_conditions", None)
        )
        if self._uses_row_versions():
            return self._row_specific(ct_attribute, codelist_conds, term_conds)
        return self._static(ct_attribute, codelist_conds, term_conds)

    def _uses_row_versions(self) -> bool:
        columns = self.params.dataframe.columns
        version = self.params.ct_version
        target = self.params.target
        return (
            isinstance(version, str)
            and version in columns
            and isinstance(target, str)
            and target in columns
        )

    # ---- row-specific------------------------

    def _row_specific(self, ct_attribute, codelist_conds, term_conds):
        df = self.params.dataframe
        args = (
            self.params.target,
            self.params.ct_version,
            self.params.standard,
            self.params.standard_substandard,
        )
        is_dask = isinstance(df, DaskDataset)
        if is_dask:
            row_packages = df.data.apply(
                _get_ct_package, axis=1, meta=(None, "object"), args=args
            )
            unique_packages = set(row_packages.compute().unique())
        else:
            row_packages = df.data.apply(_get_ct_package, axis=1, args=args)
            unique_packages = set(row_packages.unique())
        unique_packages.discard("")

        package_to_values = {
            pkg: self._extract(
                self._load_package(pkg), ct_attribute, codelist_conds, term_conds
            )
            for pkg in unique_packages
        }

        def lookup(pkg):
            return package_to_values.get(pkg, set()) if pkg else set()

        if is_dask:
            return row_packages.apply(lookup, meta=(None, "object"))
        return row_packages.apply(lookup)

    # ---- static ----------------------------

    def _static(self, ct_attribute, codelist_conds, term_conds):
        packages = self._static_packages()
        if not packages:
            logger.warning(
                "get_codelist_attributes: no CT packages resolved (no -ct packages, "
                "no literal version, no CT loaded in library metadata); "
                "returning empty set."
            )
        values = set()
        for pkg in packages:
            values |= self._extract(
                self._load_package(pkg), ct_attribute, codelist_conds, term_conds
            )
        return values

    def _static_packages(self) -> list:
        provided = getattr(self.params, "ct_packages", None)
        if provided:
            return list(provided)
        version = self.params.ct_version
        if not version:
            return self._loaded_ct_packages()
        versions = version if isinstance(version, list) else [version]
        prefix = _ct_prefix(self.params.standard, self.params.standard_substandard)
        packages = []
        for v in versions:
            if not isinstance(v, str) or not v.strip():
                continue
            v = v.strip()
            packages.append(v if "ct-" in v else f"{prefix}-{v}")
        return packages

    def _loaded_ct_packages(self) -> list:
        """Fallback: CT packages the engine already loaded into library
        metadata (e.g. from the define.xml), narrowed to the ones matching
        the standard being validated (SDTM/SEND/ADaM)."""
        loaded = getattr(self.library_metadata, "_ct_package_metadata", None) or {}
        packages = [p for p in loaded if isinstance(p, str)]
        wanted = _ct_prefix(self.params.standard, self.params.standard_substandard)
        matching = [p for p in packages if p.startswith(f"{wanted}-")]
        return matching or packages

    def _load_package(self, package: str) -> dict:
        parts = package.rsplit("-", 3)
        if len(parts) >= 4:
            self.library_metadata._load_ct_package_data(parts[0], "-".join(parts[1:]))
        return self.library_metadata.get_ct_package_metadata(package) or {}

    @staticmethod
    def _split_conditions(conditions) -> tuple:
        codelist_conds, term_conds = {}, {}
        for condition in conditions or []:
            for raw_key, expected in condition.items():
                key = str(raw_key).replace("_", " ").strip()
                if key in _CODELIST_FIELDS:
                    codelist_conds[_CODELIST_FIELDS[key]] = expected
                elif key in _TERM_FIELDS:
                    term_conds[_TERM_FIELDS[key]] = expected
                else:
                    raise ValueError(f"Unsupported ct_conditions key: {raw_key}")
        return codelist_conds, term_conds

    @staticmethod
    def _matches(item: dict, conds: dict) -> bool:
        for field, expected in conds.items():
            actual = item.get(field)
            if expected is None:
                if actual not in (None, ""):
                    return False
            elif (
                actual is None
                or str(actual).strip().lower() != str(expected).strip().lower()
            ):
                return False
        return True

    @staticmethod
    def _add(values: set, value):
        if value is None:
            return
        if isinstance(value, (list, tuple, set)):
            values.update(
                v.strip() if isinstance(v, str) else v for v in value if v is not None
            )
        else:
            values.add(value)

    def _extract(self, pkg: dict, ct_attribute, codelist_conds, term_conds) -> set:
        values = set()
        for codelist in pkg.get("codelists", []):
            if not self._matches(codelist, codelist_conds):
                continue
            terms = codelist.get("terms", [])
            if ct_attribute in _CODELIST_FIELDS:
                if term_conds and not any(self._matches(t, term_conds) for t in terms):
                    continue
                self._add(values, codelist.get(_CODELIST_FIELDS[ct_attribute]))
            else:
                field = _TERM_FIELDS[ct_attribute]
                for term in terms:
                    if self._matches(term, term_conds):
                        self._add(values, term.get(field))
        return values
