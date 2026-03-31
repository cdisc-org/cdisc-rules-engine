import re

from cdisc_rules_engine.constants.domains import (
    AP_DOMAIN,
    APFA_DOMAIN,
    APRELSUB_DOMAIN,
    SUPPLEMENTARY_DOMAINS,
)
from cdisc_rules_engine.constants.metadata_columns import (
    SOURCE_FILENAME,
    SOURCE_ROW_NUMBER,
)
from cdisc_rules_engine.interfaces.data_service_interface import DataServiceInterface
from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata
from cdisc_rules_engine.utilities.utils import (
    search_in_list_of_dicts,
)
from cdisc_rules_engine.constants.classes import (
    DETECTABLE_CLASSES,
    GENERAL_OBSERVATIONS_CLASS,
    FINDINGS,
    FINDINGS_ABOUT,
    FINDINGS_TEST_VARIABLE,
    SPECIAL_PURPOSE,
    SPECIAL_PURPOSE_MODEL,
)
from cdisc_rules_engine.constants.permissibility import (
    PERMISSIBILITY_DEFAULT,
    PERMISSIBILITY_KEY,
)
from cdisc_rules_engine.enums.variable_roles import VariableRoles
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
import copy
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from typing import Iterable, Tuple, List, Optional


def is_custom_domain(
    library_metadata: LibraryMetadataContainer, dataset_name: str
) -> bool:
    """
    Checks if the given dataset is a custom domain based on the standard metadata.

    Args:
        dataset_name: The dataset name to check.
        library_metadata: The library metadata container containing standard metadata.
    """
    standard_details = library_metadata.standard_metadata
    model_details = library_metadata.model_metadata
    is_custom = dataset_name not in standard_details.get(
        "dataset_names", {}
    ) and dataset_name not in model_details.get("dataset_names", {})
    return is_custom


def get_class_and_dataset_metadata(
    library_metadata: LibraryMetadataContainer, dataset_name: str
) -> Tuple[dict, dict]:
    """
    Extracts metadata of a certain class and dataset
    from given standards details. Checks IG first, then model. If not found, returns empty dicts.

    Args:
        library_metadata: Library metadata container containing standard metadata.
        dataset_name: Name of the target dataset

    Returns:
        The class metadata and dataset metadata from the standard.
        Ex:
            {class_details}, {dataset_details}

    """
    for c in library_metadata.standard_metadata.get("classes", []):
        dataset_details = search_in_list_of_dicts(
            c.get("datasets", []), lambda item: item["name"] == dataset_name
        )
        if dataset_details:
            return c, dataset_details
    for c in library_metadata.model_metadata.get("classes", []):
        dataset_details = search_in_list_of_dicts(
            c.get("datasets", []), lambda item: item["name"] == dataset_name
        )
        if dataset_details:
            return c, dataset_details
    return {}, {}


def convert_library_class_name_to_ct_class(class_name: str):
    conversions = {
        "special-purpose": SPECIAL_PURPOSE,
        "special-purpose datasets": SPECIAL_PURPOSE_MODEL,
    }
    return conversions.get(class_name.lower(), class_name.upper())


def get_tabulation_model_type_and_version(model_link: dict) -> Tuple:
    link = model_link.get("href")
    model_type = "sdtm"
    model_version = link.split("/")[-1]
    return model_type, model_version


def get_variables_metadata_from_standard(  # noqa
    library_metadata,
    data_service,
    dataset_metadata: SDTMDatasetMetadata,
    dataset_path: str,
    datasets: Iterable[SDTMDatasetMetadata],
):
    add_AP = False
    domain = dataset_metadata.unsplit_name
    original_domain = domain
    if (
        domain
        and (domain.upper().startswith("SUPP") or domain.upper().startswith("SQ"))
        and len(domain) > 2
    ):
        if domain.upper().startswith("SQ"):
            parent_domain = domain[2:]
            if parent_domain.upper().startswith("AP"):
                add_AP = True
        domain = "SUPPQUAL"
    elif domain and domain.upper().startswith("AP"):
        domain = domain[2:]
        original_domain = domain
        add_AP = True
    model_details = library_metadata.model_metadata
    is_custom = is_custom_domain(library_metadata, domain)
    variables_metadata = []
    if not is_custom:
        IG_class_details, IG_domain_details = get_class_and_dataset_metadata(
            library_metadata, domain
        )
        class_name = convert_library_class_name_to_ct_class(
            IG_class_details.get("name")
        )
    else:
        class_name = data_service._handle_custom_domains(
            data_service.get_dataset(dataset_name=dataset_metadata.full_path),
            dataset_metadata,
            dataset_path,
            datasets,
        )
    model_class_details = get_class_metadata(model_details, class_name)
    # Both custom and standard General Observations pull from model
    if is_custom or class_name in DETECTABLE_CLASSES:
        (
            identifiers_metadata,
            class_variables_metadata,
            timing_metadata,
        ) = get_allowed_class_variables(model_details, model_class_details)
        if add_AP:
            ap_class_details = get_class_metadata(model_details, "ASSOCIATED PERSONS")
            ap_identifiers = ap_class_details.get("classVariables", [])
            identifiers_metadata = [
                v
                for v in identifiers_metadata + ap_identifiers
                if v.get("name") != "USUBJID"
            ]
            identifiers_metadata.sort(key=lambda item: int(item["ordinal"]))
        model_variables = []
        for var_list in [
            identifiers_metadata,
            class_variables_metadata,
            timing_metadata,
        ]:
            replace_variable_wildcards(var_list, original_domain, model_variables)
    # Custom domains only pull from model hierarchy
    if is_custom:
        variables_metadata = model_variables
    # All non-custom domains pull from IG and overwrite the model variables
    else:
        ig_variables = IG_domain_details.get("datasetVariables", [])
        ig_variables.sort(key=lambda item: int(item["ordinal"]))
        if class_name in DETECTABLE_CLASSES:
            variables_metadata = model_variables.copy()
            model_vars_by_name = {
                var["name"]: i for i, var in enumerate(variables_metadata)
            }
            for ig_var in ig_variables:
                if "--" in ig_var["name"]:
                    ig_var_copy = copy.deepcopy(ig_var)
                    ig_var_copy["name"] = ig_var_copy["name"].replace(
                        "--", original_domain
                    )
                    ig_var_to_use = ig_var_copy
                else:
                    ig_var_to_use = ig_var
                ig_var_name = ig_var_to_use["name"]
                if ig_var_name in model_vars_by_name:
                    variables_metadata[model_vars_by_name[ig_var_name]] = ig_var_to_use
                else:
                    # if a variable exists in the IG but not in the model,
                    # insert it at the end of the its section
                    ig_var_role = ig_var_to_use.get("role")
                    if ig_var_role == "Identifier":
                        identifiers_length = len(identifiers_metadata)
                        insertion_point = identifiers_length
                    elif ig_var_role == "Timing":
                        insertion_point = len(variables_metadata)
                    else:
                        timing_metadata_length = len(timing_metadata)
                        insertion_point = (
                            len(variables_metadata) - timing_metadata_length
                        )
                    variables_metadata.insert(insertion_point, ig_var_to_use)
                    model_vars_by_name = {
                        var["name"]: i for i, var in enumerate(variables_metadata)
                    }
        else:
            if add_AP:
                ap_class_details = get_class_metadata(
                    model_details, "ASSOCIATED PERSONS"
                )
                ap_identifiers = ap_class_details.get("classVariables", [])
                ig_variables = [
                    v
                    for v in ig_variables + ap_identifiers
                    if v.get("name") != "USUBJID"
                ]
                ig_variables.sort(key=lambda item: int(item["ordinal"]))
                variables_metadata = []
                replace_variable_wildcards(
                    ig_variables, original_domain, variables_metadata
                )
            else:
                variables_metadata = ig_variables
    set_default_variable_permissibility(variables_metadata)
    return variables_metadata


def get_allowed_class_variables(
    model_details: dict, class_details: dict
) -> Tuple[List[dict], List[dict], List[dict]]:
    """
    Get the variables allowed from the model for a given class.

    Args:
        model_details: Model metadata from cdisc library
        class_details: Model class metadata from cdisc library

    Returns:
        A tuple containing three lists:
            1. The allowed identifier variables
            2. All class variables
            3. Allowed timing variables
    """
    # General Observation class variables to variables metadata
    class_name = convert_library_class_name_to_ct_class(class_details.get("name"))
    class_variables_metadata = class_details.get("classVariables", [])
    class_variables_metadata.sort(key=lambda item: int(item["ordinal"]))
    if class_name == FINDINGS_ABOUT:
        # Add FINDINGS class variables. Findings About class variables should
        # Appear in the list after the --TEST variable
        findings_class_metadata: dict = get_class_metadata(model_details, FINDINGS)
        findings_class_variables = findings_class_metadata["classVariables"]
        findings_class_variables.sort(key=lambda item: int(item["ordinal"]))
        test_index = len(findings_class_variables) - 1
        for i, v in enumerate(findings_class_variables):
            if v["name"] == FINDINGS_TEST_VARIABLE:
                test_index = i
                class_variables_metadata = (
                    findings_class_variables[: test_index + 1]
                    + class_variables_metadata
                    + findings_class_variables[test_index + 1 :]
                )
                break
    if class_name in DETECTABLE_CLASSES:
        gen_obs_class_metadata: dict = get_class_metadata(
            model_details, GENERAL_OBSERVATIONS_CLASS
        )
        gen_obs_class_variables = gen_obs_class_metadata["classVariables"]
        identifiers_metadata, timing_metadata = group_class_variables_by_role(
            gen_obs_class_variables
        )
        identifiers_metadata.sort(key=lambda item: int(item["ordinal"]))
        timing_metadata.sort(key=lambda item: int(item["ordinal"]))
        return identifiers_metadata, class_variables_metadata, timing_metadata
    return [], class_variables_metadata, []


def get_class_metadata(
    model_details: dict,
    dataset_class: str,
) -> dict:
    """
    Extracts metadata of a certain class
    from given standard model details.

    Args:
        model_details: Library model metadata.
        dataset_class: Name of the target class

    Returns:
        The class metadata for the given class name.
        Ex:
            {
                "datasets": [<datasets in the class>],
                "description": class description
                "label": class label
                "name": class name
                "ordinal": class ordinal
            }

    """
    class_metadata: Optional[dict] = search_in_list_of_dicts(
        model_details.get("classes", []),
        lambda item: convert_library_class_name_to_ct_class(item["name"])
        == dataset_class,
    )
    if not class_metadata:
        raise ValueError(
            f"Class metadata is not found in CDISC Library. " f"class={dataset_class}"
        )
    return class_metadata


def group_class_variables_by_role(
    class_variables: List[dict],
) -> Tuple[List[dict], List[dict]]:
    """
    Sorts given class variables by role into 2 lists:
    Identifiers and Timing

    Args:
        class_variables: A list of class variable metadata
    Returns:
        identifier_variables_metadata, timing_variables_metadata
    """
    identifier_vars: List[dict] = []
    timing_vars: List[dict] = []
    for variable in class_variables:
        role: str = variable.get("role")
        if role == VariableRoles.IDENTIFIER.value:
            identifier_vars.append(variable)
        elif role == VariableRoles.TIMING.value:
            timing_vars.append(variable)
    return identifier_vars, timing_vars


def get_variables_metadata_from_standard_model(  # noqa
    dataframe,
    datasets: Iterable[SDTMDatasetMetadata],
    dataset_path: str,
    data_service: DataServiceInterface,
    library_metadata: LibraryMetadataContainer,
    dataset_metadata: SDTMDatasetMetadata,
) -> List[dict]:
    """
    gets class via the IG then uses the class to get the variables via the model
    classes outside of general observation, we check the model for their definition
    if they are not there, differ to the standard definition of the domain
    if custom, IDs class and uses class variables.
    """
    add_AP = False
    domain = dataset_metadata.unsplit_name
    original_domain = domain
    if (
        domain
        and (domain.upper().startswith("SUPP") or domain.upper().startswith("SQ"))
        and len(domain) > 2
    ):
        if domain.upper().startswith("SQ"):
            parent_domain = domain[2:]
            if parent_domain.upper().startswith("AP"):
                add_AP = True
        domain = "SUPPQUAL"
    elif domain and domain.upper().startswith("AP"):
        domain = domain[2:]
        original_domain = domain
        add_AP = True
    model_details = library_metadata.model_metadata
    is_custom = is_custom_domain(library_metadata, domain)
    if not is_custom:
        IG_class_details, IG_domain_details = get_class_and_dataset_metadata(
            library_metadata, domain
        )
        class_name = convert_library_class_name_to_ct_class(
            IG_class_details.get("name")
        )
    else:
        class_name = data_service._handle_custom_domains(
            dataframe, dataset_metadata, dataset_path, datasets
        )
    if class_name in DETECTABLE_CLASSES:
        model_class_details = get_class_metadata(model_details, class_name)
        (
            identifiers_metadata,
            class_variables_metadata,
            timing_metadata,
        ) = get_allowed_class_variables(model_details, model_class_details)
        if add_AP:
            ap_class_details = get_class_metadata(model_details, "ASSOCIATED PERSONS")
            ap_identifiers = ap_class_details.get("classVariables", [])
            identifiers_metadata = identifiers_metadata + ap_identifiers
            # Remove USUBJID from identifiers and re-sort
            identifiers_metadata = [
                v for v in identifiers_metadata if v.get("name") != "USUBJID"
            ]
            identifiers_metadata.sort(key=lambda item: int(item["ordinal"]))
        variables_metadata = []
        for var_list in [
            identifiers_metadata,
            class_variables_metadata,
            timing_metadata,
        ]:
            replace_variable_wildcards(var_list, original_domain, variables_metadata)
            set_default_variable_permissibility(variables_metadata)
        return variables_metadata
    else:
        # First, try to get class metadata and check for classVariables
        class_details = get_class_metadata(model_details, class_name)
        class_variables = class_details.get("classVariables", [])
        if class_variables:
            if add_AP:
                ap_class_details = get_class_metadata(
                    model_details, "ASSOCIATED PERSONS"
                )
                ap_identifiers = ap_class_details.get("classVariables", [])
                # Filter out USUBJID from AP identifiers only, then add to class_variables
                filtered_ap_identifiers = [
                    v for v in ap_identifiers if v.get("name") != "USUBJID"
                ]
                class_variables = class_variables + filtered_ap_identifiers
            class_variables.sort(key=lambda item: int(item["ordinal"]))
            variables_metadata = []
            replace_variable_wildcards(
                class_variables, original_domain, variables_metadata
            )
            set_default_variable_permissibility(variables_metadata)
            return variables_metadata
        else:
            # Second, check if domain exists in model datasets
            domain_details = get_model_domain_metadata(model_details, domain)
            if domain_details:
                dataset_variables = domain_details.get("datasetVariables", [])
                dataset_variables.sort(key=lambda item: int(item["ordinal"]))
                if add_AP:
                    ap_class_details = get_class_metadata(
                        model_details, "ASSOCIATED PERSONS"
                    )
                    ap_identifiers = ap_class_details.get("classVariables", [])
                    dataset_variables = [
                        v
                        for v in dataset_variables + ap_identifiers
                        if v.get("name") != "USUBJID"
                    ]
                variables_metadata = []
                replace_variable_wildcards(
                    dataset_variables, original_domain, variables_metadata
                )
                variables_metadata.sort(key=lambda item: int(item["ordinal"]))
                set_default_variable_permissibility(variables_metadata)
                return variables_metadata
            # Third, fall back to standard datasets
            if IG_domain_details:
                dataset_variables = IG_domain_details.get("datasetVariables", [])
                dataset_variables.sort(key=lambda item: int(item["ordinal"]))
                if add_AP:
                    ap_class_details = get_class_metadata(
                        model_details, "ASSOCIATED PERSONS"
                    )
                    ap_identifiers = ap_class_details.get("classVariables", [])
                    dataset_variables = [
                        v
                        for v in dataset_variables + ap_identifiers
                        if v.get("name") != "USUBJID"
                    ]
                variables_metadata = []
                replace_variable_wildcards(
                    dataset_variables, original_domain, variables_metadata
                )
                set_default_variable_permissibility(variables_metadata)
                return variables_metadata
        return None


def get_model_domain_metadata(model_details: dict, domain_name: str) -> dict:
    # Get domain metadata from model
    domain_details: Optional[dict] = search_in_list_of_dicts(
        model_details.get("datasets", []), lambda item: item["name"] == domain_name
    )

    return domain_details


def replace_variable_wildcards(var_list, domain, target_list):
    """Add variables from var_list to target_list, replacing '--' with domain in names."""
    for var in var_list:
        # Create a deepcopy to avoid modifying cached library metadata
        var_copy = copy.deepcopy(var)
        var_copy["name"] = var_copy["name"].replace("--", domain)
        target_list.append(var_copy)


def set_default_variable_permissibility(var_list):
    for variable_metadata in var_list:
        if PERMISSIBILITY_KEY not in variable_metadata:
            variable_metadata[PERMISSIBILITY_KEY] = PERMISSIBILITY_DEFAULT


def get_all_model_wildcard_variables(model_details: dict):
    return {
        classVariable["name"]
        for cls in model_details.get("classes", [])
        for classVariable in cls.get("classVariables", [])
        if classVariable["name"].startswith("--")
    }


def add_variable_wildcards(
    model_details: dict, variables: list[str], domain: str, wildcard: str
):
    all_model_wildcard_variables = get_all_model_wildcard_variables(model_details)
    return {
        variable: (
            variable.replace(domain, wildcard, 1)
            if variable.startswith(domain)
            and variable.replace(domain, "--", 1) in all_model_wildcard_variables
            else variable
        )
        for variable in variables
    }


def is_supp_domain(dataset_domain: str) -> bool:
    """
    Returns true if domain name starts with SUPP or SQ
    """
    return dataset_domain.startswith(SUPPLEMENTARY_DOMAINS)


def is_ap_domain(dataset_domain: str) -> bool:
    """
    Returns true if domain name is like AP-- / APFA APRELSUB.
    """
    if dataset_domain == APRELSUB_DOMAIN:
        return True
    if len(dataset_domain) == 6:
        domain_to_check: str = APFA_DOMAIN
    else:
        domain_to_check: str = AP_DOMAIN
    regex = r"^" + re.escape(domain_to_check) + "[a-zA-Z]{2,4}$"
    return bool(re.match(regex, dataset_domain))


def get_corresponding_datasets(
    datasets: Iterable[SDTMDatasetMetadata], dataset_metadata: SDTMDatasetMetadata
) -> List[SDTMDatasetMetadata]:
    return [
        other
        for other in datasets
        if dataset_metadata.unsplit_name == other.unsplit_name
    ]


def tag_source(
    dataset: DatasetInterface, dataset_metadata: DatasetMetadata
) -> DatasetInterface:
    """
    For sdtm split datasets,
    Adds source filename and row number to dataset
    """
    dataset[SOURCE_FILENAME] = dataset_metadata.filename
    dataset[SOURCE_ROW_NUMBER] = list(range(1, dataset.len() + 1))
    return dataset
