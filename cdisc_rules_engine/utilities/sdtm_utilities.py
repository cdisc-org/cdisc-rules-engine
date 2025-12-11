from cdisc_rules_engine.interfaces.data_service_interface import DataServiceInterface
from cdisc_rules_engine.utilities.utils import (
    search_in_list_of_dicts,
    convert_library_class_name_to_ct_class,
)
from cdisc_rules_engine.constants.classes import (
    DETECTABLE_CLASSES,
    GENERAL_OBSERVATIONS_CLASS,
    FINDINGS,
    FINDINGS_ABOUT,
    FINDINGS_TEST_VARIABLE,
)
from cdisc_rules_engine.enums.variable_roles import VariableRoles
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
import copy
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from typing import Iterable, Tuple, List, Optional


def get_class_and_domain_metadata(
    standard_details: dict, domain: str
) -> Tuple[dict, dict]:
    """
    Extracts metadata of a certain class and domain
    from given standards details.

    Args:
        standard_details: Library implementation guide metadata.
        domain: Name of the target domain

    Returns:
        The class metadata and domain metadata from the standard.
        Ex:
            {class_details}, {domain_details}

    """
    # Get domain and class details for domain.
    for c in standard_details.get("classes"):
        domain_details = search_in_list_of_dicts(
            c.get("datasets", []), lambda item: item["name"] == domain
        )
        if domain_details:
            return c, domain_details
    return {}, {}


def get_tabulation_model_type_and_version(model_link: dict) -> Tuple:
    link = model_link.get("href")
    model_type = "sdtm"
    model_version = link.split("/")[-1]
    return model_type, model_version


def get_variables_metadata_from_standard(domain, library_metadata):  # noqa
    standard_details = library_metadata.standard_metadata
    model_details = library_metadata.model_metadata
    is_custom = domain not in standard_details.get("domains", {})
    variables_metadata = []
    IG_class_details, IG_domain_details = get_class_and_domain_metadata(
        standard_details, domain
    )
    class_name = convert_library_class_name_to_ct_class(IG_class_details.get("name"))
    model_class_details = get_class_metadata(model_details, class_name)
    # Both custom and standard General Observations pull from model
    if is_custom or class_name in DETECTABLE_CLASSES:
        (
            identifiers_metadata,
            class_variables_metadata,
            timing_metadata,
        ) = get_allowed_class_variables(model_details, model_class_details)
        model_variables = []
        for var_list in [
            identifiers_metadata,
            class_variables_metadata,
            timing_metadata,
        ]:
            replace_variable_wildcards(var_list, domain, model_variables)
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
                ig_var_name = ig_var["name"]
                if ig_var_name in model_vars_by_name:
                    variables_metadata[model_vars_by_name[ig_var_name]] = ig_var
                else:
                    # if a variable exists in the IG but not in the model,
                    # insert it at the end of the its section
                    ig_var_role = ig_var.get("role")
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
                    variables_metadata.insert(insertion_point, ig_var)
                    model_vars_by_name = {
                        var["name"]: i for i, var in enumerate(variables_metadata)
                    }
        else:
            variables_metadata = ig_variables
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


def get_variables_metadata_from_standard_model(
    domain: str,
    dataframe,
    datasets: Iterable[SDTMDatasetMetadata],
    dataset_path: str,
    data_service: DataServiceInterface,
    library_metadata: LibraryMetadataContainer,
) -> List[dict]:
    """
    Gets variables metadata for the given class and domain from cache.
    The cache stores CDISC Library metadata.
    Retrieves variables metadata from IG,
    unless the dataset class is a GENERAL OBSERVATIONS domain.
    In this case variables metadata is pulled from the model.

    Args:
        standard: Standard to validate against
        standard_version: Version of the standard to validate against
        domain: The domain being validated
        dataframe: The dataset being a evaluated.
        datasets: List of all datasets in the study
        dataset_path: File path of the target dataset
        cache: Cache service for retrieving previously cached library data
        data_service: Data service instance
    Returns:
    [
        {
            "label":"Study Identifier",
            "name":"STUDYID",
            "ordinal":"1",
            "role":"Identifier",
               ...
        },
        {
            "label":"Domain Abbreviation",
            "name":"DOMAIN",
            "ordinal":"2",
            "role":"Identifier"
        },
            ...
    ]
    """
    # get model details from cache
    model_details = library_metadata.model_metadata
    if (
        domain
        and (domain.upper().startswith("SUPP") or domain.upper().startswith("SQ"))
        and len(domain) > 2
    ):
        domain = "SUPPQUAL"
    domain_details = get_model_domain_metadata(model_details, domain)
    variables_metadata = []

    if domain_details:
        # Domain found in the model
        class_name = convert_library_class_name_to_ct_class(
            domain_details["_links"]["parentClass"]["title"]
        )
        class_details = get_class_metadata(model_details, class_name)
        variables_metadata = domain_details.get("datasetVariables", [])
        if variables_metadata:
            variables_metadata.sort(key=lambda item: int(item["ordinal"]))
    else:
        # Domain not found in the model. Detect class name from data
        domain_details = search_in_list_of_dicts(
            datasets,
            lambda item: domain == (item.domain or item.name),
        )
        class_name = data_service.get_dataset_class(
            dataframe, dataset_path, datasets, domain_details
        )
        class_name = convert_library_class_name_to_ct_class(class_name)
        class_details = get_class_metadata(model_details, class_name)

    if class_name in DETECTABLE_CLASSES:
        (
            identifiers_metadata,
            variables_metadata,
            timing_metadata,
        ) = get_allowed_class_variables(model_details, class_details)
        # Identifiers are added to the beginning and Timing to the end
        if identifiers_metadata:
            variables_metadata = identifiers_metadata + variables_metadata
        if timing_metadata:
            variables_metadata = variables_metadata + timing_metadata

    return variables_metadata


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
