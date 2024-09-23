from cdisc_rules_engine.interfaces.cache_service_interface import CacheServiceInterface
from cdisc_rules_engine.interfaces.config_interface import ConfigInterface
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
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from typing import Tuple, List, Optional


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


def get_variables_metadata_from_standard(
    standard: str,
    standard_version: str,
    domain: str,
    config: ConfigInterface,
    cache: CacheServiceInterface,
    library_metadata: LibraryMetadataContainer,
    include_model_variables: bool = True,
) -> List[dict]:
    """
    Gets variables metadata for the given class and domain from cache.
    The cache stores CDISC Library metadata.

    Args:
        standard: Standard to validate against
        standard_version: Version of the standard to validate against
        domain: The domain being validated
        cache: Cache service for retrieving previously cached library data
        config: Config used to create a cdisc library client.
        include_model_variables: Boolean flag signifying whether or not
                                 model variables are included
    Returns:
    [
        {
            "label":"Study Identifier",
            "name":"STUDYID",
            "ordinal":"1",
            "role":"Identifier",
            "description": "desc",
            "simpleDatatype": "Char"
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
    if not standard or not standard_version:
        raise Exception("Please provide standard and version")
    standard_details: dict = retrieve_standard_metadata(
        standard, standard_version, cache, config, library_metadata
    )
    model = standard_details.get("_links", {}).get("model")
    class_details, domain_details = get_class_and_domain_metadata(
        standard_details, domain
    )
    model_type, model_version = get_tabulation_model_type_and_version(model)
    model_details = library_metadata.model_metadata

    variables_metadata = domain_details.get("datasetVariables", [])
    sort_key = "ordinal" if "ordinal" in variables_metadata[0] else "order_number"
    variables_metadata.sort(key=lambda item: item[sort_key])
    class_name = convert_library_class_name_to_ct_class(class_details.get("name"))
    if class_name in DETECTABLE_CLASSES and model_details and include_model_variables:
        existing_variables = set([var["name"] for var in variables_metadata])
        (
            identifiers_metadata,
            class_variables_metadata,
            timing_metadata,
        ) = get_allowed_class_variables(model_details, class_details)

        """
         In some cases an identifier variable appears in both
         the model class and the ig domain. We want to make sure to only add variables
         that don't already appear in the ig domain to the list of variables
         in order to avoid having duplicate variables in the output.
        """
        if identifiers_metadata:
            new_identifiers = [
                idvar
                for idvar in identifiers_metadata
                if idvar["name"].replace("--", domain) not in existing_variables
            ]
            variables_metadata = new_identifiers + variables_metadata
        if timing_metadata:
            new_timing_vars = [
                timing_var
                for timing_var in timing_metadata
                if timing_var["name"].replace("--", domain) not in existing_variables
            ]
            variables_metadata = variables_metadata + new_timing_vars

    return variables_metadata


def retrieve_standard_metadata(
    standard: str,
    standard_version: str,
    cache: CacheServiceInterface,
    config: ConfigInterface,
    library_metadata: LibraryMetadataContainer,
) -> dict:
    """
    Gets library metadata from LibraryMetadataContainer.
    If the metadata is not found in the LibraryMetadataContainer,
    query the library for the required data.

    Args:
        standard: Standard to validate against
        standard_version: Version of the standard to validate against
        cache: Cache service for retrieving previously cached library data
        config: Config used to create a cdisc library client.

    Returns:
        The CDISC library response for the provided standard and version.
    """
    standard_data: dict = library_metadata.standard_metadata
    if not standard_data:
        cdisc_library_service = CDISCLibraryService(config, cache)
        standard_data = cdisc_library_service.get_standard_details(
            standard.lower(), standard_version
        )
        library_metadata.standard_metadata = standard_data
    return standard_data


def get_allowed_class_variables(
    model_details: dict, class_details: dict
) -> Tuple[List[dict], List[dict], List[dict]]:
    """
    Get the variables allowed from the model for a given class.

    Args:
        model_details: Model metadata from cdisc library
        class_details: IG class metadata from cdisc library

    Returns:
        A tuple containing three lists:
            1. The allowed identifier variables
            2. All class variables
            3. Allowed timing variables
    """
    # General Observation class variables to variables metadata
    class_name = convert_library_class_name_to_ct_class(class_details.get("name"))
    variables_metadata = class_details.get("classVariables", [])
    if variables_metadata:
        sort_key = "ordinal" if "ordinal" in variables_metadata[0] else "order_number"
        variables_metadata.sort(key=lambda item: item[sort_key])

    if class_name == FINDINGS_ABOUT:
        # Add FINDINGS class variables. Findings About class variables should
        # Appear in the list after the --TEST variable
        findings_class_metadata: dict = get_class_metadata(model_details, FINDINGS)
        findings_class_variables = findings_class_metadata["classVariables"]
        FAsort_key = (
            "ordinal" if "ordinal" in findings_class_variables[0] else "order_number"
        )
        findings_class_variables.sort(key=lambda item: item[FAsort_key])
        test_index = len(findings_class_variables) - 1
        for i, v in enumerate(findings_class_variables):
            if v["name"] == FINDINGS_TEST_VARIABLE:
                test_index = i
                variables_metadata = (
                    findings_class_variables[: test_index + 1]
                    + variables_metadata
                    + findings_class_variables[test_index + 1 :]
                )
                break

    gen_obs_class_metadata: dict = get_class_metadata(
        model_details, GENERAL_OBSERVATIONS_CLASS
    )
    identifiers_metadata, timing_metadata = group_class_variables_by_role(
        gen_obs_class_metadata["classVariables"]
    )

    def standardize_order_number(var):
        if "ordinal" in var:
            var["order_number"] = var.pop("ordinal")
        return var

    identifiers_metadata = list(map(standardize_order_number, identifiers_metadata))
    timing_metadata = list(map(standardize_order_number, timing_metadata))
    # Identifiers are added to the beginning and Timing to the end
    identifiers_metadata.sort(key=lambda item: item["order_number"])
    timing_metadata.sort(key=lambda item: item["order_number"])
    return identifiers_metadata, variables_metadata, timing_metadata


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
    standard: str,
    standard_version: str,
    domain: str,
    dataframe,
    datasets: List[dict],
    dataset_path: str,
    cache: CacheServiceInterface,
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
            sort_key = (
                "ordinal" if "ordinal" in variables_metadata[0] else "order_number"
            )
            variables_metadata.sort(key=lambda item: item[sort_key])
    else:
        # Domain not found in the model. Detect class name from data
        class_name = data_service.get_dataset_class(
            dataframe, dataset_path, datasets, domain
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


def replace_variable_wildcards(variables_metadata, domain):
    return [var["name"].replace("--", domain) for var in variables_metadata]


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
        variable: variable.replace(domain, wildcard, 1)
        if variable.startswith(domain)
        and variable.replace(domain, "--", 1) in all_model_wildcard_variables
        else variable
        for variable in variables
    }
