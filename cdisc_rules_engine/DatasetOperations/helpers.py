from datetime import datetime
import pandas as pd
import dask.dataframe as dd
import logging
from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService
from collections import defaultdict
from cdisc_rules_engine.models.dictionaries.whodrug.whodrug_record_types import (
    WhodrugRecordTypes,
)
from typing import Set


def parse_timestamp(timestamp: str) -> datetime:
    try:
        return datetime.fromisoformat(timestamp)
    except TypeError:
        # Null date time
        return None
    except ValueError:
        # Value is not iso format
        return None


def get_day_difference(delta: datetime) -> int:
    # Return 1 if the --DTC value is the same as the DY
    return delta.days if delta.days < 0 else delta.days + 1


def _get_codelist_attributes(params, library_metadata, cache):
    """
    Fetches codelist for a given codelist package and version from the TS
    dataset.
    Returns it as a Series of lists like:
        0    ["STUDYID", "DOMAIN", ...]
        1    ["STUDYID", "DOMAIN", ...]
        2    ["STUDYID", "DOMAIN", ...]
        ...

    pd.Series: A Series of lists containing codelist, where each list
        represents the codelist package and version.
        The length of the Series is equal to the length of the given
        dataframe.
    """

    # 1.0 get input variables
    # -------------------------------------------------------------------
    ct_name = "CT_PACKAGE"  # a column for controlled term package names
    # Get controlled term attribute column name specified in rule
    ct_attribute = params.ct_attribute

    # 2.0 build codelist from cache
    # -------------------------------------------------------------------
    ct_cache = _get_ct_from_library_metadata(
        ct_key=ct_name,
        ct_val=ct_attribute,
        params=params,
        library_metadata=library_metadata,
        cache=cache,
    )

    # 3.0 get dataset records
    # -------------------------------------------------------------------
    ct_data = _get_ct_from_dataset(params=params, ct_key=ct_name, ct_val=ct_attribute)

    # 4.0 merge the two datasets by CC
    # -------------------------------------------------------------------
    if isinstance(ct_data, pd.DataFrame):
        # If it's a Pandas DataFrame
        cc_key = ct_data[ct_name].to_list()
        ct_list = ct_cache[ct_cache[ct_name].isin(cc_key)]

        ds_len = params.dataframe.shape[0]  # dataset length
        result = pd.Series([ct_list[ct_attribute].values[0] for _ in range(ds_len)])
        return result
    elif isinstance(ct_data, dd.DataFrame):
        # Step 1: Compute the Dask DataFrame to Pandas DataFrame
        cc_key = ct_data.compute()[ct_name].tolist()

        # Step 2: Filter the Pandas DataFrame using the computed `cc_key`
        ct_list = ct_cache[ct_cache[ct_name].isin(cc_key)]
        # Step 3: Calculate the length of the dataset
        ds_len = params.dataframe.shape[0].compute()

        # Step 4: Create the result list using a list comprehension
        result = [ct_list[ct_attribute].values[0] for _ in range(ds_len)]
        return result


def _get_ct_from_library_metadata(
    ct_key: str,
    ct_val: str,
    params,
    library_metadata,
    cache,
):
    """
    Retrieves the codelist information from the cache based on the given
    ct_key and ct_val.

    Args:
        ct_key (str): The key for identifying the codelist.
        ct_val (str): The value associated with the codelist.

    Returns:
        pd.DataFrame: A DataFrame containing the codelist information
        retrieved from the cache.
    """
    ct_packages = params.ct_packages
    ct_term_maps = (
        []
        if ct_packages is None
        else [
            library_metadata.get_ct_package_metadata(package) or {}
            for package in ct_packages
        ]
    )

    # convert codelist to dataframe
    ct_result = {ct_key: [], ct_val: []}
    ct_result = _add_codelist(ct_key, ct_val, ct_term_maps, ct_result)

    is_contained = set(ct_packages).issubset(set(ct_result[ct_key]))
    # if all the CT packages exist in Cache, we return the result
    if is_contained:
        return pd.DataFrame(ct_result)

    # if not, we need to get them from library
    config = ConfigService()
    logger = logging.getLogger()
    api_key = config.getValue("CDISC_LIBRARY_API_KEY")
    ct_diff = list(set(ct_packages) - set(set(ct_result[ct_key])))

    cls = CDISCLibraryService(api_key, cache)
    ct_pkgs = cls.get_all_ct_packages()
    ct_names = [item["href"].split("/")[-1] for item in ct_pkgs]

    for ct in ct_diff:
        if ct not in ct_names:
            logger.info(f"Requested package {ct} not in CT library.")
            continue
        ct_code = cls.get_codelist_terms_map(ct)
        ct_result = _add_codelist(ct_key, ct_val, ct_code, ct_result)
    return pd.DataFrame(ct_result)


def _get_ct_from_dataset(params, ct_key: str, ct_val: str):
    """
    Retrieves the codelist information from the dataset based on the given
    ct_key and ct_val.

    Args:
        ct_key (str): The key for identifying the codelist.
        ct_val (str): The value associated with the codelist.

    Returns:
        pd.DataFrame: A DataFrame containing the codelist information
        retrieved from the dataset.
    """
    ct_packages = params.ct_packages
    # get attribute variable specified in rule
    ct_attribute = params.ct_attribute

    ct_target = params.target  # target variable specified in rule
    ct_version = params.ct_version  # controlled term version
    if ct_attribute == "Term CCODE":
        ct_attribute = "TSVALCD"
    sel_cols = [ct_target, ct_version, ct_attribute, ct_key]

    # get dataframe from dataset records
    df = params.dataframe

    # add CT_PACKAGE column

    if isinstance(df, dd.DataFrame):
        df[ct_key] = df.apply(
            lambda row: "sdtmct-" + row[ct_version]
            if row[ct_target] is not None and row[ct_target] in ("CDISC", "CDISC CT")
            else row[ct_target] + "-" + row[ct_version],
            axis=1,
            meta=("str"),
        )
    elif isinstance(df, pd.DataFrame):
        df[ct_key] = df.apply(
            lambda row: "sdtmct-" + row[ct_version]
            if row[ct_target] is not None and row[ct_target] in ("CDISC", "CDISC CT")
            else row[ct_target] + "-" + row[ct_version],
            axis=1,
        )

    # select records
    df_sel = df[(df[ct_key].isin(ct_packages))].loc[:, sel_cols]

    # group the records
    result = df_sel.groupby(ct_key)[ct_attribute].unique().reset_index()
    result.rename(columns={ct_attribute: ct_val})

    return result


def _add_codelist(ct_key, ct_val, ct_term_maps, ct_result):
    """
    Adds codelist information to the result dictionary.

    Args:
        ct_key (str): The key for identifying the codelist.
        ct_val (str): The value associated with the codelist.
        ct_term_maps (list[dict]): A list of dictionaries containing
            codelist information.
        ct_result (dict): The dictionary to store the codelist information.

    Returns:
        dict: The updated ct_result dictionary.
    """
    for item in ct_term_maps:
        ct_result[ct_key].append(item.get("package"))
        codes = set(code for code in item.keys() if code != "package")
        ct_result[ct_val].append(codes)
    return ct_result


def _get_domain_to_datasets(params):
    domain_to_datasets = defaultdict(list)
    for dataset in params.datasets:
        domain_to_datasets[dataset["domain"]].append(dataset)
    return domain_to_datasets


def get_code_hierarchies(term_map: dict) -> Set[str]:
    valid_codes = set()
    for atc_class in term_map.get(
        WhodrugRecordTypes.ATC_CLASSIFICATION.value, {}
    ).values():
        atc_text = term_map.get(WhodrugRecordTypes.ATC_TEXT.value, {}).get(
            atc_class.code
        )
        if atc_text:
            drug_dict = term_map.get(WhodrugRecordTypes.DRUG_DICT.value, {}).get(
                atc_class.get_parent_identifier()
            )
            if drug_dict:
                valid_codes.add(
                    f"{drug_dict.drugName}/{atc_text.text}/{atc_class.code}"
                )

    return valid_codes


def _is_target_variable_null(dataframe, target_variable: str) -> bool:
    if target_variable not in dataframe:
        return True
    if isinstance(dataframe, dd.DataFrame):
        series = dataframe.compute()[target_variable]
    else:
        series = dataframe[target_variable]
    return series.mask(series == "").isnull().all()


def _is_applicable_ct_package(params, package: str) -> bool:
    standard_to_package_type_mapping = {
        "sdtmig": {"sdtmct"},
        "sendig": {"sendct"},
        "cdashig": {"cdashct"},
        "adamig": {"sdtmct", "adamct"},
    }
    package_type = package.split("-", 1)[0]
    applicable_package_types = standard_to_package_type_mapping.get(
        params.standard.lower(), set()
    )
    return package_type in applicable_package_types


def _parse_date_from_ct_package(package: str) -> str:
    return package.split("-", 1)[-1]
