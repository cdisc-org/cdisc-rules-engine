import json
import yaml

from cdisc_rules_engine.interfaces import CacheServiceInterface
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from cdisc_rules_engine.interfaces.data_service_interface import DataServiceInterface
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.services.data_services import (
    DataServiceFactory,
)
from typing import List, Iterable
from cdisc_rules_engine.config import config
from cdisc_rules_engine.services import logger as engine_logger
import os
import pickle
from cdisc_rules_engine.models.dictionaries.get_dictionary_terms import (
    extract_dictionary_terms,
)
from cdisc_rules_engine.models.rule import Rule
from cdisc_rules_engine.utilities.utils import (
    get_rules_cache_key,
    get_standard_details_cache_key,
    get_model_details_cache_key_from_ig,
    get_library_variables_metadata_cache_key,
    get_standard_codelist_cache_key,
)
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)


def get_library_metadata_from_cache(args) -> LibraryMetadataContainer:  # noqa
    if args.define_xml_path:
        define_xml_reader = DefineXMLReaderFactory.from_filename(args.define_xml_path)
        define_version = define_xml_reader.class_define_xml_version()
        if (
            define_version.model_package == "define_2_1"
            and len(args.controlled_terminology_package) > 0
        ):
            raise ValueError(
                "Cannot use -ct controlled terminology package command with Define-XML2.1 submission"
            )
        elif (
            define_version.model_package == "define_2_0"
            and len(args.controlled_terminology_package) > 1
        ):
            raise ValueError(
                "Cannot provide multiple controlled terminology packages with Define-XML2.0 submission"
            )
    standards_file = os.path.join(args.cache, "standards_details.pkl")
    models_file = os.path.join(args.cache, "standards_models.pkl")
    variables_codelist_file = os.path.join(args.cache, "variable_codelist_maps.pkl")
    variables_metadata_file = os.path.join(args.cache, "variables_metadata.pkl")
    standard_details_cache_key = get_standard_details_cache_key(
        args.standard, args.version.replace(".", "-")
    )
    with open(standards_file, "rb") as f:
        data = pickle.load(f)
        standard_metadata = data.get(standard_details_cache_key, {})

    if standard_metadata:
        model_cache_key = get_model_details_cache_key_from_ig(standard_metadata)
        with open(models_file, "rb") as f:
            data = pickle.load(f)
            model_details = data.get(model_cache_key, {})
    else:
        model_details = {}

    with open(variables_codelist_file, "rb") as f:
        data = pickle.load(f)
        cache_key = get_standard_codelist_cache_key(args.standard, args.version)
        variable_codelist_maps = data.get(cache_key)

    with open(variables_metadata_file, "rb") as f:
        data = pickle.load(f)
        cache_key = get_library_variables_metadata_cache_key(
            args.standard, args.version
        )
        variables_metadata = data.get(cache_key)

    ct_package_data = {}
    cache_files = next(os.walk(args.cache), (None, None, []))[2]
    ct_files = [file_name for file_name in cache_files if "ct-" in file_name]
    published_ct_packages = set()
    for file_name in ct_files:
        ct_version = file_name.split(".")[0]
        published_ct_packages.add(ct_version)
        if (
            args.controlled_terminology_package
            and ct_version in args.controlled_terminology_package
        ):
            with open(os.path.join(args.cache, file_name), "rb") as f:
                data = pickle.load(f)
                ct_package_data[ct_version] = data
    if args.define_xml_path and define_version.model_package == "define_2_1":
        (
            standards,
            merged_CT_packages,
            extensible,
            merged_flag,
        ) = define_xml_reader.get_ct_standards_metadata()
        for standard in standards:
            pickle_filename = (
                f"{standard.publishing_set.lower()}ct-{standard.version}.pkl"
            )
            if pickle_filename in ct_files:
                with open(os.path.join(args.cache, pickle_filename), "rb") as f:
                    data = pickle.load(f)
                    ct_package_data[pickle_filename.split(".")[0]] = data
        if merged_flag:
            ct_package_data["define_XML_merged_CT"] = merged_CT_packages
            ct_package_data["extensible"] = extensible
        else:
            extensible_terms = define_xml_reader.get_extensible_codelist_mappings()
            ct_package_data["extensible"] = extensible_terms
    if args.define_xml_path:
        extensible_terms = define_xml_reader.get_extensible_codelist_mappings()
        ct_package_data["extensible"] = extensible_terms
    return LibraryMetadataContainer(
        standard_metadata=standard_metadata,
        model_metadata=model_details,
        variable_codelist_map=variable_codelist_maps,
        variables_metadata=variables_metadata,
        ct_package_metadata=ct_package_data,
        published_ct_packages=published_ct_packages,
    )


def fill_cache_with_dictionaries(cache: CacheServiceInterface, args):
    """
    Extracts file contents from provided dictionaries files
    and saves to cache (inmemory or redis).
    """
    data_service = DataServiceFactory(config, cache).get_data_service()
    versions_map = {}

    for (
        dictionary_type,
        dictionary_path,
    ) in args.external_dictionaries.dictionary_path_mapping.items():
        if not dictionary_path:
            continue
        if dictionary_type == DictionaryTypes.SNOMED.value:
            versions_map[
                dictionary_type
            ] = f'MAIN/{dictionary_path.get("edition")}/{dictionary_path.get("version")}'
            continue
        terms = extract_dictionary_terms(data_service, dictionary_type, dictionary_path)
        cache.add(dictionary_path, terms)
        versions_map[dictionary_type] = terms.version

    return versions_map


def get_cache_service(manager):
    cache_service_type = config.getValue("CACHE_TYPE")
    if cache_service_type == "redis":
        return manager.RedisCacheService(
            config.getValue("REDIS_HOST_NAME"), config.getValue("REDIS_ACCESS_KEY")
        )
    else:
        return manager.InMemoryCacheService()


def get_rules(args) -> List[dict]:
    return (
        load_rules_from_local(args) if args.local_rules else load_rules_from_cache(args)
    )


def rule_cache_file(args) -> str:
    if args.local_rules_cache:
        return os.path.join(args.cache, "local_rules.pkl")
    else:
        return os.path.join(args.cache, "rules.pkl")


def load_rules_from_cache(args) -> List[dict]:
    core_ids = set()
    rules_file = rule_cache_file(args)
    rules = []
    rules_data = {}
    try:
        with open(rules_file, "rb") as f:
            rules_data = pickle.load(f)
    except FileNotFoundError:
        engine_logger.error(f"Rules file not found: {rules_file}")
        return []
    except Exception as e:
        engine_logger.error(f"Error loading rules file: {e}")
        return []

    if args.local_rules_id:
        local_prefix = f"local/{args.local_rules_id}/"
        rules = {k: v for k, v in rules_data.items() if k.startswith(local_prefix)}
        rules = list(rules.values())
    elif args.rules:
        keys = [
            get_rules_cache_key(args.standard, args.version.replace(".", "-"), rule)
            for rule in args.rules
        ]
        rules = [rules_data.get(key) for key in keys]
        missing_rules = [rule for rule, data in zip(args.rules, rules) if data is None]
        for missing_rule in missing_rules:
            engine_logger.error(
                f"The rule specified '{missing_rule}' is not"
                " in the standard {args.standard} and version {args.version}"
            )
        rules = [rule for rule in rules if rule is not None]
        if not rules:
            raise ValueError(
                "All specified rules were excluded because they are not in the standard and version specified."
            )
    else:
        engine_logger.info(
            f"No rules specified. Running all local rules for {args.standard}"
            + f" version {args.version}"
        )
        for key, rule in rules_data.items():
            core_id = rule.get("core_id")
            rule_identifier = get_rules_cache_key(
                args.standard, args.version.replace(".", "-"), core_id
            )
            if core_id not in core_ids and key == rule_identifier:
                rules.append(rule)
                core_ids.add(core_id)
    return rules


def load_rules_from_local(args) -> List[dict]:
    rules = []
    rule_files = [
        os.path.join(args.local_rules, file) for file in os.listdir(args.local_rules)
    ]
    rule_data = {}

    if args.rules:
        keys = set(
            get_rules_cache_key(args.standard, args.version.replace(".", "-"), rule)
            for rule in args.rules
        )
    else:
        engine_logger.info(
            "No rules specified with -r rules flag. "
            "Validating with all rules in local directory"
        )
        keys = None

    for rule_file in rule_files:
        rule = load_and_parse_rule(rule_file)
        if rule:
            process_rule(rule, args, rule_data, rules, keys)

    missing_keys = set()
    if keys:
        missing_keys = keys - rule_data.keys()
    if missing_keys:
        missing_keys_str = ", ".join(missing_keys)
        engine_logger.error(
            f"Specified rules not found in the local directory: {missing_keys_str}"
        )
    return rules


def load_and_parse_rule(rule_file):
    _, file_extension = os.path.splitext(rule_file)
    try:
        with open(rule_file, "r", encoding="utf-8") as file:
            if file_extension in [".yml", ".yaml"]:
                loaded_data = yaml.safe_load(file)
                return Rule.from_cdisc_metadata(replace_yml_spaces(loaded_data))
            elif file_extension == ".json":
                return Rule.from_cdisc_metadata(json.load(file))
            else:
                raise ValueError(f"Unsupported file type: {file_extension}")
    except Exception as e:
        engine_logger.error(f"error while loading {rule_file}: {e}")
        return None


def replace_rule_keys(rule):
    if "Operations" in rule:
        rule["actions"] = rule.pop("Operations")
    if "Check" in rule:
        rule["conditions"] = rule.pop("Check")
        return rule


def load_and_parse_local_rule(rule_file: str) -> dict:
    _, file_extension = os.path.splitext(rule_file)
    try:
        with open(rule_file, "r", encoding="utf-8") as file:
            if file_extension in [".yml", ".yaml"]:
                loaded_data = yaml.safe_load(file)
                loaded_data = replace_yml_spaces(loaded_data)
            elif file_extension == ".json":
                loaded_data = json.load(file)
            else:
                raise ValueError(f"Unsupported file type: {file_extension}")
            return replace_rule_keys(loaded_data)
    except Exception as e:
        print(f"Error while loading {rule_file}: {e}")
        return None


def process_rule(rule, args, rule_data, rules, keys):
    rule_identifier = get_rules_cache_key(
        args.standard, args.version.replace(".", "-"), rule.get("core_id")
    )
    if rule_identifier in rule_data:
        engine_logger.error(
            f"Duplicate rule {rule.get('core_id')} in local directory. Skipping..."
        )
        return
    if keys is None or rule_identifier in keys:
        rule_data[rule_identifier] = rule
        rules.append(rule)
    else:
        engine_logger.info(
            f"Rule {rule.get('core_id')} not specified with "
            "-r rule flag and in local directory.  Skipping..."
        )
    return


def get_datasets(
    data_service: DataServiceInterface, dataset_paths: Iterable[str]
) -> List[dict]:
    datasets = []
    if data_service.standard == "usdm":
        return data_service.get_datasets()
    for dataset_path in dataset_paths:
        metadata = data_service.get_raw_dataset_metadata(dataset_name=dataset_path)
        datasets.append(
            {
                "domain": metadata.domain_name,
                "filename": metadata.filename,
                "full_path": dataset_path,
                "length": metadata.records,
                "label": metadata.label,
                "size": metadata.size,
                "modification_date": metadata.modification_date,
                "temp_filename": None,
            }
        )

    return datasets


def get_max_dataset_size(dataset_paths: Iterable[str]):
    max_dataset_size = 0
    for file_path in dataset_paths:
        file_size = os.path.getsize(file_path)
        if file_size > max_dataset_size:
            max_dataset_size = file_size
    return max_dataset_size


def replace_yml_spaces(data):
    if isinstance(data, dict):
        return {
            key.replace(" ", "_"): replace_yml_spaces(value)
            for key, value in data.items()
        }
    elif isinstance(data, list):
        return [replace_yml_spaces(item) for item in data]
    else:
        return data
