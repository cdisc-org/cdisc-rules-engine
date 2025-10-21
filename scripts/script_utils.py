import json
import yaml

from cdisc_rules_engine.enums.default_file_paths import DefaultFilePaths
from cdisc_rules_engine.interfaces import CacheServiceInterface
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from cdisc_rules_engine.interfaces.data_service_interface import DataServiceInterface
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
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
    if args.custom_standard:
        check = check_custom_standard(args)
        # custom standard not requiring library metadata
        if not check:
            return LibraryMetadataContainer(
                standard_metadata={},
                model_metadata={},
                variable_codelist_map={},
                variables_metadata={},
                ct_package_metadata={},
                published_ct_packages=[],
            )
    if args.define_xml_path:
        define_xml_reader = DefineXMLReaderFactory.from_filename(args.define_xml_path)
        define_version = define_xml_reader.class_define_xml_version()
        if (
            define_version.model_package == "define_2_1"
            and len(args.controlled_terminology_package) > 0
        ):
            engine_logger.error(
                "Cannot use -ct controlled terminology package command with Define-XML2.1 submission"
            )
            raise SystemError(2)
        elif (
            define_version.model_package == "define_2_0"
            and len(args.controlled_terminology_package) > 1
        ):
            engine_logger.error(
                "Cannot provide multiple controlled terminology packages with Define-XML2.0 submission"
            )
            raise SystemError(2)
    standards_file = os.path.join(args.cache, "standards_details.pkl")
    models_file = os.path.join(args.cache, "standards_models.pkl")
    variables_codelist_file = os.path.join(args.cache, "variable_codelist_maps.pkl")
    variables_metadata_file = os.path.join(args.cache, "variables_metadata.pkl")
    standard_details_cache_key = get_standard_details_cache_key(
        args.standard, args.version.replace(".", "-"), args.substandard
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
        cache_key = get_standard_codelist_cache_key(
            args.standard, args.version.replace(".", "-")
        )
        variable_codelist_maps = data.get(cache_key)

    with open(variables_metadata_file, "rb") as f:
        data = pickle.load(f)
        cache_key = get_library_variables_metadata_cache_key(
            args.standard, args.version.replace(".", "-"), args.substandard
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
        cache_path=args.cache,
    )


def check_custom_standard(args):
    standards_path = os.path.join(args.cache, DefaultFilePaths.RULES_DICTIONARY.value)
    try:
        with open(standards_path, "rb") as f:
            standards_dict = pickle.load(f)
    except FileNotFoundError:
        engine_logger.error(f"Rules file not found: check {standards_path}")
        return False
    except Exception as e:
        engine_logger.error(f"Error checking standards in library service: {e}")
        return False
    key = get_rules_cache_key(
        args.standard, args.version.replace(".", "-"), args.substandard
    )
    check = standards_dict.get(key, {})
    if check:
        return True
    return False


def fill_cache_with_dictionaries(
    cache: CacheServiceInterface, args, data_service: DataServiceInterface
) -> dict:
    """
    Extracts file contents from provided dictionaries files
    and saves to cache (inmemory or redis).
    """
    versions_map = {}

    for (
        dictionary_type,
        dictionary_path,
    ) in args.external_dictionaries.dictionary_path_mapping.items():
        if not dictionary_path:
            continue
        if dictionary_type == DictionaryTypes.SNOMED.value:
            if dictionary_path.get("edition") and dictionary_path.get("version"):
                versions_map[dictionary_type] = (
                    f'MAIN/{dictionary_path.get("edition")}/{dictionary_path.get("version")}'
                )
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
    if args.custom_standard:
        return (
            os.path.join(args.cache, DefaultFilePaths.CUSTOM_RULES_CACHE_FILE.value),
            os.path.join(args.cache, DefaultFilePaths.RULES_CACHE_FILE.value),
            os.path.join(args.cache, DefaultFilePaths.CUSTOM_RULES_DICTIONARY.value),
        )
    else:
        return (
            os.path.join(args.cache, DefaultFilePaths.RULES_CACHE_FILE.value),
            None,
            os.path.join(args.cache, DefaultFilePaths.RULES_DICTIONARY.value),
        )


def load_custom_rules(custom_data, cdisc_data, standard, version, rules, standard_dict):
    key = f"{standard}/{version}"
    standard_rules = standard_dict.get(key, {})
    rules_dict = {}
    ids = set()
    if rules:
        for rule in rules:
            if rule not in standard_rules:
                engine_logger.error(
                    f"The rule specified '{rule}' is not in the standard {standard} and version {version}"
                )
            else:
                ids.add(rule)
    else:
        for rule in standard_rules:
            ids.add(rule)
    for rule in ids:
        if rule.startswith("CORE-"):
            rules_dict[rule] = cdisc_data[rule]
        else:
            rules_dict[rule] = custom_data[rule]
    return list(rules_dict.values())


def load_specified_rules(
    rules_data,
    rule_ids,
    excluded_rule_ids,
    standard,
    version,
    standard_dict,
    substandard,
):
    key = get_rules_cache_key(standard, version, substandard)
    standard_rules = standard_dict.get(key, {})
    valid_rule_ids = set()

    # Determine valid rules based on inclusion and exclusion lists
    for rule in standard_rules:
        if (not rule_ids or rule in rule_ids) and (
            not excluded_rule_ids or rule not in excluded_rule_ids
        ):
            valid_rule_ids.add(rule)
    # Check that all specified rules are valid
    if rule_ids:
        for rule in rule_ids:
            if rule not in standard_rules:
                raise ValueError(
                    f"The rule specified to include '{rule}' is not in the standard {standard} and version {version}"
                )
    else:
        for rule in excluded_rule_ids:
            if rule not in standard_rules:
                raise ValueError(
                    f"The rule specified to exclude '{rule}' is not in the standard {standard} and version {version}"
                )
    rules = []
    for rule_id in valid_rule_ids:
        rule_data = rules_data.get(rule_id)
        rules.append(rule_data)
    # If no valid rules were found, raise an error
    if not rules:
        raise ValueError(
            f"All specified rules were excluded because they are not in the standard {standard} and version {version}"
        )
    return rules


def load_all_rules_for_standard(
    rules_data, standard, version, substandard, standard_dict
):
    rules = []
    log_message = (
        f"No rules specified. Running all rules for {standard} version {version}"
    )
    if substandard:
        log_message += f" with substandard {substandard}"
    engine_logger.info(log_message)

    key = get_rules_cache_key(standard, version, substandard)
    standard_rules = standard_dict.get(key, {})
    for rule in standard_rules:
        rules.append(rules_data[rule])
    return rules


def load_all_rules(rules_data):
    rules = []
    core_ids = set()
    engine_logger.info(
        "No rules, standard, or version specified. Running all local rules."
    )
    for rule in rules_data.values():
        core_id = rule.get("core_id")
        if core_id not in core_ids:
            rules.append(rule)
            core_ids.add(core_id)
    return rules


def load_rules_from_cache(args) -> List[dict]:
    rules_file, cdisc_file, standard_dict = rule_cache_file(args)
    rules_data = {}
    try:
        with open(rules_file, "rb") as f:
            rules_data = pickle.load(f)
        if cdisc_file:
            with open(cdisc_file, "rb") as f:
                cdisc_data = pickle.load(f)
        with open(standard_dict, "rb") as f:
            standard_dict = pickle.load(f)
    except FileNotFoundError:
        engine_logger.error(
            f"Rules file or dictionary not found: check {rules_file}, {cdisc_file}, {standard_dict}"
        )
        return []
    except Exception as e:
        engine_logger.error(f"Error loading rules file: {e}")
        return []
    if args.custom_standard:
        return load_custom_rules(
            rules_data,
            cdisc_data,
            args.standard,
            args.version.replace(".", "-"),
            args.rules,
            standard_dict,
        )
    elif args.rules or args.exclude_rules:
        return load_specified_rules(
            rules_data,
            args.rules,
            args.exclude_rules,
            args.standard,
            args.version.replace(".", "-"),
            standard_dict,
            args.substandard,
        )
    elif args.standard and args.version:
        return load_all_rules_for_standard(
            rules_data,
            args.standard,
            args.version.replace(".", "-"),
            args.substandard,
            standard_dict,
        )
    else:
        return load_all_rules(rules_data)


def load_rules_from_local(args) -> List[dict]:
    rules = []
    rule_files = []
    for path in args.local_rules:
        if os.path.isdir(path):
            rule_files.extend([os.path.join(path, file) for file in os.listdir(path)])
        else:
            rule_files.append(path)
    rule_data = {}

    if args.rules:
        keys = set(
            get_rules_cache_key(args.standard, args.version.replace(".", "-"), rule)
            for rule in args.rules
        )
        excluded_keys = None
    elif args.exclude_rules:
        excluded_keys = set(
            get_rules_cache_key(args.standard, args.version.replace(".", "-"), rule)
            for rule in args.exclude_rules
        )
        keys = None
    else:
        engine_logger.info(
            "No rules specified with -r or -er rules flags. "
            "Validating with rules in local directory"
        )
        excluded_keys = None
        keys = None

    for rule_file in rule_files:
        rule = load_and_parse_rule(rule_file)
        if rule:
            process_rule(rule, args, rule_data, rules, keys, excluded_keys)

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


def rule_matches_standard_version(rule, standard, version, substandard=None):
    normalized_version = version.replace("-", ".")
    for standard_info in rule["standards"]:
        std_name = standard_info.get("Name", "")
        std_version = standard_info.get("Version", "")
        std_substandard = standard_info.get("Substandard")

        if std_name.lower() == standard.lower() and std_version == normalized_version:
            if substandard:
                if std_substandard and std_substandard.lower() == substandard.lower():
                    return True
            else:
                return True
    return False


def process_rule(rule, args, rule_data, rules, keys, excluded_keys):
    """Process a rule and add it to the rules list if applicable."""
    core_id = rule.get("core_id")
    if not core_id:
        engine_logger.error("Rule missing core_id. Skipping...")
        return
    rule_identifier = get_rules_cache_key(
        args.standard, args.version.replace(".", "-"), core_id
    )
    if rule_identifier in rule_data:
        engine_logger.error(f"Duplicate rule {core_id} in local directory. Skipping...")
        return
    if (
        rule.get("status", "").lower() == "draft"
        and (keys is None or rule_identifier in keys)
        and (excluded_keys is None or rule_identifier not in excluded_keys)
    ):
        rule_data[rule_identifier] = rule
        rules.append(rule)
    elif rule.get("status", None).lower() == "published":
        if not rule_matches_standard_version(
            rule, args.standard, args.version, args.substandard
        ):
            substandard_msg = (
                f" with substandard '{args.substandard}'" if args.substandard else ""
            )
            engine_logger.info(
                f"Rule {core_id} does not apply to standard '{args.standard}' "
                f"version '{args.version}'{substandard_msg}. Skipping..."
            )
            return
        if (keys is None or rule_identifier in keys) and (
            excluded_keys is None or rule_identifier not in excluded_keys
        ):
            rule_data[rule_identifier] = rule
            rules.append(rule)
        else:
            engine_logger.info(
                f"Rule {core_id} not specified with "
                "-r rule flag or excluded with -er rule flag and in local directory. Skipping..."
            )


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


def set_max_errors_per_rule(args):
    env_value = (
        int(os.getenv("MAX_ERRORS_PER_RULE"))
        if os.getenv("MAX_ERRORS_PER_RULE")
        else None
    )
    cli_limit, cli_per_dataset = args.max_errors_per_rule
    if env_value is not None and cli_limit > 0:
        max_errors_per_rule = max(env_value, cli_limit)
    elif env_value is not None:
        max_errors_per_rule = env_value
    elif cli_limit > 0:
        max_errors_per_rule = cli_limit
    else:
        max_errors_per_rule = None

    if max_errors_per_rule is not None and max_errors_per_rule <= 0:
        max_errors_per_rule = None

    return max_errors_per_rule, cli_per_dataset
