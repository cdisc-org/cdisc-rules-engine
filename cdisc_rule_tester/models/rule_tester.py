from cdisc_rules_engine.rules_engine import RulesEngine
from cdisc_rules_engine.utilities.rule_processor import RuleProcessor
from cdisc_rules_engine.utilities.data_processor import DataProcessor
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
from cdisc_rules_engine.services.data_services.dummy_data_service import (
    DummyDataService,
)
from cdisc_rules_engine.dummy_models.dummy_dataset import DummyDataset
from cdisc_rules_engine.models.rule import Rule
from cdisc_rules_engine.models.rule_conditions import ConditionCompositeFactory
from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.utilities.utils import (
    get_standard_details_cache_key,
    get_model_details_cache_key_from_ig,
    get_library_variables_metadata_cache_key,
    get_variable_codelist_map_cache_key,
)


class RuleTester:
    def __init__(
        self,
        datasets,
        define_xml: str = None,
        cache: InMemoryCacheService = None,
        standard: str = None,
        standard_version: str = "",
        standard_substandard: str = None,
        codelists=[],
        rule=None,
    ):
        self.datasets = [DummyDataset(dataset_data) for dataset_data in datasets]
        self.cache = cache or InMemoryCacheService()
        standard_details_cache_key = get_standard_details_cache_key(
            standard, standard_version, standard_substandard
        )
        variable_details_cache_key = get_library_variables_metadata_cache_key(
            standard, standard_version, standard_substandard
        )
        standard_metadata = self.cache.get(standard_details_cache_key)
        if standard_metadata:
            model_cache_key = get_model_details_cache_key_from_ig(standard_metadata)
            model_metadata = self.cache.get(model_cache_key)
        else:
            model_metadata = {}
        variable_codelist_cache_key = get_variable_codelist_map_cache_key(
            standard, standard_version, standard_substandard
        )

        ct_package_metadata = {}
        for codelist in codelists:
            ct_package_metadata[codelist] = self.cache.get(codelist)

        self.library_metadata = LibraryMetadataContainer(
            standard_metadata=standard_metadata,
            model_metadata=model_metadata,
            variables_metadata=self.cache.get(variable_details_cache_key),
            variable_codelist_map=self.cache.get(variable_codelist_cache_key),
            ct_package_metadata=ct_package_metadata,
        )
        if not standard and not standard_version and rule:
            standard = (
                rule.get("Authorities")[0].get("Standards")[0].get("Name").lower()
            )
            standard_substandard = (
                rule.get("Authorities")[0].get("Standards")[0].get("Substandard", None)
            )
            if standard_substandard is not None:
                standard_substandard = standard_substandard.lower()
            standard_version = (
                rule.get("Authorities")[0].get("Standards")[0].get("Version")
            )
        self.data_service = DummyDataService.get_instance(
            self.cache,
            ConfigService(),
            standard=standard,
            standard_version=standard_version,
            standard_substandard=standard_substandard,
            data=self.datasets,
            define_xml=define_xml,
            library_metadata=self.library_metadata,
        )
        self.engine = RulesEngine(
            self.cache,
            self.data_service,
            standard=standard,
            standard_version=standard_version,
            standard_substandard=standard_substandard,
            library_metadata=self.library_metadata,
        )
        self.engine.rule_processor = RuleProcessor(
            self.data_service, self.cache, self.library_metadata
        )
        self.engine.data_processor = DataProcessor(self.data_service, self.cache)

    def validate(self, rule) -> dict:
        results = {}
        validated_domains = set()
        rule = Rule.from_cdisc_metadata(rule)
        dataset_dictionaries = [
            {"domain": domain.domain, "filename": domain.filename}
            for domain in self.datasets
        ]
        for dataset in dataset_dictionaries:
            if dataset["domain"] in validated_domains:
                continue  # handling split datasets
            rule["conditions"] = ConditionCompositeFactory.get_condition_composite(
                rule["conditions"]
            )
            results[dataset["domain"]] = self.engine.validate_single_rule(
                rule,
                f"/{dataset['filename']}",
                dataset_dictionaries,
                dataset["domain"],
            )
            validated_domains.add(dataset["domain"])
        return results
