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


class RuleTester:
    def __init__(
        self,
        datasets,
        define_xml: str = None,
        cache: InMemoryCacheService = None,
        standard: str = None,
        standard_version: str = None,
    ):
        self.datasets = [DummyDataset(dataset_data) for dataset_data in datasets]
        self.cache = cache or InMemoryCacheService()
        self.data_service = DummyDataService.get_instance(
            self.cache, ConfigService(), data=self.datasets, define_xml=define_xml
        )
        self.engine = RulesEngine(
            self.cache,
            self.data_service,
            standard=standard,
            standard_version=standard_version,
        )
        self.engine.rule_processor = RuleProcessor(self.data_service, self.cache)
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
                rule, f"/{dataset['filename']}", dataset_dictionaries, dataset["domain"]
            )
            validated_domains.add(dataset["domain"])
        return results
