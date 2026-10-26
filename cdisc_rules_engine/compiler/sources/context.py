from cdisc_rules_engine.metadata.store import MetadataStore


class SourceContext:
    """Everything a rule type needs to build its rows for one rule on one dataset."""

    def __init__(self, rule: dict, data_service, dataset_metadata, standards_context, datasets: list):
        self.rule = rule
        self.data_service = data_service
        self.pgi = data_service.pgi
        self.dataset_metadata = dataset_metadata
        self.standards_context = standards_context
        self.datasets = datasets or []
        self.store = MetadataStore.of(data_service, standards_context)

    def rule_dataset(self) -> str:
        """Table id of the rule's dataset, with whatever the rule's `datasets` merge in already joined."""
        return self.data_service.get_dataset_for_rule(self.dataset_metadata, self.rule, self.standards_context)

    def all_datasets(self) -> list:
        # TODO: in practice this is the same list as self.datasets.
        # The engine already passes every uploaded dataset in.
        # Kept separate because a few OLD builders were written against each.
        # Worth collapsing once nothing constructs a context by hand.
        return [
            self.data_service.get_dataset_metadata(dataset_id)
            for dataset_id in self.data_service.get_uploaded_dataset_ids()
        ]
