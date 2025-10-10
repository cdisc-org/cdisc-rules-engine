from abc import ABC, abstractmethod
from typing import List
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
    SQLDatasetMetadata,
)


class SqlBaseDatasetBuilder(ABC):
    """
    Base class for SQL dataset builders.
    """

    def __init__(
        self,
        rule: dict,
        data_service: PostgresQLDataService,
        dataset_metadata: SQLDatasetMetadata,
        datasets: List[SQLDatasetMetadata] = None,
        **kwargs,
    ):
        self.rule = rule
        self.data_service = data_service
        self.dataset_metadata = dataset_metadata
        self.datasets = datasets or []
        # Store any additional kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)

    @abstractmethod
    def build(self) -> str:
        """
        Build and return the table/view name for this rule type.

        For mini tables: just return the pre-existing table name.
        Regular builders return DatasetInterface, we return table name string.
        """
        pass

    def get_dataset_id(self) -> str:
        """
        Main entrypoint - equivalent to get_dataset() in regular builders.
        Returns the table/view name to validate against.
        """
        return self.build()
