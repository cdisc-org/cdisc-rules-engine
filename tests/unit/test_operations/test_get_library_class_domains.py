from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.get_library_class_domains import (
    GetLibraryClassDomains,
)
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService


def test_get_library_class_domains(operation_params: OperationParams):
    """
    Test that get_library_class_domains returns the correct domains for a given class.
    """
    standard_metadata = {
        "classes": [
            {
                "name": "TRIAL DESIGN",
                "datasets": [
                    {"name": "TA"},
                    {"name": "TE"},
                    {"name": "TI"},
                    {"name": "TS"},
                    {"name": "TV"},
                ],
            },
            {
                "name": "EVENTS",
                "datasets": [
                    {"name": "AE"},
                    {"name": "CE"},
                    {"name": "DS"},
                ],
            },
            {
                "name": "FINDINGS",
                "datasets": [
                    {"name": "LB"},
                    {"name": "VS"},
                    {"name": "EG"},
                ],
            },
        ]
    }

    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"
    operation_params.domain_class = "TRIAL DESIGN"

    cache = InMemoryCacheService.get_instance()
    library_metadata = LibraryMetadataContainer(standard_metadata=standard_metadata)
    data_service = LocalDataService.get_instance(
        cache_service=cache, config=ConfigService()
    )

    operation = GetLibraryClassDomains(
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )

    domains = operation._execute_operation()

    expected_domains = {"TA", "TE", "TI", "TS", "TV"}
    assert domains == expected_domains, (
        f"Domain mismatch:\n" f"  Expected: {expected_domains}\n" f"  Got: {domains}"
    )


def test_get_library_class_domains_no_filter(operation_params: OperationParams):
    """
    Test that get_library_class_domains returns all domains when no class filter is provided.
    """
    standard_metadata = {
        "classes": [
            {
                "name": "TRIAL DESIGN",
                "datasets": [
                    {"name": "TA"},
                    {"name": "TE"},
                ],
            },
            {
                "name": "EVENTS",
                "datasets": [
                    {"name": "AE"},
                    {"name": "DS"},
                ],
            },
        ]
    }

    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"

    cache = InMemoryCacheService.get_instance()
    library_metadata = LibraryMetadataContainer(standard_metadata=standard_metadata)
    data_service = LocalDataService.get_instance(
        cache_service=cache, config=ConfigService()
    )

    operation = GetLibraryClassDomains(
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )

    domains = operation._execute_operation()

    expected_domains = {"TA", "TE", "AE", "DS"}
    assert domains == expected_domains, (
        f"Domain mismatch:\n" f"  Expected: {expected_domains}\n" f"  Got: {domains}"
    )


def test_get_library_class_domains_empty_class(operation_params: OperationParams):
    """
    Test that get_library_class_domains returns empty set for non-existent class.
    """
    standard_metadata = {
        "classes": [
            {
                "name": "TRIAL DESIGN",
                "datasets": [
                    {"name": "TA"},
                ],
            },
        ]
    }

    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"
    operation_params.domain_class = "Nonexistent Class"

    cache = InMemoryCacheService.get_instance()
    library_metadata = LibraryMetadataContainer(standard_metadata=standard_metadata)
    data_service = LocalDataService.get_instance(
        cache_service=cache, config=ConfigService()
    )

    operation = GetLibraryClassDomains(
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )

    domains = operation._execute_operation()

    assert domains == set(), f"Expected empty set, got: {domains}"
