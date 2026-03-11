import pytest

from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.operations.related_domain_is_custom import (
    RelatedDomainIsCustom,
)


class DummyDataset:
    def __init__(self, name: str, is_supp: bool, rdomain: str):
        self.name = name
        self.is_supp = is_supp
        self.rdomain = rdomain


class DummyParams:
    def __init__(self, datasets, domain: str):
        self.datasets = datasets
        self.domain = domain


@pytest.mark.parametrize(
    "description, standard_domains, study_datasets, domain, expected",
    [
        (
            # Related SUPP domain is not custom when its referenced domain
            # exists in standard domains.
            "supp_related_domain_not_custom_when_rdomain_in_standard",
            {"AE"},
            [
                DummyDataset(name="SUPPAE", is_supp=True, rdomain="AE"),
            ],
            "SUPPAE",
            False,
        ),
        (
            # Related SUPP domain is custom when its referenced domain is not
            # present in standard domains.
            "supp_related_domain_custom_when_rdomain_not_in_standard",
            {"AE"},
            [
                DummyDataset(name="SUPPXX", is_supp=True, rdomain="XX"),
            ],
            "SUPPXX",
            True,
        ),
        (
            # If there is no matching supplementary dataset for the domain,
            # operation should treat it as non-custom (fallback to False).
            "no_matching_supp_dataset_returns_false",
            {"AE"},
            [
                DummyDataset(name="SUPPAE", is_supp=True, rdomain="AE"),
            ],
            "DM",
            False,
        ),
    ],
)
def test_related_domain_is_custom(
    description, standard_domains, study_datasets, domain, expected
):
    """Verify RelatedDomainIsCustom behaviour for several scenarios.

    Scenarios covered:
    - supplementary domain whose referenced domain is standard (not custom);
    - supplementary domain whose referenced domain is not standard (custom);
    - domain without matching supplementary dataset (falls back to False).
    """

    library_metadata = LibraryMetadataContainer(
        standard_metadata={"domains": standard_domains}
    )
    params = DummyParams(datasets=study_datasets, domain=domain)

    op = RelatedDomainIsCustom(
        params=params,
        library_metadata=library_metadata,
        original_dataset=None,
        cache_service=None,
        data_service=None,
    )

    assert op._execute_operation() is expected
