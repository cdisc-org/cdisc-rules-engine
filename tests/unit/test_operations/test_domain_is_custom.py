from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
import pytest
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.domain_is_custom import DomainIsCustom
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService


@pytest.mark.parametrize(
    "dataframe, domain, dataframe_metadata, standard, standard_version, expected",
    [
        (
            PandasDataset.from_dict(
                {"STUDYID": ["TEST_STUDY"] * 3, "AETERM": ["test"] * 3}
            ),
            "AE",
            SDTMDatasetMetadata(name="AE", first_record={"DOMAIN": "AE"}),
            "sdtmig",
            "3-4",
            False,
        ),
        (
            DaskDataset.from_dict(
                {"STUDYID": ["TEST_STUDY"] * 3, "AETERM": ["test"] * 3}
            ),
            "AE",
            SDTMDatasetMetadata(name="AE", first_record={"DOMAIN": "AE"}),
            "sdtmig",
            "3-4",
            False,
        ),
        (
            PandasDataset.from_dict(
                {"STUDYID": ["TEST_STUDY"] * 3, "BCTERM": ["test"] * 3}
            ),
            "BC",
            SDTMDatasetMetadata(name="BC", first_record={"DOMAIN": "BC"}),
            "sdtmig",
            "3-4",
            True,
        ),
        (
            PandasDataset.from_dict(
                {"STUDYID": ["TEST_STUDY"] * 3, "APID": ["AP001"] * 3}
            ),
            "APMH",
            SDTMDatasetMetadata(
                name="APMH", first_record={"DOMAIN": "APMH", "APID": "AP001"}
            ),
            "sdtmig-ap",
            "1-0",
            False,
        ),
        (
            PandasDataset.from_dict(
                {"STUDYID": ["TEST_STUDY"] * 3, "RDOMAIN": ["AE"] * 3}
            ),
            "SUPPAE",
            SDTMDatasetMetadata(name="SUPPAE", first_record={"RDOMAIN": "AE"}),
            "sdtmig",
            "3-4",
            False,
        ),
    ],
)
def test_domain_is_custom(
    operation_params: OperationParams,
    dataframe: DatasetInterface,
    domain: str,
    dataframe_metadata: SDTMDatasetMetadata,
    standard: str,
    standard_version: str,
    expected: bool,
):
    standard_metadata = {
        "dataset_names": {"AE", "MH", "SUPPQUAL"},
    }
    operation_params.dataframe = dataframe
    operation_params.domain = domain
    operation_params.dataframe_metadata = dataframe_metadata
    operation_params.standard = standard
    operation_params.standard_version = standard_version
    cache = InMemoryCacheService.get_instance()
    library_metadata = LibraryMetadataContainer(standard_metadata=standard_metadata)
    data_service = LocalDataService.get_instance(
        cache_service=cache, config=ConfigService()
    )
    operation = DomainIsCustom(
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )
    result = operation.execute()
    assert result[operation_params.operation_id].equals(
        dataframe.convert_to_series([expected, expected, expected])
    )
