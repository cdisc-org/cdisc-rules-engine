from unittest.mock import MagicMock, patch
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.dummy_models.dummy_dataset import DummyDataset
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
from pathlib import Path
from cdisc_rules_engine.dataset_builders.define_variables_with_library_metadata import (
    DefineVariablesWithLibraryMetadataDatasetBuilder,
)
from cdisc_rules_engine.services.data_services import LocalDataService
import pandas as pd
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset

resources_path: Path = Path(__file__).parent.parent.parent.joinpath("resources")
test_define_file_path: Path = resources_path.joinpath("test_defineV22-SDTM.xml")


@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService"
    + ".get_define_xml_contents",
)
@patch(
    "cdisc_rules_engine.dataset_builders.define_variables_with_library_metadata"
    ".DefineVariablesWithLibraryMetadataDatasetBuilder.get_dataset_contents"
)
@patch(
    "cdisc_rules_engine.dataset_builders.define_variables_with_library_metadata"
    ".DefineVariablesWithLibraryMetadataDatasetBuilder.get_library_variables_metadata"
)
def test_define_variables_metadata_with_library_metadata_dataset_builder(
    mock_get_library_variables_metadata: MagicMock,
    mock_get_dataset_contents: MagicMock,
    mock_get_define_xml_contents: MagicMock,
):
    with open(test_define_file_path, "rb") as f:
        define_data = f.read()

    mock_get_define_xml_contents.return_value = define_data
    mock_dataset = MagicMock()
    mock_get_dataset_contents.return_value = mock_dataset

    library_vars_data = pd.DataFrame(
        {
            "library_variable_name": ["STUDYID", "USUBJID", "AESEQ", "AETERM"],
            "library_variable_role": ["Identifier", "Identifier", "Topic", "Topic"],
            "library_variable_label": [
                "Study Identifier",
                "Unique Subject Identifier",
                "Sequence Number",
                "Reported Term for the adverse event",
            ],
            "library_variable_core": ["Req", "Req", "Req", "Req"],
            "library_variable_order_number": ["1", "2", "8", "9"],
            "library_variable_data_type": ["Char", "Char", "Num", "Char"],
        }
    )
    mock_get_library_variables_metadata.return_value = PandasDataset(library_vars_data)

    cache = InMemoryCacheService()
    standard = "sdtmig"
    standard_version = "3-4"
    standard_substandard = None
    standard_data = {
        "_links": {"model": {"href": "/mdr/sdtm/1-5"}},
        "domains": {
            "HO",
            "CO",
            "SU",
            "PP",
            "TM",
            "TD",
            "SS",
            "TR",
            "CV",
            "EX",
            "RELSPEC",
            "FA",
            "SR",
            "SV",
            "TI",
            "CM",
            "RE",
            "TU",
            "ML",
            "RELSUB",
            "SUPPQUAL",
            "TA",
            "UR",
            "RS",
            "VS",
            "EC",
            "IS",
            "DV",
            "RELREC",
            "PR",
            "SM",
            "EG",
            "MK",
            "TS",
            "DS",
            "PE",
            "DM",
            "MH",
            "GF",
            "BE",
            "OE",
            "CE",
            "CP",
            "MS",
            "DD",
            "TV",
            "MI",
            "FT",
            "PC",
            "RP",
            "IE",
            "TE",
            "LB",
            "BS",
            "QS",
            "SC",
            "AG",
            "DA",
            "SE",
            "AE",
            "OI",
            "MB",
            "NV",
        },
        "classes": [
            {
                "name": "Events",
                "datasets": [
                    {
                        "name": "AE",
                        "label": "Adverse Events",
                        "datasetVariables": [
                            {
                                "name": "STUDYID",
                                "ordinal": "1",
                                "role": "Identifier",
                                "label": "Reported Term for the adverse event",
                                "simpleDatatype": "Char",
                                "core": "Req",
                            },
                            {
                                "name": "USUBJID",
                                "ordinal": "2",
                                "role": "Identifier",
                                "label": "Reported Term for the adverse event",
                                "simpleDatatype": "Char",
                                "core": "Req",
                            },
                            {
                                "name": "AETERM",
                                "ordinal": "9",
                                "role": "Topic",
                                "label": "Reported Term for the adverse event",
                                "simpleDatatype": "Char",
                                "core": "Req",
                            },
                            {
                                "name": "AESEQ",
                                "ordinal": "8",
                                "role": "Topic",
                                "label": "Sequence Number",
                                "simpleDatatype": "Num",
                                "core": "Req",
                            },
                        ],
                    }
                ],
            }
        ],
    }
    library_metadata = LibraryMetadataContainer(standard_metadata=standard_data)
    result = DefineVariablesWithLibraryMetadataDatasetBuilder(
        rule=None,
        data_service=LocalDataService(MagicMock(), MagicMock(), MagicMock()),
        cache_service=cache,
        rule_processor=None,
        data_processor=None,
        dataset_path=test_define_file_path,
        datasets=[],
        dataset_metadata=DummyDataset(
            {
                "filename": "ae.xpt",
                "label": "Adverse Events",
                "variables": [
                    {
                        "name": "DOMAIN",
                        "label": "Domain Abbreviation",
                        "type": "Char",
                        "length": 4,
                    },
                ],
                "records": {"DOMAIN": ["AE"]},
            }
        ),
        define_xml_path=test_define_file_path,
        standard=standard,
        standard_version=standard_version,
        standard_substandard=standard_substandard,
        library_metadata=library_metadata,
    ).build()

    assert result.columns.tolist() == [
        "define_variable_name",
        "define_variable_label",
        "define_variable_data_type",
        "define_variable_is_collected",
        "define_variable_role",
        "define_variable_size",
        "define_variable_ccode",
        "define_variable_format",
        "define_variable_allowed_terms",
        "define_variable_origin_type",
        "define_variable_has_no_data",
        "define_variable_order_number",
        "define_variable_length",
        "define_variable_has_codelist",
        "define_variable_codelist_coded_values",
        "define_variable_codelist_coded_codes",
        "define_variable_mandatory",
        "define_variable_has_comment",
        "library_variable_name",
        "library_variable_role",
        "library_variable_label",
        "library_variable_core",
        "library_variable_order_number",
        "library_variable_data_type",
    ]
    intersection = {"STUDYID", "USUBJID", "AESEQ", "AETERM"}

    for _, row in result.iterrows():
        if row["define_variable_name"] in intersection:
            assert row["define_variable_name"] == row["library_variable_name"]
            assert row["library_variable_core"] is not None
