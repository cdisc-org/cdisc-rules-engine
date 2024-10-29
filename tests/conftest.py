import os
from datetime import datetime
from unittest.mock import MagicMock

from cdisc_rules_engine.models.dataset import PandasDataset
import pytest
import sys
from cdisc_rules_engine.config.config import ConfigService

from cdisc_rules_engine.enums.rule_types import RuleTypes
from cdisc_rules_engine.enums.sensitivity import Sensitivity
from cdisc_rules_engine.models.dictionaries.whodrug import WhoDrugTermsFactory
from cdisc_rules_engine.models.dictionaries.meddra import MedDRATermsFactory
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.models.rule_conditions import ConditionCompositeFactory
from cdisc_rules_engine.services.cache import (
    InMemoryCacheService,
)
from cdisc_rules_engine.models.external_dictionaries_container import (
    ExternalDictionariesContainer,
    DictionaryTypes,
)
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.constants.rule_constants import ALL_KEYWORD

meddra_path: str = f"{os.path.dirname(__file__)}/resources/dictionaries/meddra"
whodrug_path: str = f"{os.path.dirname(__file__)}/resources/dictionaries/whodrug"


def pytest_collection_modifyitems(config, items):
    run_regression_tests = config.getoption("-m") == "regression"
    if not run_regression_tests:
        items[:] = [item for item in items if "regression" not in item.keywords]


# Added the following fixture to access the
# 'run_regression_tests' flag in test functions
@pytest.fixture(scope="session")
def run_regression_tests(request):
    return request.config.getoption("-m") == "regression"


def mock_get_dataset(dataset_name):
    dataframe_map = {
        "ae.xpt": PandasDataset.from_dict(
            {"AESTDY": [1, 2, 40, 59], "USUBJID": [1, 2, 3, 45]}
        ),
        "ec.xpt": PandasDataset.from_dict(
            {"ECCOOLVAR": [3, 4, 5000, 35], "USUBJID": [1, 2, 3, 45]}
        ),
    }
    return dataframe_map.get(dataset_name.split("/")[-1])


def get_matches_regex_pattern_rule(pattern: str) -> dict:
    return {
        "core_id": "MockRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "domains": {"Include": ["AE"]},
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "matches_regex",
                        "value": {
                            "target": "AESTDY",
                            "comparator": pattern,
                        },
                    }
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {"message": f"Records have the following pattern: {pattern}"},
            }
        ],
    }


@pytest.fixture
def mock_data_service():
    yield MagicMock()


@pytest.fixture
def dataset_rule_greater_than() -> dict:
    return {
        "core_id": "DatasetRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "domains": {"Include": ["EC"]},
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "greater_than",
                        "value": {"target": "ECCOOLVAR", "comparator": 30},
                    }
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Value for ECCOOLVAR greater than 30.",
                },
            }
        ],
    }


@pytest.fixture
def dataset_rule_multiple_conditions() -> dict:
    return {
        "core_id": "DatasetRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "domains": {"Include": ["EC"]},
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "any": [
                    {
                        "name": "get_dataset",
                        "operator": "has_equal_length",
                        "value": {"target": "ECCOOLVAR", "comparator": 5},
                    },
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {"target": "ECCOOLVAR", "comparator": "cool"},
                    },
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": (
                        "Length of ECCOOLVAR is not equal to 5 " "or ECCOOLVAR == cool."
                    ),
                },
            }
        ],
    }


@pytest.fixture
def dataset_rule_has_equal_length() -> dict:
    return {
        "core_id": "DatasetRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "domains": {"Include": ["EC"]},
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "has_equal_length",
                        "value": {"target": "ECCOOLVAR", "comparator": 5},
                    }
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Length of ECCOOLVAR is equal to 5.",
                },
            }
        ],
    }


@pytest.fixture
def dataset_rule_has_not_equal_length() -> dict:
    return {
        "core_id": "DatasetRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "domains": {"Include": ["EC"]},
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "has_not_equal_length",
                        "value": {"target": "ECCOOLVAR", "comparator": 5},
                    }
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Length of ECCOOLVAR is not equal to 5.",
                },
            }
        ],
    }


@pytest.fixture
def mock_record_rule_equal_to_string_prefix():
    return {
        "core_id": "MockRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "domains": {"Include": ["AE"]},
        "output_variables": ["AESTDY"],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "prefix_matches_regex",
                        "value": {
                            "target": "AESTDY",
                            "prefix": 4,
                            "comparator": "test",
                        },
                    }
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Prefix of AESTDY is equal to test.",
                },
            }
        ],
    }


@pytest.fixture
def mock_ae_record_rule_equal_to_suffix() -> dict:
    return {
        "core_id": "MockRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "domains": {"Include": ["AE"]},
        "output_variables": ["AESTDY"],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "suffix_matches_regex",
                        "value": {
                            "target": "AESTDY",
                            "suffix": 4,
                            "comparator": "test",
                        },
                    }
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Suffix of AESTDY is equal to test.",
                },
            }
        ],
    }


@pytest.fixture
def rule_equal_to_with_extract_metadata_operation() -> dict:
    return {
        "core_id": "MockRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "domains": {"Include": ["SUPPEC"]},
        "operations": [
            {
                "operator": "extract_metadata",
                "domain": "SUPPEC",
                "name": "dataset_name",
                "id": "$dataset_name",
            }
        ],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "suffix_equal_to",
                        "value": {
                            "target": "$dataset_name",
                            "comparator": "RDOMAIN",
                            "suffix": 2,
                        },
                    }
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Value for RDOMAIN is not equal to $dataset_name.",
                },
            }
        ],
        "output_variables": [
            "RDOMAIN",
        ],
    }


@pytest.fixture
def mock_rule_distinct_operation():
    return {
        "core_id": "MockRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "domains": {"Include": ["AE"]},
        "operations": [
            {
                "operator": "distinct",
                "domain": "DM",
                "name": "USUBJID",
                "id": "$unique_usubjid",
            }
        ],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "is_not_contained_by",
                        "value": {"comparator": "$unique_usubjid", "target": "AESTDY"},
                    }
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Value for AESTDY not in DM.USUBJID",
                },
            }
        ],
        "output_variables": [
            "AESTDY",
        ],
    }


@pytest.fixture
def rule_distinct_operation_is_not_contained_by() -> dict:
    return {
        "core_id": "CDISC.SDTMIG.CG0178",
        "severity": "warning",
        "standards": [{"Name": "SDTMIG", "Version": "3.4"}],
        "classes": {"Include": [ALL_KEYWORD]},
        "domains": {"Include": ["IE"]},
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "is_not_contained_by",
                        "value": {"target": "IETEST", "comparator": "$ti_ietest"},
                    }
                ]
            }
        ),
        "operations": [
            {
                "operator": "distinct",
                "domain": "TI",
                "name": "IETEST",
                "id": "$ti_ietest",
            }
        ],
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {"message": "IETEST is not in TI.IETEST"},
            }
        ],
    }


@pytest.fixture
def dataset_rule_equal_to() -> dict:
    """
    A sample rule that can be used to check values in several datasets.
    """
    return {
        "core_id": "MockRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "classes": {"Include": [ALL_KEYWORD]},
        "domains": {"Include": ["EC"]},
        "datasets": [
            {"domain_name": "AE", "match_key": ["STUDYID", "USUBJID"]},
        ],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {
                            "target": "ECSTDY",
                            "comparator": "AESTDY",
                        },
                    }
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Value of ECSTDY is equal to AESTDY.",
                },
            }
        ],
        "output_variables": [
            "ECSTDY",
        ],
    }


@pytest.fixture
def dataset_rule_equal_to_compare_same_value() -> dict:
    """
    A sample rule that can be used to check values in several datasets.
    """
    return {
        "core_id": "MockRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "classes": {"Include": [ALL_KEYWORD]},
        "domains": {"Include": ["EC"]},
        "datasets": [
            {"domain_name": "AE", "match_key": ["STUDYID", "USUBJID"]},
        ],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {
                            "target": "VISIT",
                            "comparator": "AE.VISIT",
                        },
                    }
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Value of VISIT is equal to AE.VISIT.",
                },
            }
        ],
        "output_variables": [
            "ECSTDY",
        ],
    }


@pytest.fixture
def dataset_rule_equal_to_error_objects() -> dict:
    """
    A sample rule that can be used to check values in several datasets.
    """
    return {
        "core_id": "MockRule",
        "severity": "Warning",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "output_variables": ["AESTDY"],
        "domains": {
            "Include": [
                "AE",
                "EC",
            ]
        },
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {"target": "AESTDY", "comparator": "test"},
                    }
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Value of AESTDY is equal to test.",
                },
            }
        ],
    }


@pytest.fixture
def dataset_rule_not_equal_to_error_objects() -> dict:
    """
    A sample rule with "not" in conditions.
    """
    return {
        "core_id": "MockRule",
        "severity": "Warning",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "output_variables": ["AESTDY"],
        "domains": {
            "Include": [
                "AE",
                "EC",
            ]
        },
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "not": {
                    "all": [
                        {
                            "name": "get_dataset",
                            "operator": "equal_to",
                            "value": {"target": "AESTDY", "comparator": "test"},
                        }
                    ]
                }
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Value of AESTDY is not equal to test.",
                },
            }
        ],
    }


@pytest.fixture
def dataset_rule_one_to_one_related() -> dict:
    """
    A sample rule that can be used to check
    one-to-one relationship across 2 datasets.
    """
    return {
        "core_id": "MockRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "domains": {"Include": ["EC"]},
        "datasets": [
            {
                "domain_name": "AE",
                "match_key": [
                    "STUDYID",
                    "VISITNUM",
                ],
            },
        ],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "is_not_unique_relationship",
                        "value": {
                            "target": "VISITNUM",
                            "comparator": "VISIT",
                        },
                    }
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "VISITNUM is not one-to-one related to VISIT",
                },
            }
        ],
    }


@pytest.fixture
def dataset_metadata_not_equal_to_rule() -> dict:
    """
    A sample rule that can be used to validate dataset metadata.
    """
    return {
        "core_id": "MockRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "domains": {"Include": ["EC"]},
        "output_variables": ["dataset_label", "dataset_name", "dataset_size"],
        "rule_type": RuleTypes.DATASET_METADATA_CHECK.value,
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "not_equal_to",
                        "value": {
                            "target": "dataset_name",
                            "comparator": "AE",
                        },
                    },
                    {
                        "name": "get_dataset",
                        "operator": "not_equal_to",
                        "value": {
                            "target": "dataset_size",
                            "comparator": 5,
                            "unit": "MB",
                        },
                    },
                    {
                        "name": "get_dataset",
                        "operator": "not_equal_to",
                        "value": {
                            "target": "dataset_label",
                            "comparator": "Adverse Events",
                        },
                    },
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Dataset metadata is wrong.",
                },
            }
        ],
    }


@pytest.fixture
def variables_metadata_rule() -> dict:
    """
    A sample rule that can be used to validate variables metadata.
    """
    return {
        "core_id": "MockRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "domains": {"Include": ["EC"]},
        "output_variables": ["variable_name", "variable_label", "variable_data_type"],
        "rule_type": RuleTypes.VARIABLE_METADATA_CHECK.value,
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "longer_than",
                        "value": {
                            "target": "variable_name",
                            "comparator": 8,
                        },
                    },
                    {
                        "name": "get_dataset",
                        "operator": "longer_than",
                        "value": {
                            "target": "variable_label",
                            "comparator": 40,
                        },
                    },
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {
                            "target": "variable_data_type",
                            "comparator": "Char",
                        },
                    },
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Variable metadata is wrong.",
                },
            }
        ],
    }


@pytest.fixture
def domain_presence_rule() -> dict:
    """
    Rule that validates domain presence against datasets provided.
    """
    return {
        "core_id": "TEST1",
        "standards": [],
        "domains": {"Include": [ALL_KEYWORD]},
        "rule_type": RuleTypes.DOMAIN_PRESENCE_CHECK.value,
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "exists",
                        "value": {
                            "target": "AE",
                        },
                    }
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Domain AE exists",
                },
            }
        ],
    }


@pytest.fixture
def define_xml_validation_rule() -> dict:
    """
    Rule that validates Define XML against dataset metadata.
    """
    return {
        "core_id": "TEST1",
        "severity": "Error",
        "standards": [],
        "domains": {"Include": [ALL_KEYWORD]},
        "output_variables": ["dataset_label", "dataset_name", "dataset_location"],
        "rule_type": RuleTypes.DATASET_METADATA_CHECK_AGAINST_DEFINE.value,
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "any": [
                    {
                        "name": "get_dataset",
                        "operator": "not_equal_to",
                        "value": {
                            "target": "dataset_name",
                            "comparator": "define_dataset_name",
                        },
                    },
                    {
                        "name": "get_dataset",
                        "operator": "not_equal_to",
                        "value": {
                            "target": "dataset_label",
                            "comparator": "define_dataset_label",
                        },
                    },
                    {
                        "name": "get_dataset",
                        "operator": "not_equal_to",
                        "value": {
                            "target": "dataset_location",
                            "comparator": "define_dataset_location",
                        },
                    },
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Dataset metadata does not correspond to Define XML",
                },
            }
        ],
    }


@pytest.fixture
def define_xml_variable_validation_rule() -> dict:
    """
    Rule that validates Define XML variable metadata against dataset metadata.
    """
    return {
        "core_id": "TEST1",
        "severity": "Error",
        "standards": [],
        "domains": {"Include": [ALL_KEYWORD]},
        "output_variables": ["variable_size"],
        "rule_type": RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_DEFINE.value,
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "any": [
                    {
                        "name": "get_dataset",
                        "operator": "not_equal_to",
                        "value": {
                            "target": "variable_size",
                        },
                    }
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": (
                        "Variable metadata variable_size "
                        "does not match define variable size"
                    ),
                },
            }
        ],
    }


@pytest.fixture
def define_xml_value_level_metadata_validation_rule() -> dict:
    """
    Rule that validates Define XML variable metadata against dataset metadata.
    """
    return {
        "core_id": "TEST1",
        "severity": "Error",
        "standards": [],
        "domains": {"Include": [ALL_KEYWORD]},
        "rule_type": RuleTypes.VALUE_LEVEL_METADATA_CHECK_AGAINST_DEFINE.value,
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "any": [
                    {
                        "name": "get_dataset",
                        "operator": "non_conformant_value_length",
                        "value": {},
                    }
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": (
                        "Variable data does not match length "
                        "specified by value level metadata in define.xml"
                    ),
                },
            }
        ],
    }


@pytest.fixture
def dataset_rule_record_in_parent_domain_equal_to() -> dict:
    """
    A sample rule that can be used to check values in several datasets.
    """
    return {
        "core_id": "MockRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "classes": {"Include": [ALL_KEYWORD]},
        "domains": {"Include": ["EC"]},
        "datasets": [
            {
                "domain_name": "SUPPEC",
                "match_key": ["USUBJID"],
                "relationship_columns": {
                    "column_with_names": "IDVAR",
                    "column_with_values": "IDVARVAL",
                },
            }
        ],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {"target": "QNAM", "comparator": "ECREASOC"},
                    },
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {"target": "ECPRESP", "comparator": "Y"},
                    },
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {"message": "Dataset contents is wrong."},
            }
        ],
        "output_variables": [
            "QNAM",
            "ECPRESP",
        ],
    }


@pytest.fixture
def define_xml_allowed_terms_check_rule() -> dict:
    return {
        "core_id": "MockRule",
        "rule_type": "Define-XML",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "classes": {"Include": [ALL_KEYWORD]},
        "domains": {"Include": [ALL_KEYWORD]},
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "does_not_use_valid_codelist_terms",
                        "value": {
                            "target": "define_variable_ccode",
                            "comparator": "define_variable_allowed_terms",
                        },
                    }
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {"message": "Define specifies invalid codelist terms"},
            }
        ],
        "output_variables": [
            "define_variable_ccode",
            "define_variable_name",
            "define_variable_allowed_terms",
        ],
    }


@pytest.fixture
def dataset_rule_inconsistent_enumerated_columns() -> dict:
    """
    A sample rule that checks for inconsistencies in enumerated columns.
    """
    return {
        "core_id": "CDISC.SDTMIG.CG0262",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "classes": {"Include": [ALL_KEYWORD]},
        "domains": {"Include": ["TS"]},
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "inconsistent_enumerated_columns",
                        "value": {
                            "target": "TSVAL",
                        },
                    },
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Inconsistencies found in enumerated TSVAL columns."
                },
            }
        ],
    }


@pytest.fixture(scope="function")
def rule_check_dataset_against_library_and_define() -> dict:
    return {
        "core_id": "MockRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "classes": {"Include": [ALL_KEYWORD]},
        "domains": {"Include": ["AE"]},
        "rule_type": "Dataset Contents Check against Define XML and Library Metadata",
        "variable_origin_type": "Collected",
        "variable_core_status": "Perm",
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {"any": [{"name": "get_dataset", "operator": "empty"}]}
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {"message": "Variable metadata is wrong."},
            }
        ],
    }


@pytest.fixture(scope="function")
def rule_check_dataset_contents_against_library_metadata() -> dict:
    return {
        "core_id": "MockRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "classes": {"Include": [ALL_KEYWORD]},
        "domains": {"Include": [ALL_KEYWORD]},
        "rule_type": "Dataset Contents Check against Library Metadata",
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "any": [
                    {"name": "get_dataset", "value": {"target": "STUDYID"}},  # Req
                    {"name": "get_dataset", "value": {"target": "DOMAIN"}},  # Req
                    {"name": "get_dataset", "value": {"target": "--ORRES"}},  # Exp
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {"message": "Variable metadata is wrong."},
            }
        ],
        "output_variables": [
            "STUDYID",
            "DOMAIN",
            "--ORRES",
        ],
    }


@pytest.fixture(scope="function")
def dataset_rule_get_variable_names_in_given_standard() -> dict:
    """
    A rule that can be used to get variable names of the given standard.
    """
    return {
        "core_id": "MockRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "classes": {"Include": [ALL_KEYWORD]},
        "domains": {"Include": [ALL_KEYWORD]},
        "operations": [
            {
                "operator": "get_variable_names_in_given_standard",
                "domain": "QNAM",
                "name": "QNAMVAR",
                "id": "$get_variable_names_in_given_standard",
            }
        ],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "variable_names_in_given_standard",
                        "value": {
                            "comparator": "$get_variable_names_in_given_standard",
                            "target": "QNAM",
                        },
                    },
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {"message": "Variable names are not present"},
            }
        ],
    }


@pytest.fixture(scope="function")
def rule_dataset_references_invalid_whodrug_terms() -> dict:
    """
    This rule validates that dataset codes reference valid whodrug terms.
    """
    return {
        "core_id": "MockRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "classes": {"Include": [ALL_KEYWORD]},
        "domains": {"Include": [ALL_KEYWORD]},
        "operations": [
            {
                "operator": "valid_whodrug_references",
                "domain": "AE",
                "name": "AEINA",
                "id": "$valid_whodrug_reference",
            }
        ],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {
                            "target": "$valid_whodrug_reference",
                            "comparator": False,
                        },
                    },
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {"message": "Dataset references invalid codes"},
            }
        ],
        "output_variables": [
            "AEINA",
        ],
    }


@pytest.fixture(scope="function")
def rule_validate_columns_order_against_library_metadata() -> dict:
    """
    Rule that can be used to validate columns order against library metadata.
    """
    return {
        "core_id": "MockRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "classes": {"Include": ["EVENTS"]},
        "domains": {"Include": ["AE"]},
        "sensitivity": Sensitivity.DATASET.value,
        "operations": [
            {
                "operator": "get_column_order_from_dataset",
                "domain": "AE",
                "id": "$column_order_from_dataset",
            },
            {
                "operator": "get_column_order_from_library",
                "domain": "AE",
                "id": "$column_order_from_library",
            },
        ],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "any": [
                    {
                        "name": "get_dataset",
                        "operator": "not_equal_to",
                        "value": {
                            "target": "$column_order_from_dataset",
                            "comparator": "$column_order_from_library",
                        },
                    },
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {"message": "Order of variables is invalid"},
            }
        ],
        "output_variables": [
            "$column_order_from_dataset",
            "$column_order_from_library",
        ],
    }


@pytest.fixture(scope="function")
def installed_whodrug_dictionaries(request) -> dict:
    """
    Installs whodrug dictionaries and saves to cache.
    Deletes them afterwards.
    """
    # install dictionaries and save to cache
    cache_service = InMemoryCacheService.get_instance()
    local_data_service = LocalDataService.get_instance(
        config=ConfigService(),
        cache_service=cache_service,
        standard="sdtmig",
        standard_version="3-4",
    )
    factory = WhoDrugTermsFactory(local_data_service)

    terms: dict = factory.install_terms(whodrug_path)
    cache_service.add(whodrug_path, terms)

    def delete_terms_from_cache():
        cache_service.clear(whodrug_path)

    request.addfinalizer(delete_terms_from_cache)

    return {
        "whodrug_path": whodrug_path,
        "cache_service": cache_service,
        "data_service": local_data_service,
    }


@pytest.fixture(scope="function")
def installed_meddra_dictionaries(request) -> dict:
    """
    Installs meddra dictionaries and saves to cache.
    Deletes them afterwards.
    """
    cache_service = InMemoryCacheService.get_instance()
    # install dictionaries and save to cache
    local_data_service = LocalDataService.get_instance(cache_service=cache_service)
    factory = MedDRATermsFactory(local_data_service)

    terms: dict = factory.install_terms(meddra_path)
    cache_service.add(meddra_path, terms)

    def delete_terms_from_cache():
        cache_service.clear(meddra_path)

    request.addfinalizer(delete_terms_from_cache)
    return {
        "meddra_path": meddra_path,
        "cache_service": cache_service,
    }


@pytest.fixture(scope="function")
def operation_params() -> OperationParams:
    return OperationParams(
        operation_id="operation_id",
        operation_name="operation_name",
        dataframe=PandasDataset.from_dict({}),
        target="target",
        domain="domain",
        dataset_path="dataset_path",
        directory_path="directory_path",
        datasets=[{}],
        standard="standard",
        standard_version="standard_version",
        external_dictionaries=ExternalDictionariesContainer(
            {
                DictionaryTypes.MEDDRA.value: meddra_path,
                DictionaryTypes.WHODRUG.value: whodrug_path,
            }
        ),
        grouping=[],
        attribute_name="attribute_name",
    )


@pytest.fixture(scope="function")
def dataset_metadata() -> dict:
    return {
        "file_metadata": {
            "path": "CDISC01/test/ae.xpt",
            "name": "ae.xpt",
            "size": 38000,
        },
        "contents_metadata": {
            "dataset_label": "Adverse Events",
            "dataset_name": "AE",
            "domain_name": "AE",
            "dataset_modification_date": datetime.now().isoformat(),
            "dataset_length": 20,
        },
    }


def get_python_executable():
    return sys.executable
