from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from cdisc_rules_engine.exceptions.custom_exceptions import DomainNotFoundInDefineXMLError
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import DEFINE_VARIABLES_TYPE
from cdisc_rules_engine.sql_dataset_builders.sql_contents_define_dataset_builder import (
    SqlContentsDefineDatasetBuilder,
)
from cdisc_rules_engine.sql_dataset_builders.sql_variables_metadata_with_define_dataset_builder import (
    SqlVariablesMetadataWithDefineDatasetBuilder,
)

RESOURCES = Path(__file__).parents[3] / "resources"
# Define-XML 2.0: SUPP-- ItemGroupDefs have Domain="SUPPAE"; QS is split into QSCG, QSCS, QSMM; no HasNoData
DEFINE_2_0 = RESOURCES / "test_defineV20-SDTM.xml"
# Define-XML 2.1: SUPPDM has Domain="DM"
DEFINE_2_1 = RESOURCES / "test_defineV22-SDTM.xml"


def _builder(define, name, domain, rdomain="", split_part_filenames=None):
    data_service = MagicMock()
    data_service.define_xml_path = str(define)
    data_service.get_define_xml_contents.return_value = define.read_bytes()
    dataset_metadata = SimpleNamespace(
        name=name, domain=domain, rdomain=rdomain, split_part_filenames=split_part_filenames, variables=[]
    )
    return SqlVariablesMetadataWithDefineDatasetBuilder(
        rule={}, data_service=data_service, dataset_metadata=dataset_metadata, standards_context=MagicMock()
    )


def _var_names(define_vars):
    return [var["define_variable_name"] for var in define_vars]


@pytest.mark.parametrize("define", [DEFINE_2_0, DEFINE_2_1])
def test_define_vars_found_for_lowercase_dataset_name(define):
    assert "AETERM" in _var_names(_builder(define, "ae", "AE").get_define_vars())


@pytest.mark.parametrize(
    "define, name, domain, rdomain",
    [(DEFINE_2_0, "suppae", "SUPPAE", "AE"), (DEFINE_2_1, "suppdm", "SUPPDM", "DM")],
)
def test_define_vars_found_for_supp_dataset_whichever_domain_convention(define, name, domain, rdomain):
    assert "QNAM" in _var_names(_builder(define, name, domain, rdomain).get_define_vars())


def test_define_vars_found_for_split_dataset_from_its_parts():
    builder = _builder(DEFINE_2_0, "QS", "QS", split_part_filenames=["qscg.xpt", "qscs.xpt", "qsmm.xpt"])
    assert "QSTESTCD" in _var_names(builder.get_define_vars())


def test_define_vars_raise_when_dataset_not_in_define():
    with pytest.raises(DomainNotFoundInDefineXMLError):
        _builder(DEFINE_2_0, "xx", "XX").get_define_vars()


@pytest.mark.parametrize("define", [DEFINE_2_0, DEFINE_2_1])
def test_define_vars_have_no_empty_strings_in_boolean_columns(define):
    # '' cannot be inserted into a Bool column; Define-XML 2.0 has no HasNoData attribute
    bool_columns = [col for col, type in DEFINE_VARIABLES_TYPE.items() if type == "Bool"]
    for var in _builder(define, "ae", "AE").get_define_vars():
        assert all(var[col] != "" for col in bool_columns), var


@pytest.mark.parametrize(
    "define, name, domain, rdomain, split_part_filenames, expected_name",
    [
        (DEFINE_2_0, "ae", "AE", "", None, "AE"),
        (DEFINE_2_0, "suppae", "SUPPAE", "AE", None, "SUPPAE"),
        (DEFINE_2_0, "QS", "QS", "", ["qscg.xpt", "qscs.xpt", "qsmm.xpt"], "QSCG"),
        (DEFINE_2_1, "suppdm", "SUPPDM", "DM", None, "SUPPDM"),
    ],
)
def test_define_dataset_keeps_metadata_and_variable_order(
    define, name, domain, rdomain, split_part_filenames, expected_name
):
    metadata = _builder(define, name, domain, rdomain, split_part_filenames).get_define_dataset()
    assert metadata["define_dataset_name"] == expected_name
    assert metadata["define_dataset_key_sequence"]
    assert "USUBJID" in metadata["define_dataset_variable_order"].split(",")


def test_define_dataset_is_empty_when_dataset_not_in_define():
    assert _builder(DEFINE_2_0, "xx", "XX").get_define_dataset() == {}


def test_supp_join_on_blank_idvar_links_subject_level_records():
    join = SqlContentsDefineDatasetBuilder._supp_join(0, "")
    assert "COALESCE(ps_0.IDVAR, '') = ''" in join
    assert "a.::" not in join


def test_supp_join_on_idvar_links_records():
    join = SqlContentsDefineDatasetBuilder._supp_join(1, "AESEQ")
    assert "ps_1.IDVAR = 'AESEQ'" in join
    assert "a.AESEQ::text = ps_1.IDVARVAL::text" in join
