from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from cdisc_rules_engine.compiler.sources.records import supp_join
from cdisc_rules_engine.exceptions.custom_exceptions import DomainNotFoundInDefineXMLError
from cdisc_rules_engine.metadata.store import DEFINE_VARIABLES_TYPE, MetadataStore
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema

RESOURCES = Path(__file__).parents[3] / "resources"
# Define-XML 2.0: SUPP-- ItemGroupDefs have Domain="SUPPAE"; QS is split into QSCG, QSCS, QSMM; no HasNoData
DEFINE_2_0 = RESOURCES / "test_defineV20-SDTM.xml"
# Define-XML 2.1: SUPPDM has Domain="DM"
DEFINE_2_1 = RESOURCES / "test_defineV22-SDTM.xml"


def _store(define, name, domain, rdomain="", split_part_filenames=None):
    data_service = MagicMock()
    data_service.define_xml_path = str(define)
    data_service.get_define_xml_contents.return_value = define.read_bytes()
    dataset_metadata = SimpleNamespace(
        name=name, domain=domain, rdomain=rdomain, split_part_filenames=split_part_filenames, variables=[]
    )
    return MetadataStore(data_service, MagicMock()), dataset_metadata


def _define_vars(*args, **kwargs):
    store, dataset = _store(*args, **kwargs)
    return store.define_variables(dataset)


def _define_dataset(*args, **kwargs):
    store, dataset = _store(*args, **kwargs)
    return store.define_dataset(dataset)


def _var_names(define_vars):
    return [var["define_variable_name"] for var in define_vars]


@pytest.mark.parametrize("define", [DEFINE_2_0, DEFINE_2_1])
def test_define_vars_found_for_lowercase_dataset_name(define):
    assert "AETERM" in _var_names(_define_vars(define, "ae", "AE"))


@pytest.mark.parametrize(
    "define, name, domain, rdomain",
    [(DEFINE_2_0, "suppae", "SUPPAE", "AE"), (DEFINE_2_1, "suppdm", "SUPPDM", "DM")],
)
def test_define_vars_found_for_supp_dataset_whichever_domain_convention(define, name, domain, rdomain):
    assert "QNAM" in _var_names(_define_vars(define, name, domain, rdomain))


def test_define_vars_found_for_split_dataset_from_its_parts():
    assert "QSTESTCD" in _var_names(
        _define_vars(DEFINE_2_0, "QS", "QS", split_part_filenames=["qscg.xpt", "qscs.xpt", "qsmm.xpt"])
    )


def test_define_vars_raise_when_dataset_not_in_define():
    with pytest.raises(DomainNotFoundInDefineXMLError):
        _define_vars(DEFINE_2_0, "xx", "XX")


@pytest.mark.parametrize("define", [DEFINE_2_0, DEFINE_2_1])
def test_define_vars_have_no_empty_strings_in_boolean_columns(define):
    # '' cannot be a Bool; Define-XML 2.0 has no HasNoData attribute
    bool_columns = [col for col, type in DEFINE_VARIABLES_TYPE.items() if type == "Bool"]
    for var in _define_vars(define, "ae", "AE"):
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
    metadata = _define_dataset(define, name, domain, rdomain, split_part_filenames)
    assert metadata["define_dataset_name"] == expected_name
    assert metadata["define_dataset_key_sequence"]
    assert "USUBJID" in metadata["define_dataset_variable_order"].split(",")


def test_define_dataset_is_empty_when_dataset_not_in_define():
    assert _define_dataset(DEFINE_2_0, "xx", "XX") == {}


def _dataset_schema(*columns):
    schema = SqlTableSchema("ae", "ae", "data")
    for column in columns:
        schema.add_column(SqlColumnSchema.from_data(column, "x"))
    return schema


def test_supp_join_on_blank_idvar_links_subject_level_records():
    join = supp_join(_dataset_schema("STUDYID", "DOMAIN", "USUBJID"), 0, "")
    assert "COALESCE(ps_0.IDVAR, '') = ''" in join
    assert "t.::" not in join


def test_supp_join_on_idvar_links_records():
    join = supp_join(_dataset_schema("STUDYID", "DOMAIN", "USUBJID", "AESEQ"), 1, "AESEQ")
    assert "ps_1.IDVAR = 'AESEQ'" in join
    assert "t.aeseq::text = ps_1.IDVARVAL::text" in join
