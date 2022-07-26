from engine.models.dictionaries.meddra.meddra_terms_factory import (
    MedDRATermsFactory,
)
from engine.models.dictionaries.meddra.terms.term_types import TermTypes
from engine.services.data_services.local_data_service import LocalDataService
import os
from unittest.mock import MagicMock

dictionary_path = f"{os.path.dirname(__file__)}/../../../resources/dictionaries/meddra"


def test_install():
    storage_service = LocalDataService.get_instance(cache_service=MagicMock())
    meddra = MedDRATermsFactory(storage_service)
    dictionary = meddra.install_terms(dictionary_path)
    for term_type in TermTypes.values():
        assert len(dictionary[term_type].values()) == 5

    # Validate soc
    for i, term in enumerate(dictionary[TermTypes.SOC.value].values()):
        assert term.term == f"TESTSOC{i+1}"
        assert term.code == f"SOC{i+1}"
        assert term.abbreviation == f"TS{i+1}"
        assert term.code_hierarchy == f"SOC{i+1}"
        assert term.term_hierarchy == f"TESTSOC{i+1}"

    # Validate HLGT
    for i, term in enumerate(dictionary[TermTypes.HLGT.value].values()):
        assert term.term == f"TESTHLGT{i+1}"
        assert term.code == f"HLGT{i+1}"
        assert term.code_hierarchy == f"SOC{i+1}/HLGT{i+1}"
        assert term.term_hierarchy == f"TESTSOC{i+1}/TESTHLGT{i+1}"
        assert term.parent_code == f"SOC{i+1}"
        assert term.parent_term == f"TESTSOC{i+1}"

    # Validate HLT
    for i, term in enumerate(dictionary[TermTypes.HLT.value].values()):
        assert term.term == f"TESTHLT{i+1}"
        assert term.code == f"HLT{i+1}"
        assert term.code_hierarchy == f"SOC{i+1}/HLGT{i+1}/HLT{i+1}"
        assert term.term_hierarchy == f"TESTSOC{i+1}/TESTHLGT{i+1}/TESTHLT{i+1}"
        assert term.parent_code == f"HLGT{i+1}"
        assert term.parent_term == f"TESTHLGT{i+1}"

    # Validate PT
    for i, term in enumerate(dictionary[TermTypes.PT.value].values()):
        assert term.term == f"TESTPT{i+1}"
        assert term.code == f"PT{i+1}"
        assert term.code_hierarchy == f"SOC{i+1}/HLGT{i+1}/HLT{i+1}/PT{i+1}"
        assert (
            term.term_hierarchy
            == f"TESTSOC{i+1}/TESTHLGT{i+1}/TESTHLT{i+1}/TESTPT{i+1}"
        )
        assert term.parent_code == f"HLT{i+1}"
        assert term.parent_term == f"TESTHLT{i+1}"

    # Validate LLT
    for i, term in enumerate(dictionary[TermTypes.LLT.value].values()):
        assert term.term == f"TESTLLT{i+1}"
        assert term.code == f"LLT{i+1}"
        assert term.code_hierarchy == f"SOC{i+1}/HLGT{i+1}/HLT{i+1}/PT{i+1}/LLT{i+1}"
        assert (
            term.term_hierarchy
            == f"TESTSOC{i+1}/TESTHLGT{i+1}/TESTHLT{i+1}/TESTPT{i+1}/TESTLLT{i+1}"
        )
        assert term.parent_code == f"PT{i+1}"
        assert term.parent_term == f"TESTPT{i+1}"
