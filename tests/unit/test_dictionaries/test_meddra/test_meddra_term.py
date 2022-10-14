import os
from unittest.mock import MagicMock

from cdisc_rules_engine.models.dictionaries.meddra.meddra_terms_factory import (
    MedDRATermsFactory,
)
from cdisc_rules_engine.models.dictionaries.meddra.terms.meddra_term import MedDRATerm
from cdisc_rules_engine.models.dictionaries.meddra.terms.term_types import TermTypes
from cdisc_rules_engine.services.data_services.local_data_service import (
    LocalDataService,
)

dictionary_path = f"{os.path.dirname(__file__)}/../../../resources/dictionaries/meddra"


def test_get_code_hierarchies():
    data_service = LocalDataService.get_instance(cache_service=MagicMock())
    factory = MedDRATermsFactory(data_service)
    terms = factory.install_terms(dictionary_path)
    expected_code_hierarchies = set(
        [
            f"SOC{i + 1}/HLGT{i + 1}/HLT{i + 1}/PT{i + 1}/LLT{i + 1}"
            for i in range(len(terms[TermTypes.LLT.value]))
        ]
    )
    assert MedDRATerm.get_code_hierarchies(terms) == expected_code_hierarchies


def test_get_term_hierarchies():
    data_service = LocalDataService.get_instance(cache_service=MagicMock())
    factory = MedDRATermsFactory(data_service)
    terms = factory.install_terms(dictionary_path)
    expected_term_hierarchies = set(
        [
            f"TESTSOC{i+1}/TESTHLGT{i+1}/TESTHLT{i+1}/TESTPT{i+1}/TESTLLT{i+1}"
            for i in range(len(terms[TermTypes.LLT.value]))
        ]
    )
    assert MedDRATerm.get_term_hierarchies(terms) == expected_term_hierarchies


def test_get_code_term_pairs():
    data_service = LocalDataService.get_instance(cache_service=MagicMock())
    factory = MedDRATermsFactory(data_service)
    terms = factory.install_terms(dictionary_path)
    code_term_pairs = MedDRATerm.get_code_term_pairs(terms)
    for t in TermTypes.values():
        assert t in code_term_pairs
        assert len(code_term_pairs[t]) == len(terms[t])
