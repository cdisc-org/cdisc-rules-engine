from cdisc_rules_engine.models.dictionaries.base_external_dictionary import (
    ExternalDictionary,
)
import pytest

from cdisc_rules_engine.models.dictionaries.snomed.term import SNOMEDTerm
from cdisc_rules_engine.models.dictionaries.snomed.validator import SNOMEDValidator


@pytest.mark.parametrize(
    "term, expected_outcome",
    [
        ("T123", True),
        ("T124", False),
    ],
)
def test_is_valid_code(term: str, expected_outcome):
    terms_dictionary = {
        "T123": SNOMEDTerm(
            concept_id="T123", preferred_term="test_term", full_name="test_term (full)"
        ),
        "C123": SNOMEDTerm(
            concept_id="C123",
            preferred_term="test_concept",
            full_name="test_concept (full)",
        ),
    }
    assert (
        SNOMEDValidator(
            snomed_path={"edition": "test", "version": "test"},
            terms=ExternalDictionary(terms_dictionary),
        ).is_valid_code(term, "", "")
        == expected_outcome
    )


@pytest.mark.parametrize(
    "row, expected_outcome",
    [
        ({"term": "test_term", "code": "T123"}, True),
        ({"term": "test_term (full)", "code": "T123"}, True),
        ({"term": "test_term", "code": "C123"}, False),
        ({"term": "test_concept", "code": "C123"}, True),
        ({"term": "test_term", "code": "C444"}, False),
    ],
)
def test_is_valid_code_term_pair(row: dict, expected_outcome: bool):
    terms_dictionary = {
        "T123": SNOMEDTerm(
            concept_id="T123", preferred_term="test_term", full_name="test_term (full)"
        ),
        "C123": SNOMEDTerm(
            concept_id="C123",
            preferred_term="test_concept",
            full_name="test_concept (full)",
        ),
        "C444": SNOMEDTerm(
            concept_id="C444",
            preferred_term="test_concept",
            full_name="test_concept (full)",
        ),
    }
    assert (
        SNOMEDValidator(
            snomed_path={"edition": "test", "version": "test"},
            terms=ExternalDictionary(terms_dictionary),
        ).is_valid_code_term_pair(row, "term", "code")
        == expected_outcome
    )
