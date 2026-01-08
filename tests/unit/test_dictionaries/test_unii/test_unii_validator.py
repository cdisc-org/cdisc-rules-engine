from cdisc_rules_engine.models.dictionaries.base_external_dictionary import (
    ExternalDictionary,
)
import pytest

from cdisc_rules_engine.models.dictionaries.unii.term import UNIITerm
from cdisc_rules_engine.models.dictionaries.unii.validator import UNIIValidator


@pytest.mark.parametrize(
    "term, expected_outcome",
    [
        ("test_term", True),
        ("test_concept", True),
        ("test_terM", False),
    ],
)
def test_is_valid_term_case_sensitive(term: str, expected_outcome):
    terms_dictionary = {
        "T123": UNIITerm(unii="T123", display_name="test_term"),
        "C123": UNIITerm(unii="C123", display_name="test_concept"),
    }
    assert (
        UNIIValidator(terms=ExternalDictionary(terms_dictionary)).is_valid_term(
            term, "", "", case_sensitive=True
        )
        == expected_outcome
    )


@pytest.mark.parametrize(
    "term, expected_outcome",
    [
        ("test_term", True),
        ("test_concept", True),
        ("test_terM", True),
        ("blah", False),
    ],
)
def test_is_valid_term_case_insensitive(term: str, expected_outcome):
    terms_dictionary = {
        "T123": UNIITerm(unii="T123", display_name="test_term"),
        "C123": UNIITerm(unii="C123", display_name="test_concept"),
    }
    assert (
        UNIIValidator(terms=ExternalDictionary(terms_dictionary)).is_valid_term(
            term, "", "", case_sensitive=False
        )
        == expected_outcome
    )


@pytest.mark.parametrize(
    "term, expected_outcome",
    [
        ("T123", True),
        ("T124", False),
    ],
)
def test_is_valid_code(term: str, expected_outcome):
    terms_dictionary = {
        "T123": UNIITerm(unii="T123", display_name="test_term"),
        "C123": UNIITerm(unii="C123", display_name="test_concept"),
    }
    assert (
        UNIIValidator(terms=ExternalDictionary(terms_dictionary)).is_valid_code(
            term, "", ""
        )
        == expected_outcome
    )


@pytest.mark.parametrize(
    "row, expected_outcome",
    [
        ({"term": "test_term", "code": "T123"}, True),
        ({"term": "test_term", "code": "C123"}, False),
        ({"term": "test_concept", "code": "C123"}, True),
        ({"term": "test_term", "code": "C444"}, False),
    ],
)
def test_is_valid_code_term_pair(row: dict, expected_outcome: bool):
    terms_dictionary = {
        "T123": UNIITerm(unii="T123", display_name="test_term"),
        "C123": UNIITerm(unii="C123", display_name="test_concept"),
        "C444": UNIITerm(unii="C444", display_name="test_concept"),
    }
    assert (
        UNIIValidator(
            terms=ExternalDictionary(terms_dictionary)
        ).is_valid_code_term_pair(row, "term", "code")
        == expected_outcome
    )
