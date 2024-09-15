from cdisc_rules_engine.exceptions.custom_exceptions import InvalidDictionaryVariable
from cdisc_rules_engine.models.dictionaries.base_external_dictionary import (
    ExternalDictionary,
)
from cdisc_rules_engine.models.dictionaries.meddra.meddra_validator import (
    MedDRAValidator,
)
from cdisc_rules_engine.models.dictionaries.meddra.terms.meddra_term import MedDRATerm
from cdisc_rules_engine.models.dictionaries.meddra.meddra_variables import (
    MedDRAVariables,
)
from cdisc_rules_engine.models.dictionaries.meddra.terms.term_types import TermTypes
import pytest


@pytest.mark.parametrize(
    "term, term_type, variable, expected_outcome",
    [
        ("ABCD", "PT", f"--{MedDRAVariables.DECOD.value}", True),
        ("abcd", "PT", f"--{MedDRAVariables.DECOD.value}", False),
        ("1234", "PT", f"--{MedDRAVariables.PTCD.value}", True),
        ("A32", "PT", f"--{MedDRAVariables.PTCD.value}", False),
        ("A32", "SOC", f"--{MedDRAVariables.SOCCD.value}", True),
        ("A32", "SOC", f"--{MedDRAVariables.BDSYSCD.value}", True),
        ("SOC_TERM", "SOC", f"--{MedDRAVariables.BODSYS.value}", False),
        ("SOC_TERM", "SOC", f"--{MedDRAVariables.SOC.value}", False),
        ("soc_term", "SOC", f"--{MedDRAVariables.BODSYS.value}", True),
        ("soc_term", "SOC", f"--{MedDRAVariables.SOC.value}", True),
        ("HLT1", "HLT", f"--{MedDRAVariables.HLTCD.value}", True),
        ("HLGT1", "HLT", f"--{MedDRAVariables.HLTCD.value}", False),
        ("abcd", "HLT", f"--{MedDRAVariables.HLT.value}", False),
        ("abcd", "LLT", f"--{MedDRAVariables.LLT.value}", False),
        ("HLT_TERM", "HLT", f"--{MedDRAVariables.HLT.value}", True),
        ("llT_TERM", "LLT", f"--{MedDRAVariables.LLT.value}", False),
        ("LLT_TERM", "SOC", f"--{MedDRAVariables.LLT.value}", False),
    ],
)
def test_is_valid_term_case_sensitive(
    term: str, term_type: str, variable: str, expected_outcome
):
    terms_dictionary = {
        TermTypes.SOC.value: {"A32": MedDRATerm({"term": "soc_term"})},
        TermTypes.PT.value: {"1234": MedDRATerm({"term": "ABCD"})},
        TermTypes.HLT.value: {"HLT1": MedDRATerm({"term": "HLT_TERM"})},
        TermTypes.HLGT.value: {"HLGT1": MedDRATerm({"term": "HLGT_TERM"})},
        TermTypes.LLT.value: {"LLT1": MedDRATerm({"term": "LLT_TERM"})},
    }

    assert (
        MedDRAValidator(terms=ExternalDictionary(terms_dictionary)).is_valid_term(
            term, term_type, variable, case_sensitive=True
        )
        == expected_outcome
    )


@pytest.mark.parametrize(
    "term, term_type, variable, expected_outcome",
    [
        ("ABCD", "PT", f"--{MedDRAVariables.DECOD.value}", True),
        ("abcd", "PT", f"--{MedDRAVariables.DECOD.value}", True),
        ("1234", "PT", f"--{MedDRAVariables.PTCD.value}", True),
        ("A32", "PT", f"--{MedDRAVariables.PTCD.value}", False),
        ("A32", "SOC", f"--{MedDRAVariables.SOCCD.value}", True),
        ("A32", "SOC", f"--{MedDRAVariables.BDSYSCD.value}", True),
        ("SOC_TERM", "SOC", f"--{MedDRAVariables.BODSYS.value}", True),
        ("SOC_TERM", "SOC", f"--{MedDRAVariables.SOC.value}", True),
        ("soc_term", "SOC", f"--{MedDRAVariables.BODSYS.value}", True),
        ("soc_term", "SOC", f"--{MedDRAVariables.SOC.value}", True),
        ("HLT1", "HLT", f"--{MedDRAVariables.HLTCD.value}", True),
        ("HLGT1", "HLT", f"--{MedDRAVariables.HLTCD.value}", False),
        ("abcd", "HLT", f"--{MedDRAVariables.HLT.value}", False),
        ("abcd", "LLT", f"--{MedDRAVariables.LLT.value}", False),
        ("HLT_TERM", "HLT", f"--{MedDRAVariables.HLT.value}", True),
        ("llT_TERM", "LLT", f"--{MedDRAVariables.LLT.value}", True),
        ("LLT_TERM", "SOC", f"--{MedDRAVariables.LLT.value}", False),
    ],
)
def test_is_valid_term_case_insensitive(
    term: str, term_type: str, variable: str, expected_outcome
):
    terms_dictionary = {
        TermTypes.SOC.value: {"A32": MedDRATerm({"term": "soc_term"})},
        TermTypes.PT.value: {"1234": MedDRATerm({"term": "ABCD"})},
        TermTypes.HLT.value: {"HLT1": MedDRATerm({"term": "HLT_TERM"})},
        TermTypes.HLGT.value: {"HLGT1": MedDRATerm({"term": "HLGT_TERM"})},
        TermTypes.LLT.value: {"LLT1": MedDRATerm({"term": "LLT_TERM"})},
    }

    assert (
        MedDRAValidator(terms=ExternalDictionary(terms_dictionary)).is_valid_term(
            term, term_type, variable
        )
        == expected_outcome
    )


@pytest.mark.parametrize(
    "term, term_type, variable, expected_outcome",
    [
        ("1234", "PT", f"--{MedDRAVariables.DECOD.value}", True),
        ("A32", "SOC", f"--{MedDRAVariables.SOCCD.value}", True),
        ("A32", "SOC", f"--{MedDRAVariables.SOC.value}", True),
        ("soc_term", "SOC", f"--{MedDRAVariables.SOC.value}", False),
    ],
)
def test_is_valid_code_case_sensitive(
    term: str, term_type: str, variable: str, expected_outcome
):
    terms_dictionary = {
        TermTypes.SOC.value: {"A32": MedDRATerm({"term": "soc_term"})},
        TermTypes.PT.value: {"1234": MedDRATerm({"term": "ABCD"})},
        TermTypes.HLT.value: {"HLT1": MedDRATerm({"term": "HLT_TERM"})},
        TermTypes.HLGT.value: {"HLGT1": MedDRATerm({"term": "HLGT_TERM"})},
        TermTypes.LLT.value: {"LLT1": MedDRATerm({"term": "LLT_TERM"})},
    }

    assert (
        MedDRAValidator(terms=ExternalDictionary(terms_dictionary)).is_valid_code(
            term, term_type, variable, case_sensitive=True
        )
        == expected_outcome
    )


def test_is_valid_term_throws_error_on_invalid_variable():
    terms_dictionary = {
        TermTypes.SOC.value: {"A32": MedDRATerm({"term": "soc_term"})},
        TermTypes.PT.value: {"1234": MedDRATerm({"term": "ABCD"})},
        TermTypes.HLT.value: {"HLT1": MedDRATerm({"term": "HLT_TERM"})},
        TermTypes.HLGT.value: {"HLGT1": MedDRATerm({"term": "HLGT_TERM"})},
        TermTypes.LLT.value: {"LLT1": MedDRATerm({"term": "LLT_TERM"})},
    }

    with pytest.raises(InvalidDictionaryVariable):
        MedDRAValidator(terms=ExternalDictionary(terms_dictionary)).is_valid_term(
            "AAA", "TEST", "--INVALID_VARIABLE"
        )
