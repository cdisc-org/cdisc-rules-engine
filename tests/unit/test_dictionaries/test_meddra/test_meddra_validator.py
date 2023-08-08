from cdisc_rules_engine.exceptions.custom_exceptions import InvalidDictionaryVariable
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
    "term, variable, expected_outcome",
    [
        ("ABCD", f"--{MedDRAVariables.DECOD.value}", True),
        ("abcd", f"--{MedDRAVariables.DECOD.value}", False),
        ("1234", f"--{MedDRAVariables.PTCD.value}", True),
        ("A32", f"--{MedDRAVariables.PTCD.value}", False),
        ("A32", f"--{MedDRAVariables.SOCCD.value}", True),
        ("A32", f"--{MedDRAVariables.BDSYSCD.value}", True),
        ("SOC_TERM", f"--{MedDRAVariables.BODSYS.value}", False),
        ("SOC_TERM", f"--{MedDRAVariables.SOC.value}", False),
        ("soc_term", f"--{MedDRAVariables.BODSYS.value}", True),
        ("soc_term", f"--{MedDRAVariables.SOC.value}", True),
        ("HLT1", f"--{MedDRAVariables.HLTCD.value}", True),
        ("HLGT1", f"--{MedDRAVariables.HLTCD.value}", False),
        ("abcd", f"--{MedDRAVariables.HLT.value}", False),
        ("abcd", f"--{MedDRAVariables.LLT.value}", False),
        ("HLT_TERM", f"--{MedDRAVariables.HLT.value}", True),
        ("llT_TERM", f"--{MedDRAVariables.LLT.value}", False),
    ],
)
def test_is_valid_term_case_sensitive(term: str, variable: str, expected_outcome):
    terms_dictionary = {
        TermTypes.SOC.value: {"A32": MedDRATerm({"term": "soc_term"})},
        TermTypes.PT.value: {"1234": MedDRATerm({"term": "ABCD"})},
        TermTypes.HLT.value: {"HLT1": MedDRATerm({"term": "HLT_TERM"})},
        TermTypes.HLGT.value: {"HLGT1": MedDRATerm({"term": "HLGT_TERM"})},
        TermTypes.LLT.value: {"LLT1": MedDRATerm({"term": "LLT_TERM"})},
    }

    assert (
        MedDRAValidator(terms=terms_dictionary).is_valid_term(
            term, variable, case_sensitive=True
        )
        == expected_outcome
    )


@pytest.mark.parametrize(
    "term, variable, expected_outcome",
    [
        ("ABCD", f"--{MedDRAVariables.DECOD.value}", True),
        ("abcd", f"--{MedDRAVariables.DECOD.value}", True),
        ("1234", f"--{MedDRAVariables.PTCD.value}", True),
        ("A32", f"--{MedDRAVariables.PTCD.value}", False),
        ("A32", f"--{MedDRAVariables.SOCCD.value}", True),
        ("A32", f"--{MedDRAVariables.BDSYSCD.value}", True),
        ("SOC_TERM", f"--{MedDRAVariables.BODSYS.value}", True),
        ("SOC_TERM", f"--{MedDRAVariables.SOC.value}", True),
        ("soc_term", f"--{MedDRAVariables.BODSYS.value}", True),
        ("soc_term", f"--{MedDRAVariables.SOC.value}", True),
        ("HLT1", f"--{MedDRAVariables.HLTCD.value}", True),
        ("HLGT1", f"--{MedDRAVariables.HLTCD.value}", False),
        ("abcd", f"--{MedDRAVariables.HLT.value}", False),
        ("abcd", f"--{MedDRAVariables.LLT.value}", False),
        ("HLT_TERM", f"--{MedDRAVariables.HLT.value}", True),
        ("llT_TERM", f"--{MedDRAVariables.LLT.value}", True),
    ],
)
def test_is_valid_term_case_insensitive(term: str, variable: str, expected_outcome):
    terms_dictionary = {
        TermTypes.SOC.value: {"A32": MedDRATerm({"term": "soc_term"})},
        TermTypes.PT.value: {"1234": MedDRATerm({"term": "ABCD"})},
        TermTypes.HLT.value: {"HLT1": MedDRATerm({"term": "HLT_TERM"})},
        TermTypes.HLGT.value: {"HLGT1": MedDRATerm({"term": "HLGT_TERM"})},
        TermTypes.LLT.value: {"LLT1": MedDRATerm({"term": "LLT_TERM"})},
    }

    assert (
        MedDRAValidator(terms=terms_dictionary).is_valid_term(term, variable)
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
        MedDRAValidator(terms=terms_dictionary).is_valid_term(
            "AAA", "--INVALID_VARIABLE"
        )
