import os
from unittest.mock import MagicMock

from cdisc_rules_engine.models.dictionaries.meddra.meddra_terms_factory import (
    MedDRATermsFactory,
)
from cdisc_rules_engine.serializers import MedDRATermSerializer
from cdisc_rules_engine.services.data_services.local_data_service import (
    LocalDataService,
)

dictionary_path = f"{os.path.dirname(__file__)}/../../../resources/dictionaries/meddra"


def test_serializer_data():
    data_service = LocalDataService.get_instance(cache_service=MagicMock())
    factory = MedDRATermsFactory(data_service)
    terms = factory.install_terms(dictionary_path)
    soc = MedDRATermSerializer(list(terms["soc"].values())[0]).data
    assert soc == {
        "code": "SOC1",
        "type": "soc",
        "term": "TESTSOC1",
        "abbreviation": "TS1",
        "codeHierarchy": "SOC1",
        "termHierarchy": "TESTSOC1",
    }
    hlgt = MedDRATermSerializer(list(terms["hlgt"].values())[0]).data
    assert hlgt == {
        "code": "HLGT1",
        "type": "hlgt",
        "term": "TESTHLGT1",
        "codeHierarchy": "SOC1/HLGT1",
        "termHierarchy": "TESTSOC1/TESTHLGT1",
        "parentCode": "SOC1",
        "parentTerm": "TESTSOC1",
    }
