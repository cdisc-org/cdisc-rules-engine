from unittest.mock import MagicMock

from cdisc_rules_engine.models.dictionaries.whodrug import (
    AtcClassification,
    AtcText,
    DrugDictionary,
    WhodrugFileNames,
    WhodrugRecordTypes,
    WhoDrugTermsFactory,
)
from cdisc_rules_engine.services.data_services import LocalDataService


def test_install_terms(tmp_path):
    """
    Unit test for WhoDrugTermsFactory.install_terms method.
    """
    # create temporary files
    ina_file = tmp_path / WhodrugFileNames.INA_FILE_NAME.value
    ina_file.write_text(
        "A      1ALIMENTARY TRACT AND METABOLISM\n"
        "A01    2STOMATOLOGICAL PREPARATIONS\n"
        "A01A   3STOMATOLOGICAL PREPARATIONS"
    )

    dda_file = tmp_path / WhodrugFileNames.DDA_FILE_NAME.value
    dda_file.write_text("000001010016C02AB  111*\n" "000001030014C02AB  074\n")

    dd_file = tmp_path / WhodrugFileNames.DD_FILE_NAME.value
    dd_file.write_text(
        "000001010016N  001      01 854METHYLDOPA\n"
        "000001010024T21MEX      01 041ALDOMET [METHYLDOPA]\n"
        "000001010032T21336      01 044PRESINOL    \n"
        "000001010040T19UGA      01 044DOPAMET             "
    )

    # run the factory
    local_data_service = LocalDataService.get_instance(cache_service=MagicMock())
    factory = WhoDrugTermsFactory(local_data_service)
    terms: dict = factory.install_terms(str(tmp_path))

    # check returned data
    assert len(terms[WhodrugRecordTypes.ATC_TEXT.value]) == 3
    assert all(
        isinstance(term, AtcText)
        for term in terms[WhodrugRecordTypes.ATC_TEXT.value].values()
    )

    assert len(terms[WhodrugRecordTypes.ATC_CLASSIFICATION.value]) == 2
    assert all(
        isinstance(term, AtcClassification)
        for term in terms[WhodrugRecordTypes.ATC_CLASSIFICATION.value].values()
    )

    assert len(terms[WhodrugRecordTypes.DRUG_DICT.value]) == 4
    assert all(
        isinstance(term, DrugDictionary)
        for term in terms[WhodrugRecordTypes.DRUG_DICT.value].values()
    )
