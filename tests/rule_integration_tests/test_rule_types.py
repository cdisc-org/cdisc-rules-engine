from unittest.mock import patch

from scripts.run_sql_validation import sql_run_single_rule_validation


def test_test(get_core_rule):
    rule = get_core_rule("CORE-000254")
    assert rule is not None

    # data = validate_single_rule(datasets, rule)


@patch("cdisc_rules_engine.services.data_services.DummyDataService.get_dataset_class")
def test_rule_existing_rule(mock_get_dataset_class, get_sample_lb_rule, get_sample_lb_dataset):
    mock_get_dataset_class.return_value = None
    data = sql_run_single_rule_validation(
        [get_sample_lb_dataset], get_sample_lb_rule, None, None, "SDTMIG", "3.4", None, []
    )
    assert "LB" in data
    assert len(data["LB"]) == 1
    assert data["LB"][0]["message"] == "LBSEQ greater than 0"
    assert len(data["LB"][0]["errors"]) == 2
