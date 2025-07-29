from cdisc_rules_engine.data_service.PostgresQLDataService import PostgresQLDataService


def test_get_dataset_metadata_sql(get_sample_lb_dataset, get_sample_supp_dataset):
    sql_data_service = PostgresQLDataService.from_list_of_testdatasets([get_sample_lb_dataset])
    ds_metadata = sql_data_service.get_dataset_metadata("LB")
    assert 2 == len(ds_metadata.variables)

    sql_data_service = PostgresQLDataService.from_list_of_testdatasets([get_sample_supp_dataset])
    ds_metadata = sql_data_service.get_dataset_metadata("SUPPDM")
    assert 3 == len(ds_metadata.variables)


def test_get_rdomain(get_sample_lb_dataset, get_sample_supp_dataset):
    sql_data_service = PostgresQLDataService.from_list_of_testdatasets([get_sample_lb_dataset])
    assert sql_data_service.get_rdomain("LB") is None

    sql_data_service = PostgresQLDataService.from_list_of_testdatasets([get_sample_supp_dataset])
    assert "DM" == sql_data_service.get_rdomain("SUPPDM")
