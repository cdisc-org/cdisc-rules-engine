from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService


def test_get_dataset_metadata_sql(get_sample_lb_dataset, get_sample_supp_dataset):
    sql_data_service = PostgresQLDataService.from_list_of_testdatasets(
        [get_sample_lb_dataset, get_sample_supp_dataset], None
    )
    ds_metadata = sql_data_service.get_dataset_metadata(dataset_id="lb")
    assert 2 == len(ds_metadata.variables)
    assert ds_metadata.dataset_id is not None
    assert ds_metadata.domain is not None
    assert ds_metadata.rdomain is None
    assert not ds_metadata.is_supp
    ds_metadata = sql_data_service.get_dataset_metadata(dataset_id="suppdm")
    assert 3 == len(ds_metadata.variables)
    assert ds_metadata.dataset_id is not None
    assert ds_metadata.domain is not None
    assert "DM" == ds_metadata.rdomain
    assert ds_metadata.is_supp


def test_get_uploaded_dataset_ids(get_sample_lb_dataset, get_sample_supp_dataset):
    sql_data_service = PostgresQLDataService.from_list_of_testdatasets(
        [get_sample_lb_dataset, get_sample_supp_dataset], None
    )
    assert 2 == len(sql_data_service.get_uploaded_dataset_ids())
