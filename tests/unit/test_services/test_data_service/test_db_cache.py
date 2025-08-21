from cdisc_rules_engine.data_service.db_cache import DBCache
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService


def test_db_cache_initialization(get_sample_supp_dataset, get_sample_lb_dataset, get_sample_dm_dataset):
    ds = PostgresQLDataService.from_list_of_testdatasets(
        [get_sample_supp_dataset, get_sample_lb_dataset, get_sample_dm_dataset], None
    )
    assert "suppdm" == ds.cache.get_tables().get("suppdm")
    assert "lb" == ds.cache.get_tables().get("lb")
    assert "dm" == ds.cache.get_tables().get("dm")

    assert "suppdm" == ds.cache.get_db_table_cache("suppdm").get("db_table")
    assert 9 == len(ds.cache.get_db_table_cache("suppdm").get("columns"))
    assert "lb" == ds.cache.get_db_table_cache("lb").get("db_table")
    assert 2 == len(ds.cache.get_db_table_cache("lb").get("columns"))

    assert "suppdm" == ds.cache.get_db_table_hash("suppdm")
    assert "lb" == ds.cache.get_db_table_hash("lb")

    assert 9 == len(ds.cache.get_columns("suppdm"))
    assert 2 == len(ds.cache.get_columns("lb"))

    assert "domain" == ds.cache.get_db_column_hash("suppdm", "domain")
    assert "rdomain" == ds.cache.get_db_column_hash("suppdm", "rdomain")
    assert "lbseq" == ds.cache.get_db_column_hash("suppdm", "lbseq")

    assert "domain" == ds.cache.get_db_column_hash("lb", "domain")
    assert "lbseq" == ds.cache.get_db_column_hash("lb", "lbseq")


def test_empty_cache():
    cache = DBCache.empty_cache()
    assert {} == cache.get_tables()
    assert cache.get_tables().get("suppdm") is None
    assert cache.get_db_table_cache("suppdm") is None
    assert cache.get_db_table_hash("suppdm") is None
    assert {} == cache.get_columns("suppdm")
    assert cache.get_db_column_hash("suppdm", "domain") is None


def test_add_db_column_if_missing(get_sample_supp_dataset, get_sample_lb_dataset, get_sample_dm_dataset):
    ds = PostgresQLDataService.from_list_of_testdatasets(
        [get_sample_supp_dataset, get_sample_lb_dataset, get_sample_dm_dataset], None
    )
    # case exists
    assert (True, "lbseq", "lbseq") == ds.cache.add_db_column_if_missing(table_key="lb", column_key="lbseq")
    assert (True, "rdomain", "rdomain") == ds.cache.add_db_column_if_missing(table_key="suppdm", column_key="rdomain")
    # case not exists
    (exists, column_id, column_hash) = ds.cache.add_db_column_if_missing(table_key="suppdm", column_key="new_variable")
    assert not exists
    assert "new_variable" == column_id
    assert column_hash.startswith("v")


def test_add_db_table_if_missing(get_sample_lb_dataset):
    ds = PostgresQLDataService.from_list_of_testdatasets([get_sample_lb_dataset], None)
    # case exists
    assert (True, "lb", "lb") == ds.cache.add_db_table_if_missing(table_key="lb", columns={})
    # case not exists
    exists, table_key, table_hash = ds.cache.add_db_table_if_missing(
        table_key="new_table", columns={"old_var": "v_old_hash", "old_var_2": "v_old_hash_2"}
    )
    assert not exists
    assert table_key == "new_table"
    assert table_hash.startswith("v")
    assert 2 == len(ds.cache.get_columns("new_table"))
    assert "v_old_hash_2" == ds.cache.get_columns("new_table").get("old_var_2")

    # column exists new table
    assert (True, "old_var_2", "v_old_hash_2") == ds.cache.add_db_column_if_missing(
        table_key="new_table", column_key="old_var_2"
    )
    # column does not exist new table
    (exists, column_id, column_hash) = ds.cache.add_db_column_if_missing(
        table_key="new_table", column_key="new_variable"
    )
    assert not exists
    assert "new_variable" == column_id
    assert column_hash.startswith("v")
