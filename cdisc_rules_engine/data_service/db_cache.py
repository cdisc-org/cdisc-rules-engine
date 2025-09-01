from typing import Tuple, TypedDict, Union

from cdisc_rules_engine.data_service.util import generate_hash


class DBTableCache(TypedDict):
    db_table: str
    # key = constructed column_id, value = column name in DB (hash value)
    columns: dict[str, str]


class DBCache:

    def __init__(self, cache: list[str]):
        self.cache = cache

    @classmethod
    def from_metadata_dict(cls, data_metadata: list[dict]) -> "DBCache":
        cache = {}
        if len(data_metadata) > 0:
            for row in data_metadata:
                table = row.get("dataset_id").lower()
                col = row.get("var_name")
                if table not in cache.keys():
                    cache[table] = DBTableCache(db_table=table, columns={col: col})
                else:
                    cache.get(table).get("columns")[col] = col
        return cls(cache)

    @classmethod
    def empty_cache(cls) -> "DBCache":
        return cls({})

    def get_tables(self) -> dict:
        return {k: v["db_table"] for k, v in self.cache.items()}

    def get_db_table_cache(self, table_key: str) -> Union[DBTableCache, None]:
        return self.cache.get(table_key.lower(), None)

    def get_db_table_hash(self, table_key: str) -> Union[str, None]:
        if self.get_db_table_cache(table_key.lower()):
            return self.get_db_table_cache(table_key.lower()).get("db_table", None)
        return None

    def get_columns(self, table_key: str) -> dict:
        if self.get_db_table_cache(table_key.lower()):
            return self.get_db_table_cache(table_key.lower()).get("columns", {})
        return {}

    def get_db_column_hash(self, table_key: str, column_key: str) -> Union[str, None]:
        if self.get_columns(table_key.lower()):
            return self.get_columns(table_key.lower()).get(column_key.lower(), None)

    def add_db_column_if_missing(self, table_key: str, column_key: str) -> Tuple[bool, str, str]:
        existing_column_hash = self.get_db_column_hash(table_key.lower(), column_key.lower())
        if existing_column_hash is not None:
            return (True, column_key, existing_column_hash)
        else:
            column_hash = generate_hash(column_key.lower())
            self.get_db_table_cache(table_key.lower()).get("columns")[column_key.lower()] = column_hash
            return (False, column_key, column_hash)

    def add_db_table_if_missing(self, table_key: str, columns: dict[str, str]) -> Tuple[bool, str, str]:
        existing_table_hash = self.get_db_table_hash(table_key.lower())
        if existing_table_hash is not None:
            return (True, table_key, existing_table_hash)
        else:
            table_hash = generate_hash(table_key.lower())
            self.cache[table_key] = DBTableCache(db_table=table_key, columns=columns)
            # now comes the tricky part, which is to add columns...
            return (False, table_key, table_hash)
