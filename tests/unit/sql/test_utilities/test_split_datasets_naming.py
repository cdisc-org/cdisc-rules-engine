from types import SimpleNamespace
import pytest

from cdisc_rules_engine.standards.sdtm_standards_context import SdtmStandardsContext
from cdisc_rules_engine.check_operators.sql.string_length_comparison_operator import StringLengthComparisonOperator
from cdisc_rules_engine.constants.metadata_columns import SOURCE_DS
from cdisc_rules_engine.compiler.sources import datasets as dataset_sources


def make_operator(dataset_metadata, columns):
    schema = SimpleNamespace(
        column_exists=lambda _table, column: column in columns,
        get_column_hash=lambda _table, column: f"{column}_hash",
    )
    data_service = SimpleNamespace(pgi=SimpleNamespace(schema=schema))
    return StringLengthComparisonOperator(
        {"dataset_id": "suppqs", "data_service": data_service, "dataset_metadata": dataset_metadata}
    )


split_metadata = SimpleNamespace(name="SUPPQS", split_part_filenames=["suppqscqi.xpt", "suppqsswls.xpt"])


@pytest.mark.parametrize(
    "dataset_name, expected",
    [
        ("ae1", "ae"),
        ("aex", "ae"),
        ("qs1", "qs"),
        ("ae", "ae"),
        ("dm", "dm"),
        ("fa", "fa"),
        ("supp", "supp"),
        ("sq", "sq"),
        ("ap", "ap"),
        ("sqap", "sqap"),
        ("suppap", "suppap"),
        ("facm", "fa"),
        ("faeg", "fa"),
        ("famh", "fa"),
        ("famh1", "famh"),
        ("apfamh", "apfa"),
        ("apfamh1", "apfamh"),
        ("sqapfamh", "sqapfa"),
        ("sqapfamh2", "sqapfamh"),
        ("suppfacm", "suppfa"),
        ("suppapqs", "suppapqs"),
        ("suppapqs1", "suppapqs"),
        ("suppapfamh", "suppapfa"),
        ("suppfa", "suppfa"),
        ("suppae", "suppae"),
        ("suppae1", "suppae"),
        ("suppae12", "suppae"),
        ("supp9a9", "supp9a9"),
        ("sqapqs", "sqapqs"),
        ("sqapqsx", "sqapqs"),
        ("sqdm", "sqdm"),
        ("sqdmx", "sqdm"),
        ("apqs", "apqs"),
        ("apqsx", "apqs"),
        ("relrec", "relrec"),
        ("relreca", "relrec"),
        ("relrecb", "relrec"),
        ("ae_1", "ae_1"),
        ("a", "a"),
        ("", ""),
        ("AE1", "ae"),
        ("SUPPAE1", "suppae"),
        ("APFAMH1", "apfamh"),
        ("qscg", "qs"),
        ("qspg", "qs"),
        ("qsae", "qs"),
        ("qscgi", "qs"),
        ("qscgi2", "qscgi"),
        ("lbae", "lb"),
        ("lbds", "lb"),
        ("mha", "mh"),
        ("mhg", "mh"),
        ("mhlb", "mh"),
        ("relsub", "relsub"),
        ("aplbs", "aplb"),
        ("apmhe", "apmh"),
        ("suppqssw", "suppqs"),
        ("suppqsswls", "suppqs"),
        ("suppdmdm", "suppdm"),
        ("dmx", "dm"),
        ("ss1", "ss"),
        ("ss2", "ss"),
        ("ssa", "ss"),
        ("ssb", "ss"),
        ("apdmx", "apdm"),
        ("suppaex", "suppae"),
        ("suppqscg", "suppqs"),
        ("suppqssw", "suppqs"),
        ("apmhe", "apmh"),
        ("aplbs", "aplb"),
        ("apfamh", "apfa"),
        ("pooldef", "pooldef"),
        ("aprelsub", "aprelsub"),
        ("qs1", "qs"),
        ("mh1", "mh"),
        ("lb1", "lb"),
        ("fa1", "fa"),
    ],
)
def test_get_unsplit_name(dataset_name, expected):
    assert SdtmStandardsContext._get_unsplit_name(dataset_name) == expected


@pytest.mark.parametrize(
    "dataset_names, expected",
    [
        (["ae1", "ae2"], {"ae": ["ae1", "ae2"]}),
        (["relreca", "relrecb"], {"relrec": ["relreca", "relrecb"]}),
        (["suppae1", "suppae2"], {"suppae": ["suppae1", "suppae2"]}),
        (["apfamh1", "apfamh2"], {"apfamh": ["apfamh1", "apfamh2"]}),
        (["ae1"], {}),
        (["ae", "ae1", "ae2"], {}),
        (["dm", "ae"], {}),
        (["facm", "faeg", "famh"], {"fa": ["facm", "faeg", "famh"]}),
        (["apfacm", "apfaeg"], {"apfa": ["apfacm", "apfaeg"]}),
        (["qscg", "qspg"], {"qs": ["qscg", "qspg"]}),
        (["qscg", "qspg", "qsae", "qscgi"], {"qs": ["qscg", "qspg", "qsae", "qscgi"]}),
        (["lbae", "lbds"], {"lb": ["lbae", "lbds"]}),
        (["mha", "mhb", "mhd", "mhg", "mhlb"], {"mh": ["mha", "mhb", "mhd", "mhg", "mhlb"]}),
        (["re", "relsub"], {}),
        (["aplbef", "aplbs"], {"aplb": ["aplbef", "aplbs"]}),
        (["suppqssw", "suppqsswls", "suppqscg"], {"suppqs": ["suppqssw", "suppqsswls", "suppqscg"]}),
        (["suppdmdm", "suppdmmmm"], {"suppdm": ["suppdmdm", "suppdmmmm"]}),
        (["ss1", "ss2"], {"ss": ["ss1", "ss2"]}),
        (["qscgi", "qscgi2"], {}),
        (["qscgi2", "qscgi3"], {"qscgi": ["qscgi2", "qscgi3"]}),
        (["qscg", "qspg", "ss1", "ss2"], {"qs": ["qscg", "qspg"], "ss": ["ss1", "ss2"]}),
        (["ssa", "ssb"], {"ss": ["ssa", "ssb"]}),
        (["aex", "aey"], {"ae": ["aex", "aey"]}),
        (["ssa", "ss2"], {"ss": ["ssa", "ss2"]}),
        (["qscg", "qspg", "qsae"], {"qs": ["qscg", "qspg", "qsae"]}),
        (["mha", "mhb"], {"mh": ["mha", "mhb"]}),
        (["lbae", "lbds"], {"lb": ["lbae", "lbds"]}),
        (["facm", "famh"], {"fa": ["facm", "famh"]}),
        (["suppqscg", "suppqssw"], {"suppqs": ["suppqscg", "suppqssw"]}),
        (["qs1", "qs2"], {"qs": ["qs1", "qs2"]}),
        (["mh1", "mh2"], {"mh": ["mh1", "mh2"]}),
    ],
)
def test_detect_split_datasets(dataset_names, expected):
    context = SdtmStandardsContext.__new__(SdtmStandardsContext)
    assert context.detect_split_datasets(dataset_names) == expected


@pytest.mark.parametrize(
    "name, split_part_filenames, expected",
    [
        ("QS", ["qscg.xpt", "qspg.xpt"], True),
        ("QSCGI", None, True),
        ("QS", None, False),
        ("AE", [], False),
    ],
)
def test_split_only_rule_scope_includes_concatenated_split_datasets(monkeypatch, name, split_part_filenames, expected):
    """include_split_datasets with no included domains only validates split datasets."""
    monkeypatch.setattr(SdtmStandardsContext, "rule_applies_to_class", lambda *_args: True)
    context = SdtmStandardsContext.__new__(SdtmStandardsContext)
    rule = {"core_id": "CORE-000510", "domains": {"Exclude": ["SUPP--", "AP--"], "include_split_datasets": True}}
    metadata = SimpleNamespace(name=name, split_part_filenames=split_part_filenames)

    is_suitable, _ = context.within_rule_scope(rule, metadata)

    assert is_suitable is expected


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        ({}, "co.source_ds_hash"),
        ({"alias": False}, "source_ds_hash"),
        ({"lowercase": True}, "LOWER(co.source_ds_hash)"),
        ({"prefix": 2}, "LEFT(co.source_ds_hash, 2)"),
        ({"suffix": 3}, "RIGHT(co.source_ds_hash, 3)"),
    ],
)
def test_dataset_name_of_split_dataset_is_each_records_source_part(kwargs, expected):
    operator = make_operator(split_metadata, {SOURCE_DS})

    assert operator._column_sql("dataset_name", **kwargs) == expected


@pytest.mark.parametrize(
    "dataset_metadata, columns",
    [
        (SimpleNamespace(name="AE", split_part_filenames=None), {SOURCE_DS}),
        (split_metadata, set()),
    ],
)
def test_dataset_name_falls_back_to_the_dataset_metadata_name(dataset_metadata, columns):
    operator = make_operator(dataset_metadata, columns)

    assert operator._column_sql("dataset_name") == f"'{dataset_metadata.name}'"
    assert operator._column_sql("dataset_name", prefix=2) == f"'{dataset_metadata.name[:2]}'"


class FakePgi:
    sql_namespace = None

    def __init__(self, fetch_results):
        self.fetch_results = fetch_results
        self.schema = SimpleNamespace(
            get_table_hash=lambda name: f"{name.lower()}_hash",
            get_column_hash=lambda _table, column: f"{column}_hash",
        )

    def execute_sql(self, _query):
        pass

    def fetch_all(self):
        return self.fetch_results


def dataset_metadata_rows(monkeypatch, dataset_metadata, pgi, record_count=None):
    """The columns and rows of a Dataset Metadata Check's view of a dataset."""
    written = {}

    def rows_view(_context, column_types, rows, name=None):
        written.update(columns=list(column_types), rows=rows)
        return "view"

    monkeypatch.setattr(dataset_sources, "rows_view", rows_view)
    context = SimpleNamespace(
        dataset_metadata=dataset_metadata,
        pgi=pgi,
        store=SimpleNamespace(record_count=lambda _table: record_count),
    )
    dataset_sources.metadata(context)
    return written["columns"], written["rows"]


def test_split_dataset_metadata_has_a_row_per_part(monkeypatch):
    pgi = FakePgi([{"source_ds": "SUPPQSCQI", "count": 2}, {"source_ds": "SUPPQSSWLS", "count": 3}])
    dataset_metadata = SimpleNamespace(
        name="SUPPQS",
        filename="suppqs.xpt",
        label="Supplemental Qualifiers for QSCG",
        split_part_filenames=["suppqsswls.xpt", "suppqscqi.xpt"],
        split_part_labels={
            "suppqscqi.xpt": "Supplemental Qualifiers for QSCG",
            "suppqsswls.xpt": "Supplemental Qualifiers for QSSW",
        },
        split_part_sizes={"suppqscqi.xpt": 1024, "suppqsswls.xpt": 2048},
    )

    columns, rows = dataset_metadata_rows(monkeypatch, dataset_metadata, pgi)

    assert SOURCE_DS in columns
    assert rows == [
        {
            "dataset_location": "suppqscqi.xpt",
            "dataset_name": "SUPPQSCQI",
            "dataset_label": "Supplemental Qualifiers for QSCG",
            "record_count": 2,
            "dataset_size": 1024,
            SOURCE_DS: "SUPPQSCQI",
        },
        {
            "dataset_location": "suppqsswls.xpt",
            "dataset_name": "SUPPQSSWLS",
            "dataset_label": "Supplemental Qualifiers for QSSW",
            "record_count": 3,
            "dataset_size": 2048,
            SOURCE_DS: "SUPPQSSWLS",
        },
    ]


def test_unsplit_dataset_metadata_has_a_single_row(monkeypatch):
    dataset_metadata = SimpleNamespace(name="AE", filename="ae.xpt", label="Adverse Events", file_size=4096)

    columns, rows = dataset_metadata_rows(monkeypatch, dataset_metadata, FakePgi([]), record_count=4)

    assert SOURCE_DS not in columns
    assert rows == [
        {
            "dataset_location": "ae.xpt",
            "dataset_name": "AE",
            "dataset_label": "Adverse Events",
            "record_count": 4,
            "dataset_size": 4096,
        }
    ]
