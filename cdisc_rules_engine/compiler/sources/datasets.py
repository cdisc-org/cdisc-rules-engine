"""Rule types with one row per dataset (or per define.xml dataset)."""

from typing import Dict, List

from cdisc_rules_engine.compiler.sources.context import SourceContext
from cdisc_rules_engine.compiler.sources.views import rows_view
from cdisc_rules_engine.constants.metadata_columns import SOURCE_DS
from cdisc_rules_engine.metadata.store import DEFINE_DATASETS_TYPE


def domain_presence(context: SourceContext) -> str:
    """
    A single row, one column per domain present, holding that domain's file(s).
    Rules then just check whether a domain column exists / is populated.
    """
    domains: Dict[str, List[str]] = {}
    for dataset in context.datasets:
        if dataset.domain:
            domains.setdefault(dataset.domain, []).append(dataset.filename)
    if not domains:
        # view needs at least one column even if it has no rows
        return rows_view(context, {"_placeholder": "Num"}, [])
    columns = {domain: "Char" for domain in domains}
    return rows_view(context, columns, [{domain.lower(): ",".join(files) for domain, files in domains.items()}])


def domain_presence_against_define(context: SourceContext) -> str:
    """Every dataset the define.xml describes next to the uploaded file for it (if any)."""
    domain_files = {dataset.domain: dataset.filename for dataset in context.datasets}
    rows = []
    for define_metadata in context.store.define_datasets():
        define_dataset_name = define_metadata.get("define_dataset_name")
        row = {"domain": define_dataset_name, "filename": domain_files.get(define_dataset_name)}
        row.update({key: define_metadata.get(key) for key in DEFINE_DATASETS_TYPE})
        rows.append(row)
    return rows_view(context, {"domain": "Char", "filename": "Char", **DEFINE_DATASETS_TYPE}, rows)


def metadata(context: SourceContext) -> str:
    dataset = context.dataset_metadata
    columns = {
        "dataset_location": "Char",
        "dataset_name": "Char",
        "dataset_label": "Char",
        "record_count": "Num",
        "dataset_size": "Num",
    }

    split_parts = getattr(dataset, "split_part_filenames", None)
    if not split_parts:
        row = {
            "dataset_location": dataset.filename,
            "dataset_name": dataset.name,
            "dataset_label": dataset.label or "",
            "record_count": context.store.record_count(dataset.name),
            "dataset_size": dataset.file_size,
        }
        return rows_view(context, columns, [row])

    # For split datasets, report each file separately.
    # Otherwise e.g. a size/record-count rule would see the concatenation and
    # point at a file that doesn't exist
    columns[SOURCE_DS] = "Char"
    return rows_view(context, columns, _split_part_rows(context, split_parts))


def _split_part_rows(context: SourceContext, split_parts: list) -> list:
    dataset = context.dataset_metadata
    pgi = context.pgi
    table_hash = pgi.schema.get_table_hash(dataset.name)
    source_ds_hash = pgi.schema.get_column_hash(dataset.name, SOURCE_DS)
    pgi.execute_sql(
        f"SELECT UPPER({source_ds_hash}) as source_ds, COUNT(*) as count "
        f"FROM {table_hash} GROUP BY UPPER({source_ds_hash});"
    )
    counts = {row["source_ds"]: row["count"] for row in pgi.fetch_all()}
    labels = getattr(dataset, "split_part_labels", None) or {}
    sizes = getattr(dataset, "split_part_sizes", None) or {}

    rows = []
    for filename in sorted(split_parts):
        part_name = filename.rsplit(".", 1)[0].upper()
        rows.append(
            {
                "dataset_location": filename,
                "dataset_name": part_name,
                # parts without their own label fall back to the parent
                "dataset_label": labels.get(filename, dataset.label) or "",
                "record_count": counts.get(part_name, 0),
                "dataset_size": sizes.get(filename),
                SOURCE_DS: part_name,
            }
        )
    return rows


def metadata_against_define(context: SourceContext) -> str:
    """Every uploaded dataset's metadata, alongside the define.xml metadata for its domain."""
    define_datasets = context.store.define_all_datasets()
    missing = {key: None for key in DEFINE_DATASETS_TYPE}
    rows = []
    for dataset in context.all_datasets():
        rows.append(
            {
                "dataset_name": dataset.name,
                "dataset_location": dataset.filename,
                "dataset_label": dataset.label or "",
                "dataset_domain": dataset.domain,
                "dataset_size": dataset.file_size,
                **define_datasets.get(dataset.domain, missing),
            }
        )
    columns = {
        "dataset_name": "Char",
        "dataset_location": "Char",
        "dataset_label": "Char",
        "dataset_domain": "Char",
        "dataset_size": "Num",
        **DEFINE_DATASETS_TYPE,
    }
    return rows_view(context, columns, rows)


def define_item_group(context: SourceContext) -> str:
    """The define.xml dataset (ItemGroupDef) metadata for each uploaded domain."""
    rows = []
    for domain, define_metadata in context.store.define_all_datasets().items():
        row = {"dataset_domain": domain}
        for key, value in define_metadata.items():
            row[key] = None if value == "" else value
        rows.append(row)
    return rows_view(context, dict(DEFINE_DATASETS_TYPE), rows)
