"""
Study-wide value checks - every value of every dataset.
These are built once per study under a fixed name (operations can reference them like a
domain).
"""

from cdisc_rules_engine.compiler.sources.context import SourceContext
from cdisc_rules_engine.compiler.sources.lengths import variable_length_summary, wants_variable_length_summary
from cdisc_rules_engine.compiler.sources.values import unpivot_body, variable_value_metadata
from cdisc_rules_engine.compiler.sources.views import (
    columns_of,
    empty_body,
    ensure_view,
    literal,
    typed,
    unpivoted_columns,
)

GLOBAL_VALUE_CHECK = "global_value_check"
GLOBAL_VARIABLE_LENGTH_SUMMARY = "global_variable_length_summary"

GLOBAL_VALUES_WITH_VARIABLE_METADATA = {
    "row_number": "Num",
    "dataset_name": "Char",
    "variable_name": "Char",
    "variable_value": "Char",
    "variable_label": "Char",
    "variable_data_type": "Char",
    "variable_length": "Num",
    "variable_order_number": "Num",
    "variable_format": "Char",
    "variable_value_length": "Num",
}


def global_values(context: SourceContext) -> str:
    """Every value of every dataset with its variable's metadata."""
    if wants_variable_length_summary(context.rule):
        return global_variable_lengths(context)

    pgi = context.pgi
    if pgi.schema.get_table(GLOBAL_VALUE_CHECK) is not None:
        return GLOBAL_VALUE_CHECK

    columns = columns_of(GLOBAL_VALUES_WITH_VARIABLE_METADATA)
    # unpivot_body wants leading columns first, and dataset_name is the leading one here
    # so it moves in front of row_number for building, even though the declared order keeps it second
    ordered = [columns[1], columns[0], *columns[2:]]

    bodies, offset = [], 0
    for dataset in context.all_datasets():
        schema = pgi.schema.get_table(dataset.name)
        if not schema:
            raise ValueError(f"Table {dataset.name} not found when building global value check dataset.")
        body = unpivot_body(
            schema,
            ordered,
            variable_value_metadata(dataset.variables),
            "Num",
            leading=[typed(literal(dataset.name), "Char")],
            id_offset=offset,
        )
        if body:
            bodies.append(body)
            # each dataset's ids pick up where the previous one's left off which ensures that ids stay unique
            offset += len(unpivoted_columns(schema)) * context.store.record_count(dataset.name)

    body = " UNION ALL ".join(bodies) if bodies else empty_body(columns)
    return ensure_view(pgi, body, columns, name=GLOBAL_VALUE_CHECK, materialise=True)


def global_variable_lengths(context: SourceContext) -> str:
    if context.pgi.schema.get_table(GLOBAL_VARIABLE_LENGTH_SUMMARY) is not None:
        return GLOBAL_VARIABLE_LENGTH_SUMMARY
    datasets = [(dataset, dataset.name) for dataset in context.all_datasets()]
    return variable_length_summary(context, datasets, True, name=GLOBAL_VARIABLE_LENGTH_SUMMARY)
