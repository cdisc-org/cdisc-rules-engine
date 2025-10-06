from typing import List

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.data_service.merges.join import SqlJoinMerge
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.utilities.utils import (
    get_sided_match_keys,
    replace_pattern_in_list_of_strings,
)


class SqlChildMerge:
    """Handles child-to-parent merge operations using LEFT JOIN."""

    @staticmethod
    def perform_merge(
        pgi: PostgresQLInterface,
        child: SqlTableSchema,
        child_domain: str,
        datasets: List[SDTMDatasetMetadata],
        merge_spec: dict,
    ) -> SqlTableSchema:
        """
        Perform child merge: Find parent dataset(s) and LEFT JOIN child with parent(s).

        Child dataset is on the left, parent(s) on the right.
        Uses SqlJoinMerge with type="LEFT".

        For datasets like RELREC with multiple RDOMAIN values, this will merge
        with ALL matching parent datasets sequentially.
        """
        # Find parent dataset(s)
        parent_metadatas = SqlChildMerge._find_parents(
            pgi=pgi,
            child=child,
            datasets=datasets,
            merge_spec=merge_spec,
        )

        if not parent_metadatas:
            raise ValueError(f"Could not find parent dataset for child merge: {child.name}")

        # Perform sequential merges with each parent (already in correct order from _find_parents)
        result_schema = child
        for parent_metadata in parent_metadatas:
            # Extract and process match keys
            match_keys = merge_spec.get("match_key", [])
            child_keys = get_sided_match_keys(match_keys, "left")
            parent_keys = get_sided_match_keys(match_keys, "right")

            # Replace "--" pattern with actual domain names
            child_keys = replace_pattern_in_list_of_strings(child_keys, "--", child_domain)
            parent_keys = replace_pattern_in_list_of_strings(parent_keys, "--", parent_metadata.domain)

            # Perform LEFT JOIN
            parent = pgi.schema.get_table(parent_metadata.name)
            result_schema = SqlJoinMerge.perform_join(
                pgi=pgi,
                left=result_schema,
                right=parent,
                pivot_left=child_keys,
                pivot_right=parent_keys,
                type="LEFT",
            )

        return result_schema

    @staticmethod
    def _get_ordered_rdomain_values(pgi: PostgresQLInterface, child: SqlTableSchema) -> List[str]:
        """Get unique RDOMAIN values from a table, preserving first-appearance order."""
        if not child.has_column("rdomain"):
            return []

        rdomain_hash = child.get_column_hash("rdomain")
        pgi.execute_sql(
            f"SELECT {rdomain_hash} as rdomain "
            f"FROM {child.hash} "
            f"WHERE {rdomain_hash} IS NOT NULL "
            f"ORDER BY id"
        )

        rdomain_values = []
        seen = set()
        for row in pgi.fetch_all():
            rdomain = row.get("rdomain")
            if rdomain and rdomain not in seen:
                rdomain_values.append(rdomain)
                seen.add(rdomain)
        return rdomain_values

    @staticmethod
    def _find_parents_by_rdomain(
        pgi: PostgresQLInterface,
        child: SqlTableSchema,
        datasets: List[SDTMDatasetMetadata],
    ) -> List[SDTMDatasetMetadata]:
        """Find parent datasets using the RDOMAIN column."""
        rdomain_values = SqlChildMerge._get_ordered_rdomain_values(pgi, child)
        if not rdomain_values:
            return []

        rdomain_set = set(rdomain_values)
        matching_parents = []
        seen_domains = set()
        for dataset in datasets:
            if dataset.domain in rdomain_set and dataset.domain not in seen_domains:
                matching_parents.append(dataset)
                seen_domains.add(dataset.domain)
        return matching_parents

    @staticmethod
    def _find_parents_by_match_keys(
        pgi: PostgresQLInterface,
        child: SqlTableSchema,
        datasets: List[SDTMDatasetMetadata],
        merge_spec: dict,
    ) -> List[SDTMDatasetMetadata]:
        """Find parent datasets using match keys as a fallback."""
        match_keys = merge_spec.get("match_key", [])
        if not match_keys:
            return []

        parent_keys = get_sided_match_keys(match_keys, "right")
        matching_parents = []
        for ds in datasets:
            if ds.name == child.name:
                continue

            table = pgi.schema.get_table(ds.name)
            if table and all(table.has_column(key.lower()) for key in parent_keys):
                matching_parents.append(ds)
        return matching_parents

    @staticmethod
    def _find_parents(
        pgi: PostgresQLInterface,
        child: SqlTableSchema,
        datasets: List[SDTMDatasetMetadata],
        merge_spec: dict,
    ) -> List[SDTMDatasetMetadata]:
        """
        Find parent dataset(s) for a child using RDOMAIN or match keys.
        """
        # Strategy 1: RDOMAIN column in child data (works for CO, RELREC, etc.)
        parents = SqlChildMerge._find_parents_by_rdomain(pgi, child, datasets)
        if parents:
            return parents

        # Strategy 2: Match key-based fallback
        return SqlChildMerge._find_parents_by_match_keys(pgi, child, datasets, merge_spec)
