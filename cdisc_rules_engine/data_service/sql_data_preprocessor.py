"""
Data Preprocessor for SDTM and ADaM clinical data.
"""

import json
import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from cdisc_rules_engine.data_service.db_cache import DBCache
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface

logger = logging.getLogger(__name__)


class DataPreprocessor:
    """
    Performs preprocessing operations on clinical data.
    Operations should be performed at data ingestion time.
    """

    def __init__(self, postgres_interface: PostgresQLInterface, cache: DBCache):
        self.pgi = postgres_interface
        self.cache = cache
        self._merged_datasets_cache: Set[str] = set()
        self._relrec_catalog: Optional[List[Dict]] = None
        self._co_catalog: Optional[List[Dict]] = None
        self._supp_catalog: Optional[List[Dict]] = None

    def preprocess_all(self) -> Dict[str, Any]:
        """Execute all preprocessing stages in sequence and store results."""
        logger.info("Starting data preprocessing pipeline")

        run_id_query = "SELECT gen_random_uuid() as run_id"
        self.pgi.execute_sql(run_id_query)
        run_id = self.pgi.fetch_one()["run_id"]

        timestamp = datetime.now().astimezone()

        results = {
            "run_id": str(run_id),
            "split_processing": {},
            "relrec_catalog": {},
            "co_catalog": {},
            "supp_catalog": {},
            "validation_errors": [],
            "metadata_updates": {},
            "timestamp": timestamp.isoformat(),
        }

        self._validation_errors = []
        self._current_run_id = run_id

        self._create_preprocessing_results_table()

        results["split_processing"] = self._process_split_datasets()
        results["relrec_catalog"] = self._build_relrec_catalog()
        results["co_catalog"] = self._build_co_catalog()
        results["supp_catalog"] = self._build_supp_catalog()
        results["validation_errors"] = self._validation_errors
        results["metadata_updates"] = self._update_metadata(timestamp)

        self._store_preprocessing_results(results, run_id, timestamp)

        if self._validation_errors:
            self._log_validation_errors_to_db(run_id)

        logger.info(f"Data preprocessing pipeline completed with {len(self._validation_errors)} validation errors")
        return results

    def _create_preprocessing_results_table(self) -> None:
        """Create table to store preprocessing results."""
        create_table_query = """
            CREATE TABLE IF NOT EXISTS public.preprocessing_results (
                id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                run_id UUID DEFAULT gen_random_uuid(),
                timestamp TIMESTAMPTZ NOT NULL,
                stage TEXT NOT NULL,
                results JSONB NOT NULL,
                validation_errors JSONB,
                metadata_updates JSONB,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_preprocessing_results_run_id
                ON public.preprocessing_results(run_id);
            CREATE INDEX IF NOT EXISTS idx_preprocessing_results_stage
                ON public.preprocessing_results(stage);
            CREATE INDEX IF NOT EXISTS idx_preprocessing_results_timestamp
                ON public.preprocessing_results(timestamp);
        """

        self.pgi.execute_sql(create_table_query)

        validation_errors_table = """
            CREATE TABLE IF NOT EXISTS public.preprocessing_validation_errors (
                id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                run_id UUID,
                validation_type TEXT NOT NULL,
                source_table TEXT,
                row_number INTEGER DEFAULT -1,
                studyid TEXT,
                usubjid TEXT,
                rdomain TEXT,
                idvar TEXT,
                idvarval TEXT,
                error_message TEXT NOT NULL,
                severity TEXT DEFAULT 'ERROR',
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_validation_errors_run_id
                ON public.preprocessing_validation_errors(run_id);
            CREATE INDEX IF NOT EXISTS idx_validation_errors_type
                ON public.preprocessing_validation_errors(validation_type);
            CREATE INDEX IF NOT EXISTS idx_validation_errors_source_table
                ON public.preprocessing_validation_errors(source_table);
        """

        self.pgi.execute_sql(validation_errors_table)

    def _validate_relationships(self) -> Dict[str, Any]:
        """Validate relationships per CG0371."""
        logger.info("Validating relationships (CG0371)")

        validation_results = {"relrec_validated": False, "co_validated": False, "supp_validated": False, "errors": []}

        relrec_errors = self._validate_relrec_relationships()
        validation_results["relrec_validated"] = len(relrec_errors) == 0
        validation_results["errors"].extend(relrec_errors)

        co_errors = self._validate_co_relationships()
        validation_results["co_validated"] = len(co_errors) == 0
        validation_results["errors"].extend(co_errors)

        supp_errors = self._validate_supp_relationships()
        validation_results["supp_validated"] = len(supp_errors) == 0
        validation_results["errors"].extend(supp_errors)

        if validation_results["errors"]:
            logger.warning(f"Found {len(validation_results['errors'])} validation errors")
            for error in validation_results["errors"]:
                logger.warning(f"Validation error: {error}")

        return validation_results

    def _validate_relrec_relationships(self) -> List[Dict[str, Any]]:
        """Validate RELREC dataset relationships per CG0371."""
        errors = []

        check_query = """
            SELECT dataset_id
            FROM public.data_metadata
            WHERE dataset_domain = 'RELREC'
            LIMIT 1
        """
        self.pgi.execute_sql(check_query)
        if not self.pgi._last_results:
            return errors  # query failed

        check_result = self.pgi.fetch_one()
        if not check_result:
            errors.append({"type": "RELREC", "error": "No RELREC dataset found"})
            return errors

        relrec_table = dict(check_result)["dataset_id"]

        errors.extend(self._validate_relrec_schema(relrec_table))
        errors.extend(self._validate_relrec_values(relrec_table))

        return errors

    def _validate_relrec_schema(self, relrec_table: str) -> List[Dict[str, Any]]:
        """Validate that the table (RDOMAIN) and column (IDVAR) referenced in RELREC exist."""
        errors = []
        validation_query = f"""
            WITH relrec_data AS (
                SELECT studyid, usubjid, relid, rdomain, idvar, idvarval
                FROM public.{relrec_table}
                WHERE idvar IS NOT NULL AND idvarval IS NOT NULL
            )
            SELECT
                r.studyid, r.usubjid, r.relid, r.rdomain, r.idvar, r.idvarval,
                CASE
                    WHEN NOT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = LOWER(r.rdomain)
                    ) THEN 'Domain table does not exist'
                    WHEN NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = LOWER(r.rdomain)
                        AND column_name = LOWER(r.idvar)
                    ) THEN 'IDVAR column does not exist in domain'
                    ELSE NULL
                END as validation_error
            FROM relrec_data r
            WHERE EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = LOWER(r.rdomain)
            )
        """
        self.pgi.execute_sql(validation_query)
        validation_results = self.pgi.fetch_all()

        if validation_results:
            for row in validation_results:
                if row[6]:  # validation_error is not None
                    errors.append(
                        {
                            "type": "RELREC",
                            "studyid": row[0],
                            "usubjid": row[1],
                            "relid": row[2],
                            "rdomain": row[3],
                            "idvar": row[4],
                            "idvarval": row[5],
                            "error": row[6],
                        }
                    )
        return errors

    def _validate_relrec_values(self, relrec_table: str) -> List[Dict[str, Any]]:
        """Validate that the value (IDVARVAL) referenced in RELREC exists in the target table."""
        errors = []
        value_check_query = f"""
            SELECT studyid, usubjid, relid, rdomain, idvar, idvarval
            FROM public.{relrec_table}
            WHERE idvar IS NOT NULL AND idvarval IS NOT NULL
        """
        self.pgi.execute_sql(value_check_query)
        value_check_results = self.pgi.fetch_all()

        if value_check_results:
            for row in value_check_results:
                studyid, usubjid, relid, rdomain, idvar, idvarval = row
                parent_check_query = f"""
                    SELECT COUNT(*)
                    FROM public.{rdomain.lower()}
                    WHERE studyid = %s AND usubjid = %s AND {idvar.lower()} = %s
                """
                try:
                    self.pgi.execute_sql(parent_check_query, (studyid, usubjid, idvarval))
                    parent_check_result = self.pgi.fetch_one()
                    if not parent_check_result["exists"]:
                        errors.append(
                            {
                                "type": "RELREC",
                                "studyid": studyid,
                                "usubjid": usubjid,
                                "relid": relid,
                                "rdomain": rdomain,
                                "idvar": idvar,
                                "idvarval": idvarval,
                                "error": f"IDVARVAL '{idvarval}' not found in {rdomain}.{idvar}",
                            }
                        )
                except Exception as e:
                    logger.debug(f"Could not validate {rdomain}.{idvar}: {e}")
        return errors

    def _validate_co_relationships(self) -> List[Dict[str, Any]]:
        """Validate comment domain relationships per CG0371."""
        errors = []

        co_query = """
            SELECT DISTINCT dataset_id, dataset_domain
            FROM public.data_metadata
            WHERE dataset_domain LIKE 'CO%'
        """

        self.pgi.execute_sql(co_query)

        co_results = self.pgi.fetch_all()
        if not co_results:
            return errors

        for co_table, co_domain in co_results:
            validation_query = f"""
                SELECT
                    studyid,
                    usubjid,
                    rdomain,
                    idvar,
                    idvarval
                FROM public.{co_table}
                WHERE idvar IS NOT NULL
                AND idvarval IS NOT NULL
            """

            self.pgi.execute_sql(validation_query)

            if self.pgi._last_results:
                for row in self.pgi._last_results:
                    studyid, usubjid, rdomain, idvar, idvarval = row

                    try:
                        parent_check = f"""
                            SELECT COUNT(*)
                            FROM public.{rdomain.lower()}
                            WHERE studyid = %s
                            AND usubjid = %s
                            AND {idvar.lower()} = %s
                        """

                        self.pgi.execute_sql(parent_check, (studyid, usubjid, idvarval))

                        result = self.pgi.fetch_one()
                        if result and dict(result)["count"] == 0:
                            errors.append(
                                {
                                    "type": "CO",
                                    "domain": co_domain,
                                    "studyid": studyid,
                                    "usubjid": usubjid,
                                    "rdomain": rdomain,
                                    "idvar": idvar,
                                    "idvarval": idvarval,
                                    "error": f"IDVARVAL '{idvarval}' not found in {rdomain}.{idvar}",
                                }
                            )
                    except Exception as e:
                        logger.debug(f"Could not validate CO relationship: {e}")

        return errors

    def _validate_supp_relationships(self) -> List[Dict[str, Any]]:
        """Validate SUPP (Supplemental Qualifiers) relationships per CG0371."""
        errors = []

        supp_query = """
            SELECT DISTINCT dataset_id, dataset_domain, dataset_rdomain
            FROM public.data_metadata
            WHERE dataset_is_supp = true
               OR dataset_id LIKE 'supp%'
        """

        self.pgi.execute_sql(supp_query)

        if not self.pgi._last_results:
            return errors

        for supp_table, supp_domain, rdomain in self.pgi._last_results:
            validation_query = f"""
                SELECT
                    studyid,
                    usubjid,
                    rdomain,
                    idvar,
                    idvarval
                FROM public.{supp_table}
                WHERE idvar IS NOT NULL
                AND idvarval IS NOT NULL
            """

            self.pgi.execute_sql(validation_query)

            if self.pgi._last_results:
                for row in self.pgi._last_results:
                    studyid, usubjid, rdomain_val, idvar, idvarval = row

                    target_domain = rdomain_val or rdomain

                    if not target_domain:
                        continue

                    try:
                        parent_check = f"""
                            SELECT COUNT(*)
                            FROM public.{target_domain.lower()}
                            WHERE studyid = %s
                            AND usubjid = %s
                            AND {idvar.lower()} = %s
                        """

                        self.pgi.execute_sql(parent_check, (studyid, usubjid, idvarval))

                        result = self.pgi.fetch_one()
                        if result and dict(result)["count"] == 0:
                            errors.append(
                                {
                                    "type": "SUPP",
                                    "domain": supp_domain,
                                    "studyid": studyid,
                                    "usubjid": usubjid,
                                    "rdomain": target_domain,
                                    "idvar": idvar,
                                    "idvarval": idvarval,
                                    "error": f"IDVARVAL '{idvarval}' not found in {target_domain}.{idvar}",
                                }
                            )
                    except Exception as e:
                        logger.debug(f"Could not validate SUPP relationship: {e}")

        return errors

    def process_rule_driven_merges(self, rule_spec: Dict) -> Optional[str]:
        """Perform merges based on rule specifications."""
        datasets = rule_spec.get("datasets", [])

        for dataset_spec in datasets:
            domain_name = dataset_spec.get("domain_name") or dataset_spec.get("domain")

            if domain_name == "RELREC":
                return self._perform_relrec_merge(dataset_spec, rule_spec)
            elif domain_name and domain_name.startswith("CO"):
                return self._perform_co_merge(dataset_spec, rule_spec)
            elif domain_name in ["SUPP--", "SUPPQUAL"] or (domain_name and domain_name.startswith("SUPP")):
                return self._perform_supp_merge(dataset_spec, rule_spec)

        return None

    def _validate_idvar_idvarval(
        self, table_name: str, relationship_type: str, rdomain_col: str = "rdomain"
    ) -> List[Dict[str, Any]]:
        """
        Validate CG0371: IDVARVAL must equal a value of the variable referenced
        by IDVAR within the domain referenced by RDOMAIN.
        """
        errors = []

        # check if required columns exist
        structure_check = f"""
            SELECT
                NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public'
                    AND table_name = '{table_name}'
                    AND column_name IN ('idvar', 'idvarval', '{rdomain_col}')
                ) as missing_columns
        """

        self.pgi.execute_sql(structure_check)
        result = self.pgi.fetch_one()

        if result and result["missing_columns"]:
            errors.append(
                {
                    "type": f"CG0371_{relationship_type}_STRUCTURE",
                    "table": table_name,
                    "row_number": -1,  # -1 indicates not a row-level error (e.g. missing columns)
                    "error": f"Required columns (idvar, idvarval, or {rdomain_col}) missing in {table_name}",
                }
            )
            return errors

        # get records with row numbers
        query = f"""
            WITH numbered_records AS (
                SELECT
                    ROW_NUMBER() OVER (ORDER BY studyid, usubjid) - 1 as row_num,
                    studyid,
                    usubjid,
                    {rdomain_col} as rdomain,
                    idvar,
                    idvarval
                FROM public.{table_name}
                WHERE idvar IS NOT NULL
                AND idvarval IS NOT NULL
                AND {rdomain_col} IS NOT NULL
            )
            SELECT * FROM numbered_records
        """

        self.pgi.execute_sql(query)
        records = self.pgi.fetch_all()

        if not records:
            return errors

        by_domain = defaultdict(list)  # grouping by parent domain here to minimise queries
        for record in records:
            by_domain[record["rdomain"].lower()].append(record)

        for domain, domain_records in by_domain.items():
            table_exists_query = """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = %s
                )
            """

            self.pgi.execute_sql(table_exists_query, (domain,))
            result = self.pgi.fetch_one()

            if not result or not result["exists"]:
                for record in domain_records:
                    errors.append(
                        {
                            "type": f"CG0371_{relationship_type}",
                            "table": table_name,
                            "row_number": record["row_num"],
                            "studyid": record["studyid"],
                            "usubjid": record["usubjid"],
                            "rdomain": record["rdomain"],
                            "idvar": record["idvar"],
                            "idvarval": record["idvarval"],
                            "error": f"Parent domain table {domain} does not exist",
                        }
                    )
                continue

            for record in domain_records:
                idvar = record["idvar"].lower()

                col_exists_query = """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                        AND table_name = %s
                        AND column_name = %s
                    )
                """

                self.pgi.execute_sql(col_exists_query, (domain, idvar))
                result = self.pgi.fetch_one()

                if not result or not dict(result)["exists"]:
                    errors.append(
                        {
                            "type": f"CG0371_{relationship_type}",
                            "table": table_name,
                            "row_number": record["row_num"],
                            "studyid": record["studyid"],
                            "usubjid": record["usubjid"],
                            "rdomain": record["rdomain"],
                            "idvar": record["idvar"],
                            "idvarval": record["idvarval"],
                            "error": f"IDVAR column {idvar} does not exist in {domain}",
                        }
                    )
                    continue

                value_check_query = f"""
                    SELECT COUNT(*) as count
                    FROM public.{domain}
                    WHERE studyid = %s
                    AND usubjid = %s
                    AND {idvar} = %s
                """

                self.pgi.execute_sql(value_check_query, (record["studyid"], record["usubjid"], record["idvarval"]))
                result = self.pgi.fetch_one()

                if not result or dict(result)["count"] == 0:
                    errors.append(
                        {
                            "type": f"CG0371_{relationship_type}",
                            "table": table_name,
                            "row_number": record["row_num"],
                            "studyid": record["studyid"],
                            "usubjid": record["usubjid"],
                            "rdomain": record["rdomain"],
                            "idvar": record["idvar"],
                            "idvarval": record["idvarval"],
                            "error": f"IDVARVAL '{record['idvarval']}' not found in {domain}.{idvar}",
                        }
                    )

        return errors

    def _process_split_datasets(self) -> Dict[str, Any]:
        """Concatenate split datasets into single logical datasets."""
        logger.info("Processing split datasets")

        query = """
            SELECT DISTINCT
                dataset_unsplit_name,
                COUNT(DISTINCT dataset_id) as part_count,
                array_agg(DISTINCT dataset_id ORDER BY dataset_id) as dataset_parts
            FROM public.data_metadata
            WHERE dataset_is_split = true
            GROUP BY dataset_unsplit_name
            HAVING COUNT(DISTINCT dataset_id) > 1
        """

        self.pgi.execute_sql(query)
        split_groups = self.pgi.fetch_all()

        processed_count = 0

        for group in split_groups:
            unsplit_name = group["dataset_unsplit_name"]
            part_count = group["part_count"]
            dataset_parts = group["dataset_parts"]

            logger.info(f"Concatenating {part_count} parts for {unsplit_name}")

            self._concatenate_split_parts(unsplit_name, dataset_parts)
            processed_count += 1

        return {
            "groups_processed": processed_count,
            "total_parts_concatenated": sum(g["part_count"] for g in split_groups) if split_groups else 0,
            "source_tracking_added": True,
        }

    def _concatenate_split_parts(self, unsplit_name: str, dataset_parts: List[str]) -> None:
        """Concatenate multiple dataset parts into a single table."""
        if not dataset_parts:
            logger.warning(f"No parts to concatenate for {unsplit_name}")
            return

        union_parts = []
        for part in dataset_parts:
            union_parts.append(
                f"""
                SELECT
                    *,
                    '{part}' as source_dataset_id,
                    '{unsplit_name}' as unsplit_dataset_name,
                    ROW_NUMBER() OVER (PARTITION BY '{part}' ORDER BY
                        CASE
                            WHEN EXISTS (SELECT 1 FROM public.{part} WHERE usubjid IS NOT NULL LIMIT 1)
                            THEN usubjid
                            ELSE NULL
                        END,
                        CASE
                            WHEN EXISTS (SELECT 1 FROM public.{part} WHERE studyid IS NOT NULL LIMIT 1)
                            THEN studyid
                            ELSE NULL
                        END
                    ) as source_row_number
                FROM public.{part}
            """
            )

        union_query = " UNION ALL ".join(union_parts)

        create_query = f"""
            CREATE TABLE IF NOT EXISTS public.{unsplit_name} AS
            WITH concatenated AS (
                {union_query}
            )
            SELECT * FROM concatenated
            ORDER BY
                source_dataset_id,
                source_row_number
        """

        self.pgi.execute_sql(create_query)

        index_queries = [
            f"""
                CREATE INDEX IF NOT EXISTS idx_{unsplit_name}_source_dataset
                ON public.{unsplit_name}(source_dataset_id)
            """,
            f"""
                CREATE INDEX IF NOT EXISTS idx_{unsplit_name}_source_row
                ON public.{unsplit_name}(source_row_number)
            """,
            f"""
                CREATE INDEX IF NOT EXISTS idx_{unsplit_name}_unsplit_name
                ON public.{unsplit_name}(unsplit_dataset_name)
            """,
        ]

        for idx_query in index_queries:
            self.pgi.execute_sql(idx_query)

        logger.info(f"Created concatenated dataset: {unsplit_name}")

    def _build_relrec_catalog(self) -> Dict[str, Any]:
        """Build RELREC relationship catalog with CG0371 validation."""
        logger.info("Building RELREC catalog with validation")

        check_query = """
            SELECT dataset_id
            FROM public.data_metadata
            WHERE dataset_domain = 'RELREC'
            LIMIT 1
        """

        self.pgi.execute_sql(check_query)
        result = self.pgi.fetch_one()

        if not result:
            logger.info("No RELREC dataset found, skipping catalog creation")
            return {"catalog_created": False}

        relrec_table = result["dataset_id"]

        validation_errors = self._validate_idvar_idvarval(relrec_table, "RELREC")
        self._validation_errors.extend(validation_errors)

        if validation_errors:
            logger.warning(f"Found {len(validation_errors)} CG0371 validation errors in RELREC")
            for error in validation_errors:
                if error.get("row_number", -1) >= 0:
                    logger.debug(f"Row {error['row_number']}: {error['error']}")

        catalog_query = f"""
            CREATE TABLE IF NOT EXISTS public.relrec_catalog AS
            SELECT
                studyid,
                usubjid,
                relid,
                rdomain,
                idvar,
                idvarval,
                reltype,
                ROW_NUMBER() OVER (ORDER BY studyid, usubjid, relid) - 1 as source_row,
                ROW_NUMBER() OVER (ORDER BY studyid, usubjid, relid) as catalog_id
            FROM public.{relrec_table}
        """

        self.pgi.execute_sql(catalog_query)

        index_queries = [
            "CREATE INDEX IF NOT EXISTS idx_relrec_catalog_rdomain ON public.relrec_catalog(rdomain)",
            "CREATE INDEX IF NOT EXISTS idx_relrec_catalog_relid ON public.relrec_catalog(relid)",
            "CREATE INDEX IF NOT EXISTS idx_relrec_catalog_studyid_usubjid ON public.relrec_catalog(studyid, usubjid)",
            "CREATE INDEX IF NOT EXISTS idx_relrec_catalog_source_row ON public.relrec_catalog(source_row)",
        ]

        for idx_query in index_queries:
            self.pgi.execute_sql(idx_query)

        stats_query = """
            SELECT
                COUNT(DISTINCT relid) as unique_relationships,
                COUNT(DISTINCT rdomain) as unique_domains,
                COUNT(*) as total_records
            FROM public.relrec_catalog
        """

        self.pgi.execute_sql(stats_query)
        result = self.pgi.fetch_one()
        stats = result if result else {"unique_relationships": 0, "unique_domains": 0, "total_records": 0}

        self._cache_available_relrec_merges()

        return {
            "catalog_created": True,
            "unique_relationships": stats["unique_relationships"],
            "unique_domains": stats["unique_domains"],
            "total_records": stats["total_records"],
            "validation_errors": len(validation_errors),
            "row_level_validation": True,
        }

    def _build_co_catalog(self) -> Dict[str, Any]:
        """Build CO (Comment) domain catalog with CG0371 validation."""
        logger.info("Building CO catalog with validation")

        co_query = """
            SELECT DISTINCT dataset_id, dataset_domain
            FROM public.data_metadata
            WHERE dataset_domain LIKE 'CO%'
        """

        self.pgi.execute_sql(co_query)
        co_datasets = self.pgi.fetch_all()

        if not co_datasets:
            logger.info("No CO domains found")
            return {"catalog_created": False}

        total_validation_errors = 0
        catalogs_created = []

        for dataset in co_datasets:
            co_table = dataset["dataset_id"]
            co_domain = dataset["dataset_domain"]

            logger.info(f"Processing CO domain: {co_domain}")

            validation_errors = self._validate_idvar_idvarval(co_table, "CO")
            self._validation_errors.extend(validation_errors)
            total_validation_errors += len(validation_errors)

            if validation_errors:
                logger.warning(f"Found {len(validation_errors)} CG0371 validation errors in {co_domain}")

            catalog_name = f"{co_table}_catalog"

            coval_check = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = %s
                AND column_name = 'coval'
                LIMIT 1
            """

            self.pgi.execute_sql(coval_check, (co_table,))
            has_coval = self.pgi.fetch_one()

            coval_col = "coval" if has_coval else "NULL as coval"

            catalog_query = f"""
                CREATE TABLE IF NOT EXISTS public.{catalog_name} AS
                SELECT
                    studyid,
                    usubjid,
                    rdomain,
                    idvar,
                    idvarval,
                    {coval_col},
                    ROW_NUMBER() OVER (ORDER BY studyid, usubjid) as catalog_id
                FROM public.{co_table}
            """

            self.pgi.execute_sql(catalog_query)

            index_queries = [
                f"""
                    CREATE INDEX IF NOT EXISTS idx_{catalog_name}_rdomain
                    ON public.{catalog_name}(rdomain)
                """,
                f"""
                    CREATE INDEX IF NOT EXISTS idx_{catalog_name}_studyid_usubjid
                    ON public.{catalog_name}(studyid, usubjid)
                """,
            ]

            for idx_query in index_queries:
                self.pgi.execute_sql(idx_query)

            catalogs_created.append(catalog_name)

        self._cache_available_co_merges()

        return {
            "catalog_created": True,
            "catalogs_created": catalogs_created,
            "co_domains_processed": len(co_datasets),
            "validation_errors": total_validation_errors,
        }

    def _build_supp_catalog(self) -> Dict[str, Any]:
        """Build SUPP (Supplemental Qualifiers) catalog with CG0371 validation."""
        logger.info("Building SUPP catalog with validation")

        supp_query = """
            SELECT DISTINCT dataset_id, dataset_domain, dataset_rdomain
            FROM public.data_metadata
            WHERE dataset_is_supp = true
                OR dataset_id LIKE 'supp%'
                OR dataset_id LIKE 'sq%'
        """

        self.pgi.execute_sql(supp_query)
        supp_datasets = self.pgi.fetch_all()

        if not supp_datasets:
            logger.info("No SUPP datasets found")
            return {"catalog_created": False}

        total_validation_errors = 0
        catalogs_created = []

        for dataset in supp_datasets:
            supp_table = dataset["dataset_id"]
            rdomain = dataset["dataset_rdomain"]

            logger.info(f"Processing SUPP dataset: {supp_table}")

            validation_errors = self._validate_idvar_idvarval(supp_table, "SUPP")
            self._validation_errors.extend(validation_errors)
            total_validation_errors += len(validation_errors)

            if validation_errors:
                logger.warning(f"Found {len(validation_errors)} CG0371 validation errors in {supp_table}")

            catalog_name = f"{supp_table}_catalog"

            rdomain_check = """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    AND table_name = %s
                    AND column_name = 'rdomain'
                )
            """

            self.pgi.execute_sql(rdomain_check, (supp_table,))
            result = self.pgi.fetch_one()

            has_rdomain_col = dict(result)["exists"]

            qnam_qval_check = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = %s
                AND column_name IN ('qnam', 'qval')
            """

            self.pgi.execute_sql(qnam_qval_check, (supp_table,))
            qnam_qval_cols = [row["column_name"] for row in self.pgi.fetch_all()]

            qnam_col = "qnam" if "qnam" in qnam_qval_cols else "NULL as qnam"
            qval_col = "qval" if "qval" in qnam_qval_cols else "NULL as qval"

            if has_rdomain_col:
                catalog_query = f"""
                    CREATE TABLE IF NOT EXISTS public.{catalog_name} AS
                    SELECT
                        studyid,
                        usubjid,
                        rdomain,
                        idvar,
                        idvarval,
                        {qnam_col},
                        {qval_col},
                        ROW_NUMBER() OVER (ORDER BY studyid, usubjid) as catalog_id
                    FROM public.{supp_table}
                """
            else:
                catalog_query = f"""
                    CREATE TABLE IF NOT EXISTS public.{catalog_name} AS
                    SELECT
                        studyid,
                        usubjid,
                        '{rdomain}' as rdomain,
                        idvar,
                        idvarval,
                        {qnam_col},
                        {qval_col},
                        ROW_NUMBER() OVER (ORDER BY studyid, usubjid) as catalog_id
                    FROM public.{supp_table}
                """

            self.pgi.execute_sql(catalog_query)

            index_queries = [
                f"""
                    CREATE INDEX IF NOT EXISTS idx_{catalog_name}_rdomain
                    ON public.{catalog_name}(rdomain)
                """,
                f"""
                    CREATE INDEX IF NOT EXISTS idx_{catalog_name}_studyid_usubjid
                    ON public.{catalog_name}(studyid, usubjid)
                """,
            ]

            if "qnam" in qnam_qval_cols:
                index_queries.append(
                    f"CREATE INDEX IF NOT EXISTS idx_{catalog_name}_qnam ON public.{catalog_name}(qnam)"
                )

            for idx_query in index_queries:
                self.pgi.execute_sql(idx_query)

            catalogs_created.append(catalog_name)

        self._cache_available_supp_merges()

        return {
            "catalog_created": True,
            "catalogs_created": catalogs_created,
            "supp_datasets_processed": len(supp_datasets),
            "validation_errors": total_validation_errors,
        }

    def _cache_available_relrec_merges(self) -> None:
        """Cache the list of possible RELREC partners for each domain."""
        query = """
            WITH domain_pairs AS (
                SELECT DISTINCT
                    r1.rdomain as left_domain,
                    r2.rdomain as right_domain,
                    r1.relid
                FROM public.relrec_catalog r1
                JOIN public.relrec_catalog r2
                    ON r1.studyid = r2.studyid
                    AND r1.usubjid = r2.usubjid
                    AND r1.relid = r2.relid
                WHERE r1.rdomain != r2.rdomain
            )
            SELECT
                left_domain,
                array_agg(DISTINCT right_domain || '_' || relid) as available_merges
            FROM domain_pairs
            GROUP BY left_domain
        """

        self.pgi.execute_sql(query)
        results = self.pgi.fetch_all()

        if results:
            for result in results:
                update_query = """
                    UPDATE public.data_metadata
                    SET
                        contains_relrec_refs = true,
                        available_relrec_merges = %s
                    WHERE dataset_domain = %s
                """
                self.pgi.execute_sql(update_query, (result["available_merges"], result["left_domain"]))

    def _cache_available_co_merges(self) -> None:
        """Cache available CO merges for domains."""
        co_catalogs_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name LIKE '%_catalog'
            AND table_name LIKE 'co%'
        """

        self.pgi.execute_sql(co_catalogs_query)
        co_catalogs = self.pgi.fetch_all()

        for catalog in co_catalogs:
            catalog_name = catalog["table_name"]

            domains_query = f"""
                SELECT DISTINCT rdomain
                FROM public.{catalog_name}
                WHERE rdomain IS NOT NULL
            """

            self.pgi.execute_sql(domains_query)
            domains = self.pgi.fetch_all()

            if domains:
                for domain in domains:
                    update_query = """
                        UPDATE public.data_metadata
                        SET contains_co_refs = true
                        WHERE dataset_domain = %s
                    """
                    self.pgi.execute_sql(update_query, (domain["rdomain"],))

    def _cache_available_supp_merges(self) -> None:
        """Cache available SUPP merges for domains."""
        supp_catalogs_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name LIKE '%_catalog'
            AND (table_name LIKE 'supp%' OR table_name LIKE 'sq%')
        """

        self.pgi.execute_sql(supp_catalogs_query)
        supp_catalogs = self.pgi.fetch_all()

        for catalog in supp_catalogs:
            catalog_name = catalog["table_name"]

            rdomain_query = f"""
                SELECT DISTINCT rdomain
                FROM public.{catalog_name}
                WHERE rdomain IS NOT NULL
                LIMIT 1
            """

            self.pgi.execute_sql(rdomain_query)
            result = self.pgi.fetch_one()

            if result:
                rdomain = result["rdomain"]
                update_query = """
                    UPDATE public.data_metadata
                    SET contains_supp_refs = true
                    WHERE dataset_domain = %s
                """
                self.pgi.execute_sql(update_query, (rdomain,))

    def _perform_relrec_merge(self, dataset_spec: Dict, rule_spec: Dict) -> Optional[str]:
        """Perform a specific RELREC merge based on rule requirements."""
        wildcard = dataset_spec.get("wildcard", "__")
        left_domain = rule_spec.get("domains", {}).get("Include", [None])[0]

        if not left_domain:
            logger.warning("No left domain specified for RELREC merge")
            return None

        merge_id = f"relrec_{left_domain.lower()}_{wildcard}"

        if merge_id in self._merged_datasets_cache:
            logger.info(f"Using cached merge: {merge_id}")
            return merge_id

        merge_query = f"""
            CREATE TABLE IF NOT EXISTS public.{merge_id} AS
            WITH relrec_pairs AS (
                SELECT
                    r1.*,
                    r2.rdomain as rdomain_right,
                    r2.idvar as idvar_right,
                    r2.idvarval as idvarval_right
                FROM public.relrec_catalog r1
                JOIN public.relrec_catalog r2
                    ON r1.studyid = r2.studyid
                    AND r1.usubjid = r2.usubjid
                    AND r1.relid = r2.relid
                WHERE r1.rdomain = '{left_domain}'
                    AND r1.rdomain != r2.rdomain
            ),
            left_data AS (
                SELECT * FROM public.{left_domain.lower()}
            ),
            merged_data AS (
                SELECT
                    l.*,
                    rp.rdomain_right,
                    rp.relid,
                    rp.idvar_right,
                    rp.idvarval_right
                FROM left_data l
                JOIN relrec_pairs rp
                    ON l.studyid = rp.studyid
                    AND l.usubjid = rp.usubjid
            )
            SELECT * FROM merged_data
        """

        self.pgi.execute_sql(merge_query)
        self._merged_datasets_cache.add(merge_id)
        self._add_merged_dataset_metadata(merge_id, left_domain, "relrec_merged")

        logger.info(f"Created RELREC merge: {merge_id}")
        return merge_id

    def _perform_co_merge(self, dataset_spec: Dict, rule_spec: Dict) -> Optional[str]:
        """Perform a CO (Comment) domain merge."""
        co_domain = dataset_spec.get("domain_name") or dataset_spec.get("domain")
        left_domain = rule_spec.get("domains", {}).get("Include", [None])[0]

        if not left_domain or not co_domain:
            logger.warning("Missing domain specification for CO merge")
            return None

        merge_id = f"co_{left_domain.lower()}_{co_domain.lower()}"

        if merge_id in self._merged_datasets_cache:
            logger.info(f"Using cached merge: {merge_id}")
            return merge_id

        catalog_name = f"{co_domain.lower()}_catalog"

        merge_query = f"""
            CREATE TABLE IF NOT EXISTS public.{merge_id} AS
            WITH co_data AS (
                SELECT *
                FROM public.{catalog_name}
                WHERE rdomain = '{left_domain}'
            ),
            left_data AS (
                SELECT * FROM public.{left_domain.lower()}
            ),
            merged_data AS (
                SELECT
                    l.*,
                    c.coval as comment_value,
                    c.idvar as co_idvar,
                    c.idvarval as co_idvarval
                FROM left_data l
                LEFT JOIN co_data c
                    ON l.studyid = c.studyid
                    AND l.usubjid = c.usubjid
            )
            SELECT * FROM merged_data
        """

        self.pgi.execute_sql(merge_query)
        self._merged_datasets_cache.add(merge_id)
        self._add_merged_dataset_metadata(merge_id, left_domain, "co_merged")

        logger.info(f"Created CO merge: {merge_id}")
        return merge_id

    def _perform_supp_merge(self, dataset_spec: Dict, rule_spec: Dict) -> Optional[str]:
        """Perform a SUPP (Supplemental Qualifiers) merge."""
        left_domain = rule_spec.get("domains", {}).get("Include", [None])[0]

        if not left_domain:
            logger.warning("No left domain specified for SUPP merge")
            return None

        supp_query = """
            SELECT dataset_id
            FROM public.data_metadata
            WHERE (dataset_is_supp = true OR dataset_id LIKE 'supp%' OR dataset_id LIKE 'sq%')
            AND dataset_rdomain = %s
            LIMIT 1
        """

        self.pgi.execute_sql(supp_query, (left_domain,))
        result = self.pgi.fetch_one()

        if not result:
            logger.warning(f"No SUPP dataset found for domain {left_domain}")
            return None

        supp_table = result["dataset_id"]
        merge_id = f"supp_{left_domain.lower()}"

        if merge_id in self._merged_datasets_cache:
            logger.info(f"Using cached merge: {merge_id}")
            return merge_id

        merge_query = f"""
            CREATE TABLE IF NOT EXISTS public.{merge_id} AS
            WITH supp_pivot AS (
                SELECT
                    studyid,
                    usubjid,
                    idvar,
                    idvarval,
                    MAX(CASE WHEN qnam IS NOT NULL THEN qnam END) as qnam,
                    MAX(CASE WHEN qval IS NOT NULL THEN qval END) as qval
                FROM public.{supp_table}
                GROUP BY studyid, usubjid, idvar, idvarval
            ),
            left_data AS (
                SELECT * FROM public.{left_domain.lower()}
            ),
            merged_data AS (
                SELECT
                    l.*,
                    s.qnam as supp_qnam,
                    s.qval as supp_qval
                FROM left_data l
                LEFT JOIN supp_pivot s
                    ON l.studyid = s.studyid
                    AND l.usubjid = s.usubjid
            )
            SELECT * FROM merged_data
        """

        self.pgi.execute_sql(merge_query)
        self._merged_datasets_cache.add(merge_id)
        self._add_merged_dataset_metadata(merge_id, left_domain, "supp_merged")

        logger.info(f"Created SUPP merge: {merge_id}")
        return merge_id

    def _add_merged_dataset_metadata(self, merge_id: str, left_domain: str, merge_type: str) -> None:
        """Add metadata entries for a newly created merged dataset."""
        timestamp = datetime.now().astimezone()

        column_query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = '{merge_id}'
        """

        self.pgi.execute_sql(column_query)
        columns = self.pgi.fetch_all()

        for column in columns:
            insert_query = """
                INSERT INTO public.data_metadata (
                    created_at,
                    updated_at,
                    dataset_filename,
                    dataset_filepath,
                    dataset_id,
                    dataset_name,
                    dataset_domain,
                    dataset_is_split,
                    dataset_unsplit_name,
                    dataset_preprocessed,
                    var_name,
                    var_type,
                    preprocessing_stage
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            values = (
                timestamp,
                timestamp,
                f"{merge_id}.xpt",
                "preprocessed",
                merge_id,
                merge_id,
                left_domain,
                False,
                merge_id,
                timestamp,
                column["column_name"].upper(),
                column["data_type"],
                merge_type,
            )

            self.pgi.execute_sql(insert_query, values)

    def _update_metadata(self, timestamp: datetime) -> Dict[str, int]:
        """Update metadata for all preprocessed datasets."""
        logger.info("Updating metadata")

        split_update_query = """
            UPDATE public.data_metadata
            SET
                dataset_preprocessed = %s,
                preprocessing_stage = 'split_processed',
                updated_at = %s
            WHERE dataset_is_split = true
        """

        affected_split = self.pgi.execute_sql(split_update_query, (timestamp, timestamp))

        relrec_update_query = """
            UPDATE public.data_metadata
            SET
                preprocessing_stage = 'relrec_ready',
                updated_at = %s
            WHERE contains_relrec_refs = true
        """

        affected_relrec = self.pgi.execute_sql(relrec_update_query, (timestamp,))

        co_update_query = """
            UPDATE public.data_metadata
            SET
                preprocessing_stage = 'co_ready',
                updated_at = %s
            WHERE contains_co_refs = true
        """

        affected_co = self.pgi.execute_sql(co_update_query, (timestamp,))

        supp_update_query = """
            UPDATE public.data_metadata
            SET
                preprocessing_stage = 'supp_ready',
                updated_at = %s
            WHERE contains_supp_refs = true
        """

        affected_supp = self.pgi.execute_sql(supp_update_query, (timestamp,))

        return {
            "split_datasets_updated": affected_split,
            "relrec_ready_datasets": affected_relrec,
            "co_ready_datasets": affected_co,
            "supp_ready_datasets": affected_supp,
            "total_updated": affected_split + affected_relrec + affected_co + affected_supp,
        }

    def _log_validation_errors_to_db(self, run_id: str = None) -> None:
        """Log validation errors with row numbers if it exists to database."""

        if not self._validation_errors:
            return

        for error in self._validation_errors:
            insert_query = """
                INSERT INTO public.preprocessing_validation_errors (
                    run_id,
                    validation_type,
                    source_table,
                    row_number,
                    studyid,
                    usubjid,
                    rdomain,
                    idvar,
                    idvarval,
                    error_message,
                    severity
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            values = (
                run_id,
                error.get("type"),
                error.get("table"),
                error.get("row_number", -1),
                error.get("studyid"),
                error.get("usubjid"),
                error.get("rdomain"),
                error.get("idvar"),
                error.get("idvarval"),
                error.get("error"),
                "ERROR",
            )

            self.pgi.execute_sql(insert_query, values)

    def get_preprocessing_status(self) -> Dict[str, Any]:
        """Get current preprocessing status, statistics and run history."""
        status_query = """
            SELECT
                preprocessing_stage,
                COUNT(DISTINCT dataset_id) as dataset_count,
                COUNT(DISTINCT var_name) as variable_count
            FROM public.data_metadata
            GROUP BY preprocessing_stage
        """

        self.pgi.execute_sql(status_query)
        results = self.pgi.fetch_all()

        status = {}
        if results:
            for result in results:
                stage = result["preprocessing_stage"] or "raw"
                status[stage] = {"datasets": result["dataset_count"], "variables": result["variable_count"]}

        history_query = """
            SELECT
                run_id,
                timestamp,
                stage,
                (SELECT COUNT(*) FROM preprocessing_validation_errors WHERE run_id = pr.run_id) as error_count
            FROM public.preprocessing_results pr
            WHERE stage = 'complete'
            ORDER BY timestamp DESC
            LIMIT 10
        """

        self.pgi.execute_sql(history_query)
        history = self.pgi.fetch_all()

        status["cached_merges"] = len(self._merged_datasets_cache)
        status["validation_errors"] = len(self._validation_errors)
        status["run_history"] = history if history else []

        return status

    def get_validation_errors(self) -> List[Dict[str, Any]]:
        """Get all validation errors found during preprocessing."""
        return self._validation_errors

    def clear_validation_errors(self) -> None:
        """Clear stored validation errors."""
        self._validation_errors = []

    def _store_preprocessing_results(self, results: Dict[str, Any], run_id: str, timestamp: datetime) -> None:
        """Store preprocessing results in the database."""

        insert_query = """
            INSERT INTO public.preprocessing_results (
                run_id, timestamp, stage, results, validation_errors, metadata_updates
            ) VALUES (
                %s, %s, %s, %s, %s, %s
            )
        """

        self.pgi.execute_sql(
            insert_query,
            (
                run_id,
                timestamp,
                "complete",
                json.dumps(results),
                json.dumps(results.get("validation_errors", [])),
                json.dumps(results.get("metadata_updates", {})),
            ),
        )

        stages = ["split_processing", "relrec_catalog", "co_catalog", "supp_catalog"]
        for stage in stages:
            if stage in results and results[stage]:
                self.pgi.execute_sql(insert_query, (run_id, timestamp, stage, json.dumps(results[stage]), None, None))
