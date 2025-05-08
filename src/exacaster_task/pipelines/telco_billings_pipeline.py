"""
Telco Billings ETL Pipeline Module.

This module contains all the logic specific to processing the Telco Billings
usage data. It handles CSV validation, data loading into the database,
data quality checks, creation of analytical views, and data retention
for the telco billings data.
"""

import logging
import time
import os
import csv
import io
from psycopg2.extensions import connection as Psycopg2Connection, cursor as Psycopg2Cursor
from typing import Tuple, List, Optional

from ..common import sql_queries
from ..core.database import log_and_run_sql 
from .. import config_settings as app_config

logger = logging.getLogger(__name__)

def setup_pipeline_db_structure(conn: Psycopg2Connection) -> None:
    """
    Sets up database structures specific to the Telco Billings pipeline.
    Creates the main table, UUID function, and initial non-PK indexes.
    """
    step_start_time = time.time()
    logger.info("Setting up Telco Billings pipeline DDL structures...")
    try:
        with conn.cursor() as cursor:
            log_and_run_sql(cursor, "Creating main table 'telco_billings_usage'...", sql_queries.SQL_CREATE_MAIN_TABLE)
            log_and_run_sql(cursor, "Creating UUID generation SQL function...", sql_queries.SQL_GENERATE_UUID_FUNCTION)

            logger.info("Creating pipeline-specific performance indexes if they do not exist...")
            index_creation_start_time = time.time()
            for index_name, create_sql in sql_queries.NON_PK_INDEXES_TO_CREATE.items():
                log_and_run_sql(cursor, f"Creating index {index_name}...", create_sql)
            logger.info(f"Index creation part took {time.time() - index_creation_start_time:.2f}s.")
        conn.commit()
        logger.info(f"Telco Billings pipeline DDL setup complete in {time.time() - step_start_time:.2f}s.")
    except Exception as e:
        if conn and not conn.closed: 
            conn.rollback()
        raise

def validate_csv_structure(file_path: str) -> bool:
    """Validates basic CSV file existence and column structure for the first few rows."""
    logger.info(f"Validating CSV structure for Telco Billings: {file_path}")
    if not os.path.exists(file_path):
        msg = f"CSV Validation Error: File '{file_path}' not found."
        logger.error(msg)
        raise FileNotFoundError(msg)

    expected_cols = app_config.EXPECTED_CSV_COLUMNS
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            for i, row in enumerate(csv_reader):
                if i >= 5: break
                if len(row) != expected_cols:
                    msg = f"CSV Validation Error (Row {i+1}): Expected {expected_cols}, found {len(row)} in '{file_path}'."
                    logger.error(msg)
                    raise ValueError(msg)
        logger.info(f"CSV column count validation passed for '{file_path}'.")
        return True
    except Exception as e:
        logger.error(f"Error during CSV validation for '{file_path}': {e}", exc_info=True)
        raise

def load_telco_data(file_path: str, conn: Psycopg2Connection) -> int:
    """Loads Telco Billings data from CSV to the database."""
    overall_start_time = time.time()
    staging_table_name = f"telco_staging_{int(time.time())}"
    inserted_count: int = 0
    processed_rows_for_copy: int = 0
    staged_rows_count: int = 0
    staging_csv_buffer = io.StringIO()

    logger.info(f"Starting data load for Telco Billings from '{file_path}'.")

    try:
        with conn.cursor() as cursor:
            logger.info(f"Creating TEMP staging table: {staging_table_name}")
            cursor.execute(f"""
                CREATE TEMP TABLE {staging_table_name} (
                    customer_id INTEGER, event_start_time TIMESTAMP WITH TIME ZONE,
                    event_type VARCHAR(50), rate_plan_id INTEGER,
                    billing_flag_one INTEGER, billing_flag_two INTEGER,
                    duration FLOAT8, charge NUMERIC(18,8), month VARCHAR(7)
                ) ON COMMIT DROP;
            """)

            logger.info(f"Processing CSV '{file_path}' into in-memory buffer...")
            py_process_start_time = time.time()
            csv_writer = csv.writer(staging_csv_buffer)
            with open(file_path, 'r', encoding='utf-8') as infile:
                csv_reader = csv.reader(infile)
                for i, row_parts in enumerate(csv_reader):
                    if len(row_parts) != app_config.EXPECTED_CSV_COLUMNS:
                        logger.warning(f"Skipping malformed row {i+1} in '{file_path}'.")
                        continue
                    csv_writer.writerow(row_parts)
                    processed_rows_for_copy += 1
            staging_csv_buffer.seek(0)
            logger.info(f"In-memory CSV prepared in {time.time() - py_process_start_time:.2f}s ({processed_rows_for_copy} rows).")

            if processed_rows_for_copy == 0:
                logger.info("No valid rows in CSV to load.")
                return 0

            logger.info(f"Loading data from buffer to staging table '{staging_table_name}'...")
            copy_stg_start_time = time.time()
            cursor.copy_from(
                file=staging_csv_buffer, table=staging_table_name, sep=',', null='',
                columns=('customer_id', 'event_start_time', 'event_type', 'rate_plan_id',
                         'billing_flag_one', 'billing_flag_two', 'duration', 'charge', 'month')
            )
            cursor.execute(f"SELECT COUNT(*) FROM {staging_table_name}")
            res = cursor.fetchone()
            staged_rows_count = res[0] if res else 0
            logger.info(f"Loaded {staged_rows_count} rows into staging in {time.time() - copy_stg_start_time:.2f}s.")

            logger.info("Dropping non-PK indexes from 'telco_billings_usage' before main insert...")
            for index_name in sql_queries.NON_PK_INDEXES_TO_CREATE.keys():
                cursor.execute(f"SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = %s AND n.nspname = 'public';", (index_name.lower(),))
                if cursor.fetchone():
                    log_and_run_sql(cursor, f"Dropping index {index_name}...", f"DROP INDEX IF EXISTS {index_name};")

            logger.info("Inserting data from staging to 'telco_billings_usage'...")
            insert_main_start_time = time.time()
            sql_insert = f"""
                INSERT INTO telco_billings_usage (
                    customer_id, event_start_time, event_type, rate_plan_id,
                    billing_flag_one, billing_flag_two, duration, charge, month, event_uuid
                ) SELECT
                    s.*, generate_event_uuid(s.customer_id, s.event_start_time, s.event_type, s.rate_plan_id, s.charge)
                FROM {staging_table_name} s
                ON CONFLICT (event_uuid) DO NOTHING;
            """
            cursor.execute(sql_insert)
            inserted_count = cursor.rowcount if cursor.rowcount is not None else -1
            logger.info(f"INSERT from staging reported {inserted_count} affected rows in {time.time() - insert_main_start_time:.2f}s.")
            if inserted_count != -1 and staged_rows_count > 0:
                skipped_count = staged_rows_count - inserted_count if inserted_count <= staged_rows_count else 0
                logger.info(f"Estimated {skipped_count} duplicate rows skipped.")

            logger.info("Recreating non-PK indexes on 'telco_billings_usage'...")
            for index_name, create_sql in sql_queries.NON_PK_INDEXES_TO_RECREATE.items():
                log_and_run_sql(cursor, f"Recreating index {index_name}...", create_sql)
        
        conn.commit()
        logger.info(f"Telco Billings data loading complete in {time.time() - overall_start_time:.2f}s.")
    except Exception as e:
        if conn and not conn.closed: conn.rollback()
        logger.error(f"Error during Telco Billings data load: {e}", exc_info=True)
        raise
    finally:
        staging_csv_buffer.close()
    return inserted_count

def run_data_quality_checks(conn: Psycopg2Connection) -> Tuple[bool, List[str]]:
    """Runs data quality checks specific to the Telco Billings data."""
    logger.info("Performing Telco Billings data quality checks...")
    quality_good: bool = True
    issues: List[str] = []
    try:
        with conn.cursor() as cursor:
            logger.info("DQ Check: Missing values in recent data...")
            cursor.execute(sql_queries.SQL_CHECK_MISSING_VALUES)
            res_missing = cursor.fetchone()
            if res_missing:
                m_cid, m_time, m_etype, total_chk = (val if val is not None else 0 for val in res_missing)
                if total_chk > 0 and (m_cid > 0 or m_time > 0 or m_etype > 0):
                    issues.append(f"Missing values (cust/time/type): {m_cid}/{m_time}/{m_etype} in {total_chk} recent rows.")
                    quality_good = False
            else: logger.warning("DQ (Missing Values): Query returned no row.")

            logger.info("DQ Check: Future-dated events...")
            cursor.execute(sql_queries.SQL_CHECK_FUTURE_DATES)
            res_future = cursor.fetchone()
            if res_future:
                future_count = res_future[0] if res_future[0] is not None else 0
                if future_count > 0:
                    issues.append(f"Future-dated events: {future_count}.")
                    quality_good = False
            else: logger.warning("DQ (Future Dates): Query returned no row.")

        if issues: logger.warning(f"Telco Billings DQ Issues: {'; '.join(issues)}")
        else: logger.info("Telco Billings data quality checks passed.")
        return quality_good, issues
    except Exception as e:
        logger.error(f"Error during Telco Billings DQ checks: {e}", exc_info=True)
        return False, [f"DQ Exception: {str(e)}"]

def create_pipeline_analytics_views(conn: Psycopg2Connection) -> None:
    """Creates analytics views relevant to the Telco Billings pipeline."""
    logger.info("Creating/updating Telco Billings analytics views...")
    try:
        with conn.cursor() as cursor:
            log_and_run_sql(cursor, "Creating view analytics_usage_distribution...", sql_queries.SQL_CREATE_USAGE_DISTRIBUTION_VIEW)
            log_and_run_sql(cursor, "Creating view analytics_monthly_trends...", sql_queries.SQL_CREATE_MONTHLY_TRENDS_VIEW)
        conn.commit()
    except Exception as e:
        if conn and not conn.closed: conn.rollback()
        raise

def apply_pipeline_data_retention(conn: Psycopg2Connection) -> None:
    """Applies data retention policy for Telco Billings data."""
    if not app_config.ENABLE_RETENTION_POLICY:
        logger.info("Telco Billings data retention skipped (disabled in config).")
        return

    logger.info(f"Applying Telco Billings data retention (older than {app_config.RETENTION_PERIOD_MONTHS} months)...")
    try:
        with conn.cursor() as cursor:
            delete_sql = sql_queries.get_sql_delete_old_data(app_config.RETENTION_PERIOD_MONTHS)
            log_and_run_sql(cursor, "Executing DELETE for old Telco Billings records...", delete_sql)
            deleted_count = cursor.rowcount if cursor.rowcount is not None else "N/A"
            logger.info(f"Telco Billings retention: Deleted {deleted_count} old records.")
        conn.commit()
    except Exception as e:
        if conn and not conn.closed: conn.rollback()
        raise