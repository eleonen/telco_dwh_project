"""
Main Orchestrator for the Telco DWH ETL Process.

This script serves as the main entry point to run the ETL jobs.
It initializes configurations, sets up logging, and orchestrates
the sequence of operations by calling functions from various
modules within the exacaster_task package.
"""

import logging
import os
import sys
import time
from datetime import datetime
from typing import Optional
import psycopg2
import argparse

from .core.logger import setup_logging
from .core.database import managed_db_connection
from .pipelines import telco_billings_pipeline
from .common import utils as common_utils 
from . import config_settings as app_settings 

setup_logging()
logger = logging.getLogger(__name__)

def run_etl_process(csv_file_path: str) -> None:
    """
    Orchestrates the Telco Billings DWH ETL process.
    """
    process_start_time = time.time()
    current_step_start_time = process_start_time
    logger.info("=" * 80)
    logger.info(f"Starting Telco Billings DWH ETL process at {datetime.now():%Y-%m-%d %H:%M:%S}")
    logger.info(f"Input data file: '{csv_file_path}'")
    logger.info(f"Retention Policy Enabled: {app_settings.ENABLE_RETENTION_POLICY}, Period: {app_settings.RETENTION_PERIOD_MONTHS} months")
    logger.info("=" * 80)

    def log_step_duration(step_name_and_num: str) -> None:
        nonlocal current_step_start_time
        logger.info(f"STEP {step_name_and_num} completed in {time.time() - current_step_start_time:.2f}s.")
        current_step_start_time = time.time()

    try:
        telco_billings_pipeline.validate_csv_structure(csv_file_path)
        log_step_duration("1 (Input CSV Validation)")

        with managed_db_connection() as conn:
            conn.autocommit = False 

            logger.info("Database connection established successfully.")
            log_step_duration("2 (Database Connection)")

            telco_billings_pipeline.setup_pipeline_db_structure(conn)
            log_step_duration("3 (Pipeline DB Structure Setup)")

            rows_loaded_info = telco_billings_pipeline.load_telco_data(csv_file_path, conn)
            log_step_duration(f"4 (Data Loading - reported {rows_loaded_info} rows)")

            is_quality_good, dq_issues = telco_billings_pipeline.run_data_quality_checks(conn)
            if not is_quality_good:
                logger.warning(f"ALERT: Data quality issues detected: {dq_issues}")
                common_utils.send_alert("Telco DWH ETL: Data Quality Issues Found", dq_issues)
            log_step_duration("5 (Data Quality Checks)")

            telco_billings_pipeline.create_pipeline_analytics_views(conn)
            log_step_duration("6 (Analytics Views Creation)")

            telco_billings_pipeline.apply_pipeline_data_retention(conn)
            retention_status = "Executed" if app_settings.ENABLE_RETENTION_POLICY else "Skipped (disabled)"
            log_step_duration(f"7 (Data Retention Policy - {retention_status})")

        logger.info("=" * 80)
        logger.info(f"Telco Billings DWH ETL process completed successfully in {time.time() - process_start_time:.2f}s.")
        logger.info("=" * 80)

    except FileNotFoundError as e:
        logger.critical(f"ETL ABORTED (FileNotFound): {e}", exc_info=True)
        common_utils.send_alert("Telco ETL Critical Failure: Input File Missing", [str(e)])
        sys.exit(1)
    except ValueError as e:
        logger.critical(f"ETL ABORTED (ValueError): {e}", exc_info=True)
        common_utils.send_alert("Telco ETL Critical Failure: Data Validation Error", [str(e)])
        sys.exit(1)
    except psycopg2.Error as e:
        logger.critical(f"ETL ABORTED (DatabaseError): {e}", exc_info=True)
        common_utils.send_alert("Telco ETL Critical Failure: Database Error", [f"DB Error: {e.pgcode if hasattr(e, 'pgcode') else 'N/A'}. Check logs."])
        sys.exit(1)
    except Exception as e:
        logger.critical(f"ETL ABORTED (Unexpected Error): {e}", exc_info=True)
        common_utils.send_alert("Telco ETL Critical Failure: Unexpected System Error", [f"Error: {type(e).__name__}"])
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Telco DWH ETL process.")
    parser.add_argument(
        "--csv-path",
        type=str,
        help="Path to the input CSV file. Overrides the CSV_FILE_PATH environment variable."
    )
    args = parser.parse_args()
    if args.csv_path:
        input_csv = args.csv_path
        logger.info(f"Using CSV path from command-line argument: {input_csv}")
    else:
        input_csv = os.getenv("CSV_FILE_PATH", app_settings.DEFAULT_CSV_FILE_PATH)
        logger.info(f"Using CSV path from environment variable or default: {input_csv}")
    run_etl_process(input_csv)