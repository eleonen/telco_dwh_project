"""
Logging Configuration Module.

Provides a centralized function to set up logging for the application.
"""

import logging
import sys
import os

def setup_logging() -> None:
    """
    Configures basic logging for the application.
    Logs to a file ('telco_etl.log' in project root) and to the console.
    """
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    log_file_path = os.path.join(project_root, 'telco_etl.log')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, mode='a'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info(f"Logging initialized. Log file at: {log_file_path}")