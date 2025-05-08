"""
Application Configuration Settings.

This module loads environment variables (from .env) and defines
application-wide settings and constants used throughout the
Telco DWH ETL project.
"""

import os
from dotenv import load_dotenv
from typing import Dict

dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
else:
    load_dotenv()

DEFAULT_CSV_FILE_PATH: str = "/app/data/usage_sample.csv"

EXPECTED_CSV_COLUMNS: int = 9

ENABLE_RETENTION_POLICY: bool = bool(os.getenv("ENABLE_RETENTION_POLICY", "False").lower() == "true")
RETENTION_PERIOD_MONTHS: int = int(os.getenv("RETENTION_PERIOD_MONTHS", "6"))

# Alerting Configuration (Example)
# These would be used by a notification utility if implemented
ALERT_SENDER_EMAIL: str | None = os.getenv("ALERT_SENDER_EMAIL")
ALERT_RECEIVER_EMAIL: str | None = os.getenv("ALERT_RECEIVER_EMAIL")
SMTP_SERVER: str | None = os.getenv("SMTP_SERVER")
SMTP_PORT: int = int(os.getenv("SMTP_PORT", 587))
SMTP_USER: str | None = os.getenv("SMTP_USER")
SMTP_PASSWORD: str | None = os.getenv("SMTP_PASSWORD")