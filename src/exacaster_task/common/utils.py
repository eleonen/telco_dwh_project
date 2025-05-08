"""
General Utility Functions.

This module contains miscellaneous helper functions that are broadly
applicable across the Exacaster Task application, such as alerting.
"""

import logging
import smtplib
from email.mime.text import MIMEText
from typing import List

from .. import config_settings as app_config

logger = logging.getLogger(__name__)

def send_alert(subject: str, issues_list: List[str]) -> None:
    """
    Sends an alert notification about issues found.
    Currently logs a warning. Can be extended to send emails if SMTP is configured.

    Args:
        subject (str): The subject line for the alert.
        issues_list (List[str]): A list of strings, each describing an issue.
    """
    body = "The following issues were detected during the Telco DWH ETL process:\n\n" + "\n".join(
        [f"- {issue}" for issue in issues_list]
    )
    logger.warning(f"ALERT TRIGGERED: Subject: {subject}\nBody:\n{body}")

    # Example Email Sending (requires SMTP config in config_settings.py and .env)
    if all([app_config.ALERT_SENDER_EMAIL, app_config.ALERT_RECEIVER_EMAIL, app_config.SMTP_SERVER]):
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = app_config.ALERT_SENDER_EMAIL
        msg['To'] = app_config.ALERT_RECEIVER_EMAIL
        
        try:
            logger.info(f"Attempting to send alert email to {app_config.ALERT_RECEIVER_EMAIL} via {app_config.SMTP_SERVER}:{app_config.SMTP_PORT}")
            with smtplib.SMTP(app_config.SMTP_SERVER, app_config.SMTP_PORT) as server:
                server.set_debuglevel(0)
                if app_config.SMTP_USER and app_config.SMTP_PASSWORD:
                    server.ehlo()
                    if server.has_extn('STARTTLS'):
                        server.starttls()
                        server.ehlo()
                    server.login(app_config.SMTP_USER, app_config.SMTP_PASSWORD)
                server.sendmail(
                    app_config.ALERT_SENDER_EMAIL,
                    [addr.strip() for addr in app_config.ALERT_RECEIVER_EMAIL.split(',')],
                    msg.as_string()
                )
            logger.info(f"Alert email successfully sent to {app_config.ALERT_RECEIVER_EMAIL}")
        except Exception as e:
            logger.error(f"Failed to send alert email: {e}", exc_info=True)
    else:
        logger.warning("Email alert configuration (sender, receiver, server) is incomplete. Alert not sent via email.")