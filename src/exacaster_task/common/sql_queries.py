"""
SQL Query Definitions.

This module centralizes all SQL query strings and DDL statements used by
the Telco DWH ETL project. This makes queries easier to manage and review.
"""

from typing import Dict

SQL_CREATE_MAIN_TABLE: str = """
CREATE TABLE IF NOT EXISTS telco_billings_usage (
    customer_id INTEGER,
    event_start_time TIMESTAMP WITH TIME ZONE,
    event_type VARCHAR(50),
    rate_plan_id INTEGER,
    billing_flag_one INTEGER,
    billing_flag_two INTEGER,
    duration FLOAT8,
    charge NUMERIC(18, 8),
    month VARCHAR(7),
    event_uuid VARCHAR(32) PRIMARY KEY
);
"""

SQL_GENERATE_UUID_FUNCTION: str = """
CREATE OR REPLACE FUNCTION generate_event_uuid(
    p_customer_id INTEGER,
    p_event_time TIMESTAMP WITH TIME ZONE,
    p_event_type TEXT,
    p_rate_plan_id INTEGER,
    p_charge NUMERIC
)
RETURNS VARCHAR(32) AS $$
BEGIN
    RETURN MD5(
        COALESCE(p_customer_id::TEXT, '') || '_' ||
        COALESCE(p_event_time::TEXT, '') || '_' ||
        COALESCE(p_event_type, '') || '_' ||
        COALESCE(p_rate_plan_id::TEXT, '') || '_' ||
        COALESCE(p_charge::TEXT, '')
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;
"""

SQL_CHECK_MISSING_VALUES: str = """
    SELECT
        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as missing_customer_id,
        SUM(CASE WHEN event_start_time IS NULL THEN 1 ELSE 0 END) as missing_time,
        SUM(CASE WHEN event_type IS NULL THEN 1 ELSE 0 END) as missing_event_type,
        COUNT(*) as total_rows_checked
    FROM telco_billings_usage
    WHERE (event_start_time >= (CURRENT_TIMESTAMP - INTERVAL '1 day')) AND (event_start_time < CURRENT_TIMESTAMP);
"""

SQL_CHECK_FUTURE_DATES: str = """
    SELECT COUNT(*) as future_date_count
    FROM telco_billings_usage
    WHERE event_start_time > CURRENT_TIMESTAMP;
"""

SQL_CREATE_USAGE_DISTRIBUTION_VIEW: str = """
    CREATE OR REPLACE VIEW analytics_usage_distribution AS
    SELECT
        event_type as service_type,
        rate_plan_id,
        COUNT(*) as event_count,
        SUM(duration) as total_duration,
        SUM(charge) as total_charge,
        COUNT(DISTINCT customer_id) as customer_count
    FROM telco_billings_usage
    GROUP BY event_type, rate_plan_id
    ORDER BY event_type, rate_plan_id;
"""

SQL_CREATE_MONTHLY_TRENDS_VIEW: str = """
    CREATE OR REPLACE VIEW analytics_monthly_trends AS
    SELECT
        month,
        event_type as service_type,
        COUNT(*) as event_count,
        COUNT(DISTINCT customer_id) as customer_count,
        SUM(duration) as total_duration,
        SUM(charge) as total_charge
    FROM telco_billings_usage
    GROUP BY month, event_type
    ORDER BY month, event_type;
"""

def get_sql_delete_old_data(retention_period_months: int) -> str:
    """
    Returns the SQL string for deleting old data based on the retention period.
    """
    return f"""
        DELETE FROM telco_billings_usage
        WHERE event_start_time < (CURRENT_TIMESTAMP - INTERVAL '{retention_period_months} months');
    """

NON_PK_INDEXES_TO_CREATE: Dict[str, str] = {
    "idx_billing_customer_id": "CREATE INDEX IF NOT EXISTS idx_billing_customer_id ON telco_billings_usage(customer_id);",
    "idx_billing_event_time": "CREATE INDEX IF NOT EXISTS idx_billing_event_time ON telco_billings_usage(event_start_time);",
    "idx_billing_event_type": "CREATE INDEX IF NOT EXISTS idx_billing_event_type ON telco_billings_usage(event_type);",
    "idx_billing_month": "CREATE INDEX IF NOT EXISTS idx_billing_month ON telco_billings_usage(month);"
}

NON_PK_INDEXES_TO_RECREATE: Dict[str, str] = {
    "idx_billing_customer_id": "CREATE INDEX idx_billing_customer_id ON telco_billings_usage(customer_id);",
    "idx_billing_event_time": "CREATE INDEX idx_billing_event_time ON telco_billings_usage(event_start_time);",
    "idx_billing_event_type": "CREATE INDEX idx_billing_event_type ON telco_billings_usage(event_type);",
    "idx_billing_month": "CREATE INDEX idx_billing_month ON telco_billings_usage(month);"
}
