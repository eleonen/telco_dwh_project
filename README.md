# Telco DWH ETL Project (Exacaster Homework Task)

This project implements an ETL (Extract, Transform, Load) pipeline for processing Telco billing usage data. It loads data from a CSV file into a PostgreSQL database, performs data quality checks, creates analytical views, and handles data retention based on specified requirements. The application is packaged using Docker for consistent and reproducible execution.

This project was completed as a homework task for a Junior Big Data Engineer position application.

## Project Structure

```text
telco_dwh_project/
├── .env.example         <-- Example environment variables needed
├── .gitignore
├── .dockerignore        <-- Specifies files to ignore for Docker build
├── Dockerfile           <-- Recipe for building the Docker image
├── pyproject.toml       <-- Python dependencies (Poetry)
├── poetry.lock
├── README.md            <-- This file
├── src/                 <-- Source code root
│   └── exacaster_task/  <-- Main Python package
│       ├── __init__.py
│       ├── config_settings.py
│       ├── main.py
│       ├── core/          <-- Core components (DB, Logger)
│       │   ├── __init__.py
│       │   ├── database.py
│       │   └── logger.py
│       ├── common/        <-- Common utilities (SQL Queries, Alerts)
│       │   ├── __init__.py
│       │   ├── sql_queries.py
│       │   └── utils.py
│       └── pipelines/     <-- ETL pipeline logic
│           ├── __init__.py
│           └── telco_billings_pipeline.py
└── (usage.csv)   <-- Needs to be placed here manually (Not in Git)
```

## Prerequisites

*   **Docker & Docker Compose:** Required for the recommended way to run the application. Install from [Docker official website](https://www.docker.com/products/docker-desktop/). Docker Compose is usually included.
*   **Git:** For cloning the repository.
*   **Input CSV Data:** The `usage.csv` (or sampled `usage_sample.csv`) file provided for the task. This file is **not** included in the repository and must be obtained separately.
*   **PostgreSQL Server:** A running PostgreSQL instance (version 12+ recommended). This can be running locally or as another Docker container. The database specified in the `.env` file must exist.

*(Optional for Local Development without Docker)*
*   **Python:** Version 3.11+ (as specified in `pyproject.toml` and `Dockerfile`).
*   **Poetry:** For managing Python dependencies locally. Install from [Poetry official website](https://python-poetry.org/docs/#installation).

## Setup Instructions

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/eleonen/telco_dwh_project
    cd telco_dwh_project
    ```

2.  **Place Input Data:**
    *   Copy the `usage.csv` file (or `usage_sample.csv` if preferred) provided for the task into the project's root directory (`telco_dwh_project/`).

3.  **Configure Environment Variables:**
    *   Rename the `.env.example` file to `.env`.
        ```bash
        # Linux/macOS/Git Bash
        cp .env.example .env
        # Windows Cmd
        # copy .env.example .env
        ```
    *   **VERY IMPORTANT:** Edit the `.env` file with your specific PostgreSQL connection details and desired settings. Pay close attention to `TELCO_DATABASE_HOST` depending on how you run PostgreSQL and Docker.

        ```dotenv
        # .env - EDIT THIS FILE

        # --- Database Connection ---

        TELCO_DATABASE_NAME=telco_database # Make sure this database exists
        TELCO_DATABASE_USER=postgres       # Your PostgreSQL user
        TELCO_DATABASE_PASSWORD=your_secret_pg_password # Your PostgreSQL password
        
        # If running PostgreSQL on your HOST machine and using Docker Desktop (Win/Mac):
        TELCO_DATABASE_HOST=host.docker.internal
        # If running PostgreSQL on your HOST machine and using Docker on Linux:
        # TELCO_DATABASE_HOST=172.17.0.1 # (Often the default Docker bridge IP, check yours)
        # If running PostgreSQL in ANOTHER Docker container:
        # TELCO_DATABASE_HOST=<name_of_postgres_container> # (Requires Docker networking setup)
        # If running WITHOUT Docker (local Python):
        # TELCO_DATABASE_HOST=localhost

        TELCO_DATABASE_PORT=5432
        

        # --- ETL Settings ---
        # Path where the application looks for the CSV *inside* the container
        # This MUST match the target path in the 'docker run -v' command's volume mount.
        CSV_FILE_PATH=/app/data/usage.csv

        # --- Retention Policy ---
        # Set to true to delete data older than RETENTION_PERIOD_MONTHS, false to keep all data.
        ENABLE_RETENTION_POLICY=true
        RETENTION_PERIOD_MONTHS=6

        # --- Alerting (Optional - Email) ---
        # Uncomment and fill if you want to test email alerts via common.utils.send_alert
        # SMTP_USER= # Often same as sender email
        # SMTP_PASSWORD= # App password or SMTP password
        # SMTP_SERVER=
        # SMTP_PORT=587
        # ALERT_SENDER_EMAIL=
        # ALERT_RECEIVER_EMAIL= # Can be comma-separated for multiple recipients
        ```

## Running the Application

### Option 1: Using Docker (Recommended)

This ensures the application runs in a consistent environment matching the one defined in the `Dockerfile`.

1.  **Build the Docker Image:**
    (Make sure Docker Desktop / Docker Daemon is running)
    From the project root directory (`telco_dwh_project/`):
    ```bash
    docker build -t telco-etl-app .
    ```

2.  **Run the ETL Process Container:**
    From the project root directory:
    ```bash
    # --- Linux/macOS/Git Bash ---
    docker run --rm --name my_telco_etl_run \
      --env-file .env \
      -v "$(pwd)/usage.csv:/app/data/usage.csv:ro" \
      -v "$(pwd)/telco_etl.log:/app/telco_etl.log" \
      telco-etl-app

    # --- Windows Command Prompt (cmd.exe) ---
    # docker run --rm --name my_telco_etl_run --env-file .env -v "%cd%/usage.csv:/app/data/usage.csv:ro" -v "%cd%/telco_etl.log:/app/telco_etl.log" telco-etl-app

    # --- Windows PowerShell ---
    # docker run --rm --name my_telco_etl_run --env-file .env -v "${pwd}/usage.csv:/app/data/usage.csv:ro" -v "${pwd}/telco_etl.log:/app/telco_etl.log" telco-etl-app
    ```
    *   `--rm`: Automatically removes the container when the process finishes.
    *   `--name`: Assigns a temporary name to the running container.
    *   `--env-file .env`: Loads environment variables from your configured `.env` file.
    *   `-v HOST_PATH:CONTAINER_PATH:ro`: Mounts files/directories.
        *   Mounts your local `usage.csv` to `/app/data/usage.csv` inside the container (read-only).
        *   Mounts your local `telco_etl.log` to `/app/telco_etl.log` inside the container, allowing logs generated inside to persist on your host.
    *   `telco-etl-app`: The name of the image to run.

3.  **Monitor Output:** Observe logs in the terminal and check the `telco_etl.log` file. Verify results in your PostgreSQL database.

### Option 2: Running Locally with Poetry (For Development)

1.  Ensure Python 3.11+ and Poetry are installed.
2.  Follow Setup steps 1-3 above (Clone, Place Data, Configure `.env`). **Important:** Set `TELCO_DATABASE_HOST=localhost` (or your DB address) in `.env` for local runs.
3.  Install dependencies:
    ```bash
    cd telco_dwh_project
    poetry install
    ```
4.  Activate the virtual environment:
    ```bash
    poetry shell
    ```
5.  Run the main script (ensure your current directory is `telco_dwh_project/`):
    ```bash
    python -m src.exacaster_task.main --csv-path usage.csv
    ```
    *(The `-m` flag runs the module, allowing relative imports within the `src` directory to work correctly)*

## Homework Task Implementation Details

### Task 1: DWH Design

*   **Initial Table:** A single fact table `telco_billings_usage` is created to store the raw usage data.
    *   **Schema:**
        *   `customer_id` (INTEGER)
        *   `event_start_time` (TIMESTAMP WITH TIME ZONE)
        *   `event_type` (VARCHAR(50)) - e.g., VOICE, DATA
        *   `rate_plan_id` (INTEGER)
        *   `billing_flag_one` (INTEGER) - Meaning TBD
        *   `billing_flag_two` (INTEGER) - Meaning TBD
        *   `duration` (FLOAT8)
        *   `charge` (NUMERIC(18, 8))
        *   `month` (VARCHAR(7)) - e.g., "2016-01"
        *   `event_uuid` (VARCHAR(32)) - **Primary Key**, generated hash.
*   **Analytical Views:** To meet the Product team's initial requirement ("usage distribution and number of customers by Service Type and Rate Plan"):
    *   `analytics_usage_distribution`: Aggregates data by `event_type` (Service Type) and `rate_plan_id`, providing `event_count`, `total_duration`, `total_charge`, and distinct `customer_count`. This directly answers their request and can be easily consumed by BI tools.
    *   `analytics_monthly_trends`: Provides monthly aggregations by service type for observing trends over time.

### Task 2: ETL Scripts

The ETL process is implemented as a structured Python package (`src/exacaster_task`) and orchestrated by `main.py`.

*   **ETL Flow:**
    1.  **Validation (`validate_csv_structure`):** Checks for CSV file existence and correct column count in initial rows.
    2.  **DB Connection:** Establishes a connection using parameters from `.env`.
    3.  **DDL Setup (`setup_pipeline_db_structure`):** Ensures the target table, helper functions (UUID generation), and necessary indexes exist using `CREATE IF NOT EXISTS`.
    4.  **Load Data (`load_telco_data`):**
        *   Reads the CSV into an in-memory buffer (`io.StringIO`).
        *   Uses PostgreSQL's efficient `COPY` command to load data from the buffer into a temporary staging table.
        *   Drops non-PK indexes on the target table (`telco_billings_usage`) to optimize insert speed.
        *   Inserts data from the staging table into `telco_billings_usage`.
        *   **DB-Side UUIDs:** Calls the `generate_event_uuid` SQL function during insert to create a deterministic hash based on key record fields.
        *   **Idempotency:** Uses `ON CONFLICT (event_uuid) DO NOTHING` to prevent duplicate records from being inserted if the same data is processed multiple times.
        *   Recreates the non-PK indexes.
    5.  **Data Quality Checks (`run_data_quality_checks`):** Queries the database to check for critical nulls in recent data and any future-dated events.
    6.  **Views Creation (`create_pipeline_analytics_views`):** Creates/replaces the analytical views using `CREATE OR REPLACE VIEW`.
    7.  **Retention (`apply_pipeline_data_retention`):** Deletes records older than the configured period (if enabled via `ENABLE_RETENTION_POLICY`).
*   **Key Decisions:**
    *   **Modularity:** Code is split into logical modules (core, common, pipelines) for maintainability.
    *   **Configuration:** Environment variables (`.env`) used for sensitive/environment-specific settings.
    *   **DB-Side UUIDs:** Leverages PostgreSQL's MD5 function for efficient, deterministic record identification within the database transaction.
    *   **Staging Table & Bulk Load:** Uses `COPY` for efficient data ingestion.
    *   **Index Management:** Drops/recreates indexes around bulk load for performance.
    *   **Idempotency:** `ON CONFLICT` ensures rerunning the ETL on the same data doesn't create duplicates.

### Task 3: Support and Monitoring

*   **Logging:** The Python `logging` module is configured (`core/logger.py`) to log messages with timestamps, levels, and module names to both a file (`telco_etl.log` at the project root) and the console (`stdout`). This provides detailed traceability for successful runs and errors.
*   **Data Quality Alerts:**
    *   `pipelines/telco_billings_pipeline.py` includes `run_data_quality_checks` which executes SQL queries (`common/sql_queries.py`) to detect:
        *   Missing `customer_id`, `event_start_time`, or `event_type` in data loaded within the last day.
        *   Any records with `event_start_time` in the future.
    *   If issues are found, a warning is logged, and the `common.utils.send_alert` function is called.
    *   Currently, `send_alert` logs a prominent warning. It includes placeholder logic to send email alerts if SMTP details are configured in `.env` and `config_settings.py`, allowing "asap" notification to relevant teams (e.g., the billing provider contact).
    *   Further DQ checks (e.g., range checks, consistency checks) can be added easily.
*   **Error Handling:** The main orchestration logic in `main.py` uses `try...except` blocks to catch specific errors (`FileNotFoundError`, `ValueError`, `psycopg2.Error`) and general `Exception`s. Critical errors are logged, an attempt is made to send an alert, database transactions are rolled back (if applicable), and the script exits with a non-zero status code (`sys.exit(1)`) to signal failure to potential schedulers.
*   **Monitoring ETL Runs (Production Considerations):**
    *   **Scheduling:** In a production environment, this script would be triggered by a scheduler (e.g., Linux `cron`, systemd timers, Apache Airflow, Google Cloud Scheduler).
    *   **Status Monitoring:** The scheduler would monitor the exit code of the script (0 for success, non-zero for failure). Scheduler logs would capture `stdout`/`stderr`.
    *   **Log Aggregation:** Logs (`telco_etl.log`) would typically be forwarded to a central logging system (e.g., Elasticsearch/Logstash/Kibana (ELK), Splunk, AWS CloudWatch Logs, Google Cloud Logging).
    *   **Metrics:** Key metrics (e.g., run duration, rows processed, rows inserted, DQ issues found) could be explicitly logged or pushed to a monitoring system (e.g., Prometheus/Grafana, Datadog, CloudWatch Metrics) for dashboarding and alerting on anomalies (e.g., run takes too long, 0 rows processed).

### Task 4: DWH Evolution & Challenges

The current single-table DWH is a starting point. Future evolution would likely involve:

*   **Data Sources & Modeling:**
    *   **New Tables:** Incorporating new data sources (customer details, product catalog, marketing info) would require creating new tables. Typically, **Dimension Tables** (e.g., `dim_customer`, `dim_rate_plan`, `dim_date`) would be created to store descriptive attributes.
    *   **Schema:** The `telco_billings_usage` table would become a **Fact Table**. The DWH would likely evolve into a **Star Schema** (or Snowflake Schema), where the central fact table connects to multiple dimension tables via foreign keys (e.g., `telco_billings_usage.customer_id` links to `dim_customer.customer_sk`). This improves query performance, data consistency, and analytical flexibility.
    *   **Data Lake:** For handling very raw, large-volume, or semi-structured data sources before they are cleaned and loaded into the DWH, a Data Lake (e.g., files stored on AWS S3, Google Cloud Storage, Azure Blob Storage) might be introduced. The DWH would then source data from the curated parts of the Data Lake.
*   **ETL Complexity:**
    *   Integrating new sources requires more complex transformation logic (joining, cleaning, standardizing).
    *   Tools like dbt (data build tool) might be introduced for managing SQL transformations.
    *   Workflow orchestrators (Airflow, Prefect, Dagster) would become essential for managing dependencies between different ETL jobs.
*   **Technical Challenges (Scalability):**
    *   **Database Scaling:** As data volume grows beyond what a single PostgreSQL instance can handle efficiently:
        *   Vertical Scaling: More powerful server (CPU, RAM, IO).
        *   PostgreSQL Features: Table partitioning (e.g., by `event_start_time`), connection pooling (PgBouncer), read replicas.
        *   Distributed DWH: Migration to cloud-native data warehouses built for massive scale (e.g., Amazon Redshift, Google BigQuery, Snowflake).
    *   **ETL Processing Scaling:** If the Python script becomes too slow for larger files or complex logic:
        *   Optimization: Further optimizing SQL, Python code, and database tuning.
        *   **Distributed Processing:** Migrating the transformation logic to a distributed framework like **Apache Spark**. Spark could read the CSV in parallel, perform transformations across a cluster, and write to the database (potentially still using a staging table approach for the final merge/conflict handling).
*   **Organizational Challenges:**
    *   **Data Governance:** Establishing clear rules for data definitions, quality standards, access control, security, and compliance (like GDPR for the 6-month retention).
    *   **Team Growth:** Expanding from a single technical expert to a dedicated data team (engineers, analysts, potentially BI developers, data scientists).
    *   **Communication:** Maintaining effective communication between the data team and business stakeholders (Product, Marketing, Legal, Finance) regarding requirements, priorities, and data issues.
    *   **Change Management:** Implementing processes to manage changes to source systems, DWH schema, ETL logic, and reports without breaking downstream dependencies.
    *   **Prioritization:** Balancing requests for new data sources, features, reports, and technical improvements with limited resources.

## Cloud Deployment Strategy

This Dockerized application is well-suited for cloud deployment. A typical cloud deployment strategy would involve:

1.  **Container Registry:** Build the Docker image (`telco-etl-app`) using a CI/CD pipeline and push it to a private container registry (e.g., AWS ECR, Google Artifact Registry, Azure ACR).
2.  **Database:** Use a managed PostgreSQL service (e.g., AWS RDS for PostgreSQL, Google Cloud SQL for PostgreSQL, Azure Database for PostgreSQL) for scalability, backups, and easier management. Configure the `.env` file (managed via secure secrets management in the cloud) with the cloud DB connection details.
3.  **Data Ingestion:** Configure the billing provider to drop the weekly CSV file into cloud object storage (e.g., AWS S3, Google Cloud Storage (GCS), Azure Blob Storage).
4.  **ETL Execution:**
    *   **Option A (Serverless Container):** Use services like AWS Fargate, Google Cloud Run, or Azure Container Instances to run the Docker container on a schedule or triggered by the file arrival in object storage. The container would pull the image from the registry, read credentials from a secret manager, read the CSV from object storage, and connect to the managed database.
    *   **Option B (Workflow Orchestration):** Use a managed workflow service (AWS Step Functions, Google Cloud Composer (Airflow), Azure Data Factory) to define the ETL pipeline. One step in the workflow would execute the Docker container (using services like Fargate/Cloud Run/ACI). This is better for complex pipelines with multiple steps or dependencies.
    *   **Option C (Managed ETL Service with Spark):** For very large scale, rewrite the core transformation logic using a managed Spark service (AWS EMR, AWS Glue, Google Dataproc, Azure Databricks/HDInsight) reading from/writing to cloud storage and the managed database.
5.  **Scheduling:** Use cloud-native schedulers (AWS EventBridge Scheduler, Google Cloud Scheduler, Azure Logic Apps timer) to trigger the ETL process (e.g., daily or weekly).
6.  **Logging & Monitoring:** Configure the application/container to send logs to the cloud provider's logging service (AWS CloudWatch Logs, Google Cloud Logging, Azure Monitor Logs). Use cloud monitoring services (CloudWatch Metrics, Cloud Monitoring, Azure Monitor) to track execution status, duration, resource usage, and potentially custom metrics. Set up alerts for failures or anomalies.

This cloud-native approach provides scalability, reliability, and integrates well with other cloud services.
