from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator  # type: ignore
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator                # type: ignore
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator              # type: ignore
from airflow import DAG                                                                     # type: ignore
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig              # type: ignore    
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping                     # type: ignore 
from airflow.models.baseoperator import chain                                                   # type: ignore
import logging
from datetime import datetime
import os

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# --- Helper function ---
def parse_csv(filepath):
    import pandas as pd
    
    """Nettoyage et parsing du CSV avant insertion"""
    df = pd.read_csv(filepath, encoding="utf-8")
    df = df.drop_duplicates(subset="InvoiceNo", keep="first")
    df = df[df["Quantity"] > 0]
    df = df[df["UnitPrice"] >= 0]
    df = df[df["CustomerID"].notnull()]
    df["CustomerID"] = df["CustomerID"].astype(str)
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    df = df[df["InvoiceDate"].notnull()]
    for row in df.itertuples(index=False, name=None):
        yield row


# --- Cosmos Configuration ---
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
DBT_PROJECT_PATH = f"{AIRFLOW_HOME}/include/dbt_retail"
DBT_EXECUTABLE_PATH = f"{AIRFLOW_HOME}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="dbt_retail",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",
        profile_args={"schema": "analytics"},
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


# --- DAG Definition ---
with DAG(
    dag_id="retail_data_pipeline",
    # schedule_interval= every 5 minutes
    schedule="*/5 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["retail", "data_pipeline", "dbt"],
) as dag:

    # Task 1: Upload local retail data to S3
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename="./include/dataset/online_retail_data.csv",
        dest_key="data/retail_data.csv",
        dest_bucket="retail-bucket",
        aws_conn_id="aws_default",
        replace=True,
    )

    # Task 2: Create schema and table if not exists
    create_schema_and_table = SQLExecuteQueryOperator(
        task_id="create_schema_and_table_postgres",
        conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS stg_retail;
        CREATE TABLE IF NOT EXISTS stg_retail.raw_invoices (
            InvoiceNo VARCHAR(50),
            StockCode VARCHAR(50),
            Description TEXT,
            Quantity INTEGER,
            InvoiceDate TIMESTAMP,
            UnitPrice NUMERIC(10, 2),
            CustomerID VARCHAR(100),
            Country VARCHAR(100),
            PRIMARY KEY (InvoiceNo)
        );
        """,
    )

    # Task 3: Clear old data
    clear_table = SQLExecuteQueryOperator(
        task_id="clear_table",
        conn_id="postgres_default",
        sql="TRUNCATE TABLE stg_retail.raw_invoices;"
    )

    # Task 4: Load cleaned CSV data from S3 to PostgreSQL
    load_data_to_postgres = S3ToSqlOperator(
        task_id="load_data_to_postgres",
        s3_key="data/retail_data.csv",
        s3_bucket="retail-bucket",
        table="stg_retail.raw_invoices",
        parser=parse_csv,
        column_list=[
            "InvoiceNo", "StockCode", "Description",
            "Quantity", "InvoiceDate", "UnitPrice",
            "CustomerID", "Country"
        ],
        commit_every=100,
        schema=None,
        sql_conn_id="postgres_default",
        aws_conn_id="aws_default",
        sql_hook_params={"on_conflict": "do_nothing"},
    )

    # Task 5: Run dbt transformations via Cosmos
    transform_retail_data = DbtTaskGroup(
        group_id="transform_retail_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH+'/transform'),
        profile_config=profile_config,
        execution_config=execution_config,
        default_args={"retries": 2},
    )
    
     # Task 6: Run dbt report via Cosmos
    transform_retail_data = DbtTaskGroup(
        group_id="report_retail_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH+'/marts'),
        profile_config=profile_config,
        execution_config=execution_config,
        default_args={"retries": 2},
    )

    # --- Task Dependencies ---
    chain(upload_to_s3 ,create_schema_and_table ,load_data_to_postgres ,transform_retail_data)
