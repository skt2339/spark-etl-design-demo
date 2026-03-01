from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

# Default arguments with retry + backoff
default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

dag = DAG(
    dag_id="customer_ingestion_pipeline",
    default_args=default_args,
    description="Client ingestion pipeline with validation and reconciliation",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
)


# 1️⃣ Detect new files
def detect_new_files(**context):
    """
    - List files from cloud storage
    - Compare against ingestion_metadata table
    - Return only unprocessed files
    """
    # Placeholder logic
    new_files = ["s3://bucket/customer_delta_2024-03-01.csv"]

    if not new_files:
        raise ValueError("No new files found")

    context['ti'].xcom_push(key="new_files", value=new_files)


detect_files = PythonOperator(
    task_id="detect_new_files",
    python_callable=detect_new_files,
    provide_context=True,
    dag=dag,
)


# 2️⃣ Run data validation
def validate_data(**context):
    """
    - Run hard + soft validations
    - Raise exception on hard failure
    """
    files = context['ti'].xcom_pull(key="new_files")

    # Placeholder validation logic
    if not files:
        raise Exception("Validation failed: no files to process")


validate = PythonOperator(
    task_id="validate_data",
    python_callable=validate_data,
    provide_context=True,
    dag=dag,
)


# 3️⃣ Execute Spark job
spark_ingestion = BashOperator(
    task_id="spark_ingestion_job",
    bash_command="spark-submit src/scenario1_dedup_idempotent.py",
    dag=dag,
)


# 4️⃣ Generate reconciliation metrics
def reconcile(**context):
    """
    - Compare raw vs curated counts
    - Write metrics to monitoring table
    """
    print("Reconciling raw and curated counts...")


reconcile_metrics = PythonOperator(
    task_id="reconcile_metrics",
    python_callable=reconcile,
    provide_context=True,
    dag=dag,
)


# 5️⃣ Notification (success)
notify_success = EmailOperator(
    task_id="notify_success",
    to="data-team@example.com",
    subject="Ingestion Success",
    html_content="Customer ingestion pipeline completed successfully.",
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS
)


# 6️⃣ Notification (failure)
notify_failure = EmailOperator(
    task_id="notify_failure",
    to="data-team@example.com",
    subject="Ingestion Failed",
    html_content="Customer ingestion pipeline failed.",
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED
)


# Task Dependencies
detect_files >> validate >> spark_ingestion >> reconcile_metrics >> notify_success
[dectect_files, validate, spark_ingestion, reconcile_metrics] >> notify_failure
