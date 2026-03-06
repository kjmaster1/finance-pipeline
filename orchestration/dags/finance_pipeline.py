from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os

# Add ingestion directory to Python path so Airflow can find our modules
sys.path.insert(0, '/opt/airflow/ingestion')

# ─────────────────────────────────────────────
# Default arguments applied to every task
# These are standard Airflow best practices:
# - Don't catch up on missed runs
# - Retry once if a task fails
# - Wait 5 minutes before retrying
# ─────────────────────────────────────────────
default_args = {
    'owner': 'finance-pipeline',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# ─────────────────────────────────────────────
# Task functions
# Each function is one unit of work.
# Airflow calls these functions when the task runs.
# ─────────────────────────────────────────────

def ingest_sample_files():
    """
    Ingest all CSV files from the sample data directory.
    In production this would scan the raw/ directory for new files.
    """
    from bronze_loader import ingest_file

    sample_dir = '/opt/airflow/data/sample'
    results = {}

    for filename in os.listdir(sample_dir):
        if filename.endswith('.csv'):
            filepath = os.path.join(sample_dir, filename)
            print(f"Ingesting {filename}...")
            result = ingest_file(filepath)
            results[filename] = result

    print(f"Ingestion results: {results}")
    return results


def transform_to_silver():
    """Transform all unprocessed Bronze rows to Silver."""
    from silver_transformer import transform_bronze_to_silver
    return transform_bronze_to_silver()


def aggregate_to_gold():
    """Run all Gold layer aggregations."""
    from gold_aggregator import run_gold_aggregations
    return run_gold_aggregations()


def check_data_quality():
    """
    Basic data quality checks on the Silver layer.
    Raises an exception if quality checks fail — this will
    cause the task to fail and Airflow will alert us.

    In a production pipeline these checks would be much more
    extensive, possibly using a dedicated tool like Great Expectations.
    """
    import psycopg2

    conn = psycopg2.connect(
        host=os.getenv("FINANCE_DB_HOST", "finance-postgres"),
        port=int(os.getenv("FINANCE_DB_PORT", "5432")),
        dbname=os.getenv("FINANCE_DB_NAME", "finance"),
        user=os.getenv("FINANCE_DB_USER", "finance"),
        password=os.getenv("FINANCE_DB_PASSWORD", "finance"),
    )

    try:
        with conn.cursor() as cur:
            # Check 1: No null transaction dates
            cur.execute("""
                SELECT COUNT(*) FROM silver_transactions
                WHERE transaction_date IS NULL
            """)
            null_dates = cur.fetchone()[0]
            if null_dates > 0:
                raise ValueError(f"Data quality failure: {null_dates} rows with null dates")

            # Check 2: No zero amounts
            cur.execute("""
                SELECT COUNT(*) FROM silver_transactions
                WHERE amount = 0
            """)
            zero_amounts = cur.fetchone()[0]
            if zero_amounts > 0:
                print(f"Warning: {zero_amounts} transactions with zero amount")

            # Check 3: No future dates
            cur.execute("""
                SELECT COUNT(*) FROM silver_transactions
                WHERE transaction_date > CURRENT_DATE
            """)
            future_dates = cur.fetchone()[0]
            if future_dates > 0:
                raise ValueError(f"Data quality failure: {future_dates} rows with future dates")

            # Check 4: Silver row count matches Bronze
            cur.execute("SELECT COUNT(*) FROM bronze_transactions")
            bronze_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM silver_transactions")
            silver_count = cur.fetchone()[0]

            if silver_count < bronze_count:
                print(f"Warning: Silver ({silver_count}) has fewer rows than Bronze ({bronze_count})")

            print(f"Data quality checks passed. Bronze: {bronze_count}, Silver: {silver_count}")

    finally:
        conn.close()


# ─────────────────────────────────────────────
# DAG definition
# schedule_interval='@daily' means run once per day at midnight
# catchup=False means don't run for past dates we missed
# ─────────────────────────────────────────────
with DAG(
    dag_id='finance_pipeline',
    default_args=default_args,
    description='Ingest bank transactions, transform to silver, aggregate to gold',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['finance', 'etl'],
) as dag:

    # Start marker - purely visual in the Airflow UI
    start = EmptyOperator(task_id='start')

    # Ingest all CSV files to Bronze
    ingest = PythonOperator(
        task_id='ingest_to_bronze',
        python_callable=ingest_sample_files,
    )

    # Transform Bronze to Silver
    transform = PythonOperator(
        task_id='transform_to_silver',
        python_callable=transform_to_silver,
    )

    # Run data quality checks
    quality_check = PythonOperator(
        task_id='data_quality_checks',
        python_callable=check_data_quality,
    )

    # Aggregate Silver to Gold
    aggregate = PythonOperator(
        task_id='aggregate_to_gold',
        python_callable=aggregate_to_gold,
    )

    # End marker
    end = EmptyOperator(task_id='end')

    # ─────────────────────────────────────────────
    # Task dependencies
    # The >> operator means "then run"
    # This defines the DAG structure
    # ─────────────────────────────────────────────
    start >> ingest >> transform >> quality_check >> aggregate >> end