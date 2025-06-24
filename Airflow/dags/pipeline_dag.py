
from airflow import DAG
import pendulum
import sys
from airflow.operators.python import PythonOperator
import logging
from process_data import process_data


# Import modules
sys.path.insert(1, '../../src')

logger = logging.getLogger(__name__)





with DAG(
    dag_id="process_data",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@once",  # Run once manually or on deployment
    catchup=False,
    tags=["backfill", "history"],
) as dag_2024:

    grab_2024_data_task = PythonOperator(
        task_id="process_last_8_months",
        python_callable=process_data,
        op_kwargs={},  # You can pass explicit months/years if needed
    )