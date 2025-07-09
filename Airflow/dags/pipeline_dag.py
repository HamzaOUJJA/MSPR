
import pendulum
import sys
import logging
from datetime       import datetime
from airflow        import DAG
from process_data   import process_data
from delete_data    import delete_data
from airflow.operators.python import PythonOperator


# Import modules
sys.path.insert(1, '../../src')

logger = logging.getLogger(__name__)



# Function to delete data exactly 13 months old
def delete_expired_table(**kwargs):
    today = datetime.today()
    year = today.year
    month = today.month - 13

    if month <= 0:
        year -= 1
        month += 12

    delete_data(year, month)




# DAG definition for data deletion
with DAG(
    dag_id="delete_13_months_old_table",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@once",  # You can change to "0 0 1 * *" to run monthly
    catchup=False,
    tags=["cleanup", "monthly"],
) as dag:

    delete_old_data_task = PythonOperator(
        task_id="delete_expired_table",
        python_callable=delete_expired_table,
    )


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