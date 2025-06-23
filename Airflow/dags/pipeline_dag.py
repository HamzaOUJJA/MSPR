
from airflow import DAG
import pendulum
import os
import sys
import datetime
from dateutil.relativedelta import relativedelta
from airflow.operators.python import PythonOperator


# Import modules
sys.path.insert(1, '../../src')


from grab_Data                  import grab_Data
from dump_to_minio_spark        import dump_to_minio_spark
from clean_data                 import clean_data
from train_model                import train_model
from dump_to_warehouse          import dump_to_warehouse



def clean_local_folder():
    folder_path = '../../data/raw'
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        os.remove(file_path) 
    print("\033[1;32m ######## Local Folder Cleaned!\033[0m")
 



def process_previous_data():
    months_to_process = [
        (2019, 10), (2019, 11), (2019, 12),
        (2020, 1), (2020, 2), (2020, 3), (2020, 4)
    ]

    for year, month in months_to_process:
        print(f"📦 Processing data for {year}-{month:02}")
        clean_local_folder()
        grab_Data(year, month)
        dump_to_minio_spark(year, month)
        clean_data(year, month)
        train_model(year, month)
        dump_to_warehouse(year, month)




def process_last_month():
    today = datetime.date.today()
    last_month = today - relativedelta(months=1)
    year, month = last_month.year, last_month.month
    print(f"🚀 Processing last month's data: {year}-{month:02}")
    clean_local_folder()
    grab_Data(year, month)
    dump_to_minio_spark(year, month)
    clean_data(year, month)
    train_model(year, month)
    dump_to_warehouse(year, month)







with DAG(
    dag_id="grab_last_month_data",
    start_date=pendulum.today('UTC').replace(day=1).add(months=-1),
    schedule="0 0 1 * *",  # At midnight on the 1st of every month
    catchup=False,
    tags=["monthly"],
) as monthly_dag:

    grab_last_month_data_task = PythonOperator(
        task_id="grab_last_month_data_task",
        python_callable=process_last_month,
    )



with DAG(
    dag_id="process_previous_data",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@once",  # Run once manually or on deployment
    catchup=False,
    tags=["backfill", "history"],
) as dag_2024:

    grab_2024_data_task = PythonOperator(
        task_id="process_last_8_months",
        python_callable=process_previous_data,
        op_kwargs={},  # You can pass explicit months/years if needed
    )