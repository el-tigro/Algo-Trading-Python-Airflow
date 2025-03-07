from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from utils import db, tinkoff_functions
from datetime import timedelta
import os

DAG_ID=os.path.basename(__file__).replace('.pyc','').replace('.py','')
CONN_ID='postgres_stocks'

# Словарь дефолтных значений
default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1), # From AirFlow
    'retries': 2,  # Count Retries if Error
    'retry_delay': timedelta(minutes=5), # TimeDelta between Error and Retry
    'email_on_failure':False,
    'email_on_retry': False,
}

with DAG(
    dag_id=DAG_ID, # File Name
    default_args=default_args,
    schedule_interval=None # Schedule
    # Catch_up = False # Загрузка всех невыполненных дагов

) as dag:
    load_aapl_data=PythonOperator(
        task_id='load_aapl_data', # Loading Apple data
        python_callable=db.load_df_to_db,
        op_kwargs={
            'connector':CONN_ID,
            'df': tinkoff_functions.get_data_by_ticker_and_period(
                'AAPL'
            ),
            'table_name':'aapl',
        }
    )