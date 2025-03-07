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
    schedule_interval='5 1 * * *' # Schedule # Minutes (5)  Hour (1) day (month) month day week -> At 01:05 Every Day
    # Catch_up = False # Загрузка всех невыполненных дагов

) as dag:

    update_stock_prices_aapl = PythonOperator(
        task_id='update_stock_prices_aapl',
        python_callable=db.load_df_to_db,
        op_kwargs={
            'connector': CONN_ID,
            'df': tinkoff_functions.get_data_by_ticker_and_period(
                'AAPL',
                2,
            ).tail(1),
            'table_name':'aapl',
        }
    )