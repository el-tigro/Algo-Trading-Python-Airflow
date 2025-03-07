from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from utils import strategy
from datetime import timedelta
import os

DAG_ID=os.path.basename(__file__).replace('.pyc','').replace('.py','')
CONN_ID='postgres_stocks'

SMA = 30
DEV = 2

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
    schedule_interval='10 1 * * *' # Schedule # Minutes (10)  Hour (1) day (month) month day week -> At 01:10 Every Day
    # Catch_up = False # Загрузка всех невыполненных дагов
) as dag:

    bollinger_bands_strategy = PythonOperator(
        task_id='bollinger_bands_strategy',
        python_callable=strategy.apply_strategy,
        op_kwargs={
            'connector': CONN_ID,
            'source_table_name': 'aapl',
            'ticker': 'AAPL',
            'strategy_func':strategy.bollinger_bands_strategy,
            'op_kwargs': {
                'sma': SMA,
                'dev': DEV
            }
        }
    )