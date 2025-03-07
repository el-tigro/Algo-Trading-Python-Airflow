from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from utils import order
from datetime import timedelta
import os

DAG_ID=os.path.basename(__file__).replace('.pyc','').replace('.py','')
CONN_ID='postgres_stocks'

STRATEGY_WEIGHTS = {
    'sma': 0.7,
    'bollinger': 0.3,
}
THRESHOLD = 0.5

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
    schedule_interval='20 1 * * *' # Schedule # Minutes (10)  Hour (1) day (month) month day week -> At 01:10 Every Day
    # Catch_up = False # Загрузка всех невыполненных дагов
) as dag:

    create_limit_order_aapl = PythonOperator(
        task_id='create_limit_order_aapl',
        python_callable=order.create_limit_order_by_signals,
        op_kwargs={
            'connector': CONN_ID,
            'ticker': 'AAPL',
            'weights': STRATEGY_WEIGHTS,
            'threshold': THRESHOLD,
        }
    )