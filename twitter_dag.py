from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from twitter_etl import run_twitter_etl

# define params for DAG
default_args={
    'owner':'dhruv-airflow',
    'depends_on_past':False,
    'start_date':datetime(2023,10,1),
    'email':['dhruvAirflow@gmail.com'],
    'email_on_failure':False,
    'retries':3,
    'retry_delay':timedelta(minutes=1)
}

dag=DAG(
    'twitter_dag',
    default_args=default_args,
    description=' simple DAG file to run twitter etl'
)

# use python operator to run dag
run_etl=PythonOperator(
    task_id='run_twitter_etl',
    python_callable=run_twitter_etl,
    dag=dag
)

run_etl