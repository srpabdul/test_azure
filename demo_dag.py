from airflow import DAG
from airflow.operators.python_operator import PythonOperator,ShortCircuitOperator

from datetime import datetime,timedelta
import logging
import os
import boto3
import time

from demo import fetch_csv


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 6, 29),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)

}

def demo_sample():
    fetch_csv()    
    
with DAG('demo_dag', default_args=default_args, schedule_interval='@once',catchup=False) as dag:

    demo_sample = PythonOperator(
            task_id='demo_sample',                        
            python_callable=demo_sample,
            provide_context=True                      
    )

demo_sample