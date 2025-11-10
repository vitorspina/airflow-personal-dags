from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import time

# Define the function to be run by the PythonOperator
def say_hello():
    print("Hello, World!")

def say_goodbye():
    print("Goodbye !!")

def sleep_5_sec():
    time.sleep(5)

# Define the DAG
with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Run on demand
    catchup=False,
    tags=["example"],
) as dag:

    hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=say_hello,
    )
    sleep_task = PythonOperator(
        task_id = "sleep_5_sec",
        python_callable=sleep_5_sec

    )
    goodbye_task = PythonOperator(
        task_id = "print_goodbye",
        python_callable = say_goodbye
    )


    hello_task >> sleep_task >> goodbye_task 
