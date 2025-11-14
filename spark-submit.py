from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime

def say_hello():
    print("Hello from Airflow!")

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

    hello_pod = KubernetesPodOperator(
        namespace="analytics",  # Change if needed
        image="bash:5.2",       # lightweight image with bash
        cmds=["/bin/bash", "-c"],
        arguments=["echo 'Hello from KubernetesPodOperator!' && sleep 60"],
        name="hello-pod",
        task_id="hello_pod_task",
        get_logs=True,
        is_delete_operator_pod=True,  # Delete pod after completion
    )

    # Set dependency: first Python task, then KubernetesPodOperator
    hello_task >> hello_pod
