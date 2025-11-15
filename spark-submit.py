from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime

def say_hello():
    print("Hello from Airflow!")

with DAG(
    dag_id="spark_submit_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Run on demand
    catchup=False,
    tags=["example"],
) as dag:

    hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=say_hello,
    )

    spark_submit_task = KubernetesPodOperator(
    namespace='analytics',
    service_account_name='spark-role',

    # ✔ official spark image built for k8s
    image='apache/spark:3.5.6',

    # ✔ override entrypoint to run spark-submit
    cmds=['/opt/spark/bin/spark-submit'],

    # ✔ submit a SparkPi example packaged inside the image
    arguments=[
        '--master', 'k8s://https://kubernetes.default.svc:443',
        '--deploy-mode', 'cluster',
        '--name', 'spark-pi-job',
        '--class', 'org.apache.spark.examples.SparkPi',

        # RBAC — use your service account
        '--conf', 'spark.kubernetes.authenticate.driver.serviceAccountName=spark-role',
        '--conf', 'spark.kubernetes.executor.serviceAccountName=spark-role',

        # Executors (workers)
        '--conf', 'spark.executor.instances=3',
        '--conf', 'spark.executor.memory=2G',
        '--conf', 'spark.executor.cores=1',
        '--conf', 'spark.kubernetes.driver.deleteOnTermination=false',
        'local:///opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar',
        '100'
    ],
    name='spark-submit-task',
    task_id='spark_submit_task',
    get_logs=True,
    is_delete_operator_pod=False,
    )


    # Set dependency: first Python task, then KubernetesPodOperator
    hello_task >> spark_submit_task
