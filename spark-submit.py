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

    hello_pod = KubernetesPodOperator(
    namespace='analytics',
    image='nauedu/nau-analytics-external-data-product:latest',
    cmds=['/opt/spark/bin/spark-submit'],
    arguments=[
        '--master', 'k8s://https://kubernetes.default.svc:443',
        '--deploy-mode', 'cluster',
        '--name', 'hello-spark-job',
        '--class', 'org.apache.spark.examples.SparkPi',

        # RBAC — use your service account
        '--conf', 'spark.kubernetes.authenticate.driver.serviceAccountName=spark-role',
        '--conf', 'spark.kubernetes.executor.serviceAccountName=spark-role',

        # Executors (workers)
        '--conf', 'spark.executor.instances=3',
        '--conf', 'spark.executor.memory=2G',
        '--conf', 'spark.executor.cores=1',

        # Use the same image for executors
        '--conf', 'spark.kubernetes.container.image=nauedu/nau-analytics-external-data-product:latest',

        # Example SparkPi JAR already included in the image
        'local:///opt/spark/examples/jars/spark-examples_2.12-3.5.6.jar',
        '100'
    ],
    name='spark-submit-task',
    task_id='spark_submit_task',
    get_logs=True,
    )


    # Set dependency: first Python task, then KubernetesPodOperator
    hello_task >> spark_submit_task
