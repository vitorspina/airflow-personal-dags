from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator #type: ignore

from airflow.operators.python import PythonOperator #type: ignore
from airflow import DAG #type: ignore
from datetime import datetime

def say_hello() -> None:
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
    image='masterpingas/spark-shell:v1',
    # ✔ override entrypoint to run spark-submit
    cmds=["/bin/bash", "-c"],

    # ✔ submit a SparkPi example packaged inside the image
    arguments=[
        """
            spark-submit \
          --master k8s://https://kubernetes.default.svc:443 \
          --deploy-mode cluster \
          --name spark-pi \
          --conf spark.kubernetes.container.image=nauedu/nau-analytics-external-data-product:feature-ingestion-script-improvements \
          --conf spark.kubernetes.namespace=analytics \
          --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-role \
          --conf spark.executor.instances=3 \
          --conf spark.executor.cores=2 \
          --conf spark.executor.memory=2g \
          --conf spark.kubernetes.submission.waitAppCompletion=true \
          --conf spark.kubernetes.submission.reportFailureOnDriverError=true \
          --conf spark.kubernetes.driver.deleteOnTermination=true \
          --conf spark.kubernetes.executor.deleteOnTermination=true \
          --conf spark.kubernetes.container.image.pullPolicy=Always \
          local:///opt/spark/work-dir/src/bronze/get_full_tables.py \
          2>&1 | tee log.txt; LAST_EXIT=$(grep -Ei "exit code" log.txt | tail -n1 | sed 's/.*: *//'); echo "Parsed Spark exit code: $LAST_EXIT"; exit "$LAST_EXIT"
        """
    ],
    name='spark-submit-task',
    task_id='spark_submit_task',
    get_logs=True,
        on_finish_action="keep_pod",
    )


    # Set dependency: first Python task, then KubernetesPodOperator
    hello_task >> spark_submit_task
