from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime,timedelta




with DAG(
    dag_id = "process_clickstream_events",
    description="A DAG to source flatten click stream data and store it in iceberg tables",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 10, 12),
    catchup=False,
    default_args={
            'owner': 'storeez_de',
            'depends_on_past': False,
            'retries': 1,
        },

) as dag :

    start_task = EmptyOperator(task_id = "start", dag=dag)
    end_task = EmptyOperator(task_id="end", dag=dag)

    bash_command_prefix = "spark-submit --packages org.apache.hadoop:hadoop-client:3.3.4,org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.1.0 /opt/airflow/scripts/spark/spark-transform.py"
    with TaskGroup("unnest_jobs_group") as unnest_jobs_group:
        for task_name in ["consumption_end", "consumption_start", "session_start", "session_end"]:
            unnest_job = BashOperator(
                task_id=f"unnest_job_{task_name}",
                bash_command=f"{bash_command_prefix} {task_name} {{ds}}",
            )

    start_task >> unnest_jobs_group >> end_task


