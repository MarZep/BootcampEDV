from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='ingest-transform-load-clase-nueve',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['ingest', 'transform'],
    params={"example_key": "example_value"},
) as dag:

    comienza_proceso = DummyOperator(
        task_id='comienza_proceso',
    )

    finaliza_proceso = DummyOperator(
        task_id='finaliza_proceso',
    )

    with TaskGroup('ingest', tooltip='ingest') as ingest:
        task_1 = BashOperator(task_id='export_table_1', bash_command='export PATH={{ var.value.SQOOP_HOME }}/bin:$PATH && /usr/bin/sh /home/hadoop/scripts/ingest_sqoop.sh ')
        task_2 = BashOperator(task_id='export_table_2', bash_command='export PATH={{ var.value.SQOOP_HOME }}/bin:$PATH && /usr/bin/sh /home/hadoop/scripts/ingest_sqoop_dos.sh ')
        task_3 = BashOperator(task_id='export_table_3', bash_command='export PATH={{ var.value.SQOOP_HOME }}/bin:$PATH && /usr/bin/sh /home/hadoop/scripts/ingest_sqoop_tres.sh ')

    with TaskGroup('process', tooltip='process') as process:
        task_1 = BashOperator(task_id='processing_table_1', bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transform_uno.py ')
        task_2 = BashOperator(task_id='processing_table_2_3', bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transform_dos.py ')


    comienza_proceso >> ingest >> process >> finaliza_proceso
