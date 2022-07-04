from airflow import DAG
import pendulum
from airflow.operators.dummy import DummyOperator



dag=DAG(dag_id='Learn1',schedule_interval=None, start_date=pendulum.datetime(2022, 1, 1))

task1=DummyOperator(task_id="task1", dag=dag)
task2=DummyOperator(task_id="task2", dag=dag)

task1 >> task2