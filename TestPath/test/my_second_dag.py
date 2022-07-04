from airflow import DAG
import pendulum
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

def MyFunction():
    print('Hello')

with DAG(dag_id='Learn2',schedule_interval=None, start_date=pendulum.datetime(2022, 1, 1)) as dag:
    task1=DummyOperator(task_id="task1")
    task2 = PythonOperator(task_id="hello", python_callable=MyFunction)
    
    task1 >> task3
    task2 >> task3