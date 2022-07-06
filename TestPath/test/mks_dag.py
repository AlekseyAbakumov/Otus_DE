from airflow import DAG
import pendulum
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
import requests
import psycopg2

def getjson():
    url="http://api.open-notify.org/iss-now.json"
    request=requests.get(url)
    api_json=request.json()
    return api_json

def pars_dict(**kwargs):
    ti = kwargs['ti']
    api_json=ti.xcom_pull(task_ids='getjson')
    iss_position=api_json['iss_position']
    latitude=iss_position['latitude']
    longitude=iss_position['longitude']

    timestamp=api_json['timestamp']

    return latitude, longitude, timestamp

def post_data(**kwargs):
    ti = kwargs['ti']
    x=ti.xcom_pull(task_ids='pars_dict')

    timestamp=x[2]
    latitude=x[0]
    longitude=x[1]

    host = "rc1b-fsebsbna7j41vi8o.mdb.yandexcloud.net"
    port = 6432
    user = "srv-user"
    password = "12345678"
    db_name = "analytics"

    try:
        # connect to exist database
        connection = psycopg2.connect(host=host, port=port, user=user, password=password, database=db_name)
        connection.autocommit = True

        with connection.cursor() as cursor:
            # get request 
            cursor.execute(
                "SELECT max(period) as period FROM where_mks;"
            )
            last_period=cursor.fetchone()[0]

            if last_period < timestamp: 
                # post request 
                cursor.execute(
                    f"INSERT INTO where_mks (period, latitude, longitude) VALUES('{timestamp}', '{latitude}', '{longitude}');"
                )
    except Exception as _ex:
        print("[INFO] Error while working with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")

with DAG(
    dag_id='Where_MKS',
    schedule_interval='* * * * *',
    start_date=pendulum.datetime(2022, 1, 1),
    catchup=False
    ) as dag:

    task_getjson = PythonOperator(task_id="getjson", python_callable=getjson)
    task_pars_dict = PythonOperator(task_id="pars_dict", python_callable=pars_dict)
    task_post_data = PythonOperator(task_id="post_data", python_callable=post_data)
    
    task_getjson >> task_pars_dict >> task_post_data
