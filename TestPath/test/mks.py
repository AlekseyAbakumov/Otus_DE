import requests
import psycopg2

# from config_prod import host, port, user, password, db_name

host = "rc1b-fsebsbna7j41vi8o.mdb.yandexcloud.net"
port = 6432
user = "srv-user"
password = "12345678"
db_name = "analytics"

def getjson(url):
    request=requests.get(url)
    api_json=request.json()
    return api_json

def pars_dict(mks_dict):
    yy=mks_dict
    print(yy)
    print(type(yy))
    iss_position=mks_dict['iss_position']
    latitude=iss_position['latitude']
    longitude=iss_position['longitude']

    timestamp=mks_dict['timestamp']

    return latitude, longitude, timestamp

def post_data(x):
    timestamp=x[2]
    latitude=x[0]
    longitude=x[1]

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

mks_coordinat=getjson("http://api.open-notify.org/iss-now.json")
x=pars_dict(mks_coordinat)
post_data(x)