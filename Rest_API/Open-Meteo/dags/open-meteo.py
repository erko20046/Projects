from airflow import DAG
from datetime import datetime
import requests
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
import logging
import pandas as pd
import clickhouse_connect
from airflow.operators.python import PythonOperator

def get_weather():
    url = 'https://api.open-meteo.com/v1/forecast'
    cities = {
        "Moscow" : (55.7522, 37.6156),
        "Astana" : (51.1801, 71.446),
        "Almaty" : (43.25, 76.9167)
    }

    data = []
    for city, (lat, lon) in cities.items():
        params = {
            "latitude" : lat,
            "longitude" : lon,
            "daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean"],
            "timezone": "Europe/Moscow",
            "forecast_days": 3
        }

        try:
            r = requests.get(url, params=params, timeout=5)
            r.raise_for_status()

            daily = r.json()["daily"]

            for i in range(len(daily["time"])):
                data.append({
                    "city" : city,
                    "forecast_date" : daily["time"][i],
                    "temperature_max" : daily["temperature_2m_max"][i],
                    "temperature_min" : daily["temperature_2m_min"][i],
                    "temperature_mean" : daily["temperature_2m_mean"][i]
                })
        except requests.exceptions.RequestException as e:
            logging.error(f"when get the {city}, we got the error {e}")

    df = pd.DataFrame(data)
    df.to_csv("/tmp/weather.csv", index=False, mode='w')



def load_clickhouse():
    hook = BaseHook.get_connection('clickhouse_default')

    client = clickhouse_connect.get_client(
        host=hook.host,
        port=hook.port,
        username=hook.login,
        password=hook.password,
    )

    client.command("""
                        CREATE TABLE IF NOT EXISTS default.weather(
                            city String,
                            forecast_date Date,
                            temperature_max Float32,
                            temperature_min Float32,
                            temperature_mean Float32
                        )
                        ENGINE = MergeTree()
                        ORDER BY (city, forecast_date);
    
    """)

    df = pd.read_csv("/tmp/weather.csv")
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
    df = df[["city", "forecast_date", "temperature_max", "temperature_min", "temperature_mean"]]
    client.insert_df(table="default.weather", df=df)
    logging.info(f"Successfully inserted {len(df)} rows into weather table")


default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'email_on_failure' : False,
    'email_on_retry' : False,
    'start_date' : datetime(2024, 5, 10),
    'retries' : 0
}


with DAG(
    dag_id='open-meteo',
    default_args=default_args,
    catchup = False,
    schedule_interval = "0 15 */3 * *",
    tags=['API']
):
    start_op = EmptyOperator(task_id ='start')

    get_weather_op = PythonOperator(task_id='get_weather', python_callable=get_weather)

    load_clickhouse_op = PythonOperator(task_id='load_clickhouse', python_callable=load_clickhouse)

    end_op = EmptyOperator(task_id ='end')

    start_op >> get_weather_op >> load_clickhouse_op >> end_op