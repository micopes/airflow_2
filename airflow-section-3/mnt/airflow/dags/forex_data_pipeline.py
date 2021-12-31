from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta
import csv
import requests
import json

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    # failure로 처리 전 retry할 횟수
    "retries": 1, 
    # retry전에 가질 delay
    "retry_delay": timedelta(minutes = 5) 
}

def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open('/opt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for idx, row in enumerate(reader):
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')

with DAG("forex_data_pipeline", start_date = datetime(2021, 1, 1), 
    schedule_interval = "@daily", default_args = default_args, catchup = False) as dag:

    is_forex_rates_available = HttpSensor(
        task_id = "is_forex_rates_available",
        http_conn_id = "forex_api", # airflow connections - forex_api 
        endpoint = "marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b", # host + endpoint
        response_check = lambda response: "rates" in response.text,
        poke_interval = 5, # Sensor가 condition이 true/false인지를 verify하는 빈도
        timeout = 20 # Sensor가 running 후 20초가 지나면 timeout 후 failure로 종료
    )

    is_forex_file_available = FileSensor(
        task_id = "is_forex_file_available",
        fs_conn_id = "forex_path",
        filepath = "forex_currencies.csv",
        poke_interval = 5,
        timeout = 20
    )

    download_forex_rates = PythonOperator(
        task_id = "download_forex_rates",
        python_callable = download_rates
    )

    save_forex_rates = BashOperator(
        task_id = "save_forex_rates",
        bash_command = """
            
        """
    )

    
    



