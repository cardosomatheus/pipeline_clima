from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import requests
import json 
from datetime import datetime

"""
Pendencias de melhorias do projeto de estudo:
    - fazer o merge de insert, update
    - criar grupo para as tarefas
    - definir retrys
    - documentar cada task
    - Definir mais params no Dag ex: description
    - Definir mais params nas tasks ex: execution_timeout
"""

@dag(
    schedule=None,
    start_date= datetime(2025,10,10),
    catchup=False,
    tags=["Clima", "Temperatura","pipeline", "postgres"],
)
def abc_processo_temperatura():
    vdict_return = None


    @task.bash
    def bash_start_pipeline( value_int :int) -> str:
        return f'echo "Iniciando a pipeline!!" && sleep {value_int}'


    @task()
    def construct_url_temperature()-> str:
        latitude=-19.9208
        longitude=-43.9378
        textual_date = datetime.strftime(datetime.now(),'%Y-%m-%d')   

        if latitude is None or longitude is None:
            raise Exception ('Não pode ter paramentros de entradas nulos.')

        if not isinstance(latitude,float) or not isinstance(longitude,float):
            raise Exception('Longitude  e latitude precisam ser um numeros quebrados', longitude, latitude)
                
        return f'https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&hourly=temperature_2m&timezone=America%2FSao_Paulo&start_date={textual_date}&end_date={textual_date}'


    @task()
    def get_temperature_to_hour(url :str) -> dict:
        r = requests.get(url=url)
        if r.status_code != 200:
            print('Erro na requisição ', r.status_code)
            r.raise_for_status()
        
        return dict(r.json())


    @task()
    def extract_hourly_temperatures(dict_return :dict) -> dict:
        list_temperature_to_hours = []
        for i in range(24):
            dict_temperature_to_hours = {}
            hourly = dict_return['hourly']['time']
            temperature = dict_return['hourly']['temperature_2m']
            
            if hourly[i] is None or temperature[i] is None:
                raise Exception(f'O valor contido na lista está vazio\n Hora:{hourly[i]} temperatura: {temperature[i]}.')
                
            dict_temperature_to_hours[hourly[i]] = temperature[i]
            list_temperature_to_hours.append(dict_temperature_to_hours)
        
        dict_return['temperature_to_hour'] = list_temperature_to_hours


    table_temperatura = SQLExecuteQueryOperator(
        task_id='table_temperatura',
        conn_id='conn_airflow_postgres',
        sql='sql/table_temperature.sql'
    )

    merge_temperatura = SQLExecuteQueryOperator(
        task_id='merge_temperatura',
        conn_id='conn_airflow_postgres',
        sql='sql/merge_temperature.sql',
        parameters={"CONTEUDO_JSON": json.dumps(vdict_return)}
    )



    vurl =  bash_start_pipeline(value_int=3) >> construct_url_temperature()
    vdict_return = get_temperature_to_hour(url=vurl)
    extract_hourly_temperatures(vdict_return) >> table_temperatura >> merge_temperatura


abc_processo_temperatura()
