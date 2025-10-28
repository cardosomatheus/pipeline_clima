from airflow.sdk import dag, task, task_group

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator

import requests
import json 
from datetime import datetime, timedelta


@dag(
    schedule='5 * * * *', # a cada hora + 5minutos ex: 16:05
    description='fetch the temperature of the day every hour and save it in the postgres database',
    start_date=datetime(2025,10,10),
    end_date=datetime(2027,1,1),
    catchup=False,
    default_args={'retries':2},
    tags=["Clima", "Temperatura","pipeline", "postgres"]
)
def processo_temperatura():
    empty_task = EmptyOperator(task_id='Empty_task')

    @task.bash
    def bash_start_pipeline( value_int :int) -> str:
        # Retorna a mensagem e dorme po nº segundos
        return f'echo "Iniciando a pipeline!!" && sleep {value_int}'


    @task(execution_timeout=timedelta(seconds=30))
    def build_temperature_api_url()-> str:
        # Constroi a url usada para buscar a temperatura.
        latitude=-19.9208
        longitude=-43.9378
        textual_date = datetime.strftime(datetime.now(),'%Y-%m-%d')   

        if latitude is None or longitude is None:
            raise Exception ('Não pode ter paramentros de entradas nulos.')

        if not isinstance(latitude,float) or not isinstance(longitude,float):
            raise Exception('Longitude  e latitude precisam ser um numeros quebrados', longitude, latitude)
                
        return (f'https://api.open-meteo.com/v1/forecast?latitude={latitude}'
                f'&longitude={longitude}&hourly=temperature_2m&timezone=America'
                f'%2FSao_Paulo&start_date={textual_date}&end_date={textual_date}')



    @task(execution_timeout=timedelta(seconds=30), retries=2)
    def load_hourly_temperatures_today(url :str) -> dict:
        # Faz a requisição da temperatura.
        r = requests.get(url=url)
        if r.status_code != 200:
            print('Erro na requisição ', r.status_code)
            r.raise_for_status()
        
        return dict(r.json())


    @task(execution_timeout=timedelta(seconds=30),retries=2)
    def mapping_hourly_temperatures(dict_return :dict) -> json:
        # Mapeia {datahora: temperatura} e salva no json
        list_temperature_to_hours = []
        for i in range(24):
            dict_temperature_to_hours = {}
            dict_hourly = dict_return.get('hourly', {})
            hourly = dict_hourly.get('time',[None])
            temperature = dict_hourly.get('temperature_2m',[None])

            if hourly[i] is None or temperature[i] is None:
                raise Exception(f'O valor contido na lista está vazio\n Hora:{hourly[i]} temperatura: {temperature[i]}.')
                
            dict_temperature_to_hours[hourly[i]] = temperature[i]
            list_temperature_to_hours.append(dict_temperature_to_hours)
        
        dict_return['temperature_to_hour'] = list_temperature_to_hours
        return json.dumps(dict_return)


    @task_group(group_id = 'postgres')
    def group_database_postgres():
        # Cria a tabela de log junto a sequence
        create_table_temperatures = SQLExecuteQueryOperator(
            task_id='create_table_temperature',
            conn_id='conn_airflow_postgres',
            autocommit=True,
            sql='sql/table_temperature.sql'
        )

        # Faz o merge da nova requisição.
        merge_hourly_temperatures = SQLExecuteQueryOperator(
            task_id='merge_hourly_temperatures',
            conn_id='conn_airflow_postgres',
            autocommit=True,
            sql='sql/merge_temperature.sql'
        )
        
        create_table_temperatures >> merge_hourly_temperatures

    # Ordenação e execução.
    bash_start     = bash_start_pipeline(value_int=3)
    url_task       = build_temperature_api_url()
    loading_task   = load_hourly_temperatures_today(url_task)
    mapping_task   = mapping_hourly_temperatures(loading_task)

    [empty_task, bash_start, url_task] >> loading_task >> mapping_task >> group_database_postgres() 


processo_temperatura()
