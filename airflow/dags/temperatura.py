from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
import requests
from datetime import datetime

@dag(
    schedule=None,
    start_date= datetime(2025,10,10),
    catchup=False,
    tags=["Clima", "Temperatura","pipeline"],
)
def abc_processo_temperatura():

    bash_time_sleep = BashOperator(
        task_id='sleeping3seconds',
        bash_command='sleep 3'
    )


    @task()
    def start_process():
        texto = 'Iniciando processo de busca de temperatura a cada hora!!'
        vtamanho = len(texto)+2
        print('='*vtamanho)
        print(f'={texto}=')
        print('='*vtamanho)


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
    

    vurl = start_process() >> bash_time_sleep >> construct_url_temperature()
    vdict_return = get_temperature_to_hour(url=vurl)
    extract_hourly_temperatures(vdict_return)


abc_processo_temperatura()
