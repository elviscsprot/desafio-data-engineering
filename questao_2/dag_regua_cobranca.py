from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import shutil
import os


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}


def processar_pagamentos(**context):
    execution_date = context['execution_date']
    dia_semana = execution_date.weekday()
    
    arquivo_origem = '/opt/airflow/data/pagamentos_d-1.csv'
    
    if dia_semana <= 4:
        carregar_banco_producao(arquivo_origem, execution_date)
    else:
        arquivar_arquivo(arquivo_origem, execution_date)


def carregar_banco_producao(arquivo, execution_date):
    print(f'Carregando dados no banco de produção: {arquivo}')
    print(f'Data de execução: {execution_date}')
    print(f'Dia da semana: {execution_date.strftime("%A")}')
    
    with open(arquivo, 'r') as f:
        linhas = f.readlines()
    
    print(f'Total de registros encontrados: {len(linhas) - 1}')
    print('Dados carregados com sucesso no banco de produção')


def arquivar_arquivo(arquivo, execution_date):
    print(f'Final de semana detectado - arquivando sem processar')
    print(f'Data de execução: {execution_date}')
    print(f'Dia da semana: {execution_date.strftime("%A")}')
    
    destino = f'/opt/airflow/logs/pagamentos_d-1_{execution_date.strftime("%Y%m%d")}.csv'
    
    os.makedirs('/opt/airflow/logs', exist_ok=True)
    shutil.copy(arquivo, destino)
    
    print(f'Arquivo arquivado em: {destino}')


with DAG(
    'dag_regua_cobranca',
    default_args=default_args,
    description='Pipeline de régua de cobrança com validação de dia útil',
    schedule_interval='@daily',
    catchup=False,
    tags=['cobranca', 'pagamentos']
) as dag:
    
    verificar_arquivo = FileSensor(
        task_id='verificar_arquivo_pagamentos',
        filepath='/opt/airflow/data/pagamentos_d-1.csv',
        poke_interval=300,
        timeout=60 * 60 * 2,
        mode='poke'
    )
    
    processar = PythonOperator(
        task_id='processar_pagamentos',
        python_callable=processar_pagamentos,
        provide_context=True
    )
    
    verificar_arquivo >> processar

