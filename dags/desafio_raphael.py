from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sqlite3
import pandas as pd

# Defina os argumentos padrão do DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 1),
    'retries': 2,
}

# Crie uma instância do DAG
dag = DAG(
    'DesafioRaphael',
    default_args=default_args,
    description='DAG para processamento de dados',
    schedule_interval=None,
)

# Função para ler dados da tabela "Order" e exportar para CSV
def read_orders_and_export_csv():
    conn = sqlite3.connect('data/Northwind_small.sqlite')
    query = "SELECT * FROM 'Order'"
    df = pd.read_sql_query(query, conn)
    df.to_csv('/mnt/c/Airflow/airflow_tooltorial/data/output_orders.csv', index=False)

# Função para calcular a soma da quantidade vendida para o Rio de Janeiro e exportar em "count.txt"
def calculate_and_export_count():
    # Ler o DataFrame do arquivo "output_orders.csv"
    orders_df = pd.read_csv('/mnt/c/Airflow/airflow_tooltorial/data/output_orders.csv')
    #filtro para somente pedidos do RJ
    orders_rj_df = orders_df[orders_df['ShipCity']=='Rio de Janeiro']

    conn = sqlite3.connect('data/Northwind_small.sqlite')
    query = "SELECT orderid as Id, quantity FROM 'orderdetail'"
    order_details_df = pd.read_sql_query(query, conn)

    df_inner = pd.merge(orders_rj_df, order_details_df, on='Id', how='inner')
    contegem_pedidos = df_inner['Quantity'].sum()
    
    with open('/mnt/c/Airflow/airflow_tooltorial/data/final_output.txt', 'w') as file:
        file.write(str(contegem_pedidos))

# Task 1: Ler dados da tabela "Order" e exportar para CSV
task_read_and_export_csv = PythonOperator(
    task_id='read_and_export_csv',
    python_callable=read_orders_and_export_csv,
    dag=dag,
)

# Task 2: Calcular a soma da quantidade vendida para o Rio de Janeiro e exportar em "count.txt"
task_calculate_and_export_count = PythonOperator(
    task_id='calculate_and_export_count',
    python_callable=calculate_and_export_count,
    dag=dag,
)

# Defina a ordem de execução das tarefas
task_read_and_export_csv >> task_calculate_and_export_count
