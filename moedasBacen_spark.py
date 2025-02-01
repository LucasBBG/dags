import requests
import logging
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

# Extracting csv data
def extract(**kwargs):
    date = kwargs["date_nodash"] 
    today = datetime.strptime(date, "%Y%m%d")
    onedayago = today - timedelta(days=1)
    date_onedayago = onedayago.strftime("%Y%m%d")
    base_url = "https://www4.bcb.gov.br/Download/fechamento/"
    full_url = f"{base_url}{date_onedayago}.csv"
    logging.warning(f"URL: {full_url}")

    try:
        response = requests.get(full_url)
        if response.status_code == 200:
            csv_data = response.content.decode("utf-8")

            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"fechamento_bcb_{date_onedayago}_{timestamp}.csv"
            save_dir = "/home/dev-linux/airflow/files/"
            os.makedirs(save_dir, exist_ok=True)
            file_path = os.path.join(save_dir, filename)

            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(csv_data)
            logging.info(f"Arquivo CSV salvo em: {file_path}")

            kwargs['ti'].xcom_push(key='file_path', value=file_path)
            return file_path
        
    except Exception as e:
        logging.error(f"Erro ao extrair dados: {e}")


# Creating table if not exists
def create(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_bacen')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS moedas (
                data_fechamento DATE NOT NULL,
                cod VARCHAR(10) NOT NULL,
                tipo VARCHAR(20),
                desc_moeda VARCHAR(100),
                taxa_compra DECIMAL(10, 4),
                taxa_venda DECIMAL(10, 4),
                paridade_compra FLOAT,
                paridade_venda FLOAT,
                processed_at TIMESTAMP NOT NULL
            );
        """)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Erro ao criar tabela: {e}")
    finally:
        cursor.close()
        conn.close()

# Defining the DAG
with DAG(
    dag_id='moedasBacen-spark',
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=True,
) as dag:
    
    # Task: Create
    create_task = PythonOperator(
        task_id='create',
        python_callable=create,
        provide_context=True
    )

    # Task: Extract
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True,
        op_kwargs={'date_nodash': '{{ ds_nodash }}'},
    )

    # Task: Transform and Load
    transform_and_load_task = SparkSubmitOperator(
        task_id='transform_and_load',
        application="/home/dev-linux/airflow/spark/scritps/transform_and_load.py",
        conn_id="spark_default",
        jars="/opt/spark/jars/postgresql-42.7.5.jar",
        verbose=True,
        application_args=[
            "{{ ti.xcom_pull(task_ids='extract', key='file_path') }}"
        ]
    )

    # Setting up task dependencies
    create_task >> extract_task >> transform_and_load_task
    
