import requests
import logging
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

# Function to create stg and prod tables
def createTable(table_name, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_bacen')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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
    """
    try:
        cursor.execute(create_table_sql)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Erro ao criar tabela {table_name}: {e}")
    finally:
        cursor.close()
        conn.close()

# Creating tables
def creatingTables(**kwargs):
    createTable("stg_moedas")
    createTable("moedas")

# Extracting csv data
def extractingData(**kwargs):
    date = kwargs["date_nodash"] 
    # today = datetime.strptime(date, "%Y%m%d")
    # onedayago = today - timedelta(days=1)
    # date_onedayago = onedayago.strftime("%Y%m%d")
    base_url = "https://www4.bcb.gov.br/Download/fechamento/"
    full_url = f"{base_url}{date}.csv"
    logging.warning(f"URL: {full_url}")
    try:
        response = requests.get(full_url)
        if response.status_code == 200 and response.content != None:
            csv_data = response.content.decode("utf-8")
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"fechamento_bcb_{date}_{timestamp}.csv"
            save_dir = "/home/dev-linux/airflow/files/"
            os.makedirs(save_dir, exist_ok=True)
            file_path = os.path.join(save_dir, filename)

            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(csv_data)
            logging.info(f"Arquivo CSV salvo em: {file_path}")

            kwargs['ti'].xcom_push(key='file_path', value=file_path)
            return file_path
        else:
            logging.error(f"Erro ao extrair dados: {e}")
            raise AirflowFailException(f"Falha na extração de dados: Resposta vazia ou erro {response.status_code}")
    except Exception as e:
        logging.error(f"Erro ao extrair dados: {e}")
        raise AirflowFailException(f"Falha na extração de dados: {e}")

# Loading to production (upsert)
def loadingToProduction(**kwargs):
    # date_airflow = datetime.strptime(kwargs["date"], "%Y-%m-%d").date()
    # date_extracted = date_airflow - timedelta(days=1)
    date_extracted = kwargs["date"]
    pg_hook = PostgresHook(postgres_conn_id='postgres_bacen')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    upsert_data_sql = f"""
        INSERT INTO moedas
        SELECT * FROM stg_moedas WHERE data_fechamento = '{date_extracted}' AND processed_at = (SELECT MAX(processed_at) FROM stg_moedas WHERE data_fechamento = '{date_extracted}')
        ON CONFLICT (data_fechamento, cod)
        DO UPDATE SET
            tipo = EXCLUDED.tipo,
            desc_moeda = EXCLUDED.desc_moeda,
            taxa_compra = EXCLUDED.taxa_compra,
            taxa_venda = EXCLUDED.taxa_venda,
            paridade_compra = EXCLUDED.paridade_compra,
            paridade_venda = EXCLUDED.paridade_venda,
            processed_at = EXCLUDED.processed_at;
    """
    try:
        cursor.execute(upsert_data_sql)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Erro ao realizar UPSERT in bulk: {e}")
    finally:
        cursor.close()
        conn.close()

# Defining the DAG
with DAG(
    dag_id='moedasBacen_spark',
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    # max_active_runs=1,
    catchup=True
) as dag:

    # Task: Create Tables
    createTables_task = PythonOperator(
        task_id='createTables',
        python_callable=creatingTables,
        provide_context=True
    )

    # Task: Extract Data
    extractData_task = PythonOperator(
        task_id='extractData',
        python_callable=extractingData,
        provide_context=True,
        op_kwargs={'date_nodash': '{{ ds_nodash }}'}
    )

    # Task: Transform and Load to Stage
    transformAndLoadToStage_task = SparkSubmitOperator(
        task_id='transformAndLoadToStage',
        conn_id="spark_default",
        application="/home/dev-linux/airflow/spark/scritps/transformAndLoadToStage_moedas.py",
        jars="/opt/spark/jars/postgresql-42.7.5.jar",
        verbose=True,
        application_args=["{{ ti.xcom_pull(task_ids='extractData', key='file_path') }}"]
    )

    # Task: Load to Production (Upsert)
    loadToProduction_task = PythonOperator(
        task_id='loadToProduction',
        python_callable=loadingToProduction,
        provide_context=True,
        op_kwargs={'date': '{{ ds }}'}
    )

    # Setting up task dependencies
    extractData_task >> createTables_task >> transformAndLoadToStage_task >> loadToProduction_task
    
