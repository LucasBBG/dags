import pandas as pd  # se for um volume grande de dados não usar pandas, usar pyspark
import psycopg2
from psycopg2.extras import execute_values
import requests
import logging
from datetime import datetime, timedelta
from io import StringIO
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
            return csv_data
    except Exception as e:
        logging.error(f"Erro ao extrair dados: {e}")


# Transforming csv data into dataframe
def transform_and_load(**kwargs):
    csv_data = kwargs["csv_data"]
    columns = [
        "data_fechamento",
        "cod",
        "tipo",
        "desc_moeda",
        "taxa_compra",
        "taxa_venda",
        "paridade_compra",
        "paridade_venda"
    ]
    columns_types = {
        "data_fechamento": str,
        "cod": str,
        "tipo": str,
        "desc_moeda": str,
        "taxa_compra": float,
        "taxa_venda": float,
        "paridade_compra": float,
        "paridade_venda": float
    }
    if csv_data:
        df = pd.read_csv(
            StringIO(csv_data),
            sep=";",
            decimal=",",
            thousands=".",
            encoding="utf-8",
            header=None,
            names=columns,
            dtype=columns_types
        )
        df['data_fechamento'] = pd.to_datetime(df['data_fechamento'], format='%d/%m/%Y', dayfirst=True)
        df['data_fechamento'] = df['data_fechamento'].dt.strftime('%Y-%m-%d')
        df['processed_at'] = datetime.now()
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_bacen')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Transformar o DataFrame em uma lista de tuplas
        data_tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]

        # Estruturar o comando SQL para o UPSERT
        upsert_query = """
            INSERT INTO moedas (data_fechamento, cod, tipo, desc_moeda, taxa_compra, taxa_venda, paridade_compra, paridade_venda, processed_at)
            VALUES %s
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
            execute_values(cursor, upsert_query, data_tuples)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"Error while performing UPSERT in bulk: {e}")
        finally:
            cursor.close()
            conn.close()


# Creating tabel if not exists
def create():
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
    dag_id='moedas_bacen',
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=True,
) as dag:

    # Task: Extract
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True,
        op_kwargs={'date_nodash': '{{ ds_nodash }}'},
    )

    # Task: Transform and Load
    transform_and_load_task = PythonOperator(
        task_id='transform_and_load',
        python_callable=transform_and_load,
        provide_context=True,
        op_kwargs={'csv_data': '{{ task_instance.xcom_pull(task_ids="extract") }}'},
    )

    # Task: Create
    create_task = PythonOperator(
        task_id='create',
        python_callable=create,
        provide_context=True
    )

    # Setting up task dependencies
    create_task >> extract_task >> transform_and_load_task
