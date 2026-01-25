from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'extract_pets_data',
    default_args=default_args,
    schedule=timedelta(minutes=30),
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    flatten_pets = SQLExecuteQueryOperator(
        task_id='extract_pets_json',
        conn_id='demo_db',
        sql="""
            DROP TABLE IF EXISTS extract_demo.pets_flat;

            CREATE TABLE extract_demo.pets_flat AS
            SELECT
                post.value->>'name' AS name,
                post.value->>'species' AS species,
                post.value->>'favFoods' AS fav_foods,
                (post.value->>'birthYear')::int AS birth_year,
                post.value->>'photo' AS photo
            FROM 
                extract_demo.json_content,
                jsonb_array_elements(json_data->'pets') AS post(value);
        """
    )
