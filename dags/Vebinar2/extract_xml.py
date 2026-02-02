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
    'extract_xml_nutrition',
    default_args=default_args,
    schedule=timedelta(minutes=30),
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    flatten_xml = SQLExecuteQueryOperator(
        task_id='extract_xml_nutrition',
        conn_id='demo_db',
        sql="""
            DROP TABLE IF EXISTS extract_demo.nutrition_flat;

            CREATE TABLE extract_demo.nutrition_flat AS
            SELECT
                (xpath('/food/name/text()', food_node))[1]::text AS food_name,
                (xpath('/food/mfr/text()', food_node))[1]::text AS manufacturer,
                (xpath('/food/serving/text()', food_node))[1]::text AS serving_value,
                (xpath('/food/serving/@units', food_node))[1]::text AS serving_units,
                (xpath('/food/calories/@total', food_node))[1]::text::int AS calories_total,
                (xpath('/food/calories/@fat', food_node))[1]::text::int AS calories_fat,
                (xpath('/food/total-fat/text()', food_node))[1]::text::numeric AS total_fat,
                (xpath('/food/saturated-fat/text()', food_node))[1]::text::numeric AS saturated_fat,
                (xpath('/food/cholesterol/text()', food_node))[1]::text::numeric AS cholesterol,
                (xpath('/food/sodium/text()', food_node))[1]::text::numeric AS sodium,
                (xpath('/food/carb/text()', food_node))[1]::text::numeric AS carb,
                (xpath('/food/fiber/text()', food_node))[1]::text::numeric AS fiber,
                (xpath('/food/protein/text()', food_node))[1]::text::numeric AS protein,
                (xpath('/food/vitamins/a/text()', food_node))[1]::text::int AS vitamin_a,
                (xpath('/food/vitamins/c/text()', food_node))[1]::text::int AS vitamin_c,
                (xpath('/food/minerals/ca/text()', food_node))[1]::text::int AS mineral_ca,
                (xpath('/food/minerals/fe/text()', food_node))[1]::text::int AS mineral_fe
            FROM (
                SELECT unnest(xpath('/nutrition/food', xml_data)) AS food_node
                FROM extract_demo.xml_content
            ) AS subquery;
        """
    )

    flatten_xml
