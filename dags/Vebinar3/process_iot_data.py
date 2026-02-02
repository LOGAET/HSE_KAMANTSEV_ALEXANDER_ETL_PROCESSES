from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.bases.hook import BaseHook
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pandas as pd
import logging

def process_iot_data():
    df = pd.read_csv('/opt/airflow/data/IOT-temp.csv')
    
    df = df[df['out/in'] == 'In'].copy()
    
    df['noted_date'] = pd.to_datetime(df['noted_date'], format="%d-%m-%Y %H:%M").dt.date
    
    low, high = df['temp'].quantile(0.05), df['temp'].quantile(0.95)

    filtered = df[(df['temp'] >= low) & (df['temp'] <= high)].copy()
    
    daily_avg = filtered.groupby('noted_date')['temp'].mean().round(1).reset_index()
    
    hottest = daily_avg.sort_values(by='temp', ascending=False).head(5).copy()
    coldest = daily_avg.sort_values(by='temp', ascending=True).head(5).copy()
    
    hottest['type'] = 'hottest'
    coldest['type'] = 'coldest'
    results = pd.concat([hottest, coldest])
    results['created_at'] = datetime.now()
    
    conn = BaseHook.get_connection('demo_db')
    uri = conn.get_uri()
    
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)
        
    engine = create_engine(uri)
    
    results.to_sql('temp_extremes_report', engine, schema='extract_demo', if_exists='replace', index=False)
    
    logging.info("Результаты успешно сохранены в таблицу temp_extremes_report")
    logging.info(f"Hottest:\n{hottest}")
    logging.info(f"Coldest:\n{coldest}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'iot_temp_analysis_db',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    analyze_task = PythonOperator(
        task_id='analyze_temperatures',
        python_callable=process_iot_data
    )

    analyze_task