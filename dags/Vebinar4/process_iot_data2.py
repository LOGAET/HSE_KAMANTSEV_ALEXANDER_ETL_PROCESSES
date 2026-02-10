from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.bases.hook import BaseHook
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import pandas as pd
import logging

def process_iot_data(mode='full', days=5, **kwargs):
    df = pd.read_csv('/opt/airflow/data/IOT-temp.csv')
    df = df[df['out/in'] == 'In'].copy()
    df['noted_date'] = pd.to_datetime(df['noted_date'], format="%d-%m-%Y %H:%M").dt.date
    
    if mode == 'incremental':
        execution_date = kwargs['logical_date'].date()
        start_filter_date = execution_date - timedelta(days=days)
        logging.info(f"Инкрементальная загрузка: данные с {start_filter_date}")
        df = df[df['noted_date'] >= start_filter_date]
        
        if df.empty:
            logging.info("Нет новых данных для обработки")
            return

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
    
    if mode == 'full':
        results.to_sql('temp_extremes_report', engine, schema='extract_demo', if_exists='replace', index=False)
        logging.info("Исторические данные полностью перезаписаны")
    else:
        target_dates = results['noted_date'].unique().tolist()
        dates_str = ", ".join([f"'{d}'" for d in target_dates])
        
        with engine.connect() as connection:
            connection.execute(text(f"DELETE FROM extract_demo.temp_extremes_report WHERE noted_date IN ({dates_str})"))
            connection.commit()
            
        results.to_sql('temp_extremes_report', engine, schema='extract_demo', if_exists='append', index=False)
        logging.info(f"Инкрементальные данные добавлены/обновлены за даты: {dates_str}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'iot_temp_data_loading',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    historical_load = PythonOperator(
        task_id='historical_full_load',
        python_callable=process_iot_data,
        op_kwargs={'mode': 'full'}
    )

    incremental_load = PythonOperator(
        task_id='incremental_load',
        python_callable=process_iot_data,
        op_kwargs={'mode': 'incremental','days':5}
    )

    historical_load >> incremental_load