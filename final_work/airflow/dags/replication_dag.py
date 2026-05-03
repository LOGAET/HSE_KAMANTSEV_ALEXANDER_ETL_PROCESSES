from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import psycopg2
import psycopg2.extras

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

PG_CONN = dict(host='postgres', dbname='airflow', user='airflow', password='airflow')
MONGO_URI = 'mongodb://admin:admin@mongo:27017/'

def etl_sessions(**context):
    mongo = MongoClient(MONGO_URI)
    raw = list(mongo['etl_db']['UserSessions'].find({}, {'_id': 0}))

    seen, cleaned = set(), []
    for s in raw:
        if not s.get('session_id') or not s.get('user_id'):
            continue
        if s['session_id'] in seen:
            continue
        seen.add(s['session_id'])
        cleaned.append((
            s['session_id'],
            s['user_id'],
            s.get('start_time'),
            s.get('end_time'),
            s.get('pages_visited', []),
            s.get('device', 'unknown').lower(),
            s.get('actions', [])
        ))

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, """
        INSERT INTO staging.user_sessions
            (session_id, user_id, start_time, end_time, pages_visited, device, actions)
        VALUES %s
        ON CONFLICT (session_id) DO NOTHING
    """, cleaned)
    conn.commit()
    print(f"✅ Sessions loaded: {len(cleaned)}")


def etl_tickets(**context):
    mongo = MongoClient(MONGO_URI)
    raw = list(mongo['etl_db']['SupportTickets'].find({}, {'_id': 0}))

    seen, cleaned = set(), []
    valid_statuses = {'open', 'closed', 'pending', 'resolved'}
    for t in raw:
        if not t.get('ticket_id') or t['ticket_id'] in seen:
            continue
        seen.add(t['ticket_id'])
        cleaned.append((
            t['ticket_id'],
            t.get('user_id'),
            t.get('status', 'unknown') if t.get('status') in valid_statuses else 'unknown',
            t.get('issue_type', 'unknown'),
            t.get('created_at'),
            t.get('updated_at')
        ))

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, """
        INSERT INTO staging.support_tickets
            (ticket_id, user_id, status, issue_type, created_at, updated_at)
        VALUES %s
        ON CONFLICT (ticket_id) DO NOTHING
    """, cleaned)
    conn.commit()
    print(f"✅ Tickets loaded: {len(cleaned)}")


def etl_events(**context):
    mongo = MongoClient(MONGO_URI)
    raw = list(mongo['etl_db']['EventLogs'].find({}, {'_id': 0}))

    seen, cleaned = set(), []
    for e in raw:
        if not e.get('event_id') or e['event_id'] in seen:
            continue
        if not e.get('timestamp'):
            continue
        seen.add(e['event_id'])
        cleaned.append((
            e['event_id'],
            e['timestamp'],
            e.get('event_type', 'unknown'),
            str(e.get('details', ''))
        ))

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, """
        INSERT INTO staging.event_logs
            (event_id, event_timestamp, event_type, details)
        VALUES %s
        ON CONFLICT DO NOTHING
    """, cleaned)
    conn.commit()
    print(f"✅ Events loaded: {len(cleaned)}")


with DAG(
        dag_id='mongo_to_postgres_replication',
        default_args=default_args,
        description='Репликация данных MongoDB → PostgreSQL с трансформацией',
        schedule_interval='0 1 * * *',
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['replication', 'etl']
) as dag:
    t1 = PythonOperator(task_id='etl_user_sessions', python_callable=etl_sessions)
    t2 = PythonOperator(task_id='etl_support_tickets', python_callable=etl_tickets)
    t3 = PythonOperator(task_id='etl_event_logs', python_callable=etl_events)

    t1 >> t2 >> t3
