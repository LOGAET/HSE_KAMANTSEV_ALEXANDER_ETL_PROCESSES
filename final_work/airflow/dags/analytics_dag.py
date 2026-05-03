from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

CONN = 'postgres_default'

SQL_USER_ACTIVITY = """
    TRUNCATE TABLE analytics.user_activity;
    INSERT INTO analytics.user_activity
        (user_id, activity_date, total_sessions, avg_session_min, total_pages, most_used_device)
    SELECT
        user_id,
        DATE(start_time)                                                    AS activity_date,
        COUNT(session_id)                                                   AS total_sessions,
        ROUND(AVG(EXTRACT(EPOCH FROM (end_time - start_time)) / 60), 2)    AS avg_session_min,
        COALESCE(SUM(array_length(pages_visited, 1)), 0)                   AS total_pages,
        MODE() WITHIN GROUP (ORDER BY device)                              AS most_used_device
    FROM staging.user_sessions
    WHERE start_time IS NOT NULL
      AND end_time IS NOT NULL
      AND end_time > start_time
    GROUP BY user_id, DATE(start_time);
"""

SQL_SUPPORT_STATS = """
    TRUNCATE TABLE analytics.support_stats;
    INSERT INTO analytics.support_stats
        (status, issue_type, week, ticket_count, avg_resolution_hours, open_tickets)
    SELECT
        status,
        issue_type,
        DATE_TRUNC('week', created_at)::DATE                               AS week,
        COUNT(ticket_id)                                                    AS ticket_count,
        ROUND(AVG(EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600), 2) AS avg_resolution_hours,
        SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END)                  AS open_tickets
    FROM staging.support_tickets
    WHERE created_at IS NOT NULL
    GROUP BY status, issue_type, DATE_TRUNC('week', created_at)::DATE;
"""

with DAG(
    dag_id='build_analytics_marts',
    description='Построение аналитических витрин из staging-данных',
    schedule_interval='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['analytics', 'marts']
) as dag:

    build_activity = PostgresOperator(
        task_id='build_user_activity',
        postgres_conn_id=CONN,
        sql=SQL_USER_ACTIVITY
    )

    build_support = PostgresOperator(
        task_id='build_support_stats',
        postgres_conn_id=CONN,
        sql=SQL_SUPPORT_STATS
    )

    build_activity >> build_support
