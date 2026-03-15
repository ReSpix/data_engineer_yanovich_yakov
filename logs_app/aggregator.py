import csv
import psycopg2
from datetime import datetime, timedelta
from .config import Config

def check_and_refresh_mv(connection: psycopg2.extensions.connection):
    with connection.cursor() as cursor:
        query_check = """
            WITH max_dates AS (
                SELECT 
                    (SELECT MAX((timestamp AT TIME ZONE 'UTC')::date) FROM logs) AS date1,
                    (SELECT MAX((day AT TIME ZONE 'UTC')) FROM mv_topic_growth) AS date2
            )
            SELECT 
                date1 = date2 AS dates_are_equal
            FROM max_dates;
        """
        cursor.execute(query_check)
        result = cursor.fetchone()
        if result and not result[0]:
            upate_mv_query = """
            REFRESH MATERIALIZED VIEW CONCURRENTLY mv_topic_growth;
            """
            try:
                cursor.connection.commit()
                cursor.execute(upate_mv_query)
                cursor.connection.commit()
            except psycopg2.DatabaseError as e:
                print(f"Ошибка обновления materialized view: {e}")
                cursor.connection.rollback()


def aggregate_and_export(start_date, end_date, output_file):
    conn = Config.get_connection()
    cur = conn.cursor()

    check_and_refresh_mv(conn)

    query = """
    WITH report_period AS (
	    SELECT generate_series(%s::date, %s::date, '1 day')::date AS day
    ),
    daily_stats AS (
        SELECT 
            (l.timestamp AT TIME ZONE 'UTC')::date AS day,
            COUNT(DISTINCT CASE WHEN l.action = 2 THEN l.user_id END) AS  new_accounts,
            COUNT(CASE WHEN l.action = 8 AND l.user_id IS NULL THEN 1 END) AS anon_messages,
            COUNT(CASE WHEN l.action = 8 THEN 1 END) AS total_messages,
            COUNT(CASE WHEN l.action = 5 AND l.success = true THEN 1 END) AS new_topics
        FROM logs l
        GROUP BY (l.timestamp AT TIME ZONE 'UTC')::date
    )
    SELECT 
        rp.day,
        COALESCE(new_accounts, 0) AS new_accounts,
        COALESCE(ROUND(100.0 * anon_messages / NULLIF(total_messages, 0), 2), 0) AS anon_messages_pct,
        COALESCE(total_messages, 0) AS total_messages,
        COALESCE(new_topics, 0) AS new_topics,
        COALESCE(topic_growth_pct, 0) AS topic_growth_pct
    FROM report_period AS rp
    LEFT JOIN daily_stats AS ds ON ds.DAY = rp.DAY
    LEFT JOIN mv_topic_growth AS vtg ON vtg.day = rp.DAY;
    """
    
    cur.execute(query, (start_date, end_date))
    rows = cur.fetchall()
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([desc[0] for desc in cur.description])
        writer.writerows(rows)
    
    cur.close()
    conn.close()
    
    return rows