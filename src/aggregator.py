import csv
import psycopg2
from datetime import datetime, timedelta
from .config import Config

def aggregate_and_export(start_date, end_date, output_file):
    conn = Config.get_connection()
    cur = conn.cursor()
    
    query = """
    WITH report_period AS (
	    SELECT generate_series(%s::date, %s::date, '1 day')::date AS day
    ),
    daily_stats AS (
        SELECT 
            DATE(l.timestamp) as day,
            COUNT(DISTINCT CASE WHEN l.action = 2 THEN l.user_id END) AS  new_accounts,
            COUNT(CASE WHEN l.action = 8 AND l.user_id IS NULL THEN 1 END) AS anon_messages,
            COUNT(CASE WHEN l.action = 8 THEN 1 END) AS total_messages,
            COUNT(CASE WHEN l.action = 5 AND l.success = true THEN 1 END) AS new_topics
        FROM logs l
        GROUP BY DATE(l.timestamp)
    )
    SELECT 
        rp.day,
        COALESCE(new_accounts, 0),
        COALESCE(ROUND(100.0 * anon_messages / NULLIF(total_messages, 0), 2), 0) AS anon_messages_pct,
        COALESCE(total_messages, 0),
        COALESCE(new_topics, 0),
        COALESCE(topic_growth_pct, 0)
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