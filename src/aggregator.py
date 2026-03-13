import csv
import psycopg2
from datetime import datetime, timedelta
from .config import Config

def aggregate_and_export(start_date, end_date, output_file):
    conn = Config.get_connection()
    cur = conn.cursor()
    
    query = """
    """
    
    cur.execute(query, (start_date, end_date, end_date))
    rows = cur.fetchall()
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['day', 'new_accounts', 'anon_messages_pct', 
                        'total_messages', 'topic_growth_pct'])
        writer.writerows(rows)
    
    cur.close()
    conn.close()
    
    return rows