import psycopg2
from psycopg2 import sql
from pathlib import Path
from .config import Config


def load_csv_to_table(table_name, csv_path, columns):
    conn = Config.get_connection()
    cur = conn.cursor()

    with open(csv_path, "r", encoding="utf-8") as f:
        cur.copy_expert(
            sql.SQL(
                f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH CSV HEADER"
            ),
            f,
        )

    conn.commit()

    cur.execute(sql.SQL(f"SELECT COUNT(*) FROM {table_name}"))
    count = cur.fetchone()[0]

    cur.close()
    conn.close()
    return count


def load_all_data(truncate_tables=False):
    conn = Config.get_connection()
    cur = conn.cursor()

    if truncate_tables:
        cur.execute("TRUNCATE TABLE logs CASCADE")
        cur.execute("TRUNCATE TABLE messages CASCADE")
        cur.execute("TRUNCATE TABLE topics CASCADE")
        cur.execute("TRUNCATE TABLE users CASCADE")
        conn.commit()

    cur.close()
    conn.close()

    data_dir = Path(Config.DATA_DIR)

    stats = {}
    stats["users"] = load_csv_to_table(
        "users", data_dir / "users.csv", ["id", "username", "created_at"]
    )
    stats["topics"] = load_csv_to_table(
        "topics",
        data_dir / "topics.csv",
        ["id", "title", "text", "author", "deleted", "created_at"],
    )
    stats["messages"] = load_csv_to_table(
        "messages",
        data_dir / "messages.csv",
        ["id", "text", "topic", "author", "created_at"],
    )
    stats["logs"] = load_csv_to_table(
        "logs",
        data_dir / "logs.csv",
        ["action", "user_id", "timestamp", "target_type", "target_id", "success"],
    )

    return stats
