import os
from dotenv import load_dotenv
from datetime import datetime
import psycopg2

load_dotenv()


class Config:
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "forum")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

    DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

    DEFAULT_START_DATE = datetime(2026, 1, 1)
    DEFAULT_DAYS = 30
    MIN_USERS = 50
    MIN_ACTIONS_PER_TYPE = 5

    @classmethod
    def get_db_url(cls):
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"

    @classmethod
    def get_db_dict(cls):
        return {
            "host": cls.DB_HOST,
            "port": cls.DB_PORT,
            "database": cls.DB_NAME,
            "user": cls.DB_USER,
            "password": cls.DB_PASSWORD,
            "options": "-c timezone=UTC"
        }

    @classmethod
    def get_connection(cls):
        return psycopg2.connect(**Config.get_db_dict()) # type: ignore
