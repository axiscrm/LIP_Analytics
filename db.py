import os
import time
import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv

load_dotenv()

_pool = None

def get_pool():
    global _pool
    if _pool is None:
        _pool = pooling.MySQLConnectionPool(
            pool_name="lip_pool",
            pool_size=5,          # increased from 3 → avoids exhaustion on rapid navigation
            host=os.environ["DB_HOST"],
            port=int(os.environ.get("DB_PORT", 3306)),
            database=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            connect_timeout=30,
            autocommit=True,
        )
    return _pool

def get_connection(retries=4, delay=0.4):
    """Get a pooled connection with automatic retry on pool exhaustion."""
    pool = get_pool()
    last_err = None
    for attempt in range(retries):
        try:
            return pool.get_connection()
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(delay)   # wait briefly then retry
    raise last_err
