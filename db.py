import os
import json
import time
import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv

load_dotenv()

_pool = None


def _load_db_config():
    """Load DB credentials from AWS Secrets Manager if configured, else from env vars."""
    secret_name = os.environ.get("AWS_SECRET_NAME")
    if secret_name:
        try:
            import boto3
            region = os.environ.get("AWS_REGION", "ap-southeast-2")
            client = boto3.client("secretsmanager", region_name=region)
            resp = client.get_secret_value(SecretId=secret_name)
            secret = json.loads(resp["SecretString"])
            print(f"[db] Loaded credentials from AWS secret '{secret_name}'")
            return {
                "host":     secret["DB_HOST"],
                "port":     int(secret.get("DB_PORT", 3306)),
                "database": secret["DB_NAME"],
                "user":     secret["DB_USER"],
                "password": secret["DB_PASSWORD"],
            }
        except Exception as e:
            print(f"[db] AWS Secrets Manager failed ({e}), falling back to .env")

    return {
        "host":     os.environ["DB_HOST"],
        "port":     int(os.environ.get("DB_PORT", 3306)),
        "database": os.environ["DB_NAME"],
        "user":     os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
    }


def get_pool():
    global _pool
    if _pool is None:
        cfg = _load_db_config()
        _pool = pooling.MySQLConnectionPool(
            pool_name="lip_pool",
            pool_size=3,
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            user=cfg["user"],
            password=cfg["password"],
            connect_timeout=10,
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
