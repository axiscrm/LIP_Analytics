import os
import json
import time
import logging
import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("lip_analytics.db")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")

_pool = None


def _load_db_config():
    """Load DB credentials from AWS Secrets Manager if configured, else from env vars.

    When AWS_SECRET_NAME is set the app expects credentials to come from the
    secret — it will NOT fall back to .env for DB vars.  If the secret cannot
    be reached, the error is raised so it surfaces clearly in the logs.
    """
    secret_name = os.environ.get("AWS_SECRET_NAME")

    if secret_name:
        region = os.environ.get("AWS_REGION", "ap-southeast-2")
        log.info("AWS_SECRET_NAME is set — loading DB credentials from Secrets Manager "
                 "(secret=%s, region=%s)", secret_name, region)
        try:
            import boto3
        except ImportError:
            raise RuntimeError(
                "boto3 is required when AWS_SECRET_NAME is set. "
                "Install it with: pip install boto3"
            )

        try:
            client = boto3.client("secretsmanager", region_name=region)
            resp = client.get_secret_value(SecretId=secret_name)
        except Exception as e:
            raise RuntimeError(
                f"Failed to retrieve secret '{secret_name}' from AWS Secrets Manager "
                f"(region={region}): {e}"
            ) from e

        try:
            secret = json.loads(resp["SecretString"])
        except (json.JSONDecodeError, KeyError) as e:
            raise RuntimeError(
                f"Secret '{secret_name}' is not valid JSON: {e}"
            ) from e

        missing = [k for k in ("DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD") if k not in secret]
        if missing:
            raise RuntimeError(
                f"Secret '{secret_name}' is missing required keys: {', '.join(missing)}"
            )

        log.info("Successfully loaded DB credentials from AWS secret '%s' "
                 "(host=%s, db=%s, user=%s)",
                 secret_name, secret["DB_HOST"], secret["DB_NAME"], secret["DB_USER"])
        return {
            "host":     secret["DB_HOST"],
            "port":     int(secret.get("DB_PORT", 3306)),
            "database": secret["DB_NAME"],
            "user":     secret["DB_USER"],
            "password": secret["DB_PASSWORD"],
        }

    # No AWS secret configured — read from environment / .env
    log.info("AWS_SECRET_NAME not set — loading DB credentials from environment variables")
    missing = [k for k in ("DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD") if k not in os.environ]
    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}. "
            "Set them in .env or configure AWS_SECRET_NAME."
        )

    log.info("Loaded DB credentials from .env (host=%s, db=%s, user=%s)",
             os.environ["DB_HOST"], os.environ["DB_NAME"], os.environ["DB_USER"])
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
        log.info("Creating connection pool (host=%s, db=%s, pool_size=3)", cfg["host"], cfg["database"])
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
        log.info("Connection pool created successfully")
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
            log.warning("get_connection attempt %d/%d failed: %s", attempt + 1, retries, e)
            if attempt < retries - 1:
                time.sleep(delay)
    log.error("get_connection failed after %d retries: %s", retries, last_err)
    raise last_err
