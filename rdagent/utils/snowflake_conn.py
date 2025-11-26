"""Snowflake connection helper.

Usage:
 - Set environment variables or Streamlit secrets: SNOW_USER, SNOW_PWD, SNOW_ACCOUNT,
   SNOW_WAREHOUSE, SNOW_DATABASE, SNOW_SCHEMA, SNOW_ROLE
 - Optionally provide SNOW_PRIVATE_KEY (PEM) for key-pair auth.

Example:
    from rdagent.utils.snowflake_conn import get_snowflake_conn
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute('SELECT current_version()')
    print(cur.fetchone())
    cur.close()
    conn.close()
"""
from __future__ import annotations

import os
from typing import Dict, Optional

try:
    import snowflake.connector
except Exception:  # pragma: no cover - only at runtime if package missing
    snowflake = None  # type: ignore


def _env(name: str) -> Optional[str]:
    return os.environ.get(name) or os.environ.get(name.lower())


def get_snowflake_conn() -> "snowflake.connector.connection.SnowflakeConnection":
    """Create and return a Snowflake connection using environment variables.

    Required (via env or Streamlit secrets): SNOW_USER, SNOW_ACCOUNT and either SNOW_PWD
    or SNOW_PRIVATE_KEY.
    Optional: SNOW_WAREHOUSE, SNOW_DATABASE, SNOW_SCHEMA, SNOW_ROLE
    """
    if snowflake is None:
        raise RuntimeError("snowflake-connector-python is not installed")

    user = _env("SNOW_USER")
    account = _env("SNOW_ACCOUNT")
    password = _env("SNOW_PWD")
    private_key_pem = _env("SNOW_PRIVATE_KEY")

    if not user or not account:
        raise RuntimeError("Missing SNOW_USER or SNOW_ACCOUNT environment variables")

    conn_kwargs: Dict[str, str] = {
        "user": user,
        "account": account,
    }

    for opt in ("SNOW_WAREHOUSE", "SNOW_DATABASE", "SNOW_SCHEMA", "SNOW_ROLE"):
        v = _env(opt)
        if v:
            conn_kwargs[opt.split("SNOW_")[1].lower()] = v

    # Prefer key-pair auth if provided
    if private_key_pem:
        # Defer import of cryptography to runtime only when needed
        try:
            from cryptography.hazmat.primitives import serialization
        except Exception as ex:  # pragma: no cover - informative
            raise RuntimeError("cryptography is required for private key auth") from ex

        pkey = serialization.load_pem_private_key(private_key_pem.encode(), password=None)
        conn_kwargs["private_key"] = pkey
    elif password:
        conn_kwargs["password"] = password
    else:
        raise RuntimeError("Provide SNOW_PWD or SNOW_PRIVATE_KEY for authentication")

    return snowflake.connector.connect(**conn_kwargs)


def run_query(query: str):
    """Execute a query and return results (list of tuples). Caller must not assume huge results."""
    conn = get_snowflake_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(query)
            return cur.fetchall()
        finally:
            cur.close()
    finally:
        conn.close()
