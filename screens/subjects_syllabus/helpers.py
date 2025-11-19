"""
Helper functions for data conversion and utilities
"""

from typing import Any, Dict, Optional
import pandas as pd
import json
from sqlalchemy import text as sa_text


def safe_int(val: Any, default: int = 0) -> int:
    """Safely convert value to integer."""
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return default
        return int(val)
    except Exception:
        return default


def safe_float(val: Any, default: float = 0.0) -> float:
    """Safely convert value to float."""
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return default
        return float(val)
    except Exception:
        return default


def to_bool(val: Any, default: bool = True) -> bool:
    """Safely convert value to boolean."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default

    if isinstance(val, bool):
        return val

    s = str(val).strip().lower()

    if s in ("1", "true", "yes", "y", "t"):
        return True

    if s in ("0", "false", "no", "n", "f"):
        return False

    return default


def exec_query(conn, sql: str, params: Dict[str, Any] = None):
    """Execute a SQL query using SQLAlchemy."""
    return conn.execute(sa_text(sql), params or {})


def table_exists(conn, table: str) -> bool:
    """Check if a table exists in the database."""
    result = exec_query(
        conn,
        "SELECT name FROM sqlite_master WHERE type='table' AND name=:t",
        {"t": table}
    ).fetchone()
    return result is not None


def dict_from_row(row):
    """Convert SQLAlchemy row to dictionary."""
    return dict(row._mapping) if row else None


def rows_to_dicts(rows):
    """Convert list of SQLAlchemy rows to list of dictionaries."""
    return [dict(r._mapping) for r in rows] if rows else []
