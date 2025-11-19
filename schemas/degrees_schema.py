# app/schemas/degrees_schema.py
"""
Degrees schema and migration for curriculum flags.
Keeps audit + indexes. Safe to run multiple times.
"""

from __future__ import annotations
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

# Helper to run DDL idempotently
def _exec(conn, sql: str):
    conn.execute(sa_text(sql))

def _has_column(conn, table: str, col: str) -> bool:
    """Helper to check if a column exists in a SQLite table."""
    rows = conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()
    return any(r[1].lower() == col.lower() for r in rows)


def ensure_degrees_schema(engine: Engine):
    """
    Original helper – kept for compatibility, but not used as the main entry.
    """
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS degrees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            cohort_splitting_mode TEXT NOT NULL DEFAULT 'both',
            roll_number_scope TEXT NOT NULL DEFAULT 'degree',
            logo_file_name TEXT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 100,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)

        _exec(conn, "CREATE UNIQUE INDEX IF NOT EXISTS uq_degrees_code ON degrees(code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_degrees_active ON degrees(active)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_degrees_sort ON degrees(sort_order)")

        _exec(conn, """
        CREATE TABLE IF NOT EXISTS degrees_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            degree_code TEXT NOT NULL,
            action TEXT NOT NULL,
            note TEXT NULL,
            changed_fields TEXT NULL,    -- JSON blob
            actor TEXT NULL,
            at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_deg_audit_code ON degrees_audit(degree_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_deg_audit_at ON degrees_audit(at)")


def migrate_degrees(engine: Engine):
    """
    Ensures the degrees table exists and adds new curriculum-governance (cg) flags.
    New behaviour:
    - degrees.active defaults to 0 (inactive) until programs/branches/semesters exist.
    """
    with engine.begin() as conn:
        # Base table – using your latest structure (code as PK, created_at present).
        _exec(conn, """
            CREATE TABLE IF NOT EXISTS degrees(
                code TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                cohort_splitting_mode TEXT NOT NULL DEFAULT 'both',
                roll_number_scope TEXT NOT NULL DEFAULT 'degree',
                logo_file_name TEXT,
                active INTEGER NOT NULL DEFAULT 0,
                sort_order INTEGER NOT NULL DEFAULT 100,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Curriculum flags
        if not _has_column(conn, "degrees", "cg_degree"):
            _exec(conn, "ALTER TABLE degrees ADD COLUMN cg_degree INTEGER NOT NULL DEFAULT 0")
        if not _has_column(conn, "degrees", "cg_program"):
            _exec(conn, "ALTER TABLE degrees ADD COLUMN cg_program INTEGER NOT NULL DEFAULT 0")
        if not _has_column(conn, "degrees", "cg_branch"):
            _exec(conn, "ALTER TABLE degrees ADD COLUMN cg_branch INTEGER NOT NULL DEFAULT 0")

        # Audit table
        _exec(conn, """
            CREATE TABLE IF NOT EXISTS degrees_audit(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                degree_code TEXT NOT NULL,
                action TEXT NOT NULL,
                note TEXT,
                changed_fields TEXT,
                actor TEXT,
                at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_deg_audit_code ON degrees_audit(degree_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_deg_audit_at ON degrees_audit(at)")
        
        # Ensure indexes for degrees table
        _exec(conn, "CREATE UNIQUE INDEX IF NOT EXISTS uq_degrees_code ON degrees(code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_degrees_active ON degrees(active)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_degrees_sort ON degrees(sort_order)")


def run(engine: Engine):
    """Entry point for schema registry auto-discovery."""
    migrate_degrees(engine)
