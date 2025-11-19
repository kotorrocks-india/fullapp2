# schemas/rubrics_schema.py
"""
Complete schema for the Rubrics module.
Based on operations in rubrics_service.py
"""

from __future__ import annotations
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
import logging

# Assuming you have a schema registry like in your other files
try:
    from core.schema_registry import register
except ImportError:
    # Fallback if no registry exists
    def register(func):
        return func

logger = logging.getLogger(__name__)


def _exec(conn, sql: str, params: dict = None):
    """Execute SQL with parameters."""
    return conn.execute(sa_text(sql), params or {})


@register
def install_rubrics_schema(engine: Engine):
    """
    Install all tables for the Rubrics module.
    Safe to run multiple times (idempotent).
    """
    logger.info("Installing Rubrics schema...")
    
    with engine.begin() as conn:
        
        # 1. Rubric Configurations
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS rubric_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            offering_id INTEGER NOT NULL,
            scope TEXT NOT NULL DEFAULT 'subject',
            component_key TEXT,
            mode TEXT NOT NULL DEFAULT 'analytic_points',
            co_linking_enabled INTEGER NOT NULL DEFAULT 0,
            normalization_enabled INTEGER NOT NULL DEFAULT 1,
            visible_to_students INTEGER NOT NULL DEFAULT 1,
            show_before_assessment INTEGER NOT NULL DEFAULT 1,
            version INTEGER NOT NULL DEFAULT 1,
            is_locked INTEGER NOT NULL DEFAULT 0,
            locked_reason TEXT,
            status TEXT NOT NULL DEFAULT 'draft',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            created_by TEXT,
            updated_at DATETIME,
            updated_by TEXT,
            
            UNIQUE(offering_id, scope, component_key)
        )
        """)
        
        # 2. Rubric Assessments
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS rubric_assessments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rubric_config_id INTEGER NOT NULL,
            code TEXT NOT NULL,
            title TEXT NOT NULL,
            max_marks REAL NOT NULL,
            mode TEXT NOT NULL,
            component_key TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME,
            
            FOREIGN KEY (rubric_config_id) REFERENCES rubric_configs(id) ON DELETE CASCADE,
            UNIQUE(rubric_config_id, code)
        )
        """)
        
        # 3. Criteria (for analytic_points)
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS rubric_assessment_criteria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            assessment_id INTEGER NOT NULL,
            criterion_key TEXT NOT NULL,
            weight_pct REAL NOT NULL,
            linked_cos TEXT, -- JSON list
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME,
            
            FOREIGN KEY (assessment_id) REFERENCES rubric_assessments(id) ON DELETE CASCADE,
            UNIQUE(assessment_id, criterion_key)
        )
        """)
        
        # 4. Levels (for analytic_levels)
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS rubric_assessment_levels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            assessment_id INTEGER NOT NULL,
            criterion_key TEXT NOT NULL,
            criterion_weight_pct REAL NOT NULL,
            level_label TEXT NOT NULL,
            level_score REAL NOT NULL,
            level_descriptor TEXT,
            level_sequence INTEGER NOT NULL DEFAULT 0,
            linked_cos TEXT, -- JSON list
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME,
            
            FOREIGN KEY (assessment_id) REFERENCES rubric_assessments(id) ON DELETE CASCADE
        )
        """)
        
        # 5. Criteria Catalog (THE MISSING TABLE)
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS rubric_criteria_catalog (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT NOT NULL UNIQUE,
            label TEXT NOT NULL,
            description TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME
        )
        """)
        
        # 6. Rubrics Audit Trail
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS rubrics_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            occurred_at_utc DATETIME DEFAULT CURRENT_TIMESTAMP,
            rubric_config_id INTEGER,
            offering_id INTEGER,
            scope TEXT,
            action TEXT NOT NULL,
            note TEXT,
            changed_fields TEXT,
            actor_id TEXT,
            actor_role TEXT,
            operation TEXT,
            reason TEXT,
            source TEXT,
            step_up_performed INTEGER DEFAULT 0
        )
        """)
        
        # 7. Version Snapshots
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS version_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            entity_type TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            snapshot_reason TEXT,
            actor TEXT,
            snapshot_data TEXT, -- JSON blob
            version_number INTEGER
        )
        """)
        
        logger.info("✓ Installed rubrics_schema")
