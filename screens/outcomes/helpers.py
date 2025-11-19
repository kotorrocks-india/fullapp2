# screens/outcomes/helpers.py
"""
Helper functions for outcomes module.
Database queries, validation, and utility functions.
"""

from __future__ import annotations
from typing import Optional, List, Dict, Any
from sqlalchemy import text as sa_text


# ============================================================================
# DATABASE HELPERS
# ============================================================================

def table_exists(conn, table_name: str) -> bool:
    """Check if a table OR view exists in the database."""
    # FIX: Checks for both 'table' and 'view' types
    row = conn.execute(sa_text(
        "SELECT name FROM sqlite_master WHERE (type='table' OR type='view') AND name=:t"
    ), {"t": table_name}).fetchone()
    return bool(row)


def has_column(conn, table_name: str, col: str) -> bool:
    """Check if a column exists in a table."""
    rows = conn.execute(sa_text(f"PRAGMA table_info({table_name})")).fetchall()
    return any(r[1].lower() == col.lower() for r in rows)


def fetch_degrees(conn):
    """
    Fetch degrees for UI selection.
    Uses the 'degrees' VIEW to ensure only fully configured degrees are shown.
    """
    if not table_exists(conn, "degrees"):
        return []

    return conn.execute(sa_text("""
        SELECT code, title, cohort_splitting_mode, active, sort_order
        FROM degrees
        WHERE active = 1
        ORDER BY sort_order, code
    """)).fetchall()


def fetch_programs(conn, degree_code: str):
    """Fetch programs for a degree."""
    if not table_exists(conn, "programs"):
        return []

    return conn.execute(sa_text("""
        SELECT id, program_code, program_name, active, sort_order
        FROM programs
        WHERE lower(degree_code) = lower(:dc) AND active = 1
        ORDER BY sort_order, program_code
    """), {"dc": degree_code}).fetchall()


def fetch_branches(conn, degree_code: str, program_id: Optional[int] = None):
    """Fetch branches for a degree/program."""
    if not table_exists(conn, "branches"):
        return []
    
    # Check schema type
    has_degree_col = has_column(conn, "branches", "degree_code")
    has_program_col = has_column(conn, "branches", "program_id")
    
    if program_id and has_program_col:
        # Get branches for specific program
        return conn.execute(sa_text("""
            SELECT id, branch_code, branch_name, active, sort_order
            FROM branches
            WHERE program_id = :pid AND active = 1
            ORDER BY sort_order, branch_code
        """), {"pid": program_id}).fetchall()
    elif has_degree_col:
        # Get all branches for degree (direct link)
        return conn.execute(sa_text("""
            SELECT id, branch_code, branch_name, active, sort_order
            FROM branches
            WHERE lower(degree_code) = lower(:dc) AND active = 1
            ORDER BY sort_order, branch_code
        """), {"dc": degree_code}).fetchall()
    elif has_program_col:
        # Get all branches via programs
        return conn.execute(sa_text("""
            SELECT b.id, b.branch_code, b.branch_name, b.active, b.sort_order
            FROM branches b
            JOIN programs p ON b.program_id = p.id
            WHERE lower(p.degree_code) = lower(:dc) AND b.active = 1
            ORDER BY b.sort_order, b.branch_code
        """), {"dc": degree_code}).fetchall()
    
    return []


def get_scope_config(conn, degree_code: str) -> str:
    """Get scope configuration for a degree."""
    if not table_exists(conn, "outcomes_scope_config"):
        return "per_program"

    # Read configured scope (or default)
    result = conn.execute(
        sa_text(
            """
            SELECT scope_level
            FROM outcomes_scope_config
            WHERE lower(degree_code) = lower(:dc)
            """
        ),
        {"dc": degree_code},
    ).fetchone()

    configured_scope = result[0] if result else "per_program"

    # If there is no programs table, just return configured_scope
    if not table_exists(conn, "programs"):
        return configured_scope

    # Check if this degree actually has any active programs
    has_program = conn.execute(
        sa_text(
            """
            SELECT 1
            FROM programs
            WHERE lower(degree_code) = lower(:dc)
              AND active = 1
            LIMIT 1
            """
        ),
        {"dc": degree_code},
    ).fetchone()

    # If no programs exist for this degree, force per_degree scope
    if has_program is None:
        return "per_degree"

    # Otherwise, use whatever is configured
    return configured_scope


def get_outcome_sets(conn, degree_code: str, program_code: Optional[str] = None,
                     branch_code: Optional[str] = None, set_type: Optional[str] = None,
                     status: Optional[str] = None, include_archived: bool = False):
    """Fetch outcome sets with filters."""
    if not table_exists(conn, "outcomes_sets"):
        return []

    conditions = ["lower(degree_code) = lower(:dc)"]
    params = {"dc": degree_code}
    
    if program_code:
        conditions.append("lower(program_code) = lower(:pc)")
        params["pc"] = program_code
    else:
        conditions.append("program_code IS NULL")
        
    if branch_code:
        conditions.append("lower(branch_code) = lower(:bc)")
        params["bc"] = branch_code
    else:
        conditions.append("branch_code IS NULL")
    
    if set_type:
        conditions.append("set_type = :st")
        params["st"] = set_type
    
    if status:
        conditions.append("status = :status")
        params["status"] = status
    elif not include_archived:
        conditions.append("status != 'archived'")
    
    where_clause = " AND ".join(conditions)
    
    return conn.execute(sa_text(f"""
        SELECT id, degree_code, program_code, branch_code, set_type, status,
               version, is_current, created_by, created_at, published_at, 
               published_by, archived_at, archived_by, archive_reason
        FROM outcomes_sets
        WHERE {where_clause}
        ORDER BY set_type, version DESC, created_at DESC
    """), params).fetchall()


def get_outcome_items(conn, set_id: int):
    """Fetch items for an outcome set."""
    if not table_exists(conn, "outcomes_items"):
        return []

    return conn.execute(sa_text("""
        SELECT id, code, title, description, bloom_level, timeline_years,
               tags, sort_order, created_by, created_at, updated_by, updated_at
        FROM outcomes_items
        WHERE set_id = :sid
        ORDER BY sort_order, code
    """), {"sid": set_id}).fetchall()


def get_set_by_id(conn, set_id: int):
    """Get a single outcome set by ID."""
    if not table_exists(conn, "outcomes_sets"):
        return None

    return conn.execute(sa_text("""
        SELECT id, degree_code, program_code, branch_code, set_type, status,
               version, is_current, created_by, created_at, updated_by, updated_at,
               published_by, published_at, archived_by, archived_at, archive_reason
        FROM outcomes_sets
        WHERE id = :sid
    """), {"sid": set_id}).fetchone()


# ============================================================================
# CRUD OPERATIONS
# ============================================================================

def create_outcome_set(conn, degree_code: str, set_type: str, 
                       program_code: Optional[str], branch_code: Optional[str],
                       actor: str) -> int:
    """Create a new outcome set and return its ID."""
    conn.execute(sa_text("""
        INSERT INTO outcomes_sets 
        (degree_code, program_code, branch_code, set_type, status, 
         created_by, created_at)
        VALUES (:dc, :pc, :bc, :st, 'draft', :actor, CURRENT_TIMESTAMP)
    """), {
        "dc": degree_code,
        "pc": program_code,
        "bc": branch_code,
        "st": set_type,
        "actor": actor
    })
    
    result = conn.execute(sa_text("SELECT last_insert_rowid()")).fetchone()
    return result[0]


def add_outcome_item(conn, set_id: int, code: str, description: str,
                     title: Optional[str], bloom_level: Optional[str],
                     timeline_years: Optional[int], tags: str, 
                     sort_order: int, actor: str):
    """Add an item to an outcome set."""
    conn.execute(sa_text("""
        INSERT INTO outcomes_items
        (set_id, code, title, description, bloom_level, timeline_years,
         tags, sort_order, created_by, created_at)
        VALUES (:sid, :code, :title, :desc, :bloom, :years, 
                :tags, :sort, :actor, CURRENT_TIMESTAMP)
    """), {
        "sid": set_id,
        "code": code,
        "title": title,
        "desc": description,
        "bloom": bloom_level,
        "years": timeline_years,
        "tags": tags,
        "sort": sort_order,
        "actor": actor
    })


def update_outcome_item(conn, item_id: int, title: Optional[str], 
                        description: str, bloom_level: Optional[str],
                        timeline_years: Optional[int], tags: str, actor: str):
    """Update an outcome item."""
    conn.execute(sa_text("""
        UPDATE outcomes_items
        SET title = :title,
            description = :desc,
            bloom_level = :bloom,
            timeline_years = :years,
            tags = :tags,
            updated_by = :actor,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = :id
    """), {
        "id": item_id,
        "title": title,
        "desc": description,
        "bloom": bloom_level,
        "years": timeline_years,
        "tags": tags,
        "actor": actor
    })


def delete_outcome_item(conn, item_id: int):
    """Delete an outcome item."""
    conn.execute(sa_text("DELETE FROM outcomes_items WHERE id = :id"), {"id": item_id})


def publish_outcome_set(conn, set_id: int, actor: str) -> None:
    """Mark an outcome set as published."""
    conn.execute(
        sa_text(
            """
            UPDATE outcomes_sets
            SET status = 'published',
                is_current = 1,
                published_by = :actor,
                published_at = CURRENT_TIMESTAMP,
                updated_by = :actor,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :set_id
            """
        ),
        {"actor": actor, "set_id": set_id},
    )


def unpublish_outcome_set(conn, set_id: int, actor: str) -> None:
    """Mark an outcome set back to draft."""
    conn.execute(
        sa_text(
            """
            UPDATE outcomes_sets
            SET status = 'draft',
                is_current = 1,
                published_by = NULL,
                published_at = NULL,
                updated_by = :actor,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :set_id
            """
        ),
        {"actor": actor, "set_id": set_id},
    )


def archive_outcome_set(conn, set_id: int, reason: str, actor: str) -> None:
    """Archive an outcome set."""
    conn.execute(
        sa_text(
            """
            UPDATE outcomes_sets
            SET status = 'archived',
                is_current = 0,
                archived_by = :actor,
                archived_at = CURRENT_TIMESTAMP,
                archive_reason = :reason,
                updated_by = :actor,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :set_id
            """
        ),
        {"actor": actor, "set_id": set_id, "reason": reason},
    )


def delete_outcome_set(conn, set_id: int):
    """Delete an outcome set and all its items."""
    conn.execute(sa_text("DELETE FROM outcomes_sets WHERE id = :id"), {"id": set_id})


# ============================================================================
# AUDIT & VERSIONING
# ============================================================================

def audit_operation(conn, event_type: str, actor: str, actor_role: str,
                   set_id: Optional[int] = None, item_id: Optional[int] = None,
                   code: Optional[str] = None,
                   reason: Optional[str] = None, 
                   before_data: Optional[str] = None,
                   after_data: Optional[str] = None,
                   change_type: Optional[str] = None,
                   approval_id: Optional[int] = None,
                   direction: Optional[str] = None,
                   **kwargs):
    """Record an audit event."""
    import json
    
    if not table_exists(conn, "outcomes_audit"):
        return

    # Build metadata from extra kwargs
    metadata = {}
    if change_type:
        metadata['change_type'] = change_type
    if approval_id:
        metadata['approval_id'] = approval_id
    if direction:
        metadata['direction'] = direction
    if code:
        metadata['code'] = code
    
    # Add any other kwargs to metadata
    for k, v in kwargs.items():
        if k not in ['change_type', 'approval_id', 'direction', 'code']:
            metadata[k] = v
    
    # Convert metadata to JSON if present
    metadata_json = json.dumps(metadata) if metadata else None
    
    # Insert audit record
    conn.execute(sa_text("""
        INSERT INTO outcomes_audit
        (event_type, actor_id, actor_role, operation, set_id, item_id,
         before_data, after_data, reason, source, occurred_at)
        VALUES (:event, :actor, :role, :operation, :set_id, :item_id,
                :before, :after, :reason, 'ui', CURRENT_TIMESTAMP)
    """), {
        "event": event_type,
        "actor": actor,
        "role": actor_role,
        "operation": f"{event_type}_{metadata_json}" if metadata_json else event_type,
        "set_id": set_id,
        "item_id": item_id,
        "before": before_data,
        "after": after_data,
        "reason": reason or f"{event_type} operation"
    })


def check_mappings(conn, set_id: int) -> bool:
    """Check if a set has any active mappings (is being used)."""
    if not table_exists(conn, "outcomes_mappings"):
        return False
    
    result = conn.execute(sa_text("""
        SELECT COUNT(*) FROM outcomes_mappings
        WHERE outcome_set_id = :set_id
    """), {"set_id": set_id}).fetchone()
    
    return result[0] > 0


# ============================================================================
# VALIDATION
# ============================================================================

def validate_degree_exists(conn, degree_code: str) -> bool:
    """
    Check if degree exists.
    PERMISSIVE: Checks 'degrees_internal' (table) OR 'degrees_for_config' (view).
    This ensures validation passes even if the degree is missing some configuration
    details but exists in the system.
    """
    # Check internal table first (safest bet for existence)
    if table_exists(conn, "degrees_internal"):
        result = conn.execute(sa_text("""
            SELECT 1 FROM degrees_internal WHERE lower(code) = lower(:dc)
        """), {"dc": degree_code}).fetchone()
        if result:
            return True

    # Fallback to config view
    if table_exists(conn, "degrees_for_config"):
        result = conn.execute(sa_text("""
            SELECT 1 FROM degrees_for_config WHERE lower(code) = lower(:dc)
        """), {"dc": degree_code}).fetchone()
        return bool(result)
        
    # Legacy check
    if table_exists(conn, "degrees"):
        result = conn.execute(sa_text("""
            SELECT 1 FROM degrees WHERE lower(code) = lower(:dc)
        """), {"dc": degree_code}).fetchone()
        return bool(result)
        
    return False


def validate_program_exists(conn, degree_code: str, program_code: str) -> bool:
    """Check if program exists."""
    if not table_exists(conn, "programs"):
        return False
        
    result = conn.execute(sa_text("""
        SELECT 1 FROM programs 
        WHERE lower(degree_code) = lower(:dc) 
        AND lower(program_code) = lower(:pc)
    """), {"dc": degree_code, "pc": program_code}).fetchone()
    return bool(result)


def validate_branch_exists(conn, branch_code: str, program_code: Optional[str] = None) -> bool:
    """Check if branch exists."""
    if not table_exists(conn, "branches"):
        return False
    
    if program_code:
        result = conn.execute(sa_text("""
            SELECT 1 FROM branches WHERE lower(branch_code) = lower(:bc)
        """), {"bc": branch_code}).fetchone()
    else:
        result = conn.execute(sa_text("""
            SELECT 1 FROM branches 
            WHERE lower(branch_code) = lower(:bc)
        """), {"bc": branch_code}).fetchone()
    
    return bool(result)


# ============================================================================
# FORMATTING & DISPLAY
# ============================================================================

def format_scope_display(scope_level: str) -> str:
    """Format scope level for display."""
    display = {
        "per_degree": "Per Degree",
        "per_program": "Per Program",
        "per_branch": "Per Branch"
    }
    return display.get(scope_level, scope_level)


def format_set_type_display(set_type: str) -> str:
    """Format set type for display."""
    display = {
        "peos": "PEOs (Program Educational Objectives)",
        "pos": "POs (Program Outcomes)",
        "psos": "PSOs (Program Specific Outcomes)"
    }
    return display.get(set_type, set_type.upper())


def truncate_text(text: str, max_length: int = 100) -> str:
    """Truncate text with ellipsis."""
    if not text:
        return ""
    if len(text) <= max_length:
        return text
    return text[:max_length] + "..."
