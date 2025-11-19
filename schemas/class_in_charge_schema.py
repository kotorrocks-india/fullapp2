# schemas/class_in_charge_schema.py
"""
Schema for Class-in-Charge (CIC) Assignments (Slide 17)
Manages faculty assignments as class coordinators for specific cohorts.
"""

from __future__ import annotations
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from core.schema_registry import register
import logging

log = logging.getLogger(__name__)


def _ensure_column(conn, table: str, col: str, col_def: str) -> None:
    """Helper to add column if it doesn't exist."""
    try:
        conn.execute(sa_text(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}"))
        log.info(f"Added column {col} to {table}")
    except Exception:
        pass  # Column already exists


@register
def install_class_in_charge_schema(engine: Engine) -> None:
    """
    Install complete schema for Class-in-Charge assignments.
    
    Features:
    - Per AY, Degree, Program, Branch, Year, Term, Division assignments
    - Faculty-only assignments (not admin positions)
    - Conflict detection (Branch Head, overlapping dates)
    - Approval workflow integration
    - Comprehensive audit trail
    """
    
    with engine.begin() as conn:
        log.info("🔧 Installing Class-in-Charge schema...")
        
        # =====================================================================
        # MAIN ASSIGNMENTS TABLE
        # =====================================================================
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS class_in_charge_assignments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                
                -- Scope Definition
                ay_code TEXT NOT NULL,
                degree_code TEXT NOT NULL,
                program_code TEXT, -- Optional: Not all degrees have programs
                branch_code TEXT, -- Optional: Not all degrees have branches
                year INTEGER NOT NULL CHECK(year >= 1 AND year <= 10),
                term INTEGER NOT NULL CHECK(term >= 1 AND term <= 5),
                division_code TEXT,  -- Optional: for when batch is split into divisions
                
                -- Faculty Assignment
                faculty_id INTEGER NOT NULL,
                faculty_email TEXT NOT NULL,  -- Denormalized for quick access
                faculty_name TEXT NOT NULL,   -- Denormalized for display
                
                -- Assignment Period
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                
                -- Status & Workflow
                status TEXT NOT NULL DEFAULT 'active' 
                    CHECK(status IN ('active', 'inactive', 'suspended')),
                approval_status TEXT DEFAULT 'pending'
                    CHECK(approval_status IN ('pending', 'approved', 'rejected')),
                approval_note TEXT,
                approved_by TEXT,
                approved_at DATETIME,
                
                -- Metadata
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                created_by TEXT NOT NULL,
                updated_at DATETIME,
                updated_by TEXT,
                
                -- Constraints
                CHECK(end_date > start_date),
                
                FOREIGN KEY(faculty_id) REFERENCES faculty_profiles(id) ON DELETE RESTRICT
            )
        """))
        
        # =====================================================================
        # INDEXES FOR PERFORMANCE & UNIQUENESS
        # =====================================================================
        
        # Unique active assignment per scope WITHOUT division
        conn.execute(sa_text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uniq_cic_active_scope_no_div
            ON class_in_charge_assignments(
                ay_code, degree_code, program_code, branch_code, 
                year, term, status
            )
            WHERE status = 'active' 
            AND (division_code IS NULL OR division_code = '')
        """))
        
        # Unique active assignment per scope WITH division
        conn.execute(sa_text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uniq_cic_active_scope_with_div
            ON class_in_charge_assignments(
                ay_code, degree_code, program_code, branch_code, 
                year, term, division_code, status
            )
            WHERE status = 'active' 
            AND division_code IS NOT NULL 
            AND division_code != ''
        """))
        
        # Performance indexes
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_cic_ay 
            ON class_in_charge_assignments(ay_code)
        """))
        
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_cic_faculty 
            ON class_in_charge_assignments(faculty_id, status)
        """))
        
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_cic_degree 
            ON class_in_charge_assignments(degree_code, program_code, branch_code)
        """))
        
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_cic_dates 
            ON class_in_charge_assignments(start_date, end_date, status)
        """))
        
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_cic_status 
            ON class_in_charge_assignments(status, approval_status)
        """))
        
        # =====================================================================
        # AUDIT TRAIL TABLE
        # =====================================================================
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS class_in_charge_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                
                -- Event Details
                assignment_id INTEGER,
                action TEXT NOT NULL,  -- CREATE, UPDATE, DELETE, SUSPEND, REACTIVATE, APPROVE, REJECT
                
                -- Context
                ay_code TEXT,
                degree_code TEXT,
                program_code TEXT,
                branch_code TEXT,
                year INTEGER,
                term INTEGER,
                division_code TEXT,
                faculty_email TEXT,
                
                -- Change Tracking
                changed_fields TEXT,  -- JSON: {"field": {"old": "value", "new": "value"}}
                reason TEXT,
                note TEXT,
                
                -- Actor Information
                actor_email TEXT NOT NULL,
                actor_role TEXT,
                step_up_performed INTEGER DEFAULT 0,
                
                -- Technical Details
                ip_address TEXT,
                user_agent TEXT,
                session_id TEXT,
                correlation_id TEXT,
                source TEXT,  -- ui, import, api, system
                
                -- Timing
                occurred_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY(assignment_id) 
                    REFERENCES class_in_charge_assignments(id) 
                    ON DELETE SET NULL
            )
        """))
        
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_cic_audit_assignment 
            ON class_in_charge_audit(assignment_id)
        """))
        
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_cic_audit_ay 
            ON class_in_charge_audit(ay_code, degree_code)
        """))
        
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_cic_audit_actor 
            ON class_in_charge_audit(actor_email, occurred_at)
        """))
        
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_cic_audit_date 
            ON class_in_charge_audit(occurred_at DESC)
        """))
        
        # =====================================================================
        # CONFIGURATION TABLE
        # =====================================================================
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS class_in_charge_config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                description TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_by TEXT
            )
        """))
        
        # Insert default configuration
        conn.execute(sa_text("""
            INSERT OR IGNORE INTO class_in_charge_config 
            (key, value, description) VALUES
            ('min_assignment_days', '30', 'Minimum assignment duration in days'),
            ('expiry_notification_days', '30,15,7,3,1', 'Days before expiry to send notifications'),
            ('auto_deactivate_on_expiry', 'true', 'Auto-deactivate assignments past end date'),
            ('grace_period_days', '5', 'Grace period before auto-deactivation'),
            ('allow_admin_as_cic', 'warn', 'Policy: block, warn, or allow'),
            ('require_approval', 'true', 'Require approval for assignments'),
            ('check_branch_head_conflict', 'true', 'Check if faculty is Branch Head in same AY')
        """))
        
        # =====================================================================
        # VIEWS FOR REPORTING
        # =====================================================================
        
        # Drop views first to ensure they are recreated with the latest definition
        log.info("   Dropping existing CIC views...")
        conn.execute(sa_text("DROP VIEW IF EXISTS v_cic_current_assignments"))
        conn.execute(sa_text("DROP VIEW IF EXISTS v_cic_assignment_history"))
        conn.execute(sa_text("DROP VIEW IF EXISTS v_cic_coverage_analysis"))
        conn.execute(sa_text("DROP VIEW IF EXISTS v_class_in_charge_detail"))
        
        
        # Current active assignments with full details
        conn.execute(sa_text("""
            CREATE VIEW v_cic_current_assignments AS
            SELECT 
                cic.id,
                cic.ay_code,
                cic.degree_code,
                d.title AS degree_name,
                cic.program_code,
                p.program_name,
                cic.branch_code,
                b.branch_name,
                cic.year,
                cic.term,
                cic.division_code,
                cic.faculty_id,
                cic.faculty_email,
                cic.faculty_name,
                fp.employee_id,
                fp.phone AS faculty_phone,
                cic.start_date,
                cic.end_date,
                cic.status,
                cic.approval_status,
                cic.created_at,
                cic.created_by,
                -- Computed fields
                CASE 
                    WHEN DATE(cic.end_date) < DATE('now') THEN 'expired'
                    WHEN DATE(cic.end_date) <= DATE('now', '+30 days') THEN 'expiring_soon'
                    ELSE 'active'
                END AS expiry_status,
                CAST(JULIANDAY(cic.end_date) - JULIANDAY('now') AS INTEGER) AS days_until_expiry,
                CAST(JULIANDAY(cic.end_date) - JULIANDAY(cic.start_date) AS INTEGER) AS assignment_duration_days
            FROM class_in_charge_assignments cic
            LEFT JOIN degrees d ON d.code = cic.degree_code
            LEFT JOIN programs p ON p.program_code = cic.program_code 
                AND p.degree_code = cic.degree_code
            LEFT JOIN branches b ON b.branch_code = cic.branch_code 
                AND b.degree_code = cic.degree_code
            LEFT JOIN faculty_profiles fp ON fp.id = cic.faculty_id
            WHERE cic.status = 'active'
        """))
        
        # Assignment history view
        conn.execute(sa_text("""
            CREATE VIEW v_cic_assignment_history AS
            SELECT 
                cic.id,
                cic.ay_code,
                cic.degree_code,
                d.title AS degree_name,
                cic.program_code,
                p.program_name,
                cic.branch_code,
                b.branch_name,
                cic.year,
                cic.term,
                cic.division_code,
                cic.faculty_email,
                cic.faculty_name,
                cic.start_date,
                cic.end_date,
                cic.status,
                cic.approval_status,
                cic.created_at,
                cic.created_by,
                cic.updated_at,
                cic.updated_by,
                CAST(JULIANDAY(cic.end_date) - JULIANDAY(cic.start_date) AS INTEGER) AS duration_days
            FROM class_in_charge_assignments cic
            LEFT JOIN degrees d ON d.code = cic.degree_code
            LEFT JOIN programs p ON p.program_code = cic.program_code 
                AND p.degree_code = cic.degree_code
            LEFT JOIN branches b ON b.branch_code = cic.branch_code 
                AND b.degree_code = cic.degree_code
            ORDER BY cic.ay_code DESC, cic.created_at DESC
        """))
        
        # Coverage analysis view - DROP and recreate to fix syntax
        conn.execute(sa_text("DROP VIEW IF EXISTS v_cic_coverage_analysis"))
        conn.execute(sa_text("""
            CREATE VIEW v_cic_coverage_analysis AS
            SELECT 
                ay_code,
                degree_code,
                program_code,
                branch_code,
                year,
                term,
                COUNT(CASE WHEN division_code IS NOT NULL AND division_code != '' THEN 1 END) AS divisions_count,
                COUNT(*) AS assignments_count,
                COUNT(CASE WHEN status = 'active' THEN 1 END) AS active_count,
                GROUP_CONCAT(faculty_name, '; ') AS assigned_faculty
            FROM class_in_charge_assignments
            GROUP BY ay_code, degree_code, program_code, branch_code, year, term
        """))
        
        # Detailed view (works with minimal table structure)
        conn.execute(sa_text("""
            CREATE VIEW v_class_in_charge_detail AS
            SELECT 
                cic.*,
                d.title AS degree_title,
                p.program_name,
                b.branch_name,
                fp.employee_id AS faculty_employee_id,
                fp.phone AS faculty_phone,
                fp.status AS faculty_status,
                -- Computed fields
                CASE 
                    WHEN DATE(cic.end_date) < DATE('now') AND cic.status = 'active' THEN 'expired'
                    WHEN DATE(cic.end_date) <= DATE('now', '+30 days') AND cic.status = 'active' THEN 'expiring_soon'
                    WHEN cic.status = 'active' THEN 'current'
                    ELSE 'inactive'
                END AS time_status,
                CAST(JULIANDAY(cic.end_date) - JULIANDAY('now') AS INTEGER) AS days_remaining,
                CAST(JULIANDAY(cic.end_date) - JULIANDAY(cic.start_date) AS INTEGER) AS duration_days,
                CASE WHEN DATE(cic.end_date) <= DATE('now', '+30 days') THEN 1 ELSE 0 END AS expiring_soon,
                CASE WHEN DATE(cic.end_date) < DATE('now') AND cic.status = 'active' THEN 1 ELSE 0 END AS expired,
                -- Admin position count (with safeguard)
                COALESCE((SELECT COUNT(*) 
                 FROM position_assignments pa 
                 WHERE pa.assignee_email = cic.faculty_email 
                   AND pa.is_active = 1
                   AND pa.assignee_type = 'faculty'
                ), 0) AS admin_position_count
            FROM class_in_charge_assignments cic
            LEFT JOIN degrees d ON d.code = cic.degree_code
            LEFT JOIN programs p ON p.program_code = cic.program_code 
                AND p.degree_code = cic.degree_code
            LEFT JOIN branches b ON b.branch_code = cic.branch_code 
                AND b.degree_code = cic.degree_code
            LEFT JOIN faculty_profiles fp ON fp.id = cic.faculty_id
        """))
        
        log.info("✅ Class-in-Charge schema installed successfully")


def migrate_legacy_cic_data(engine: Engine) -> None:
    """
    Migration helper for existing CIC data.
    Run this once if upgrading from an older schema.
    """
    with engine.begin() as conn:
        log.info("🔄 Checking for CIC schema migrations...")
        
        # Check if table exists
        result = conn.execute(sa_text("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='class_in_charge_assignments'
        """)).fetchone()
        
        if not result:
            log.info("ℹ️  No existing CIC table found, skipping migration")
            return
        
        # Get existing columns
        existing_cols = conn.execute(sa_text(
            "PRAGMA table_info(class_in_charge_assignments)"
        )).fetchall()
        existing_col_names = {col[1].lower() for col in existing_cols}
        
        log.info(f"📋 Found existing columns: {', '.join(sorted(existing_col_names))}")
        
        # Define required columns with their definitions
        required_columns = {
            'division_code': 'TEXT',
            'approval_status': "TEXT DEFAULT 'approved' CHECK(approval_status IN ('draft', 'pending_approval', 'approved', 'rejected'))",
            'approval_request_id': 'INTEGER',
            'approval_note': 'TEXT',
            'approved_by': 'TEXT',
            'approved_at': 'DATETIME',
            'rejection_reason': 'TEXT',
            'updated_at': 'DATETIME',
            'updated_by': 'TEXT',
        }
        
        # Add missing columns
        added_count = 0
        for col_name, col_def in required_columns.items():
            if col_name.lower() not in existing_col_names:
                try:
                    _ensure_column(conn, 'class_in_charge_assignments', col_name, col_def)
                    log.info(f"   ✅ Added column: {col_name}")
                    added_count += 1
                except Exception as e:
                    log.warning(f"   ⚠️  Could not add {col_name}: {e}")
        
        if added_count > 0:
            log.info(f"✅ CIC migration complete - added {added_count} column(s)")
        else:
            log.info("✅ CIC migration complete - table already up to date")
        
        # Drop old restrictive index if it exists
        try:
            conn.execute(sa_text("""
                DROP INDEX IF EXISTS uniq_cic_faculty_ay
            """))
            log.info("   ✅ Removed old restrictive faculty_ay index (faculty can now have multiple CIC assignments in same AY)")
        except Exception as e:
            log.warning(f"   ⚠️  Could not drop old index: {e}")
        
        # Recreate views to ensure they match the schema
        # This is now handled by install_class_in_charge_schema
        log.info("🔄 Views will be refreshed by main install function...")


def install_all(engine: Engine) -> None:
    """
    Complete installation of CIC schema including tables, views, and migrations.
    This is the main entry point for schema setup.
    """
    # Run install first (which now drops/recreates views)
    install_class_in_charge_schema(engine)
    
    # Run migration (which adds columns if needed)
    migrate_legacy_cic_data(engine)
    
    # Rerun install_class_in_charge_schema to ensure views 
    # pick up any newly migrated columns
    log.info("Rerunning schema install to apply views to migrated columns...")
    install_class_in_charge_schema(engine)
    
    log.info("✅ Class-in-Charge complete installation finished")


# Export for direct import
__all__ = ['install_class_in_charge_schema', 'migrate_legacy_cic_data', 'install_all']
