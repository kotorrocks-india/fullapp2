# schemas/subject_offerings_schema.py
"""
Subject Offerings Schema (Slide 19) - Complete Implementation
Integrates with:
- subjects_catalog (Slide 14)
- students (divisions, batches)
- electives_topics (Slide 18)
- semesters
- degrees, programs, branches

Safe to run multiple times (idempotent).
"""

from __future__ import annotations
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from core.schema_registry import register
import logging

logger = logging.getLogger(__name__)


def _exec(conn, sql: str, params: dict = None):
    """Execute SQL with parameters."""
    return conn.execute(sa_text(sql), params or {})


def _table_exists(conn, table: str) -> bool:
    """Check if table exists."""
    result = _exec(conn, 
        "SELECT name FROM sqlite_master WHERE type='table' AND name=:t",
        {"t": table}
    ).fetchone()
    return result is not None


def _has_column(conn, table: str, col: str) -> bool:
    """Check if column exists in table."""
    rows = _exec(conn, f"PRAGMA table_info({table})").fetchall()
    return any(r[1].lower() == col.lower() for r in rows)


# ===========================================================================
# MAIN SUBJECT OFFERINGS TABLE
# ===========================================================================

def install_subject_offerings_table(engine: Engine):
    """
    Main subject offerings table - AY-specific instances of catalog subjects.
    """
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS subject_offerings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Temporal Scope (AY-specific)
            ay_label TEXT NOT NULL COLLATE NOCASE,
            year INTEGER NOT NULL CHECK (year >= 1 AND year <= 10),
            term INTEGER NOT NULL CHECK (term >= 1 AND term <= 4),
            
            -- Organizational Scope
            degree_code TEXT NOT NULL COLLATE NOCASE,
            program_code TEXT COLLATE NOCASE,
            branch_code TEXT COLLATE NOCASE,
            curriculum_group_code TEXT COLLATE NOCASE,
            
            -- Division Scope
            division_code TEXT COLLATE NOCASE,
            applies_to_all_divisions INTEGER NOT NULL DEFAULT 1,
            
            -- Subject Reference (from catalog)
            subject_code TEXT NOT NULL COLLATE NOCASE,
            syllabus_template_id INTEGER, -- <<<< 1. ADDED THIS COLUMN
            subject_type TEXT NOT NULL CHECK (subject_type IN ('Core', 'Elective', 'College Project', 'Other')),
            is_elective_parent INTEGER NOT NULL DEFAULT 0,

            -- Elective Topic Linkage (for per-topic offerings)
            base_subject_code TEXT COLLATE NOCASE,  -- For electives: catalog subject_code; for core may be NULL or same as subject_code
            topic_code_ay   TEXT COLLATE NOCASE,    -- For electives: per-topic subject code, e.g., CS-ELECT-1
            topic_name      TEXT,                   -- For electives: human-friendly topic title
            is_elective_topic INTEGER NOT NULL DEFAULT 0,
            
            -- Academic Details (inherited from catalog, can be overridden)
            credits_total REAL NOT NULL DEFAULT 0,
            L REAL NOT NULL DEFAULT 0,
            T REAL NOT NULL DEFAULT 0,
            P REAL NOT NULL DEFAULT 0,
            S REAL NOT NULL DEFAULT 0,
            
            -- Marks Structure
            internal_marks_max INTEGER NOT NULL DEFAULT 40,
            exam_marks_max INTEGER NOT NULL DEFAULT 60,
            jury_viva_marks_max INTEGER NOT NULL DEFAULT 0,
            total_marks_max INTEGER NOT NULL DEFAULT 100,
            
            -- Attainment Weights (percentages)
            direct_weight_percent REAL NOT NULL DEFAULT 80.0,
            indirect_weight_percent REAL NOT NULL DEFAULT 20.0,
            
            -- Pass Thresholds (percentages)
            pass_threshold_overall REAL NOT NULL DEFAULT 40.0,
            pass_threshold_internal REAL NOT NULL DEFAULT 50.0,
            pass_threshold_external REAL NOT NULL DEFAULT 40.0,
            
            -- Offering-Specific Settings
            instructor_email TEXT,
            status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'published', 'archived')),
            
            -- Override Controls (rare, requires approval)
            override_inheritance INTEGER NOT NULL DEFAULT 0,
            override_reason TEXT,
            override_approved_by TEXT,
            override_approved_at DATETIME,
            
            -- Integration with Electives (Slide 18)
            elective_selection_window_id INTEGER,
            elective_selection_lead_days INTEGER DEFAULT 21,
            allow_negative_offset INTEGER DEFAULT 1,
            
            -- Freeze Controls
            is_frozen INTEGER NOT NULL DEFAULT 0,
            frozen_at DATETIME,
            frozen_by TEXT,
            frozen_reason TEXT,
            
            -- Audit Fields
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            created_by TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_by TEXT,
            
            -- Constraints
            CHECK (
                (applies_to_all_divisions = 1 AND division_code IS NULL) OR
                (applies_to_all_divisions = 0 AND division_code IS NOT NULL)
            ),
            CHECK (direct_weight_percent + indirect_weight_percent = 100.0 OR 
                   (direct_weight_percent = 0 AND indirect_weight_percent = 0)),
            CHECK (internal_marks_max + exam_marks_max + jury_viva_marks_max = total_marks_max),
            
            -- Uniqueness: One offering per scope
            UNIQUE(ay_label, degree_code, program_code, branch_code, curriculum_group_code, 
                   year, term, division_code, subject_code),
            
            -- Foreign Keys
            FOREIGN KEY (ay_label) REFERENCES academic_years(ay_code) ON DELETE CASCADE,
            FOREIGN KEY (degree_code) REFERENCES degrees(code) ON DELETE CASCADE,
            FOREIGN KEY (elective_selection_window_id) REFERENCES elective_selection_windows(id) ON DELETE SET NULL,
            FOREIGN KEY (syllabus_template_id) REFERENCES syllabus_templates(id) ON DELETE SET NULL -- <<<< 2. ADDED THIS FOREIGN KEY
        )
        """)
        
        # --- Schema Migration ---
        # This will add the column to your *existing* database,
        # since "CREATE TABLE IF NOT EXISTS" won't modify it.
        if not _has_column(conn, "subject_offerings", "syllabus_template_id"):
            _exec(conn, """
            ALTER TABLE subject_offerings
            ADD COLUMN syllabus_template_id INTEGER
            REFERENCES syllabus_templates(id) ON DELETE SET NULL
            """)
            logger.info("🔧 Altered subject_offerings: Added syllabus_template_id column.")
        
        # Performance Indexes
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_offerings_ay_degree_year_term
        ON subject_offerings(ay_label, degree_code, year, term)
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_offerings_subject_code
        ON subject_offerings(subject_code)
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_offerings_status
        ON subject_offerings(status)
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_offerings_division
        ON subject_offerings(division_code)
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_offerings_cg
        ON subject_offerings(curriculum_group_code)
        """)
        
        logger.info("✅ Installed subject_offerings table")
# ===========================================================================
# AUDIT TABLE
# ===========================================================================

def install_offerings_audit_table(engine: Engine):
    """Comprehensive audit trail for all offering changes."""
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS subject_offerings_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Offering Context
            offering_id INTEGER NOT NULL,
            subject_code TEXT NOT NULL COLLATE NOCASE,
            degree_code TEXT NOT NULL COLLATE NOCASE,
            program_code TEXT COLLATE NOCASE,
            branch_code TEXT COLLATE NOCASE,
            curriculum_group_code TEXT COLLATE NOCASE,
            ay_label TEXT NOT NULL COLLATE NOCASE,
            year INTEGER NOT NULL,
            term INTEGER NOT NULL,
            division_code TEXT COLLATE NOCASE,
            
            -- Action Details
            action TEXT NOT NULL CHECK (action IN (
                'create', 'update', 'delete', 
                'publish', 'unpublish', 'archive', 'restore',
                'freeze', 'unfreeze',
                'override_enable', 'override_disable',
                'copy_forward', 'bulk_update',
                'import', 'export'
            )),
            operation TEXT,
            note TEXT,
            reason TEXT,
            changed_fields TEXT,  -- JSON blob
            
            -- Actor Information
            actor TEXT NOT NULL,
            actor_role TEXT,
            actor_id INTEGER,
            
            -- Approval Context (for override/publish actions)
            approval_required INTEGER DEFAULT 0,
            approved_by TEXT,
            approved_at DATETIME,
            step_up_performed INTEGER DEFAULT 0,
            
            -- Source & Session
            source TEXT DEFAULT 'ui' CHECK (source IN ('ui', 'import', 'api', 'system', 'migration')),
            correlation_id TEXT,
            session_id TEXT,
            ip_address TEXT,
            user_agent TEXT,
            
            -- Snapshot (before state for rollback)
            snapshot_json TEXT,
            
            -- Timing
            occurred_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            
            -- Foreign Key
            FOREIGN KEY (offering_id) REFERENCES subject_offerings(id) ON DELETE CASCADE
        )
        """)
        
        # Audit Indexes
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_offerings_audit_offering_id
        ON subject_offerings_audit(offering_id)
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_offerings_audit_action
        ON subject_offerings_audit(action, occurred_at DESC)
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_offerings_audit_actor
        ON subject_offerings_audit(actor, occurred_at DESC)
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_offerings_audit_ay
        ON subject_offerings_audit(ay_label, degree_code, year, term)
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_offerings_audit_subject
        ON subject_offerings_audit(subject_code)
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_offerings_audit_correlation
        ON subject_offerings_audit(correlation_id)
        """)
        
        logger.info("✅ Installed subject_offerings_audit table")


# ===========================================================================
# VERSIONING / SNAPSHOTS TABLE
# ===========================================================================

def install_offerings_snapshots_table(engine: Engine):
    """Version snapshots for rollback capability."""
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS subject_offerings_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Offering Reference
            offering_id INTEGER NOT NULL,
            subject_code TEXT NOT NULL COLLATE NOCASE,
            ay_label TEXT NOT NULL COLLATE NOCASE,
            
            -- Snapshot Details
            snapshot_number INTEGER NOT NULL,
            snapshot_data TEXT NOT NULL,  -- Complete JSON of offering state
            snapshot_type TEXT NOT NULL CHECK (snapshot_type IN (
                'create', 'update', 'publish', 'archive', 'rollback', 'scheduled'
            )),
            
            -- Context
            note TEXT,
            created_by TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            
            -- Rollback Info
            is_active_version INTEGER NOT NULL DEFAULT 0,
            rolled_back_from_snapshot_id INTEGER,
            
            -- Retention
            expires_at DATETIME,
            
            -- Constraints
            UNIQUE(offering_id, snapshot_number),
            
            FOREIGN KEY (offering_id) REFERENCES subject_offerings(id) ON DELETE CASCADE,
            FOREIGN KEY (rolled_back_from_snapshot_id) 
                REFERENCES subject_offerings_snapshots(id) ON DELETE SET NULL
        )
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_snapshots_offering
        ON subject_offerings_snapshots(offering_id, snapshot_number DESC)
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_snapshots_active
        ON subject_offerings_snapshots(is_active_version)
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_snapshots_expires
        ON subject_offerings_snapshots(expires_at)
        """)
        
        logger.info("✅ Installed subject_offerings_snapshots table")


# ===========================================================================
# APPROVAL WORKFLOW TABLE
# ===========================================================================

def install_offerings_approvals_table(engine: Engine):
    """Track approval workflows for override/publish actions."""
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS subject_offerings_approvals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Offering Reference
            offering_id INTEGER NOT NULL,
            subject_code TEXT NOT NULL COLLATE NOCASE,
            ay_label TEXT NOT NULL COLLATE NOCASE,
            
            -- Request Details
            request_type TEXT NOT NULL CHECK (request_type IN (
                'publish', 'override_enable', 'delete', 'bulk_publish'
            )),
            requested_by TEXT NOT NULL,
            requested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            reason TEXT NOT NULL,
            
            -- Proposed Changes
            proposed_changes TEXT,  -- JSON blob
            
            -- Approval Status
            status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN (
                'pending', 'approved', 'rejected', 'cancelled', 'expired'
            )),
            
            -- Approver Info
            approver TEXT,
            approved_at DATETIME,
            rejection_reason TEXT,
            
            -- Step-Up Authentication
            step_up_token TEXT,
            step_up_expires_at DATETIME,
            step_up_verified INTEGER DEFAULT 0,
            
            -- Auto-Expiry
            expires_at DATETIME,
            
            -- Execution
            executed INTEGER DEFAULT 0,
            executed_at DATETIME,
            
            FOREIGN KEY (offering_id) REFERENCES subject_offerings(id) ON DELETE CASCADE
        )
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_approvals_offering
        ON subject_offerings_approvals(offering_id)
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_approvals_status
        ON subject_offerings_approvals(status, requested_at DESC)
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_approvals_requester
        ON subject_offerings_approvals(requested_by)
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_approvals_expires
        ON subject_offerings_approvals(expires_at)
        """)
        
        logger.info("✅ Installed subject_offerings_approvals table")


# ===========================================================================
# FREEZE RECORDS TABLE
# ===========================================================================

def install_offerings_freeze_table(engine: Engine):
    """Track freeze/unfreeze events (when marks exist)."""
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS subject_offerings_freeze_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Offering Reference
            offering_id INTEGER NOT NULL,
            subject_code TEXT NOT NULL COLLATE NOCASE,
            ay_label TEXT NOT NULL COLLATE NOCASE,
            
            -- Freeze Action
            action TEXT NOT NULL CHECK (action IN ('freeze', 'unfreeze')),
            reason TEXT NOT NULL,
            
            -- Context
            marks_exist INTEGER NOT NULL DEFAULT 0,
            marks_count INTEGER DEFAULT 0,
            
            -- Actor
            actor TEXT NOT NULL,
            occurred_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            
            -- Approval (if required for unfreeze)
            approved_by TEXT,
            approved_at DATETIME,
            
            FOREIGN KEY (offering_id) REFERENCES subject_offerings(id) ON DELETE CASCADE
        )
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_freeze_log_offering
        ON subject_offerings_freeze_log(offering_id, occurred_at DESC)
        """)
        
        logger.info("✅ Installed subject_offerings_freeze_log table")


# ===========================================================================
# HEALTH CHECKS TABLE
# ===========================================================================

def install_offerings_health_checks_table(engine: Engine):
    """Store health check results for monitoring."""
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS subject_offerings_health_checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Check Details
            check_name TEXT NOT NULL,
            check_type TEXT NOT NULL CHECK (check_type IN (
                'duplicate_offerings', 
                'catalog_sync_status',
                'missing_divisions',
                'orphaned_offerings',
                'marks_consistency',
                'elective_topics_required'
            )),
            
            -- Results
            status TEXT NOT NULL CHECK (status IN ('pass', 'warn', 'fail')),
            issue_count INTEGER DEFAULT 0,
            details TEXT,  -- JSON array of issues
            
            -- Scope
            ay_label TEXT,
            degree_code TEXT,
            
            -- Timing
            checked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            next_check_at DATETIME,
            
            -- Alerting
            alert_sent INTEGER DEFAULT 0,
            alert_sent_at DATETIME
        )
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_health_checks_type
        ON subject_offerings_health_checks(check_type, checked_at DESC)
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_health_checks_status
        ON subject_offerings_health_checks(status, checked_at DESC)
        """)
        
        _exec(conn, """
        CREATE INDEX IF NOT EXISTS idx_health_checks_next
        ON subject_offerings_health_checks(next_check_at)
        """)
        
        logger.info("✅ Installed subject_offerings_health_checks table")


# ===========================================================================
# VIEWS
# ===========================================================================

def install_offerings_views(engine: Engine):
    """Create useful views for queries."""
    with engine.begin() as conn:
        # View: Offerings with catalog details
        _exec(conn, """
        CREATE VIEW IF NOT EXISTS v_offerings_with_catalog AS
        SELECT 
            o.id,
            o.ay_label,
            o.degree_code,
            o.program_code,
            o.branch_code,
            o.curriculum_group_code,
            o.year,
            o.term,
            o.division_code,
            o.applies_to_all_divisions,
            o.subject_code,
            sc.subject_name,
            o.subject_type,
            o.is_elective_parent,
            o.credits_total,
            o.L, o.T, o.P, o.S,
            o.internal_marks_max,
            o.exam_marks_max,
            o.jury_viva_marks_max,
            o.total_marks_max,
            o.direct_weight_percent,
            o.indirect_weight_percent,
            o.pass_threshold_overall,
            o.pass_threshold_internal,
            o.pass_threshold_external,
            o.instructor_email,
            o.status,
            o.override_inheritance,
            o.override_reason,
            o.override_approved_by,
            o.is_frozen,
            o.frozen_reason,
            o.created_at,
            o.updated_at,
            o.created_by,
            o.updated_by,
            -- Catalog comparison flags
            CASE 
                WHEN o.override_inheritance = 1 THEN 'OVERRIDDEN'
                WHEN o.credits_total != sc.credits_total THEN 'OUT_OF_SYNC'
                WHEN o.internal_marks_max != sc.internal_marks_max THEN 'OUT_OF_SYNC'
                ELSE 'SYNCED'
            END AS catalog_sync_status
        FROM subject_offerings o
        LEFT JOIN subjects_catalog sc 
            ON sc.subject_code = o.subject_code 
            AND sc.degree_code = o.degree_code
            AND (sc.program_code = o.program_code OR sc.program_code IS NULL)
            AND (sc.branch_code = o.branch_code OR sc.branch_code IS NULL)
        """)
        
        # View: Offerings requiring elective topics
        _exec(conn, """
        CREATE VIEW IF NOT EXISTS v_offerings_needing_topics AS
        SELECT 
            o.id,
            o.subject_code,
            o.degree_code,
            o.ay_label,
            o.year,
            o.term,
            o.subject_type,
            o.status,
            COUNT(et.id) AS topic_count,
            CASE 
                WHEN COUNT(et.id) = 0 THEN 'NO_TOPICS'
                WHEN COUNT(et.id) > 0 AND o.status = 'published' THEN 'HAS_TOPICS'
                ELSE 'DRAFT_WITH_TOPICS'
            END AS topic_status
        FROM subject_offerings o
        LEFT JOIN elective_topics et 
            ON et.subject_code = o.subject_code 
            AND et.ay_label = o.ay_label
            AND et.year = o.year
            AND et.term = o.term
        WHERE o.is_elective_parent = 1
        AND o.subject_type IN ('Elective', 'College Project')
        GROUP BY o.id, o.subject_code, o.degree_code, o.ay_label, o.year, o.term, 
                 o.subject_type, o.status
        """)
        
        # View: Current term offerings
        _exec(conn, """
        CREATE VIEW IF NOT EXISTS v_current_term_offerings AS
        SELECT o.*
        FROM subject_offerings o
        JOIN academic_years ay ON ay.ay_code = o.ay_label
        WHERE ay.status = 'open'
        AND o.status = 'published'
        """)
        
        logger.info("✅ Installed offerings views")


# ===========================================================================
# TRIGGERS
# ===========================================================================

def install_offerings_triggers(engine: Engine):
    """Create triggers for data integrity."""
    with engine.begin() as conn:
        # Trigger: Auto-update updated_at timestamp
        _exec(conn, """
        CREATE TRIGGER IF NOT EXISTS trg_offerings_updated_at
        AFTER UPDATE ON subject_offerings
        FOR EACH ROW
        BEGIN
            UPDATE subject_offerings 
            SET updated_at = CURRENT_TIMESTAMP
            WHERE id = NEW.id;
        END;
        """)
        
        # Trigger: Prevent updates when frozen
        _exec(conn, """
        CREATE TRIGGER IF NOT EXISTS trg_offerings_prevent_frozen_updates
        BEFORE UPDATE ON subject_offerings
        WHEN OLD.is_frozen = 1 
        AND NEW.is_frozen = 1
        AND (
            NEW.credits_total != OLD.credits_total OR
            NEW.internal_marks_max != OLD.internal_marks_max OR
            NEW.exam_marks_max != OLD.exam_marks_max OR
            NEW.division_code != OLD.division_code OR
            NEW.applies_to_all_divisions != OLD.applies_to_all_divisions
        )
        BEGIN
            SELECT RAISE(ABORT, 'Cannot modify frozen offering. Unfreeze first or contact admin.');
        END;
        """)
        
        # Trigger: Validate total marks consistency
        _exec(conn, """
        CREATE TRIGGER IF NOT EXISTS trg_offerings_validate_marks
        BEFORE INSERT ON subject_offerings
        WHEN NEW.internal_marks_max + NEW.exam_marks_max + NEW.jury_viva_marks_max != NEW.total_marks_max
        BEGIN
            SELECT RAISE(ABORT, 'Marks structure invalid: internal + exam + jury must equal total');
        END;
        """)
        
        _exec(conn, """
        CREATE TRIGGER IF NOT EXISTS trg_offerings_validate_marks_update
        BEFORE UPDATE ON subject_offerings
        WHEN NEW.internal_marks_max + NEW.exam_marks_max + NEW.jury_viva_marks_max != NEW.total_marks_max
        BEGIN
            SELECT RAISE(ABORT, 'Marks structure invalid: internal + exam + jury must equal total');
        END;
        """)
        
        # Trigger: Validate weight percentages
        _exec(conn, """
        CREATE TRIGGER IF NOT EXISTS trg_offerings_validate_weights
        BEFORE INSERT ON subject_offerings
        WHEN (NEW.direct_weight_percent + NEW.indirect_weight_percent != 100.0)
        AND NOT (NEW.direct_weight_percent = 0 AND NEW.indirect_weight_percent = 0)
        BEGIN
            SELECT RAISE(ABORT, 'Weight percentages must sum to 100.0');
        END;
        """)
        
        _exec(conn, """
        CREATE TRIGGER IF NOT EXISTS trg_offerings_validate_weights_update
        BEFORE UPDATE ON subject_offerings
        WHEN (NEW.direct_weight_percent + NEW.indirect_weight_percent != 100.0)
        AND NOT (NEW.direct_weight_percent = 0 AND NEW.indirect_weight_percent = 0)
        BEGIN
            SELECT RAISE(ABORT, 'Weight percentages must sum to 100.0');
        END;
        """)
        
        logger.info("✅ Installed offerings triggers")


# ===========================================================================
# MASTER INSTALL FUNCTION
# ===========================================================================

@register("subject_offerings")
def install_subject_offerings_schema(engine: Engine):
    """
    Install complete subject offerings schema.
    Safe to run multiple times (idempotent).
    
    This is the entry point for the schema registry auto-discovery system.
    """
    logger.info("\n" + "="*60)
    logger.info("SUBJECT OFFERINGS SCHEMA INSTALLATION")
    logger.info("="*60)
    
    try:
        # Core tables
        install_subject_offerings_table(engine)
        install_offerings_audit_table(engine)
        install_offerings_snapshots_table(engine)
        install_offerings_approvals_table(engine)
        install_offerings_freeze_table(engine)
        install_offerings_health_checks_table(engine)
        
        # Views
        install_offerings_views(engine)
        
        # Triggers
        install_offerings_triggers(engine)
        
        logger.info("✅ Subject Offerings schema installed successfully")
        logger.info("="*60 + "\n")
        return True
        
    except Exception as e:
        logger.error(f"❌ Subject Offerings schema installation failed: {e}", exc_info=True)
        return False


# ===========================================================================
# UTILITY FUNCTIONS
# ===========================================================================

def check_catalog_sync(engine: Engine, offering_id: int = None) -> dict:
    """Check if offerings are in sync with catalog."""
    with engine.begin() as conn:
        if offering_id:
            query = """
            SELECT 
                o.id,
                o.subject_code,
                o.override_inheritance,
                CASE 
                    WHEN o.override_inheritance = 1 THEN 'OVERRIDE'
                    WHEN o.credits_total != sc.credits_total THEN 'OUT_OF_SYNC'
                    WHEN o.internal_marks_max != sc.internal_marks_max THEN 'OUT_OF_SYNC'
                    WHEN o.exam_marks_max != sc.exam_marks_max THEN 'OUT_OF_SYNC'
                    ELSE 'SYNCED'
                END AS sync_status
            FROM subject_offerings o
            LEFT JOIN subjects_catalog sc 
                ON sc.subject_code = o.subject_code 
                AND sc.degree_code = o.degree_code
            WHERE o.id = :offering_id
            """
            result = _exec(conn, query, {"offering_id": offering_id}).fetchone()
            return dict(result._mapping) if result else None
        else:
            query = """
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN override_inheritance = 1 THEN 1 ELSE 0 END) as overridden,
                SUM(CASE 
                    WHEN override_inheritance = 0 
                    AND EXISTS (
                        SELECT 1 FROM subjects_catalog sc 
                        WHERE sc.subject_code = o.subject_code 
                        AND (sc.credits_total != o.credits_total 
                             OR sc.internal_marks_max != o.internal_marks_max)
                    ) THEN 1 ELSE 0 
                END) as out_of_sync
            FROM subject_offerings o
            WHERE status = 'published'
            """
            result = _exec(conn, query).fetchone()
            return dict(result._mapping) if result else {"total": 0, "overridden": 0, "out_of_sync": 0}


def run_health_check(engine: Engine, check_type: str, ay_label: str = None, degree_code: str = None) -> dict:
    """Run a specific health check."""
    with engine.begin() as conn:
        issues = []
        issue_count = 0
        
        if check_type == "duplicate_offerings":
            query = """
            SELECT 
                ay_label, degree_code, subject_code, year, term, COUNT(*) as cnt
            FROM subject_offerings
            WHERE division_code IS NULL OR applies_to_all_divisions = 1
            GROUP BY ay_label, degree_code, subject_code, year, term
            HAVING COUNT(*) > 1
            """
            if ay_label:
                query += f" AND ay_label = '{ay_label}'"
            if degree_code:
                query += f" AND degree_code = '{degree_code}'"
            
            results = _exec(conn, query).fetchall()
            issue_count = len(results)
            issues = [dict(r._mapping) for r in results]
        
        elif check_type == "catalog_sync_status":
            sync_info = check_catalog_sync(engine)
            issue_count = sync_info.get("out_of_sync", 0)
            issues = [sync_info]
        
        elif check_type == "elective_topics_required":
            query = """
            SELECT * FROM v_offerings_needing_topics
            WHERE status = 'published' AND topic_status = 'NO_TOPICS'
            """
            if ay_label:
                query += f" AND ay_label = '{ay_label}'"
            if degree_code:
                query += f" AND degree_code = '{degree_code}'"
            
            results = _exec(conn, query).fetchall()
            issue_count = len(results)
            issues = [dict(r._mapping) for r in results]
        
        # Store check result
        status = 'pass' if issue_count == 0 else ('warn' if issue_count < 5 else 'fail')
        _exec(conn, """
        INSERT INTO subject_offerings_health_checks 
        (check_name, check_type, status, issue_count, details, ay_label, degree_code)
        VALUES (:name, :type, :status, :count, :details, :ay, :degree)
        """, {
            "name": check_type.replace("_", " ").title(),
            "type": check_type,
            "status": status,
            "count": issue_count,
            "details": str(issues),
            "ay": ay_label,
            "degree": degree_code
        })
        
        return {
            "check_type": check_type,
            "status": status,
            "issue_count": issue_count,
            "issues": issues
        }


def create_snapshot(engine: Engine, offering_id: int, snapshot_type: str, actor: str, note: str = None) -> int:
    """Create a version snapshot of an offering."""
    import json
    
    with engine.begin() as conn:
        # Get current offering data
        offering = _exec(conn, "SELECT * FROM subject_offerings WHERE id = :id", {"id": offering_id}).fetchone()
        if not offering:
            raise ValueError(f"Offering {offering_id} not found")
        
        offering_dict = dict(offering._mapping)
        
        # Get next snapshot number
        result = _exec(conn, """
        SELECT COALESCE(MAX(snapshot_number), 0) + 1 as next_num
        FROM subject_offerings_snapshots
        WHERE offering_id = :id
        """, {"id": offering_id}).fetchone()
        
        next_num = result[0]
        
        # Mark previous snapshots as inactive
        _exec(conn, """
        UPDATE subject_offerings_snapshots
        SET is_active_version = 0
        WHERE offering_id = :id
        """, {"id": offering_id})
        
        # Create new snapshot
        _exec(conn, """
        INSERT INTO subject_offerings_snapshots
        (offering_id, subject_code, ay_label, snapshot_number, snapshot_data, 
         snapshot_type, note, created_by, is_active_version)
        VALUES (:offering_id, :subject_code, :ay_label, :snapshot_number, :snapshot_data,
                :snapshot_type, :note, :created_by, 1)
        """, {
            "offering_id": offering_id,
            "subject_code": offering_dict["subject_code"],
            "ay_label": offering_dict["ay_label"],
            "snapshot_number": next_num,
            "snapshot_data": json.dumps(offering_dict, default=str),
            "snapshot_type": snapshot_type,
            "note": note,
            "created_by": actor
        })
        
        snapshot_id = conn.execute(sa_text("SELECT last_insert_rowid()")).fetchone()[0]
        
        logger.info(f"✅ Created snapshot #{next_num} for offering {offering_id}")
        return snapshot_id


def rollback_to_snapshot(engine: Engine, offering_id: int, snapshot_id: int, actor: str) -> bool:
    """Rollback an offering to a previous snapshot."""
    import json
    
    with engine.begin() as conn:
        # Get snapshot
        snapshot = _exec(conn, """
        SELECT * FROM subject_offerings_snapshots
        WHERE id = :id AND offering_id = :offering_id
        """, {"id": snapshot_id, "offering_id": offering_id}).fetchone()
        
        if not snapshot:
            raise ValueError(f"Snapshot {snapshot_id} not found for offering {offering_id}")
        
        snapshot_dict = dict(snapshot._mapping)
        offering_data = json.loads(snapshot_dict["snapshot_data"])
        
        # Check if offering is frozen
        current = _exec(conn, "SELECT is_frozen FROM subject_offerings WHERE id = :id", 
                       {"id": offering_id}).fetchone()
        if current and current[0] == 1:
            raise ValueError("Cannot rollback frozen offering. Unfreeze first.")
        
        # Create a snapshot of current state before rollback
        create_snapshot(engine, offering_id, "rollback", actor, 
                       f"Pre-rollback state before reverting to snapshot #{snapshot_dict['snapshot_number']}")
        
        # Update offering with snapshot data
        update_fields = [
            "credits_total", "L", "T", "P", "S",
            "internal_marks_max", "exam_marks_max", "jury_viva_marks_max", "total_marks_max",
            "direct_weight_percent", "indirect_weight_percent",
            "pass_threshold_overall", "pass_threshold_internal", "pass_threshold_external",
            "instructor_email", "status", "override_inheritance", "override_reason"
        ]
        
        set_clause = ", ".join([f"{field} = :{field}" for field in update_fields])
        params = {field: offering_data.get(field) for field in update_fields}
        params["id"] = offering_id
        params["actor"] = actor
        
        _exec(conn, f"""
        UPDATE subject_offerings
        SET {set_clause}, updated_by = :actor, updated_at = CURRENT_TIMESTAMP
        WHERE id = :id
        """, params)
        
        # Mark snapshot as active
        _exec(conn, """
        UPDATE subject_offerings_snapshots
        SET is_active_version = 0
        WHERE offering_id = :id
        """, {"id": offering_id})
        
        _exec(conn, """
        UPDATE subject_offerings_snapshots
        SET is_active_version = 1
        WHERE id = :snapshot_id
        """, {"snapshot_id": snapshot_id})
        
        logger.info(f"✅ Rolled back offering {offering_id} to snapshot #{snapshot_dict['snapshot_number']}")
        return True


def cleanup_old_snapshots(engine: Engine, keep_last: int = 100):
    """Clean up old snapshots, keeping only the most recent N per offering."""
    with engine.begin() as conn:
        deleted = _exec(conn, """
        DELETE FROM subject_offerings_snapshots
        WHERE id IN (
            SELECT id FROM (
                SELECT id, 
                       ROW_NUMBER() OVER (PARTITION BY offering_id ORDER BY snapshot_number DESC) as rn
                FROM subject_offerings_snapshots
            )
            WHERE rn > :keep_last
        )
        """, {"keep_last": keep_last})
        
        count = deleted.rowcount
        logger.info(f"✅ Cleaned up {count} old snapshots")
        return count


if __name__ == "__main__":
    # Test installation
    from sqlalchemy import create_engine
    
    print("\n" + "="*60)
    print("TESTING SUBJECT OFFERINGS SCHEMA INSTALLATION")
    print("="*60 + "\n")
    
    engine = create_engine("sqlite:///test_offerings.db")
    success = install_subject_offerings_schema(engine)
    
    if success:
        print("\n✅ Schema installation test PASSED!")
        
        # Run test health check
        print("\nRunning test health check...")
        result = run_health_check(engine, "duplicate_offerings")
        print(f"Health check result: {result}")
        
    else:
        print("\n❌ Schema installation test FAILED!")
    
    print("\n" + "="*60 + "\n")
