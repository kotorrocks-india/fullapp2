# screens/electives_topics/schema.py
"""
Electives & College Projects Schema (Slide 18)
Clean implementation for fresh database - integrates with subjects_catalog.
No migration code needed.
"""

from __future__ import annotations
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
import logging

logger = logging.getLogger(__name__)


def _exec(conn, sql: str, params: dict = None):
    """Execute SQL with parameters."""
    return conn.execute(sa_text(sql), params or {})


def _table_exists(conn, table: str) -> bool:
    """Check if table exists."""
    result = conn.execute(sa_text(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=:t"
    ), {"t": table}).fetchone()
    return result is not None


# ===========================================================================
# ELECTIVE TOPICS TABLE
# ===========================================================================

def install_elective_topics(engine: Engine):
    """
    Create elective topics table (per subject offering).
    Topics inherit from parent subject in subjects_catalog.
    """
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS elective_topics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Link to parent subject offering
            subject_code TEXT NOT NULL,
            subject_name TEXT,
            
            -- Scope (from parent offering)
            degree_code TEXT NOT NULL,
            program_code TEXT,
            branch_code TEXT,
            ay_label TEXT NOT NULL,
            year INTEGER NOT NULL,
            term INTEGER NOT NULL,
            division_code TEXT,
            
            -- Topic identification
            topic_no INTEGER NOT NULL,
            topic_code_ay TEXT NOT NULL,  -- e.g., CS-ELECT-1, CS-ELECT-2
            topic_name TEXT NOT NULL,
            
            -- Topic details
            owner_faculty_id INTEGER,
            owner_faculty_email TEXT,
            capacity INTEGER DEFAULT 0,  -- 0 = unlimited
            offering_id INTEGER,          -- Links to subject_offerings.id once created
            description TEXT,
            
            -- NEW: Prerequisites and learning outcomes
            prerequisites TEXT,  -- JSON array: ["Subject A", "Subject B"]
            learning_outcomes TEXT,  -- JSON array: ["LO1: ...", "LO2: ..."]
            co_mapping TEXT,  -- JSON: {"CO1": ["PO1", "PO2"]}
            reference_materials TEXT,  -- JSON array
            
            -- Status
            status TEXT NOT NULL DEFAULT 'draft',  -- draft, published, archived
            
            -- Metadata
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME,
            last_updated_by TEXT,
            
            -- Constraints
            UNIQUE(subject_code, ay_label, year, term, division_code, topic_no),
            UNIQUE(topic_code_ay, ay_label),
            
            FOREIGN KEY (owner_faculty_id) REFERENCES faculty(id),
            FOREIGN KEY (offering_id) REFERENCES subject_offerings(id),
            
            CHECK(topic_no > 0),
            CHECK(capacity >= 0),
            CHECK(status IN ('draft', 'published', 'archived'))
        )
        """)
        
        # Indexes
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_topics_subject ON elective_topics(subject_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_topics_ay ON elective_topics(ay_label, year, term)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_topics_degree ON elective_topics(degree_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_topics_status ON elective_topics(status)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_topics_owner ON elective_topics(owner_faculty_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_topics_code_ay ON elective_topics(topic_code_ay)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_topics_offering ON elective_topics(offering_id)")
        
        logger.info("✓ Installed elective_topics table")

# ===========================================================================
# STUDENT SELECTIONS TABLE
# ===========================================================================

def install_student_selections(engine: Engine):
    """Create student selections table."""
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS elective_student_selections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Student identification
            student_id INTEGER,
            student_roll_no TEXT NOT NULL,
            student_name TEXT NOT NULL,
            student_email TEXT,
            
            -- Scope (denormalized for queries)
            degree_code TEXT NOT NULL,
            program_code TEXT,
            branch_code TEXT,
            ay_label TEXT NOT NULL,
            year INTEGER NOT NULL,
            term INTEGER NOT NULL,
            division_code TEXT,
            batch TEXT,
            
            -- Subject & Topic
            subject_code TEXT NOT NULL,
            topic_code_ay TEXT NOT NULL,
            topic_name TEXT NOT NULL,
            
            -- Selection details
            rank_choice INTEGER,  -- For ranked strategy (1st, 2nd, 3rd choice)
            selection_strategy TEXT DEFAULT 'manual_assign',  -- manual_assign, student_select_ranked, student_select_first_come
            
            -- Status
            status TEXT NOT NULL DEFAULT 'draft',  -- draft, confirmed, waitlisted, withdrawn
            
            -- Timestamps
            selected_at DATETIME,
            confirmed_at DATETIME,
            confirmed_by TEXT,
            waitlisted_at DATETIME,
            withdrawn_at DATETIME,
            
            -- Metadata
            notes TEXT,
            override_reason TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME,
            last_updated_by TEXT,
            
            -- Constraints
            -- One student can only have ONE confirmed/waitlisted selection per subject
            UNIQUE(student_roll_no, subject_code, ay_label, year, term),
            
            FOREIGN KEY (student_id) REFERENCES student_profiles(id),
            
            CHECK(status IN ('draft', 'confirmed', 'waitlisted', 'withdrawn')),
            CHECK(rank_choice IS NULL OR rank_choice > 0)
        )
        """)
        
        # Indexes
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_selections_student ON elective_student_selections(student_roll_no)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_selections_topic ON elective_student_selections(topic_code_ay, ay_label)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_selections_subject ON elective_student_selections(subject_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_selections_status ON elective_student_selections(status)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_selections_ay ON elective_student_selections(ay_label, year, term)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_selections_confirmed ON elective_student_selections(confirmed_at)")
        
        # Unique index for confirmed selections (one student per topic)
        _exec(conn, """
        CREATE UNIQUE INDEX IF NOT EXISTS ix_selections_unique_confirmed
        ON elective_student_selections(topic_code_ay, student_roll_no, ay_label)
        WHERE status IN ('confirmed', 'waitlisted')
        """)
        
        logger.info("✓ Installed elective_student_selections table")


# ===========================================================================
# SELECTION WINDOWS TABLE
# ===========================================================================

def install_selection_windows(engine: Engine):
    """Create selection windows table (per subject offering)."""
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS elective_selection_windows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Subject offering scope
            subject_code TEXT NOT NULL,
            degree_code TEXT NOT NULL,
            program_code TEXT,
            branch_code TEXT,
            ay_label TEXT NOT NULL,
            year INTEGER NOT NULL,
            term INTEGER NOT NULL,
            batch TEXT,               -- ✅ NEW: batch-based access control
            division_code TEXT,
            
            -- Window times
            start_datetime TEXT NOT NULL,  -- ISO format
            end_datetime TEXT NOT NULL,    -- ISO format
            timezone TEXT NOT NULL DEFAULT 'Asia/Kolkata',
            
            -- Auto-confirm settings
            auto_confirm_enabled INTEGER NOT NULL DEFAULT 1,
            auto_confirm_order TEXT,  -- JSON array: ["manual_assign", "student_select_ranked", ...]
            min_satisfaction_percent REAL DEFAULT 50.0,
            
            -- Status
            is_active INTEGER NOT NULL DEFAULT 1,
            manually_closed INTEGER NOT NULL DEFAULT 0,
            closed_reason TEXT,
            
            -- Metadata
            created_by TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME,
            last_updated_by TEXT,
            
            -- Constraints
            UNIQUE(subject_code, degree_code, program_code, branch_code,
                   ay_label, year, term, batch, division_code),
            
            CHECK(start_datetime < end_datetime)
        )
        """)

        # Indexes
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_windows_subject ON elective_selection_windows(subject_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_windows_ay ON elective_selection_windows(ay_label, year, term)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_windows_active ON elective_selection_windows(is_active)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_windows_dates ON elective_selection_windows(start_datetime, end_datetime)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_windows_batch ON elective_selection_windows(batch)")  # ✅ NEW index

        logger.info("✓ Installed elective_selection_windows table (with batch)")

# ===========================================================================
# CAPACITY TRACKING TABLE (Materialized View)
# ===========================================================================

def install_capacity_tracking(engine: Engine):
    """Create capacity tracking table for performance."""
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS elective_capacity_tracking (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Topic identification
            topic_code_ay TEXT NOT NULL,
            ay_label TEXT NOT NULL,
            
            -- Capacity info
            max_capacity INTEGER NOT NULL DEFAULT 0,
            confirmed_count INTEGER NOT NULL DEFAULT 0,
            waitlisted_count INTEGER NOT NULL DEFAULT 0,
            remaining_capacity INTEGER NOT NULL DEFAULT 0,
            
            -- Status
            is_full INTEGER NOT NULL DEFAULT 0,
            has_waitlist INTEGER NOT NULL DEFAULT 0,
            
            -- Last updated
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            
            -- Constraints
            UNIQUE(topic_code_ay, ay_label),
            
            CHECK(confirmed_count >= 0),
            CHECK(waitlisted_count >= 0),
            CHECK(remaining_capacity >= 0 OR max_capacity = 0)
        )
        """)
        
        # Indexes
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_capacity_topic ON elective_capacity_tracking(topic_code_ay, ay_label)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_capacity_full ON elective_capacity_tracking(is_full)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_capacity_updated ON elective_capacity_tracking(updated_at)")
        
        logger.info("✓ Installed elective_capacity_tracking table")


# ===========================================================================
# ALLOCATION RUNS TABLE
# ===========================================================================

def install_allocation_runs(engine: Engine):
    """Create allocation engine tracking table."""
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS elective_allocation_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Context
            subject_code TEXT NOT NULL,
            degree_code TEXT NOT NULL,
            program_code TEXT,
            branch_code TEXT,
            ay_label TEXT NOT NULL,
            year INTEGER NOT NULL,
            term INTEGER NOT NULL,
            division_code TEXT,
            
            -- Run details
            run_number INTEGER NOT NULL,
            started_at DATETIME NOT NULL,
            completed_at DATETIME,
            status TEXT NOT NULL DEFAULT 'running',
            
            -- Algorithm settings
            strategy TEXT NOT NULL,
            min_satisfaction_percent REAL,
            
            -- Results
            total_students INTEGER,
            students_assigned INTEGER,
            students_waitlisted INTEGER,
            students_unassigned INTEGER,
            top_choice_satisfaction_percent REAL,
            
            -- Performance
            iterations_completed INTEGER DEFAULT 0,
            processing_time_ms INTEGER,
            
            -- Control
            stopped_by TEXT,
            stop_reason TEXT,
            error_message TEXT,
            
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            
            CHECK(status IN ('running', 'completed', 'stopped', 'failed')),
            CHECK(run_number > 0)
        )
        """)
        
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_allocation_runs_subject ON elective_allocation_runs(subject_code, ay_label, year, term)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_allocation_runs_status ON elective_allocation_runs(status)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_allocation_runs_started ON elective_allocation_runs(started_at)")
        
        logger.info("✓ Installed elective_allocation_runs table")


# ===========================================================================
# PREFERENCE HISTORY TABLE
# ===========================================================================

def install_preference_history(engine: Engine):
    """Create student preference history table."""
    with engine.begin() as conn:
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS elective_preference_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Student & context
            student_roll_no TEXT NOT NULL,
            student_profile_id INTEGER,
            subject_code TEXT NOT NULL,
            degree_code TEXT NOT NULL,
            ay_label TEXT NOT NULL,
            year INTEGER NOT NULL,
            term INTEGER NOT NULL,
            
            -- Preference details
            rank INTEGER NOT NULL,
            topic_code_ay TEXT NOT NULL,
            topic_name TEXT,
            
            -- Tracking
            submitted_at DATETIME NOT NULL,
            superseded INTEGER DEFAULT 0,
            superseded_by_id INTEGER,
            
            -- Metadata
            source TEXT DEFAULT 'student_portal',
            ip_address TEXT,
            user_agent TEXT,
            session_id TEXT,
            
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            
            CHECK(rank > 0),
            CHECK(rank <= 10),
            
            FOREIGN KEY(student_profile_id) REFERENCES student_profiles(id) ON DELETE SET NULL,
            FOREIGN KEY(superseded_by_id) REFERENCES elective_preference_history(id) ON DELETE SET NULL
        )
        """)
        
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_pref_history_student ON elective_preference_history(student_roll_no, subject_code, ay_label)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_pref_history_topic ON elective_preference_history(topic_code_ay, ay_label)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_pref_history_submitted ON elective_preference_history(submitted_at)")
        
        logger.info("✓ Installed elective_preference_history table")


# ===========================================================================
# AUDIT TABLES
# ===========================================================================

def install_elective_audit(engine: Engine):
    """Create audit tables for electives."""
    with engine.begin() as conn:
        # Topics audit
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS elective_topics_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic_id INTEGER,
            topic_code_ay TEXT NOT NULL,
            subject_code TEXT NOT NULL,
            ay_label TEXT NOT NULL,
            action TEXT NOT NULL,
            note TEXT,
            changed_fields TEXT,
            actor TEXT NOT NULL,
            actor_role TEXT,
            occurred_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            operation TEXT,
            reason TEXT,
            source TEXT DEFAULT 'ui',
            correlation_id TEXT,
            step_up_performed INTEGER DEFAULT 0,
            ip_address TEXT,
            user_agent TEXT,
            session_id TEXT
        )
        """)
        
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_topics_audit_topic ON elective_topics_audit(topic_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_topics_audit_at ON elective_topics_audit(occurred_at)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_topics_audit_actor ON elective_topics_audit(actor)")
        
        # Selections audit
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS elective_selections_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            selection_id INTEGER,
            student_roll_no TEXT NOT NULL,
            topic_code_ay TEXT NOT NULL,
            subject_code TEXT NOT NULL,
            ay_label TEXT NOT NULL,
            action TEXT NOT NULL,
            old_status TEXT,
            new_status TEXT,
            note TEXT,
            changed_fields TEXT,
            actor TEXT NOT NULL,
            actor_role TEXT,
            occurred_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            operation TEXT,
            reason TEXT,
            source TEXT DEFAULT 'ui',
            correlation_id TEXT,
            step_up_performed INTEGER DEFAULT 0,
            ip_address TEXT,
            user_agent TEXT,
            session_id TEXT
        )
        """)
        
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_selections_audit_selection ON elective_selections_audit(selection_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_selections_audit_student ON elective_selections_audit(student_roll_no)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_selections_audit_at ON elective_selections_audit(occurred_at)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_selections_audit_actor ON elective_selections_audit(actor)")
        
        logger.info("✓ Installed elective audit tables")


# ===========================================================================
# VIEWS
# ===========================================================================

def install_elective_views(engine: Engine):
    """Create useful views for queries."""
    with engine.begin() as conn:
        # View: Topic summary with capacity
        _exec(conn, """
        CREATE VIEW IF NOT EXISTS v_elective_topics_summary AS
        SELECT 
            t.id,
            t.topic_code_ay,
            t.topic_name,
            t.subject_code,
            t.subject_name,
            t.degree_code,
            t.program_code,
            t.branch_code,
            t.ay_label,
            t.year,
            t.term,
            t.division_code,
            t.topic_no,
            t.capacity AS max_capacity,
            t.owner_faculty_email,
            t.description,
            t.prerequisites,
            t.learning_outcomes,
            t.status,
            COALESCE(c.confirmed_count, 0) AS confirmed_count,
            COALESCE(c.waitlisted_count, 0) AS waitlisted_count,
            COALESCE(c.remaining_capacity, t.capacity) AS remaining_capacity,
            CASE 
                WHEN t.capacity = 0 THEN 0
                WHEN COALESCE(c.confirmed_count, 0) >= t.capacity THEN 1
                ELSE 0
            END AS is_full,
            t.created_at,
            t.updated_at
        FROM elective_topics t
        LEFT JOIN elective_capacity_tracking c 
            ON c.topic_code_ay = t.topic_code_ay 
            AND c.ay_label = t.ay_label
        """)
        
        # View: Student selections with topic details
        _exec(conn, """
        CREATE VIEW IF NOT EXISTS v_student_selections_detail AS
        SELECT 
            s.id,
            s.student_roll_no,
            s.student_name,
            s.student_email,
            s.degree_code,
            s.program_code,
            s.branch_code,
            s.ay_label,
            s.year,
            s.term,
            s.division_code,
            s.batch,
            s.subject_code,
            s.topic_code_ay,
            s.topic_name,
            s.rank_choice,
            s.selection_strategy,
            s.status,
            s.selected_at,
            s.confirmed_at,
            s.confirmed_by,
            s.waitlisted_at,
            t.owner_faculty_email,
            t.capacity AS topic_capacity,
            t.status AS topic_status,
            s.created_at,
            s.updated_at
        FROM elective_student_selections s
        LEFT JOIN elective_topics t 
            ON t.topic_code_ay = s.topic_code_ay 
            AND t.ay_label = s.ay_label
        """)
        
        logger.info("✓ Installed elective views")


# ===========================================================================
# TRIGGERS FOR CAPACITY TRACKING
# ===========================================================================

def install_capacity_triggers(engine: Engine):
    """Create triggers to maintain capacity tracking."""
    with engine.begin() as conn:
        # Trigger: Update capacity on new confirmed selection
        _exec(conn, """
        CREATE TRIGGER IF NOT EXISTS trg_selection_confirmed_insert
        AFTER INSERT ON elective_student_selections
        WHEN NEW.status IN ('confirmed', 'waitlisted')
        BEGIN
            INSERT INTO elective_capacity_tracking (
                topic_code_ay, ay_label, max_capacity, 
                confirmed_count, waitlisted_count, remaining_capacity, 
                is_full, has_waitlist, updated_at
            )
            SELECT 
                NEW.topic_code_ay,
                NEW.ay_label,
                t.capacity,
                CASE WHEN NEW.status = 'confirmed' THEN 1 ELSE 0 END,
                CASE WHEN NEW.status = 'waitlisted' THEN 1 ELSE 0 END,
                CASE 
                    WHEN t.capacity = 0 THEN 999999
                    ELSE t.capacity - CASE WHEN NEW.status = 'confirmed' THEN 1 ELSE 0 END
                END,
                CASE 
                    WHEN t.capacity > 0 AND NEW.status = 'confirmed' AND t.capacity <= 1 THEN 1
                    ELSE 0
                END,
                CASE WHEN NEW.status = 'waitlisted' THEN 1 ELSE 0 END,
                CURRENT_TIMESTAMP
            FROM elective_topics t
            WHERE t.topic_code_ay = NEW.topic_code_ay
            AND t.ay_label = NEW.ay_label
            ON CONFLICT(topic_code_ay, ay_label) DO UPDATE SET
                confirmed_count = confirmed_count + CASE WHEN NEW.status = 'confirmed' THEN 1 ELSE 0 END,
                waitlisted_count = waitlisted_count + CASE WHEN NEW.status = 'waitlisted' THEN 1 ELSE 0 END,
                remaining_capacity = CASE 
                    WHEN max_capacity = 0 THEN 999999
                    ELSE max_capacity - (confirmed_count + CASE WHEN NEW.status = 'confirmed' THEN 1 ELSE 0 END)
                END,
                is_full = CASE 
                    WHEN max_capacity > 0 AND (confirmed_count + CASE WHEN NEW.status = 'confirmed' THEN 1 ELSE 0 END) >= max_capacity THEN 1
                    ELSE 0
                END,
                has_waitlist = CASE 
                    WHEN (waitlisted_count + CASE WHEN NEW.status = 'waitlisted' THEN 1 ELSE 0 END) > 0 THEN 1
                    ELSE 0
                END,
                updated_at = CURRENT_TIMESTAMP;
        END;
        """)
        
        # Trigger: Update capacity on status change
        _exec(conn, """
        CREATE TRIGGER IF NOT EXISTS trg_selection_status_update
        AFTER UPDATE OF status ON elective_student_selections
        WHEN OLD.status != NEW.status
        BEGIN
            UPDATE elective_capacity_tracking
            SET 
                confirmed_count = confirmed_count 
                    - CASE WHEN OLD.status = 'confirmed' THEN 1 ELSE 0 END
                    + CASE WHEN NEW.status = 'confirmed' THEN 1 ELSE 0 END,
                waitlisted_count = waitlisted_count 
                    - CASE WHEN OLD.status = 'waitlisted' THEN 1 ELSE 0 END
                    + CASE WHEN NEW.status = 'waitlisted' THEN 1 ELSE 0 END,
                remaining_capacity = CASE 
                    WHEN max_capacity = 0 THEN 999999
                    ELSE max_capacity - (
                        confirmed_count 
                        - CASE WHEN OLD.status = 'confirmed' THEN 1 ELSE 0 END
                        + CASE WHEN NEW.status = 'confirmed' THEN 1 ELSE 0 END
                    )
                END,
                is_full = CASE 
                    WHEN max_capacity > 0 AND (
                        confirmed_count 
                        - CASE WHEN OLD.status = 'confirmed' THEN 1 ELSE 0 END
                        + CASE WHEN NEW.status = 'confirmed' THEN 1 ELSE 0 END
                    ) >= max_capacity THEN 1
                    ELSE 0
                END,
                has_waitlist = CASE 
                    WHEN (
                        waitlisted_count 
                        - CASE WHEN OLD.status = 'waitlisted' THEN 1 ELSE 0 END
                        + CASE WHEN NEW.status = 'waitlisted' THEN 1 ELSE 0 END
                    ) > 0 THEN 1
                    ELSE 0
                END,
                updated_at = CURRENT_TIMESTAMP
            WHERE topic_code_ay = NEW.topic_code_ay 
            AND ay_label = NEW.ay_label;
        END;
        """)
        
        # Trigger: Prevent topic deletion with students
        _exec(conn, """
        CREATE TRIGGER IF NOT EXISTS trg_prevent_topic_delete_with_students
        BEFORE DELETE ON elective_topics
        WHEN EXISTS (
            SELECT 1 FROM elective_student_selections 
            WHERE topic_code_ay = OLD.topic_code_ay 
            AND ay_label = OLD.ay_label 
            AND status IN ('confirmed', 'waitlisted')
        )
        BEGIN
            SELECT RAISE(ABORT, 'Cannot delete topic with assigned students. Archive instead.');
        END;
        """)
        
        logger.info("✓ Installed capacity triggers")


# ===========================================================================
# MASTER INSTALL FUNCTION
# ===========================================================================

def install_electives_schema(engine: Engine):
    """
    Install complete electives & college projects schema.
    Safe to run multiple times (idempotent).
    """
    logger.info("Installing electives schema...")
    
    try:
        # Core tables
        install_elective_topics(engine)
        install_student_selections(engine)
        install_selection_windows(engine)
        install_capacity_tracking(engine)
        install_allocation_runs(engine)
        install_preference_history(engine)
        
        # Audit
        install_elective_audit(engine)
        
        # Views
        install_elective_views(engine)
        
        # Triggers
        install_capacity_triggers(engine)
        
        logger.info("✅ Electives schema installed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Electives schema installation failed: {e}", exc_info=True)
        return False


# ===========================================================================
# UTILITY FUNCTIONS
# ===========================================================================

def refresh_capacity_tracking(engine: Engine, topic_code_ay: str = None, ay_label: str = None):
    """Manually refresh capacity tracking (for bulk operations)."""
    with engine.begin() as conn:
        if topic_code_ay and ay_label:
            # Refresh specific topic
            _exec(conn, """
            INSERT OR REPLACE INTO elective_capacity_tracking (
                topic_code_ay, ay_label, max_capacity, 
                confirmed_count, waitlisted_count, remaining_capacity,
                is_full, has_waitlist, updated_at
            )
            SELECT 
                t.topic_code_ay,
                t.ay_label,
                t.capacity,
                COALESCE(SUM(CASE WHEN s.status = 'confirmed' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN s.status = 'waitlisted' THEN 1 ELSE 0 END), 0),
                CASE 
                    WHEN t.capacity = 0 THEN 999999
                    ELSE t.capacity - COALESCE(SUM(CASE WHEN s.status = 'confirmed' THEN 1 ELSE 0 END), 0)
                END,
                CASE 
                    WHEN t.capacity > 0 AND COALESCE(SUM(CASE WHEN s.status = 'confirmed' THEN 1 ELSE 0 END), 0) >= t.capacity THEN 1
                    ELSE 0
                END,
                CASE 
                    WHEN COALESCE(SUM(CASE WHEN s.status = 'waitlisted' THEN 1 ELSE 0 END), 0) > 0 THEN 1
                    ELSE 0
                END,
                CURRENT_TIMESTAMP
            FROM elective_topics t
            LEFT JOIN elective_student_selections s 
                ON s.topic_code_ay = t.topic_code_ay 
                AND s.ay_label = t.ay_label
                AND s.status IN ('confirmed', 'waitlisted')
            WHERE t.topic_code_ay = :topic_code_ay 
            AND t.ay_label = :ay_label
            GROUP BY t.topic_code_ay, t.ay_label, t.capacity
            """, {"topic_code_ay": topic_code_ay, "ay_label": ay_label})
        else:
            # Refresh all
            _exec(conn, """
            INSERT OR REPLACE INTO elective_capacity_tracking (
                topic_code_ay, ay_label, max_capacity, 
                confirmed_count, waitlisted_count, remaining_capacity,
                is_full, has_waitlist, updated_at
            )
            SELECT 
                t.topic_code_ay,
                t.ay_label,
                t.capacity,
                COALESCE(SUM(CASE WHEN s.status = 'confirmed' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN s.status = 'waitlisted' THEN 1 ELSE 0 END), 0),
                CASE 
                    WHEN t.capacity = 0 THEN 999999
                    ELSE t.capacity - COALESCE(SUM(CASE WHEN s.status = 'confirmed' THEN 1 ELSE 0 END), 0)
                END,
                CASE 
                    WHEN t.capacity > 0 AND COALESCE(SUM(CASE WHEN s.status = 'confirmed' THEN 1 ELSE 0 END), 0) >= t.capacity THEN 1
                    ELSE 0
                END,
                CASE 
                    WHEN COALESCE(SUM(CASE WHEN s.status = 'waitlisted' THEN 1 ELSE 0 END), 0) > 0 THEN 1
                    ELSE 0
                END,
                CURRENT_TIMESTAMP
            FROM elective_topics t
            LEFT JOIN elective_student_selections s 
                ON s.topic_code_ay = t.topic_code_ay 
                AND s.ay_label = t.ay_label
                AND s.status IN ('confirmed', 'waitlisted')
            GROUP BY t.topic_code_ay, t.ay_label, t.capacity
            """)
        
        logger.info(f"✓ Refreshed capacity tracking for {topic_code_ay or 'all topics'}")


if __name__ == "__main__":
    # Test installation
    from sqlalchemy import create_engine
    
    engine = create_engine("sqlite:///test_electives.db")
    success = install_electives_schema(engine)
    
    if success:
        print("✅ Schema installation test passed!")
    else:
        print("❌ Schema installation test failed!")
