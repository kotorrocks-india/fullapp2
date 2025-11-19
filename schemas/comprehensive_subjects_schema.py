# comprehensive_subjects_schema.py
"""
Complete schema for Course Outcomes (COs) - Slide 20.
This file is responsible for creating all tables related to subject_cos
and their correlations (PO, PSO, PEO).

Other tables (offerings, rubrics) are handled by their own
dedicated schema files.
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

# ===========================================================================
# COURSE OUTCOMES (Slide 20 - COs per offering)
# ===========================================================================

def install_subject_cos(engine: Engine):
    """Course Outcomes per offering (Slide 20)."""
    with engine.begin() as conn:
        # Main CO table
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS subject_cos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Link to offering
            offering_id INTEGER NOT NULL,
            
            -- CO details
            co_code TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            
            -- Taxonomy
            bloom_level TEXT NOT NULL,
            knowledge_type TEXT,
            
            -- Weights & Thresholds
            weight_in_direct REAL NOT NULL DEFAULT 0,
            threshold_internal_percent REAL,
            threshold_external_percent REAL,
            threshold_overall_percent REAL,
            
            -- Sequence
            sequence INTEGER NOT NULL DEFAULT 0,
            
            -- Status
            status TEXT NOT NULL DEFAULT 'draft',
            
            -- Audit
            last_updated_at DATETIME,
            last_updated_by TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME,
            
            FOREIGN KEY (offering_id) REFERENCES subject_offerings(id) ON DELETE CASCADE,
            UNIQUE(offering_id, co_code)
        )
        """)
        
        # PO Correlations (0-3 scale)
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS co_po_correlations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            co_id INTEGER NOT NULL,
            po_code TEXT NOT NULL,
            correlation_value INTEGER NOT NULL DEFAULT 0,
            
            FOREIGN KEY (co_id) REFERENCES subject_cos(id) ON DELETE CASCADE,
            UNIQUE(co_id, po_code),
            CHECK(correlation_value IN (0, 1, 2, 3))
        )
        """)
        
        # PSO Correlations (0-3 scale)
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS co_pso_correlations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            co_id INTEGER NOT NULL,
            pso_code TEXT NOT NULL,
            correlation_value INTEGER NOT NULL DEFAULT 0,
            
            FOREIGN KEY (co_id) REFERENCES subject_cos(id) ON DELETE CASCADE,
            UNIQUE(co_id, pso_code),
            CHECK(correlation_value IN (0, 1, 2, 3))
        )
        """)
        
        # PEO Correlations (0-3 scale)
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS co_peo_correlations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            co_id INTEGER NOT NULL,
            peo_code TEXT NOT NULL,
            correlation_value INTEGER NOT NULL DEFAULT 0,
            
            FOREIGN KEY (co_id) REFERENCES subject_cos(id) ON DELETE CASCADE,
            UNIQUE(co_id, peo_code),
            CHECK(correlation_value IN (0, 1, 2, 3))
        )
        """)
        
        # Assessment mapping (optional detailed mapping)
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS co_assessment_mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            co_id INTEGER NOT NULL,
            assessment_component TEXT NOT NULL,
            sub_component_name TEXT,
            max_marks REAL NOT NULL DEFAULT 0,
            contribution_weight REAL NOT NULL DEFAULT 0,
            
            FOREIGN KEY (co_id) REFERENCES subject_cos(id) ON DELETE CASCADE
        )
        """)
        
        # Link COs to syllabus points
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS co_syllabus_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            co_id INTEGER NOT NULL,
            syllabus_point_id INTEGER NOT NULL,
            
            FOREIGN KEY (co_id) REFERENCES subject_cos(id) ON DELETE CASCADE,
            UNIQUE(co_id, syllabus_point_id)
        )
        """)
        
        # Indexes
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_cos_offering ON subject_cos(offering_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_cos_status ON subject_cos(status)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_co_po_corr_co ON co_po_correlations(co_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_co_pso_corr_co ON co_pso_correlations(co_id)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_co_peo_corr_co ON co_peo_correlations(co_id)")
        
        # Audit
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS subject_cos_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            co_id INTEGER NOT NULL,
            offering_id INTEGER NOT NULL,
            co_code TEXT NOT NULL,
            action TEXT NOT NULL,
            note TEXT,
            changed_fields TEXT,
            actor_id TEXT NOT NULL,
            actor_role TEXT,
            occurred_at_utc DATETIME DEFAULT CURRENT_TIMESTAMP,
            operation TEXT,
            reason TEXT,
            source TEXT,
            correlation_id TEXT,
            step_up_performed INTEGER DEFAULT 0,
            ip_address TEXT,
            user_agent TEXT,
            session_id TEXT
        )
        """)
        
        logger.info("✓ Installed subject_cos")


# ===========================================================================
# MASTER INSTALL FUNCTION
# ===========================================================================
@register
def install_comprehensive_schema(engine: Engine):
    """
    Install ONLY the Course Outcomes (COs) schema.
    Other tables (offerings, rubrics) are handled by their own
    dedicated schema files.
    """
    logger.info("Installing comprehensive subjects schema (COs ONLY)...")
    
    try:
        # Course outcomes
        install_subject_cos(engine)
        
        logger.info("✅ Course Outcomes (subject_cos) schema installed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Schema installation failed: {e}", exc_info=True)
        raise e

# ===========================================================================
# UTILITY FUNCTIONS
# ===========================================================================

def get_offering_by_context(engine: Engine, degree_code: str, ay_label: str, 
                            year: int, term: int, subject_code: str,
                            program_code: str = None, branch_code: str = None,
                            division_code: str = None):
    """Retrieve offering by context."""
    with engine.begin() as conn:
        query = """
        SELECT * FROM subject_offerings
        WHERE degree_code = :degree_code
        AND ay_label = :ay_label
        AND year = :year
        AND term = :term
        AND subject_code = :subject_code
        AND (program_code = :program_code OR (program_code IS NULL AND :program_code IS NULL))
        AND (branch_code = :branch_code OR (branch_code IS NULL AND :branch_code IS NULL))
        AND (division_code = :division_code OR (division_code IS NULL AND :division_code IS NULL) OR applies_to_all_divisions = 1)
        LIMIT 1
        """
        
        result = _exec(conn, query, {
            'degree_code': degree_code,
            'ay_label': ay_label,
            'year': year,
            'term': term,
            'subject_code': subject_code,
            'program_code': program_code,
            'branch_code': branch_code,
            'division_code': division_code
        }).fetchone()
        
        return dict(result) if result else None

def get_cos_for_offering(engine: Engine, offering_id: int, include_correlations: bool = True):
    """Retrieve all COs for an offering with optional correlations."""
    with engine.begin() as conn:
        cos = _exec(conn, """
        SELECT * FROM subject_cos
        WHERE offering_id = :offering_id
        ORDER BY sequence, co_code
        """, {'offering_id': offering_id}).fetchall()
        
        result = []
        for co in cos:
            co_dict = dict(co)
            
            if include_correlations:
                # Get PO correlations
                po_corrs = _exec(conn, """
                SELECT po_code, correlation_value FROM co_po_correlations
                WHERE co_id = :co_id
                """, {'co_id': co_dict['id']}).fetchall()
                co_dict['po_correlations'] = {r['po_code']: r['correlation_value'] for r in po_corrs}
                
                # Get PSO correlations
                pso_corrs = _exec(conn, """
                SELECT pso_code, correlation_value FROM co_pso_correlations
                WHERE co_id = :co_id
                """, {'co_id': co_dict['id']}).fetchall()
                co_dict['pso_correlations'] = {r['pso_code']: r['correlation_value'] for r in pso_corrs}
                
                # Get PEO correlations
                peo_corrs = _exec(conn, """
                SELECT peo_code, correlation_value FROM co_peo_correlations
                WHERE co_id = :co_id
                """, {'co_id': co_dict['id']}).fetchall()
                co_dict['peo_correlations'] = {r['peo_code']: r['correlation_value'] for r in peo_corrs}
            
            result.append(co_dict)
        
        return result

def get_rubric_for_offering(engine: Engine, offering_id: int, scope: str = 'subject', 
                            component_key: str = None):
    """Retrieve rubric configuration for an offering."""
    with engine.begin() as conn:
        query = """
        SELECT * FROM rubric_configs
        WHERE offering_id = :offering_id
        AND scope = :scope
        AND (component_key = :component_key OR (component_key IS NULL AND :component_key IS NULL))
        LIMIT 1
        """
        
        config = _exec(conn, query, {
            'offering_id': offering_id,
            'scope': scope,
            'component_key': component_key
        }).fetchone()
        
        if not config:
            return None
        
        config_dict = dict(config)
        
        # Get assessments
        assessments = _exec(conn, """
        SELECT * FROM rubric_assessments
        WHERE rubric_config_id = :config_id
        ORDER BY code
        """, {'config_id': config_dict['id']}).fetchall()
        
        config_dict['assessments'] = []
        for assessment in assessments:
            assessment_dict = dict(assessment)
            
            # Get criteria or levels based on mode
            if assessment_dict['mode'] == 'analytic_points':
                criteria = _exec(conn, """
                SELECT * FROM rubric_assessment_criteria
                WHERE assessment_id = :assessment_id
                """, {'assessment_id': assessment_dict['id']}).fetchall()
                assessment_dict['criteria'] = [dict(c) for c in criteria]
            
            else:  # analytic_levels
                levels = _exec(conn, """
                SELECT * FROM rubric_assessment_levels
                WHERE assessment_id = :assessment_id
                ORDER BY criterion_key, level_sequence
                """, {'assessment_id': assessment_dict['id']}).fetchall()
                assessment_dict['levels'] = [dict(l) for l in levels]
            
            config_dict['assessments'].append(assessment_dict)
        
        return config_dict

def validate_rubric_weights(engine: Engine, assessment_id: int):
    """Validate that rubric criteria weights sum to 100."""
    with engine.begin() as conn:
        total = _exec(conn, """
        SELECT SUM(weight_pct) as total FROM rubric_assessment_criteria
        WHERE assessment_id = :assessment_id
        """, {'assessment_id': assessment_id}).fetchone()
        
        return abs(total['total'] - 100.0) < 0.01 if total and total['total'] else False

def validate_co_weights(engine: Engine, offering_id: int):
    """Validate that CO direct weights sum to approximately 1.0."""
    with engine.begin() as conn:
        total = _exec(conn, """
        SELECT SUM(weight_in_direct) as total FROM subject_cos
        WHERE offering_id = :offering_id
        """, {'offering_id': offering_id}).fetchone()
        
        return abs(total['total'] - 1.0) < 0.1 if total and total['total'] else False

# ===========================================================================
# EXAMPLE USAGE
# ===========================================================================

if __name__ == "__main__":
    from sqlalchemy import create_engine
    
    # Create in-memory database for testing
    engine = create_engine("sqlite:///:memory:")
    
    # Install schema
    success = install_comprehensive_schema(engine)
    
    if success:
        print("✅ Schema installation completed successfully!")
        print("\nTables created:")
        with engine.begin() as conn:
            tables = _exec(conn, """
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            ORDER BY name
            """).fetchall()
            for table in tables:
                print(f"  - {table[0]}")
    else:
        print("❌ Schema installation failed!")
