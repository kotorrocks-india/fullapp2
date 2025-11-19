"""
Database helper functions - Enhanced with proper semester and status checking
FIXED: Proper error handling and None checks
"""

from typing import Optional, List, Dict, Any, Tuple
import streamlit as st
from sqlalchemy import text as sa_text
from .helpers import exec_query, rows_to_dicts


@st.cache_data(ttl=300)
def fetch_degrees(_engine):
    """Fetch all active degrees."""
    with _engine.begin() as conn:
        rows = exec_query(conn, """
            SELECT code, title, cohort_splitting_mode,
                   cg_degree, cg_program, cg_branch, active
            FROM degrees
            WHERE active = 1
            ORDER BY sort_order, code
        """).fetchall()
    return rows_to_dicts(rows)


@st.cache_data(ttl=300)
def fetch_programs(_engine, degree_code: str):
    """Fetch programs for a degree."""
    with _engine.begin() as conn:
        rows = exec_query(conn, """
            SELECT program_code, program_name, active
            FROM programs
            WHERE degree_code = :d AND active = 1
            ORDER BY sort_order, program_code
        """, {"d": degree_code}).fetchall()
    return rows_to_dicts(rows)


@st.cache_data(ttl=300)
def fetch_branches(_engine, degree_code: str, program_code: Optional[str] = None):
    """Fetch branches for degree/program."""
    with _engine.begin() as conn:
        if program_code:
            rows = exec_query(conn, """
                SELECT b.branch_code, b.branch_name, b.active
                FROM branches b
                JOIN programs p ON p.id = b.program_id
                WHERE p.degree_code = :d AND p.program_code = :p AND b.active = 1
                ORDER BY b.sort_order, b.branch_code
            """, {"d": degree_code, "p": program_code}).fetchall()
        else:
            rows = exec_query(conn, """
                SELECT b.branch_code, b.branch_name, b.active
                FROM branches b
                LEFT JOIN programs p ON p.id = b.program_id
                WHERE (p.degree_code = :d OR b.degree_code = :d) AND b.active = 1
                ORDER BY b.sort_order, b.branch_code
            """, {"d": degree_code}).fetchall()
    return rows_to_dicts(rows)


@st.cache_data(ttl=300)
def fetch_curriculum_groups(_engine, degree_code: str):
    """Fetch curriculum groups for a degree."""
    with _engine.begin() as conn:
        rows = exec_query(conn, """
            SELECT group_code, group_name, kind, active
            FROM curriculum_groups
            WHERE degree_code = :d AND active = 1
            ORDER BY sort_order, group_code
        """, {"d": degree_code}).fetchall()
    return rows_to_dicts(rows)


@st.cache_data(ttl=300)
def fetch_academic_years(_engine):
    """Fetch academic years (planned + open; skip closed)."""
    with _engine.begin() as conn:
        rows = exec_query(conn, """
            SELECT ay_code, start_date, end_date, status
            FROM academic_years
            WHERE status IN ('planned', 'open')
            ORDER BY start_date DESC
        """).fetchall()
    return rows_to_dicts(rows)


# ============================================================================
# NEW/ENHANCED: PROPER SEMESTER STRUCTURE FETCHING
# ============================================================================

@st.cache_data(ttl=300)
def fetch_degree_semester_structure(_engine, degree_code: str) -> Optional[Dict[str, Any]]:
    """
    Fetch the actual semester structure for a degree.
    Returns binding mode, label mode, years, and terms_per_year.
    """
    with _engine.begin() as conn:
        # Get binding configuration
        binding = exec_query(conn, """
            SELECT binding_mode, label_mode
            FROM semester_binding
            WHERE degree_code = :d
        """, {"d": degree_code}).fetchone()
        
        if not binding:
            return None
        
        binding_mode = binding[0]
        label_mode = binding[1]
        
        # Get structure based on binding mode
        if binding_mode == 'degree':
            struct = exec_query(conn, """
                SELECT years, terms_per_year
                FROM degree_semester_struct
                WHERE degree_code = :d AND active = 1
            """, {"d": degree_code}).fetchone()
        elif binding_mode == 'program':
            # For program binding, we need to know which program - return None for now
            # Caller should use fetch_program_semester_structure instead
            return {
                "binding_mode": binding_mode,
                "label_mode": label_mode,
                "years": None,
                "terms_per_year": None,
                "requires_program": True
            }
        elif binding_mode == 'branch':
            # For branch binding, we need to know which branch - return None for now
            # Caller should use fetch_branch_semester_structure instead
            return {
                "binding_mode": binding_mode,
                "label_mode": label_mode,
                "years": None,
                "terms_per_year": None,
                "requires_branch": True
            }
        
        if not struct:
            return None
        
        return {
            "binding_mode": binding_mode,
            "label_mode": label_mode,
            "years": struct[0],
            "terms_per_year": struct[1]
        }


@st.cache_data(ttl=300)
def fetch_semesters_for_degree(_engine, degree_code: str, 
                               program_id: Optional[int] = None,
                               branch_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Fetch materialized semesters from semesters table for a degree.
    Respects program/branch if provided.
    """
    with _engine.begin() as conn:
        query = """
            SELECT 
                id,
                degree_code,
                program_id,
                branch_id,
                year_index,
                term_index,
                semester_number,
                label,
                active
            FROM semesters
            WHERE degree_code = :d AND active = 1
        """
        params = {"d": degree_code}
        
        if program_id is not None:
            query += " AND (program_id = :p OR program_id IS NULL)"
            params["p"] = program_id
        
        if branch_id is not None:
            query += " AND (branch_id = :b OR branch_id IS NULL)"
            params["b"] = branch_id
        
        query += " ORDER BY semester_number"
        
        rows = exec_query(conn, query, params).fetchall()
    
    return rows_to_dicts(rows)


# ============================================================================
# NEW: BATCH AND STUDENT STATUS CHECKING
# ============================================================================

@st.cache_data(ttl=60)
def check_batch_exists(_engine, degree_code: str, ay_label: str, year: int) -> Dict[str, Any]:
    """
    Check if batch exists for this degree, AY, and year.
    FIXED: 
    - Fallback for degrees without scaffold setup
    - Treats NULL enrollment_status as 'active' for backward compatibility
    """
    with _engine.begin() as conn:
        # Try Method 1: Find batch through scaffold system
        result = exec_query(conn, """
            SELECT 
                db.id as batch_id,
                db.batch_code,
                db.batch_name,
                COUNT(DISTINCT se.id) as student_count
            FROM degree_batches db
            JOIN batch_year_scaffold bys ON bys.batch_id = db.id
            LEFT JOIN student_enrollments se ON 
                se.degree_code = db.degree_code 
                AND se.batch = db.batch_code
                AND (se.current_year = :y OR se.current_year IS NULL)
                AND (se.enrollment_status = 'active' OR se.enrollment_status IS NULL)
            WHERE 
                db.degree_code = :d
                AND bys.ay_code = :ay
                AND bys.year_number = :y
                AND db.active = 1
            GROUP BY db.id, db.batch_code, db.batch_name
            LIMIT 1
        """, {"d": degree_code, "ay": ay_label, "y": year}).fetchone()
        
        if result:
            return {
                "exists": True,
                "batch_code": result[1],
                "batch_id": result[0],
                "batch_name": result[2],
                "student_count": result[3]
            }
        
        # Method 2: Fallback for degrees without scaffold - find latest active batch
        # This handles simple degrees like BARCH with no programs/branches
        fallback_result = exec_query(conn, """
            SELECT 
                db.id as batch_id,
                db.batch_code,
                db.batch_name,
                COUNT(DISTINCT se.id) as student_count
            FROM degree_batches db
            LEFT JOIN student_enrollments se ON 
                se.degree_code = db.degree_code 
                AND se.batch = db.batch_code
                AND (se.current_year = :y OR se.current_year IS NULL)
                AND (se.enrollment_status = 'active' OR se.enrollment_status IS NULL)
            WHERE 
                db.degree_code = :d
                AND db.active = 1
            GROUP BY db.id, db.batch_code, db.batch_name
            ORDER BY db.start_date DESC
            LIMIT 1
        """, {"d": degree_code, "y": year}).fetchone()
        
        if fallback_result:
            return {
                "exists": True,
                "batch_code": fallback_result[1],
                "batch_id": fallback_result[0],
                "batch_name": fallback_result[2],
                "student_count": fallback_result[3]
            }
        
        # No batch found at all
        return {
            "exists": False,
            "batch_code": None,
            "batch_id": None,
            "batch_name": None,
            "student_count": 0
        }


@st.cache_data(ttl=60)
def check_elective_topics_status(_engine, degree_code: str, subject_code: str, 
                                 ay_label: str, year: int, term: int) -> Dict[str, Any]:
    """
    Check if elective topics exist and are allocated for this subject.
    FIXED: Uses actual elective_topics schema and checks status='published'
    """
    with _engine.begin() as conn:
        # Check if elective_topics table exists
        table_check = exec_query(conn, """
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='elective_topics'
        """).fetchone()
        
        if not table_check:
            return {
                "topics_exist": False,
                "topic_count": 0,
                "allocation_complete": False,
                "note": "Elective topics table not found"
            }
        
        # FIXED: Query actual schema structure
        topic_result = exec_query(conn, """
            SELECT 
                COUNT(*) as topic_count,
                SUM(CASE WHEN status = 'published' THEN 1 ELSE 0 END) as published_count
            FROM elective_topics
            WHERE 
                subject_code = :sc
                AND degree_code = :d
                AND ay_label = :ay
                AND year = :y
                AND term = :t
        """, {"sc": subject_code, "d": degree_code, "ay": ay_label, "y": year, "t": term}).fetchone()
        
        if not topic_result or topic_result[0] == 0:
            return {
                "topics_exist": False,
                "topic_count": 0,
                "published_count": 0,
                "allocation_complete": False,
                "note": "No topics created yet"
            }
        
        topic_count = topic_result[0]
        published_count = topic_result[1]
        
        # Check if elective_student_selections table exists
        selections_table_check = exec_query(conn, """
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='elective_student_selections'
        """).fetchone()
        
        if not selections_table_check:
            return {
                "topics_exist": True,
                "topic_count": topic_count,
                "published_count": published_count,
                "allocation_complete": False,
                "note": f"{published_count}/{topic_count} published, selections table not found"
            }
        
        # Check allocations (confirmed selections)
        alloc_result = exec_query(conn, """
            SELECT 
                COUNT(DISTINCT et.id) as topics_with_selections,
                SUM(CASE WHEN ess.status = 'confirmed' THEN 1 ELSE 0 END) as confirmed_count
            FROM elective_topics et
            LEFT JOIN elective_student_selections ess 
                ON ess.subject_code = et.subject_code
                AND ess.ay_label = et.ay_label
                AND ess.year = et.year
                AND ess.term = et.term
                AND ess.topic_code_ay = et.topic_code_ay
            WHERE 
                et.subject_code = :sc
                AND et.degree_code = :d
                AND et.ay_label = :ay
                AND et.year = :y
                AND et.term = :t
        """, {"sc": subject_code, "d": degree_code, "ay": ay_label, "y": year, "t": term}).fetchone()
        
        topics_allocated = alloc_result[0] if alloc_result else 0
        confirmed_count = alloc_result[1] if alloc_result else 0
        allocation_complete = topics_allocated > 0 and confirmed_count > 0
        
        return {
            "topics_exist": True,
            "topic_count": topic_count,
            "published_count": published_count,
            "topics_with_selections": topics_allocated,
            "confirmed_count": confirmed_count,
            "allocation_complete": allocation_complete,
            "note": f"{published_count}/{topic_count} published, {confirmed_count} students allocated"
        }


def fetch_catalog_subjects_with_status(
    conn, 
    degree_code: str, 
    ay_label: str,
    year: int,
    term: int,
    program_code: Optional[str] = None,
    branch_code: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Fetch subjects from catalog WITH status indicators.
    FIXED: Proper error handling and None checks
    """
    
    # Get all catalog subjects for this scope
    query = """
        SELECT 
            sc.id,
            sc.subject_code,
            sc.subject_name,
            sc.subject_type,
            sc.credits_total,
            sc.L, sc.T, sc.P, sc.S,
            sc.degree_code,
            sc.program_code,
            sc.branch_code,
            sc.curriculum_group_code,
            sc.semester_id
        FROM subjects_catalog sc
        WHERE sc.degree_code = :d 
        AND sc.active = 1
    """
    params = {"d": degree_code}
    
    if program_code:
        query += " AND (sc.program_code = :p OR sc.program_code IS NULL)"
        params["p"] = program_code
    
    if branch_code:
        query += " AND (sc.branch_code = :b OR sc.branch_code IS NULL)"
        params["b"] = branch_code
    
    # Filter by semester
    query += """
        AND (
            sc.semester_id IS NULL 
            OR EXISTS (
                SELECT 1 FROM semesters s 
                WHERE s.id = sc.semester_id 
                AND s.year_index = :y 
                AND s.term_index = :t
            )
        )
    """
    params["y"] = year
    params["t"] = term
    
    query += " ORDER BY sc.subject_type, sc.subject_code"
    
    try:
        rows = exec_query(conn, query, params).fetchall()
        subjects = rows_to_dicts(rows)
    except Exception as e:
        # Log error and return empty list
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error fetching catalog subjects: {e}")
        return []
    
    # FIXED: Get engine from the connection's engine attribute
    # conn is a SQLAlchemy Connection object from engine.begin()
    from sqlalchemy import create_engine
    engine = conn.engine
    
    # Enrich each subject with status
    for subj in subjects:
        subject_code = subj.get("subject_code")
        subject_type = subj.get("subject_type")
        
        # Skip if essential data is missing
        if not subject_code or not subject_type:
            continue
        
        try:
            # Check if offering already exists
            offering_check = exec_query(conn, """
                SELECT id FROM subject_offerings
                WHERE 
                    subject_code = :sc
                    AND degree_code = :d
                    AND ay_label = :ay
                    AND year = :y
                    AND term = :t
            """, {
                "sc": subject_code,
                "d": degree_code,
                "ay": ay_label,
                "y": year,
                "t": term
            }).fetchone()
            
            subj["offering_exists"] = offering_check is not None
            subj["offering_id"] = offering_check[0] if offering_check else None
            
            # Check batch status
            batch_status = check_batch_exists(engine, degree_code, ay_label, year)
            subj["batch_status"] = batch_status
            
            # FIXED: Safe elective check with None handling
            is_elective = False
            if subject_type is not None:
                is_elective = subject_type.strip().lower() == "elective"
            
            if is_elective:
                elective_status = check_elective_topics_status(
                    engine, degree_code, subject_code, ay_label, year, term
                )
                subj["elective_status"] = elective_status
            else:
                subj["elective_status"] = None
                
        except Exception as e:
            # Log error but continue with next subject
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error enriching subject {subject_code}: {e}")
            
            # Set safe defaults
            subj["offering_exists"] = False
            subj["offering_id"] = None
            subj["batch_status"] = {
                "exists": False,
                "batch_code": None,
                "batch_id": None,
                "batch_name": None,
                "student_count": 0
            }
            subj["elective_status"] = None
    
    return subjects


# ============================================================================
# EXISTING FUNCTIONS (kept for compatibility)
# ============================================================================

@st.cache_data(ttl=300)
def fetch_divisions(_engine, degree_code: str, ay_label: str, year: int):
    """
    Fetch divisions for a specific degree, AY, and year
    by joining through the batch/scaffold system.
    """
    with _engine.begin() as conn:
        rows = exec_query(conn, """
            SELECT
                dm.division_code,
                dm.division_name,
                dm.active
            FROM division_master dm
            JOIN degree_batches db
                ON db.degree_code = dm.degree_code
                AND db.batch_code = dm.batch
            JOIN batch_year_scaffold bys
                ON bys.batch_id = db.id
            WHERE
                dm.degree_code = :d
                AND bys.ay_code = :ay
                AND dm.current_year = :y
                AND dm.active = 1
            ORDER BY
                dm.division_code
        """, {"d": degree_code, "ay": ay_label, "y": year}).fetchall()
    return rows_to_dicts(rows)


def fetch_catalog_subjects(conn, degree_code: str, program_code: Optional[str] = None, 
                          branch_code: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch subjects from catalog for creating offerings (LEGACY - use fetch_catalog_subjects_with_status instead)."""
    query = """
        SELECT * FROM subjects_catalog 
        WHERE degree_code = :d AND active = 1
    """
    params = {"d": degree_code}

    if program_code:
        query += " AND (program_code = :p OR program_code IS NULL)"
        params["p"] = program_code

    if branch_code:
        query += " AND (branch_code = :b OR branch_code IS NULL)"
        params["b"] = branch_code

    query += " ORDER BY subject_code"

    rows = exec_query(conn, query, params).fetchall()
    return rows_to_dicts(rows)


def fetch_offerings(conn, degree_code: str, ay_label: str, year: int, term: int,
                   program_code: Optional[str] = None, branch_code: Optional[str] = None,
                   division_code: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch offerings for specific scope."""
    query = """
        SELECT o.*, sc.subject_name, sc.subject_type
        FROM subject_offerings o
        LEFT JOIN subjects_catalog sc ON sc.subject_code = o.subject_code 
            AND sc.degree_code = o.degree_code
        WHERE o.degree_code = :d AND o.ay_label = :ay AND o.year = :y AND o.term = :t
    """
    params = {"d": degree_code, "ay": ay_label, "y": year, "t": term}

    if program_code:
        query += " AND (o.program_code = :p OR o.program_code IS NULL)"
        params["p"] = program_code

    if branch_code:
        query += " AND (o.branch_code = :b OR o.branch_code IS NULL)"
        params["b"] = branch_code

    if division_code:
        query += " AND (o.division_code = :div OR o.applies_to_all_divisions = 1)"
        params["div"] = division_code

    query += " ORDER BY sc.subject_type, o.subject_code"

    rows = exec_query(conn, query, params).fetchall()
    return rows_to_dicts(rows)


@st.cache_data(ttl=300)
def fetch_catalog_subject_details(
    _engine, subject_code: str, degree_code: str
) -> Optional[Dict[str, Any]]:
    """Fetch a single subject's details from the catalog."""
    with _engine.begin() as conn:
        row = exec_query(conn, """
            SELECT 
                credits_total, L, T, P, S,
                internal_marks_max, exam_marks_max, jury_viva_marks_max,
                min_overall_percent, min_internal_percent, min_external_percent,
                overall_direct_weight_percent, overall_indirect_weight_percent
            FROM subjects_catalog 
            WHERE subject_code = :sc AND degree_code = :dc AND active = 1
            LIMIT 1
        """, {"sc": subject_code, "dc": degree_code}).fetchone()
    
    return dict(row._mapping) if row else None
