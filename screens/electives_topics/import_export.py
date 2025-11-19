# screens/electives_topics/import_export.py
"""
Import/Export handlers for electives module.
Supports CSV import/export for topics and selections.
"""

from __future__ import annotations

import io
import csv
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


# ===========================================================================
# DATABASE HELPERS
# ===========================================================================


def _exec(conn, sql: str, params: dict = None):
    """Execute SQL with parameters."""
    return conn.execute(sa_text(sql), params or {})


def _fetch_one(engine: Engine, sql: str, params: dict = None) -> Optional[Dict]:
    """Fetch single row."""
    with engine.begin() as conn:
        result = _exec(conn, sql, params).fetchone()
        return dict(result._mapping) if result else None
        
def _fetch_one_from_conn(conn, sql: str, params: dict = None) -> Optional[Dict]:
    """Fetch single row using existing connection."""
    result = _exec(conn, sql, params).fetchone()
    return dict(result._mapping) if result else None


def _fetch_all(engine: Engine, sql: str, params: dict = None) -> List[Dict]:
    """Fetch all rows."""
    with engine.begin() as conn:
        results = _exec(conn, sql, params).fetchall()
        return [dict(r._mapping) for r in results]


# ===========================================================================
# TOPIC IMPORT
# ===========================================================================


def get_topics_import_template() -> pd.DataFrame:
    """Get template DataFrame for topic import."""
    return pd.DataFrame(
        {
            "topic_no": [1, 2, 3],
            "topic_name": [
                "Machine Learning Fundamentals",
                "Deep Learning Applications",
                "Natural Language Processing",
            ],
            "capacity": [30, 25, 35],
            "owner_faculty_email": [
                "prof1@example.com",
                "prof2@example.com",
                "prof3@example.com",
            ],
            "description": [
                "Introduction to ML algorithms and applications",
                "Advanced deep learning techniques",
                "NLP fundamentals and modern approaches",
            ],
            "prerequisites": [
                "Data Structures, Python",
                "Machine Learning Fundamentals",
                "Machine Learning Fundamentals",
            ],
            "learning_outcomes": [
                "Understand ML concepts; Apply algorithms; Evaluate models",
                "Build neural networks; Train deep models; Deploy solutions",
                "Process text data; Build NLP models; Understand transformers",
            ],
        }
    )


def validate_topics_csv(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate topics CSV data.

    Returns:
        (is_valid, list_of_errors)
    """
    errors: List[str] = []

    # Check required columns
    required_cols = ["topic_no", "topic_name"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        errors.append(f"Missing required columns: {', '.join(missing_cols)}")
        return False, errors

    # Check for empty required fields
    for idx, row in df.iterrows():
        if pd.isna(row["topic_no"]) or row["topic_no"] == "":
            errors.append(f"Row {idx + 2}: topic_no is empty")

        if pd.isna(row["topic_name"]) or str(row["topic_name"]).strip() == "":
            errors.append(f"Row {idx + 2}: topic_name is empty")

        # Validate topic_no is positive integer
        try:
            topic_no = int(row["topic_no"])
            if topic_no <= 0:
                errors.append(f"Row {idx + 2}: topic_no must be positive")
        except (ValueError, TypeError):
            errors.append(f"Row {idx + 2}: topic_no must be a number")

        # Validate capacity if provided
        if "capacity" in df.columns and not pd.isna(row["capacity"]):
            try:
                cap = int(row["capacity"])
                if cap < 0:
                    errors.append(f"Row {idx + 2}: capacity must be non-negative")
            except (ValueError, TypeError):
                errors.append(f"Row {idx + 2}: capacity must be a number")

    # Check for duplicate topic_no
    duplicates = df[df.duplicated(subset=["topic_no"], keep=False)]
    if not duplicates.empty:
        dup_nos = duplicates["topic_no"].unique()
        errors.append(
            f"Duplicate topic numbers found: {', '.join(map(str, dup_nos))}"
        )

    return len(errors) == 0, errors


def import_topics_from_csv(
    engine: Engine,
    df: pd.DataFrame,
    subject_code: str,
    degree_code: str,
    program_code: str,
    branch_code: str,
    ay_label: str,
    year: int,
    term: int,
    actor: str,
    overwrite_existing: bool = False,
) -> Tuple[int, int, List[str]]:
    """
    Import topics from CSV DataFrame.

    Returns:
        (success_count, error_count, error_messages)
    """

    # Get subject offering details for this exact context,
    # and ensure it has a syllabus (syllabus_template_id not null).
    subject = _fetch_one(
        engine,
        """
        SELECT subject_name, syllabus_template_id
        FROM subject_offerings
        WHERE subject_code = :subj
          AND degree_code  = :deg
          AND ay_label     = :ay
          AND year         = :yr
          AND term         = :trm
        """,
        {
            "subj": subject_code,
            "deg": degree_code,
            "ay": ay_label,
            "yr": year,
            "trm": term,
        },
    )

    if not subject:
        return (
            0,
            0,
            [
                f"Subject {subject_code} not found for "
                f"{ay_label}, Year {year}, Term {term}."
            ],
        )

    if not subject.get("syllabus_template_id"):
        return (
            0,
            0,
            [
                f"Subject {subject_code} in {ay_label}, Year {year}, Term {term} "
                "does not have a syllabus yet. Please create the main syllabus in "
                "the Subjects Syllabus module before importing topics."
            ],
        )

    success_count = 0
    error_count = 0
    errors: List[str] = []

    with engine.begin() as conn:
        for idx, row in df.iterrows():
            try:
                topic_no = int(row["topic_no"])
                topic_name = str(row["topic_name"]).strip()
                capacity = (
                    int(row.get("capacity", 0))
                    if not pd.isna(row.get("capacity"))
                    else 0
                )
                owner_email = (
                    str(row.get("owner_faculty_email", "")).strip()
                    if not pd.isna(row.get("owner_faculty_email"))
                    else None
                )
                description = (
                    str(row.get("description", "")).strip()
                    if not pd.isna(row.get("description"))
                    else None
                )
                
                # --- NEW: Look up faculty ID from email ---
                owner_id = None
                if owner_email:
                    faculty_profile = _fetch_one_from_conn(
                        conn, 
                        "SELECT id FROM faculty_profiles WHERE lower(email) = lower(:email)",
                        {"email": owner_email}
                    )
                    if faculty_profile:
                        owner_id = faculty_profile['id']
                    else:
                        errors.append(f"Row {idx + 2}: Faculty email '{owner_email}' not found in faculty_profiles. Storing email but not ID.")
                # --- End NEW ---

                # Parse prerequisites (comma-separated or JSON)
                prerequisites = None
                if "prerequisites" in df.columns and not pd.isna(
                    row["prerequisites"]
                ):
                    prereq_str = str(row["prerequisites"]).strip()
                    if prereq_str:
                        try:
                            prerequisites = (
                                json.loads(prereq_str)
                                if prereq_str.startswith("[")
                                else prereq_str.split(",")
                            )
                            prerequisites = json.dumps(
                                [p.strip() for p in prerequisites]
                            )
                        except Exception:
                            prerequisites = json.dumps(
                                [p.strip() for p in prereq_str.split(",")]
                            )

                # Parse learning outcomes (semicolon-separated or JSON)
                learning_outcomes = None
                if "learning_outcomes" in df.columns and not pd.isna(
                    row["learning_outcomes"]
                ):
                    lo_str = str(row["learning_outcomes"]).strip()
                    if lo_str:
                        try:
                            learning_outcomes = (
                                json.loads(lo_str)
                                if lo_str.startswith("[")
                                else lo_str.split(";")
                            )
                            learning_outcomes = json.dumps(
                                [lo.strip() for lo in learning_outcomes]
                            )
                        except Exception:
                            learning_outcomes = json.dumps(
                                [lo.strip() for lo in lo_str.split(";")]
                            )

                # Generate topic_code_ay
                topic_code_ay = f"{subject_code}-{topic_no}"

                # Check if exists
                # Use _fetch_one_from_conn as we are inside a transaction
                existing = _fetch_one_from_conn(
                    conn,
                    """
                    SELECT id 
                    FROM elective_topics
                    WHERE subject_code = :subj
                      AND ay_label     = :ay
                      AND topic_no     = :no
                    """,
                    {
                        "subj": subject_code,
                        "ay": ay_label,
                        "no": topic_no,
                    },
                )

                if existing and not overwrite_existing:
                    errors.append(
                        f"Row {idx + 2}: Topic #{topic_no} already exists "
                        "(use overwrite option)"
                    )
                    error_count += 1
                    continue

                if existing and overwrite_existing:
                    # Update existing
                    _exec(
                        conn,
                        """
                        UPDATE elective_topics
                        SET topic_name          = :name,
                            capacity            = :cap,
                            owner_faculty_id    = :owner_id,
                            owner_faculty_email = :owner_email,
                            description         = :desc,
                            prerequisites       = :prereq,
                            learning_outcomes   = :lo,
                            updated_at          = :now,
                            last_updated_by     = :actor
                        WHERE id = :id
                        """,
                        {
                            "id": existing["id"],
                            "name": topic_name,
                            "cap": capacity,
                            "owner_id": owner_id,
                            "owner_email": owner_email,
                            "desc": description,
                            "prereq": prerequisites,
                            "lo": learning_outcomes,
                            "now": datetime.now(),
                            "actor": actor,
                        },
                    )
                else:
                    # Insert new
                    _exec(
                        conn,
                        """
                        INSERT INTO elective_topics (
                            subject_code, subject_name, degree_code, program_code, branch_code,
                            ay_label, year, term,
                            topic_no, topic_code_ay, topic_name,
                            capacity, owner_faculty_id, owner_faculty_email, description,
                            prerequisites, learning_outcomes,
                            status, last_updated_by
                        ) VALUES (
                            :subj, :subj_name, :deg, :prog, :br,
                            :ay, :yr, :trm,
                            :topic_no, :topic_code, :topic_name,
                            :cap, :owner_id, :owner_email, :desc,
                            :prereq, :lo,
                            'draft', :actor
                        )
                        """,
                        {
                            "subj": subject_code,
                            "subj_name": subject["subject_name"],
                            "deg": degree_code,
                            "prog": program_code,
                            "br": branch_code,
                            "ay": ay_label,
                            "yr": year,
                            "trm": term,
                            "topic_no": topic_no,
                            "topic_code": topic_code_ay,
                            "topic_name": topic_name,
                            "cap": capacity,
                            "owner_id": owner_id,
                            "owner_email": owner_email,
                            "desc": description,
                            "prereq": prerequisites,
                            "lo": learning_outcomes,
                            "actor": actor,
                        },
                    )

                success_count += 1
                logger.info(f"Imported topic #{topic_no}: {topic_name}")

            except Exception as e:  # noqa: BLE001
                error_count += 1
                errors.append(f"Row {idx + 2}: {str(e)}")
                logger.error(f"Error importing row {idx + 2}: {e}")

    return success_count, error_count, errors

# ===========================================================================
# TOPIC EXPORT
# ===========================================================================

def export_topics_to_csv(engine: Engine, subject_code: str, ay_label: str,
                        year: int, term: int, degree_code: str,
                        include_stats: bool = True) -> pd.DataFrame:
    """Export topics to DataFrame."""
    
    query = """
        SELECT 
            t.topic_no,
            t.topic_code_ay,
            t.topic_name,
            t.capacity,
            t.owner_faculty_email,
            t.description,
            t.prerequisites,
            t.learning_outcomes,
            t.status
    """
    
    if include_stats:
        query += """,
            COALESCE(c.confirmed_count, 0) AS confirmed_count,
            COALESCE(c.waitlisted_count, 0) AS waitlisted_count,
            COALESCE(c.remaining_capacity, t.capacity) AS remaining_capacity
        FROM elective_topics t
        LEFT JOIN elective_capacity_tracking c 
            ON c.topic_code_ay = t.topic_code_ay 
            AND c.ay_label = t.ay_label
        """
    else:
        query += " FROM elective_topics t"
    
    query += """
        WHERE t.subject_code = :subj
        AND t.ay_label = :ay
        AND t.year = :yr
        AND t.term = :trm
        AND t.degree_code = :deg
        ORDER BY t.topic_no
    """
    
    topics = _fetch_all(engine, query, {
        "subj": subject_code,
        "ay": ay_label,
        "yr": year,
        "trm": term,
        "deg": degree_code
    })
    
    # Parse JSON fields for readability
    for topic in topics:
        if topic.get('prerequisites'):
            try:
                prereqs = json.loads(topic['prerequisites'])
                topic['prerequisites'] = ', '.join(prereqs) if isinstance(prereqs, list) else prereqs
            except:
                pass
        
        if topic.get('learning_outcomes'):
            try:
                los = json.loads(topic['learning_outcomes'])
                topic['learning_outcomes'] = '; '.join(los) if isinstance(los, list) else los
            except:
                pass
    
    return pd.DataFrame(topics)

# ===========================================================================
# ROSTER EXPORT (FIXED - Merged duplicated functions)
# ===========================================================================

def export_topic_rosters_to_csv(
    engine: Engine,
    subject_code: str,
    ay_label: str,
    year: int,
    term: int,
    degree_code: str,
    status_filter: str = "confirmed",
) -> pd.DataFrame:
    """
    Export per-topic rosters for a given elective subject, including faculty info.

    Each row = one student assigned to one topic.
    Intended to be turned into a CSV and given directly to faculty.
    """

    query = """
        SELECT
            -- Subject/Context
            s.subject_code,
            s.ay_label,
            s.year,
            s.term,

            -- Topic info
            t.topic_no,
            t.topic_code_ay,
            t.topic_name,
            t.capacity AS topic_capacity,
            t.offering_id,

            -- Faculty info (from elective_topics + faculty_profiles)
            t.owner_faculty_email,
            fp.name         AS faculty_name,
            fp.employee_id  AS faculty_employee_id,

            -- Student info
            s.student_roll_no,
            s.student_name,
            s.student_email,
            s.degree_code,
            s.program_code,
            s.branch_code,
            s.division_code,
            s.batch,
            s.rank_choice,
            s.status,
            s.confirmed_at

        FROM elective_student_selections s
        JOIN elective_topics t
            ON s.topic_code_ay = t.topic_code_ay
            AND s.ay_label     = t.ay_label

        LEFT JOIN faculty_profiles fp
            ON lower(fp.email) = lower(t.owner_faculty_email)

        WHERE s.subject_code = :subj
          AND s.ay_label     = :ay
          AND s.year         = :yr
          AND s.term         = :trm
          AND s.degree_code  = :deg
    """

    params = {
        "subj": subject_code,
        "ay": ay_label,
        "yr": year,
        "trm": term,
        "deg": degree_code,
    }
    
    if status_filter:
        query += " AND s.status = :status"
        params["status"] = status_filter

    query += " ORDER BY t.topic_no, s.student_roll_no"

    rows = _fetch_all(engine, query, params)
    return pd.DataFrame(rows)


def export_topic_rosters_confirmed(
    engine: Engine,
    subject_code: str,
    ay_label: str,
    year: int,
    term: int,
    degree_code: str,
) -> pd.DataFrame:
    """
    Convenience wrapper for per-topic rosters (confirmed only).
    """
    return export_topic_rosters_to_csv(
        engine=engine,
        subject_code=subject_code,
        ay_label=ay_label,
        year=year,
        term=term,
        degree_code=degree_code,
        status_filter="confirmed",
    )


# ===========================================================================
# SELECTIONS IMPORT
# ===========================================================================

def get_selections_import_template() -> pd.DataFrame:
    """Get template DataFrame for selections import."""
    return pd.DataFrame({
        'student_roll_no': ['2021001', '2021002', '2021003'],
        'student_name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
        'topic_code_ay': ['CS-ELECT-1', 'CS-ELECT-2', 'CS-ELECT-1'],
        'rank_choice': [1, 1, 2],
        'status': ['draft', 'draft', 'draft']
    })


def validate_selections_csv(df: pd.DataFrame, engine: Engine,
                           subject_code: str, ay_label: str) -> Tuple[bool, List[str]]:
    """Validate selections CSV data."""
    errors = []
    
    # Check required columns
    required_cols = ['student_roll_no', 'topic_code_ay']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        errors.append(f"Missing required columns: {', '.join(missing_cols)}")
        return False, errors
    
    # Get valid topics
    topics = _fetch_all(engine, """
        SELECT topic_code_ay FROM elective_topics
        WHERE subject_code = :subj AND ay_label = :ay AND status = 'published'
    """, {"subj": subject_code, "ay": ay_label})
    valid_topic_codes = set(t['topic_code_ay'] for t in topics)
    
    # Validate each row
    for idx, row in df.iterrows():
        # Check student roll number
        if pd.isna(row['student_roll_no']) or str(row['student_roll_no']).strip() == '':
            errors.append(f"Row {idx + 2}: student_roll_no is empty")
        
        # Check topic code
        if pd.isna(row['topic_code_ay']) or str(row['topic_code_ay']).strip() == '':
            errors.append(f"Row {idx + 2}: topic_code_ay is empty")
        elif row['topic_code_ay'] not in valid_topic_codes:
            errors.append(f"Row {idx + 2}: topic_code_ay '{row['topic_code_ay']}' not found")
        
        # Validate rank if provided
        if 'rank_choice' in df.columns and not pd.isna(row['rank_choice']):
            try:
                rank = int(row['rank_choice'])
                if rank < 1 or rank > 10:
                    errors.append(f"Row {idx + 2}: rank_choice must be between 1 and 10")
            except (ValueError, TypeError):
                errors.append(f"Row {idx + 2}: rank_choice must be a number")
        
        # Validate status if provided
        if 'status' in df.columns and not pd.isna(row['status']):
            if row['status'] not in ['draft', 'confirmed', 'waitlisted', 'withdrawn']:
                errors.append(f"Row {idx + 2}: invalid status '{row['status']}'")
    
    return len(errors) == 0, errors


def import_selections_from_csv(engine: Engine, df: pd.DataFrame,
                              subject_code: str, degree_code: str,
                              ay_label: str, year: int, term: int,
                              actor: str) -> Tuple[int, int, List[str]]:
    """Import student selections from CSV."""
    
    success_count = 0
    error_count = 0
    errors = []
    
    with engine.begin() as conn:
        for idx, row in df.iterrows():
            try:
                student_roll_no = str(row['student_roll_no']).strip()
                topic_code_ay = str(row['topic_code_ay']).strip()
                rank_choice = int(row.get('rank_choice', 1)) if not pd.isna(row.get('rank_choice')) else 1
                status = str(row.get('status', 'draft')).strip()
                student_name = str(row.get('student_name', student_roll_no)).strip()
                
                # Get topic details
                topic = _fetch_one_from_conn(conn, """
                    SELECT topic_name FROM elective_topics
                    WHERE topic_code_ay = :code AND ay_label = :ay
                """, {"code": topic_code_ay, "ay": ay_label})
                
                if not topic:
                    errors.append(f"Row {idx + 2}: Topic {topic_code_ay} not found")
                    error_count += 1
                    continue
                
                # Get student details if available
                student = _fetch_one_from_conn(conn, """
                    SELECT 
                        sp.id, sp.email,
                        se.program_code, se.branch_code, se.batch, se.division_code
                    FROM student_profiles sp
                    JOIN student_enrollments se ON se.student_profile_id = sp.id
                    WHERE sp.student_id = :roll
                    AND se.enrollment_status = 'active'
                """, {"roll": student_roll_no})
                
                # Insert or update
                _exec(conn, """
                    INSERT INTO elective_student_selections (
                        student_id, student_roll_no, student_name, student_email,
                        degree_code, program_code, branch_code,
                        ay_label, year, term, division_code, batch,
                        subject_code, topic_code_ay, topic_name,
                        rank_choice, selection_strategy, status,
                        selected_at, last_updated_by
                    ) VALUES (
                        :sid, :roll, :name, :email,
                        :deg, :prog, :br,
                        :ay, :yr, :trm, :div, :batch,
                        :subj, :topic_code, :topic_name,
                        :rank, 'manual_assign', :status,
                        :now, :actor
                    )
                    ON CONFLICT(student_roll_no, subject_code, ay_label, year, term)
                    DO UPDATE SET
                        topic_code_ay = excluded.topic_code_ay,
                        topic_name = excluded.topic_name,
                        rank_choice = excluded.rank_choice,
                        status = excluded.status,
                        updated_at = excluded.selected_at,
                        last_updated_by = excluded.last_updated_by
                """, {
                    "sid": student['id'] if student else None,
                    "roll": student_roll_no,
                    "name": student_name,
                    "email": student['email'] if student else None,
                    "deg": degree_code,
                    "prog": student['program_code'] if student else None,
                    "br": student['branch_code'] if student else None,
                    "ay": ay_label,
                    "yr": year,
                    "trm": term,
                    "div": student['division_code'] if student else None,
                    "batch": student['batch'] if student else None,
                    "subj": subject_code,
                    "topic_code": topic_code_ay,
                    "topic_name": topic['topic_name'],
                    "rank": rank_choice,
                    "status": status,
                    "now": datetime.now(),
                    "actor": actor
                })
                
                success_count += 1
                logger.info(f"Imported selection for {student_roll_no}")
                
            except Exception as e:
                error_count += 1
                errors.append(f"Row {idx + 2}: {str(e)}")
                logger.error(f"Error importing row {idx + 2}: {e}")
    
    return success_count, error_count, errors


# ===========================================================================
# SELECTIONS EXPORT
# ===========================================================================

def export_selections_to_csv(engine: Engine, subject_code: str, ay_label: str,
                            year: int, term: int, degree_code: str,
                            status_filter: str = None) -> pd.DataFrame:
    """Export student selections to DataFrame."""
    
    query = """
        SELECT 
            s.student_roll_no,
            s.student_name,
            s.student_email,
            s.degree_code,
            s.program_code,
            s.branch_code,
            s.division_code,
            s.batch,
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
            fp.name AS faculty_name,
            t.capacity AS topic_capacity
        FROM elective_student_selections s
        LEFT JOIN elective_topics t 
            ON t.topic_code_ay = s.topic_code_ay 
            AND t.ay_label = s.ay_label
        LEFT JOIN faculty_profiles fp
            ON lower(fp.email) = lower(t.owner_faculty_email)
        WHERE s.subject_code = :subj
        AND s.ay_label = :ay
        AND s.year = :yr
        AND s.term = :trm
        AND s.degree_code = :deg
    """
    
    params = {
        "subj": subject_code,
        "ay": ay_label,
        "yr": year,
        "trm": term,
        "deg": degree_code
    }
    
    if status_filter:
        query += " AND s.status = :status"
        params["status"] = status_filter
    
    query += " ORDER BY s.topic_code_ay, s.student_roll_no"
    
    selections = _fetch_all(engine, query, params)
    
    return pd.DataFrame(selections)


def export_confirmed_assignments(engine: Engine, subject_code: str, ay_label: str) -> pd.DataFrame:
    """Export only confirmed assignments (for final reporting)."""
    # Note: This function is simplified and might not be used if the one above
    # is called directly with status_filter.
    # We'll assume the one above is the primary export.
    return export_selections_to_csv(
        engine, subject_code, ay_label, 
        None, None, None,  # Assuming year, term, degree might be wildcarded
        status_filter='confirmed'
    )


# ===========================================================================
# BULK OPERATIONS
# ===========================================================================

def bulk_confirm_selections(engine: Engine, selection_ids: List[int],
                           actor: str) -> Tuple[int, List[str]]:
    """Bulk confirm selections by ID."""
    
    success_count = 0
    errors = []
    
    with engine.begin() as conn:
        for sel_id in selection_ids:
            try:
                _exec(conn, """
                    UPDATE elective_student_selections
                    SET status = 'confirmed',
                        confirmed_at = :now,
                        confirmed_by = :actor,
                        updated_at = :now
                    WHERE id = :id
                    AND status = 'draft'
                """, {
                    "id": sel_id,
                    "now": datetime.now(),
                    "actor": actor
                })
                
                success_count += 1
                
            except Exception as e:
                errors.append(f"Selection {sel_id}: {str(e)}")
    
    return success_count, errors


def bulk_delete_topics(engine: Engine, topic_ids: List[int],
                      force: bool = False) -> Tuple[int, List[str]]:
    """Bulk delete topics."""
    
    success_count = 0
    errors = []
    
    with engine.begin() as conn:
        for topic_id in topic_ids:
            try:
                # Check if has assigned students
                if not force:
                    has_students = _fetch_one_from_conn(conn, """
                        SELECT COUNT(*) AS cnt FROM elective_student_selections s
                        JOIN elective_topics t ON t.topic_code_ay = s.topic_code_ay
                        WHERE t.id = :id
                        AND s.status IN ('confirmed', 'waitlisted')
                    """, {"id": topic_id})
                    
                    if has_students and has_students['cnt'] > 0:
                        errors.append(f"Topic {topic_id}: Has assigned students")
                        continue
                
                _exec(conn, "DELETE FROM elective_topics WHERE id = :id", {"id": topic_id})
                success_count += 1
                
            except Exception as e:
                errors.append(f"Topic {topic_id}: {str(e)}")
    
    return success_count, errors
