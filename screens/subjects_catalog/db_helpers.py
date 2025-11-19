"""
Database helper functions - Fetch operations with caching
"""

from typing import Optional, List, Dict, Any
import streamlit as st
from sqlalchemy import text as sa_text
from screens.subjects_syllabus.helpers import exec_query, rows_to_dicts


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
def fetch_curriculum_groups(
    _engine,
    degree_code: str,
    program_code: Optional[str] = None,
    branch_code: Optional[str] = None,
):
    """Fetch curriculum groups for a degree."""
    with _engine.begin() as conn:
        rows = exec_query(conn, """
            SELECT group_code, group_name, kind, active
            FROM curriculum_groups
            WHERE degree_code = :d
            AND active = 1
            ORDER BY sort_order, group_code
        """, {"d": degree_code}).fetchall()
    return rows_to_dicts(rows)


@st.cache_data(ttl=300)
def fetch_academic_years(_engine):
    """Fetch academic years (planned + open; skip closed)."""
    with _engine.begin() as conn:
        rows = exec_query(conn, """
            SELECT ay_code
            FROM academic_years
            WHERE status IN ('planned', 'open')
            ORDER BY start_date DESC
        """).fetchall()
    return [r[0] for r in rows]


def fetch_subjects(
    conn,
    degree_code: str,
    program_code: Optional[str] = None,
    branch_code: Optional[str] = None,
    curriculum_group_code: Optional[str] = None,
    active_only: bool = True,
) -> List[Dict[str, Any]]:
    """Fetch subjects for a given scope."""
    query = "SELECT * FROM subjects_catalog WHERE degree_code = :d"
    params = {"d": degree_code}

    if program_code:
        query += " AND (program_code = :p OR program_code IS NULL)"
        params["p"] = program_code

    if branch_code:
        query += " AND (branch_code = :b OR branch_code IS NULL)"
        params["b"] = branch_code

    if curriculum_group_code:
        query += " AND curriculum_group_code = :cg"
        params["cg"] = curriculum_group_code

    if active_only:
        query += " AND active = 1"

    query += " ORDER BY sort_order, subject_code"

    rows = exec_query(conn, query, params).fetchall()
    return rows_to_dicts(rows)
