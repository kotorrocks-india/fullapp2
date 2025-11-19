# screens/class_in_charge/cic_filters.py
"""
Data fetching logic for Class-in-Charge module.
Centralizes cohort selection and enforces faculty affiliation rules.
"""

from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

# ... (Keep fetch_academic_years, fetch_degrees, fetch_programs, fetch_branches, fetch_semester_structure AS IS) ...
# ... (I will reprint them briefly for context, but the main change is in fetch_faculty_for_degree) ...

def fetch_academic_years(engine: Engine) -> List[str]:
    """Fetch all academic years."""
    with engine.begin() as conn:
        result = conn.execute(sa_text("SELECT ay_code FROM academic_years ORDER BY ay_code DESC"))
        return [row.ay_code for row in result]

def fetch_degrees(engine: Engine) -> List[Dict]:
    """Fetch all active degrees."""
    with engine.begin() as conn:
        result = conn.execute(sa_text("SELECT code, title, sort_order FROM degrees WHERE active = 1 ORDER BY sort_order, code"))
        return [dict(row._mapping) for row in result]

def fetch_programs_by_degree(engine: Engine, degree_code: str) -> List[Dict]:
    """Fetch programs for a specific degree."""
    with engine.begin() as conn:
        result = conn.execute(sa_text("""
            SELECT program_code AS code, program_name AS name, id
            FROM programs WHERE degree_code = :degree_code AND active = 1 ORDER BY sort_order, program_code
        """), {"degree_code": degree_code})
        return [dict(row._mapping) for row in result]

def fetch_branches_by_program(engine: Engine, degree_code: str, program_id: Optional[int]) -> List[Dict]:
    """Fetch branches for a specific program."""
    if not program_id: return []
    with engine.begin() as conn:
        result = conn.execute(sa_text("""
            SELECT branch_code AS code, branch_name AS name, id
            FROM branches WHERE degree_code = :degree_code AND program_id = :program_id AND active = 1 ORDER BY sort_order, branch_code
        """), {"degree_code": degree_code, "program_id": program_id})
        return [dict(row._mapping) for row in result]

def fetch_semester_structure(engine: Engine, degree_code: str, 
                             program_id: Optional[int] = None, 
                             branch_id: Optional[int] = None) -> Dict[str, int]:
    """Fetch the year and term counts based on the semester binding mode."""
    default_structure = {"years": 4, "terms_per_year": 2}
    if not degree_code: return default_structure

    with engine.begin() as conn:
        binding = conn.execute(sa_text("SELECT binding_mode FROM semester_binding WHERE degree_code = :degree_code"), 
                             {"degree_code": degree_code}).fetchone()
        binding_mode = binding.binding_mode if binding else 'degree'
        structure = None
        try:
            if binding_mode == 'branch' and branch_id:
                structure = conn.execute(sa_text("SELECT years, terms_per_year FROM branch_semester_struct WHERE branch_id = :branch_id AND active = 1"), 
                                       {"branch_id": branch_id}).fetchone()
            if not structure and binding_mode in ('program', 'branch') and program_id:
                structure = conn.execute(sa_text("SELECT years, terms_per_year FROM program_semester_struct WHERE program_id = :program_id AND active = 1"), 
                                       {"program_id": program_id}).fetchone()
            if not structure:
                structure = conn.execute(sa_text("SELECT years, terms_per_year FROM degree_semester_struct WHERE degree_code = :degree_code AND active = 1"), 
                                       {"degree_code": degree_code}).fetchone()
            if structure: return dict(structure._mapping)
        except Exception as e:
            logger.error(f"Error fetching semester structure: {e}")
            return default_structure
    return default_structure

def fetch_faculty_for_degree(engine: Engine, degree_code: Optional[str] = None) -> List[Dict]:
    """
    Fetch faculty members. 
    If degree_code is provided, strictly filter for faculty who have 
    an EXPLICIT AFFILIATION with that Degree in the faculty_affiliations table.
    """
    
    # Base query for all active faculty
    base_query = """
        SELECT DISTINCT fp.id, fp.email, fp.name, fp.employee_id, fp.status 
        FROM faculty_profiles fp
    """
    
    params = {}
    where_clauses = ["fp.status = 'active'"]
    
    # If degree is specified, enforce affiliation rule via faculty_affiliations
    if degree_code:
        where_clauses.append("""
            EXISTS (
                SELECT 1 FROM faculty_affiliations fa
                WHERE fa.email = fp.email 
                AND fa.degree_code = :degree_code
                AND fa.active = 1
            )
        """)
        params['degree_code'] = degree_code

    full_query = f"{base_query} WHERE {' AND '.join(where_clauses)} ORDER BY fp.name"
    
    with engine.begin() as conn:
        try:
            # Verify table exists to prevent crash on fresh install
            table_check = conn.execute(sa_text("SELECT name FROM sqlite_master WHERE type='table' AND name='faculty_affiliations'")).fetchone()
            
            if not table_check and degree_code:
                # Fallback: If affiliations table missing, show all faculty (safety net)
                return fetch_faculty_for_degree(engine, None)

            results = conn.execute(sa_text(full_query), params).fetchall()
            return [{
                "id": r.id, 
                "email": r.email, 
                "name": r.name, 
                "employee_id": r.employee_id,
                "status": r.status
            } for r in results]
        except Exception as e:
            logger.error(f"Error fetching affiliated faculty: {e}")
            return []
