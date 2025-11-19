# screens/subject_cos_rubrics/shared_filters.py
"""
Shared filter components for the COs & Rubrics module.
FIXED: Handles all degree cohort structures (degree/program/branch/CG)
Eliminates NaN display issues
"""

import streamlit as st
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from typing import List, Dict, Optional, Tuple
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# ===========================================================================
# HELPER FUNCTION
# ===========================================================================

def is_valid(value):
    """Helper to check if a value is not None, not NaN, and not empty string."""
    if value is None:
        return False
    if pd.isna(value):
        return False
    if isinstance(value, str):
        value_stripped = value.strip().upper()
        if value_stripped == '' or value_stripped == 'NONE' or value_stripped == 'NAN':
            return False
    return True

# ===========================================================================
# DATA FETCHING FUNCTIONS
# ===========================================================================

def fetch_degrees(engine: Engine) -> List[Dict]:
    """Fetch all active degrees."""
    with engine.begin() as conn:
        result = conn.execute(sa_text("""
            SELECT code, title, sort_order 
            FROM degrees 
            WHERE active = 1 
            ORDER BY sort_order, code
        """))
        return [dict(row._mapping) for row in result]

def fetch_academic_years(engine: Engine) -> List[Dict]:
    """Fetch all academic years."""
    with engine.begin() as conn:
        result = conn.execute(sa_text("""
            SELECT ay_code 
            FROM academic_years 
            ORDER BY ay_code DESC
        """))
        return [dict(row._mapping) for row in result]

def fetch_programs_by_degree(engine: Engine, degree_code: str) -> List[Dict]:
    """Fetch programs for a specific degree."""
    with engine.begin() as conn:
        result = conn.execute(sa_text("""
            SELECT program_code AS code, program_name AS name, id
            FROM programs 
            WHERE degree_code = :degree_code 
            AND active = 1 
            ORDER BY program_code
        """), {"degree_code": degree_code})
        return [dict(row._mapping) for row in result]

def fetch_branches_by_program(engine: Engine, degree_code: str, program_id: Optional[int]) -> List[Dict]:
    """Fetch branches for a specific program."""
    if not program_id:
        return []
    
    with engine.begin() as conn:
        result = conn.execute(sa_text("""
            SELECT branch_code AS code, branch_name AS name, id
            FROM branches 
            WHERE degree_code = :degree_code 
            AND program_id = :program_id
            AND active = 1 
            ORDER BY branch_code
        """), {"degree_code": degree_code, "program_id": program_id})
        return [dict(row._mapping) for row in result]

def fetch_curriculum_groups_by_degree(engine: Engine, degree_code: str) -> List[Dict]:
    """Fetch curriculum groups for a specific degree."""
    with engine.begin() as conn:
        result = conn.execute(sa_text("""
            SELECT group_code AS code, group_name AS name, id
            FROM curriculum_groups
            WHERE degree_code = :degree_code
            AND active = 1
            ORDER BY sort_order, group_code
        """), {"degree_code": degree_code})
        return [dict(row._mapping) for row in result]

def fetch_semester_structure(engine: Engine, degree_code: str, 
                             program_id: Optional[int] = None, 
                             branch_id: Optional[int] = None) -> Dict[str, int]:
    """
    Fetch the year and term counts based on the semester binding mode.
    (Aligned with semesters_schema.py)
    """
    default_structure = {"years": 10, "terms_per_year": 4}
    
    with engine.begin() as conn:
        # 1. Find the binding mode for the degree
        binding = conn.execute(sa_text("""
            SELECT binding_mode FROM semester_binding
            WHERE degree_code = :degree_code
        """), {"degree_code": degree_code}).fetchone()
        
        binding_mode = binding._mapping['binding_mode'] if binding else 'degree'
        structure = None
        
        try:
            # 2. Fetch structure based on the binding mode
            if binding_mode == 'branch' and branch_id:
                structure = conn.execute(sa_text("""
                    SELECT years, terms_per_year FROM branch_semester_struct
                    WHERE branch_id = :branch_id AND active = 1
                """), {"branch_id": branch_id}).fetchone()
            
            if not structure and binding_mode in ('program', 'branch') and program_id:
                structure = conn.execute(sa_text("""
                    SELECT years, terms_per_year FROM program_semester_struct
                    WHERE program_id = :program_id AND active = 1
                """), {"program_id": program_id}).fetchone()

            if not structure:
                structure = conn.execute(sa_text("""
                    SELECT years, terms_per_year FROM degree_semester_struct
                    WHERE degree_code = :degree_code AND active = 1
                """), {"degree_code": degree_code}).fetchone()
            
            if structure:
                return dict(structure._mapping)
        except Exception as e:
            logger.error(f"Error fetching semester structure: {e}", exc_info=True)
            return default_structure
            
    return default_structure

def fetch_offerings_for_filters(engine: Engine, filters: Dict) -> List[Dict]:
    """Fetch offerings based on all filters - FIXED to handle NULL properly."""
    query = """
        SELECT 
            so.id,
            so.degree_code, 
            so.subject_code,
            sc.subject_name,
            so.year,
            so.term,
            so.program_code,
            so.branch_code,
            so.curriculum_group_code,
            so.division_code
        FROM subject_offerings so
        LEFT JOIN subjects_catalog sc ON 
            so.subject_code = sc.subject_code 
            AND so.degree_code = sc.degree_code
        WHERE so.status = 'published'
        AND so.degree_code = :degree_code
        AND so.ay_label = :ay_label
    """
    params = {
        "degree_code": filters["degree_code"],
        "ay_label": filters["ay_label"]
    }

    # Add optional filters
    if filters.get("year"):
        query += " AND so.year = :year"
        params["year"] = filters["year"]
    
    if filters.get("term"):
        query += " AND so.term = :term"
        params["term"] = filters["term"]
    
    # FIXED: Handle NULL/empty for program_code
    program_code = filters.get("program_code")
    if is_valid(program_code):
        query += " AND (so.program_code = :program_code OR so.program_code IS NULL OR so.program_code = '')"
        params["program_code"] = program_code
    else:
        # No specific program selected - show all including NULL
        pass
    
    # FIXED: Handle NULL/empty for branch_code
    branch_code = filters.get("branch_code")
    if is_valid(branch_code):
        query += " AND (so.branch_code = :branch_code OR so.branch_code IS NULL OR so.branch_code = '')"
        params["branch_code"] = branch_code
    else:
        # No specific branch selected - show all including NULL
        pass
    
    # FIXED: Handle NULL/empty for curriculum_group_code
    cg_code = filters.get("curriculum_group_code")
    if is_valid(cg_code):
        query += " AND (so.curriculum_group_code = :cg_code OR so.curriculum_group_code IS NULL OR so.curriculum_group_code = '')"
        params["cg_code"] = cg_code
    else:
        # No specific CG selected - show all including NULL
        pass

    query += " ORDER BY so.year, so.term, so.subject_code"
    
    with engine.begin() as conn:
        result = conn.execute(sa_text(query), params)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df.to_dict('records')

# ===========================================================================
# MAIN FILTER COMPONENT
# ===========================================================================

def render_co_filters(engine: Engine) -> Tuple[Optional[int], Optional[Dict]]:
    """
    Renders the set of filters to select a single subject offering.
    Returns (offering_id, offering_info)
    """
    st.markdown("### 🎯 Select Subject Offering")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Degree
        degrees = fetch_degrees(engine)
        if not degrees:
            st.warning("No degrees found")
            return None, None
        
        degree_options = {f"{d['code']} - {d['title']}": d['code'] for d in degrees}
        selected_degree_label = st.selectbox("Degree", options=list(degree_options.keys()), key="co_main_degree")
        degree_code = degree_options[selected_degree_label]
        
        # Academic Year
        academic_years = fetch_academic_years(engine)
        if not academic_years:
            st.warning("No academic years found")
            return None, None
        
        ay_options = {ay['ay_code']: ay['ay_code'] for ay in academic_years}
        selected_ay = st.selectbox("Academic Year", options=list(ay_options.keys()), key="co_main_ay")

    program_code = None
    program_id = None
    branch_code = None
    branch_id = None
    cg_code = None

    with col2:
        # Program (optional)
        programs = fetch_programs_by_degree(engine, degree_code)
        if programs:
            program_options = { "All Programs": (None, None) }
            program_options.update({f"{p['code']} - {p['name']}": (p['code'], p['id']) for p in programs})
            selected_program_label = st.selectbox("Program (Optional)", options=list(program_options.keys()), key="co_main_program")
            program_code, program_id = program_options[selected_program_label]
        
        # Branch (optional, only if program selected)
        if program_id:
            branches = fetch_branches_by_program(engine, degree_code, program_id)
            if branches:
                branch_options = { "All Branches": (None, None) }
                branch_options.update({f"{b['code']} - {b['name']}": (b['code'], b['id']) for b in branches})
                selected_branch_label = st.selectbox("Branch (Optional)", options=list(branch_options.keys()), key="co_main_branch")
                branch_code, branch_id = branch_options[selected_branch_label]

    # Curriculum Groups (optional)
    cgs = fetch_curriculum_groups_by_degree(engine, degree_code)
    if cgs:
        cg_options = { "All Curriculum Groups": None }
        cg_options.update({f"{cg['code']} - {cg['name']}": cg['code'] for cg in cgs})
        selected_cg_label = st.selectbox("Curriculum Group (Optional)", options=list(cg_options.keys()), key="co_main_cg")
        cg_code = cg_options[selected_cg_label]

    # Dynamic Year/Term based on semester structure
    structure = fetch_semester_structure(engine, degree_code, program_id, branch_id)
    max_years = structure.get('years', 10)
    max_terms = structure.get('terms_per_year', 4)

    col3, col4 = st.columns(2)
    
    with col3:
        # Year filter
        year = st.selectbox(
            "Year (Optional)",
            options=["All Years"] + list(range(1, max_years + 1)),
            key="co_main_year"
        )
        year = year if year != "All Years" else None
    
    with col4:
        # Term filter
        term = st.selectbox(
            "Term (Optional)",
            options=["All Terms"] + list(range(1, max_terms + 1)),
            key="co_main_term"
        )
        term = term if term != "All Terms" else None
    
    # Fetch Offerings based on filters
    filter_payload = {
        "degree_code": degree_code,
        "ay_label": selected_ay,
        "year": year,
        "term": term,
        "program_code": program_code,
        "branch_code": branch_code,
        "curriculum_group_code": cg_code
    }
    
    try:
        with st.spinner("Loading subjects..."):
            offerings = fetch_offerings_for_filters(engine, filter_payload)
        
        if not offerings:
            st.warning("No published offerings found for the selected filters.")
            return None, None
        
        # Build offering options WITHOUT NaN
        offering_options = { "--- Select a Subject ---": None }
        
        for o in offerings:
            # Build label parts
            label_parts = [
                f"{o['subject_code']} - {o['subject_name']}",
                f"(Y{o['year']}T{o['term']})"
            ]
            
            # Add scope info only if valid
            scope_parts = []
            if is_valid(o.get('program_code')):
                scope_parts.append(o['program_code'])
            if is_valid(o.get('branch_code')):
                scope_parts.append(o['branch_code'])
            if is_valid(o.get('curriculum_group_code')):
                scope_parts.append(f"CG:{o['curriculum_group_code']}")
            if is_valid(o.get('division_code')):
                scope_parts.append(f"Div:{o['division_code']}")
            
            if scope_parts:
                label_parts.insert(1, " / ".join(scope_parts))
            
            label = " ".join(label_parts)
            offering_options[label] = o['id']
        
        selected_offering_key = st.selectbox(
            "Select Subject Offering to Manage",
            options=list(offering_options.keys()),
            key="co_main_offering"
        )
        
        if offering_options[selected_offering_key] is None:
            return None, None
            
        offering_id = offering_options[selected_offering_key]
        offering_info = next(o for o in offerings if o['id'] == offering_id)
        
        return offering_id, offering_info

    except Exception as e:
        st.error(f"Error loading offerings: {e}")
        logger.error(f"Error fetching offerings for filters: {e}", exc_info=True)
        return None, None
