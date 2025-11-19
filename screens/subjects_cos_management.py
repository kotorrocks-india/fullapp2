# screens/subjects_cos_management.py
"""
Subject Catalog, Offerings, Course Outcomes, and Rubrics Management
Streamlit page for managing curriculum data
"""

import streamlit as st
from datetime import datetime
import json
import pandas as pd
import traceback
from io import StringIO
from typing import Dict, List
from sqlalchemy import text as sa_text

# Import the subject_cos module
from screens.subject_cos import (
    SubjectsApplication,
    SubjectCatalogEntry,
    SubjectOffering,
    CourseOutcome,
    RubricConfig,
    RubricAssessment,
    AuditEntry,
    BloomLevel,
    SubjectType,
    Status
)
# Import Rubrics Import/Export service
try:
    from screens.rubrics.rubrics_import_export import RubricsImportExport
except ImportError:
    st.error("Could not import RubricsImportExport. Please ensure the file is in the correct location.")
    # Add a placeholder class to avoid crashing the app
    class RubricsImportExport:
        def __init__(self, service):
            st.error("RubricsImportExport service is not loaded.")
        def export_template_analytic_points(self): return ""
        def export_template_analytic_levels(self): return ""


# ===========================================================================
# HELPERS
# ===========================================================================

def get_audit_entry(reason: str = None) -> AuditEntry:
    """Create audit entry from current session user."""
    user = st.session_state.get("user", {})
    email = user.get("email", "system@college.edu")
    roles = user.get("roles", set())
    role = list(roles)[0] if roles else "user"
    
    return AuditEntry(
        actor_id=email,
        actor_role=role,
        operation="streamlit_ui",
        reason=reason or "UI operation",
        source="streamlit_ui"
    )


def init_subjects_app() -> SubjectsApplication:
    """Initialize SubjectsApplication and cache in session state."""
    if "subjects_app" not in st.session_state:
        engine = st.session_state.get("engine")
        if not engine:
            st.error("Database engine not initialized")
            st.stop()
        
        st.session_state.subjects_app = SubjectsApplication(engine)
    
    return st.session_state.subjects_app

def can_edit() -> bool:
    """Check if user can edit."""
    user = st.session_state.get("user", {})
    roles = user.get("roles", set())
    if "admin" in roles or not roles:
        return True
    return False

# ===========================================================================
# DATA LOADING HELPERS (Updated)
# ===========================================================================

@st.cache_data(ttl=300)
def fetch_all_degrees() -> List[Dict]:
    """Fetch all degrees for selectbox."""
    engine = st.session_state.get("engine")
    if not engine:
        return []
    with engine.begin() as conn:
        result = conn.execute(sa_text("SELECT code, title FROM degrees WHERE active = 1 ORDER BY sort_order, code")).fetchall()
        return [dict(row._mapping) for row in result]

@st.cache_data(ttl=300)
def fetch_all_academic_years() -> List[Dict]:
    """Fetch all academic years for selectbox."""
    engine = st.session_state.get("engine")
    if not engine:
        return []
    with engine.begin() as conn:
        result = conn.execute(sa_text("SELECT ay_code FROM academic_years ORDER BY ay_code DESC")).fetchall()
        return [dict(row._mapping) for row in result]

@st.cache_data(ttl=300)
def fetch_programs_for_degree(degree_code: str) -> List[Dict]:
    """Fetch programs filtered by degree."""
    if not degree_code:
        return []
    engine = st.session_state.get("engine")
    with engine.begin() as conn:
        result = conn.execute(
            sa_text("SELECT program_code, program_name FROM programs WHERE degree_code = :degree AND active = 1 ORDER BY sort_order, program_code"),
            {"degree": degree_code}
        ).fetchall()
        return [dict(row._mapping) for row in result]

@st.cache_data(ttl=300)
def fetch_branches_for_degree(degree_code: str) -> List[Dict]:
    """Fetch branches filtered by degree."""
    if not degree_code:
        return []
    engine = st.session_state.get("engine")
    with engine.begin() as conn:
        result = conn.execute(
            sa_text("SELECT branch_code, branch_name FROM branches WHERE degree_code = :degree AND active = 1 ORDER BY sort_order, branch_code"),
            {"degree": degree_code}
        ).fetchall()
        return [dict(row._mapping) for row in result]

@st.cache_data(ttl=300)
def fetch_cgs_for_degree(degree_code: str) -> List[Dict]:
    """Fetch curriculum groups filtered by degree."""
    if not degree_code:
        return []
    engine = st.session_state.get("engine")
    with engine.begin() as conn:
        result = conn.execute(
            sa_text("SELECT group_code, group_name FROM curriculum_groups WHERE degree_code = :degree AND active = 1 ORDER BY sort_order, group_code"),
            {"degree": degree_code}
        ).fetchall()
        return [dict(row._mapping) for row in result]

@st.cache_data(ttl=300)
def fetch_all_subject_codes(degree_code: str) -> List[Dict]:
    """Fetch all subject codes for a degree."""
    if not degree_code:
        return []
    app = init_subjects_app()
    return app.catalog.get_all_subject_codes(degree_code)

@st.cache_data(ttl=300)
def fetch_offerings(degree_code: str, ay_label: str, year: int) -> list:
    """Fetch offerings with JOIN on catalog."""
    app = init_subjects_app()
    try:
        return app.offerings.list_offerings(
            degree_code=degree_code,
            ay_label=ay_label,
            year=year
        )
    except Exception as e:
        st.error(f"Error loading offerings: {e}")
        return []

@st.cache_data(ttl=300)
def fetch_published_offerings_for_rubrics() -> list:
    """Fetch published offerings, joining with catalog for display."""
    engine = st.session_state.get("engine")
    if not engine:
        st.error("Database engine not found in session state.")
        return []
        
    with engine.begin() as conn:
        result = conn.execute(sa_text("""
        SELECT 
            so.id, so.subject_code, so.degree_code, so.program_code, so.branch_code,
            so.ay_label, so.year, so.term, so.status,
            sc.subject_name, sc.subject_type
        FROM subject_offerings so
        JOIN subjects_catalog sc ON so.subject_code = sc.subject_code
            AND so.degree_code = sc.degree_code
            AND (so.program_code = sc.program_code OR (so.program_code IS NULL AND sc.program_code IS NULL))
            AND (so.branch_code = sc.branch_code OR (so.branch_code IS NULL AND sc.branch_code IS NULL))
            AND (so.curriculum_group_code = sc.curriculum_group_code OR (so.curriculum_group_code IS NULL AND sc.curriculum_group_code IS NULL))
        WHERE so.status = 'published'
        ORDER BY so.ay_label DESC, so.year, so.term, so.subject_code
        """)).fetchall()
        return [dict(row._mapping) for row in result]


def get_offering_label(offering: Dict) -> str:
    """Generate offering label."""
    parts = [
        offering['subject_code'],
        offering['subject_name'],
        f"({offering['ay_label']}, Y{offering['year']}, T{offering['term']})"
    ]
    if offering.get('program_code'):
        parts.insert(2, offering['program_code'])
    if offering.get('branch_code'):
        parts.insert(3, offering['branch_code'])
    return " - ".join(parts)


# ===========================================================================
# TAB 1: SUBJECTS CATALOG (Updated with dynamic dropdowns)
# ===========================================================================

def render_subjects_catalog_tab():
    """Render subjects catalog management tab (Updated for new schema)."""
    st.header("📚 Subjects Catalog")
    
    app = init_subjects_app()
    
    # --- Filters (Using selectbox for Degree, Program, Branch) ---
    col1, col2, col3 = st.columns(3)
    
    degrees = fetch_all_degrees()
    if not degrees:
        st.error("No Degrees found. Please create degrees first.")
        return
    
    with col1:
        selected_degree_option = st.selectbox(
            "Degree*",
            options=degrees,
            format_func=lambda d: f"{d['code']} - {d['title']}",
            key="catalog_degree"
        )
        degree_code = selected_degree_option['code']
    
    # --- Dynamic Program Dropdown ---
    programs = fetch_programs_for_degree(degree_code)
    program_options = [None] + programs
    
    with col2:
        selected_program_option = st.selectbox(
            "Program (optional)",
            options=program_options,
            format_func=lambda p: f"{p['program_code']} - {p['program_name']}" if p else "---",
            key="catalog_program"
        )
        program_code = selected_program_option['program_code'] if selected_program_option else None

    # --- Dynamic Branch Dropdown ---
    branches = fetch_branches_for_degree(degree_code)
    branch_options = [None] + branches

    with col3:
        selected_branch_option = st.selectbox(
            "Branch (optional)",
            options=branch_options,
            format_func=lambda b: f"{b['branch_code']} - {b['branch_name']}" if b else "---",
            key="catalog_branch"
        )
        branch_code = selected_branch_option['branch_code'] if selected_branch_option else None
    
    # --- List subjects (Using new service) ---
    try:
        subjects = app.catalog.list_subjects(
            degree_code=degree_code,
            program_code=program_code,
            branch_code=branch_code
        )
        
        st.subheader(f"📋 Subjects ({len(subjects)})")
        
        if subjects:
            for subject in subjects:
                with st.expander(f"{subject['subject_code']} - {subject['subject_name']}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Type:** {subject['subject_type']}")
                        st.write(f"**Credits:** {subject['credits_total']}")
                        st.write(f"**L-T-P-S:** {subject['L']}-{subject['T']}-{subject['P']}-{subject['S']}")
                        st.write(f"**Program:** {subject.get('program_code') or 'N/A'}")
                        st.write(f"**Branch:** {subject.get('branch_code') or 'N/A'}")
                    with col2:
                        st.write(f"**Internal Max:** {subject['internal_marks_max']}")
                        st.write(f"**External Max:** {subject['exam_marks_max']}")
                        st.write(f"**Jury Max:** {subject['jury_viva_marks_max']}")
                        st.write(f"**Group:** {subject.get('curriculum_group_code') or 'N/A'}")
                        st.write(f"**Status:** {subject['status']}")
        else:
            st.info("No subjects found for these filters.")
    
    except Exception as e:
        st.error(f"Error loading subjects: {e}")
        st.code(traceback.format_exc())


# ===========================================================================
# TAB 2: SUBJECT OFFERINGS (Updated with dynamic dropdowns)
# ===========================================================================

def render_offerings_tab():
    """Render subject offerings management tab (Updated for new schema)."""
    st.header("📘 Subject Offerings")
    
    app = init_subjects_app()
    
    # --- Filters (Using selectboxes) ---
    col1, col2, col3 = st.columns(3)
    
    degrees = fetch_all_degrees()
    academic_years = fetch_all_academic_years()
    
    if not degrees or not academic_years:
        st.error("No Degrees or Academic Years found. Please create them first.")
        return

    with col1:
        selected_degree = st.selectbox(
            "Degree*",
            options=degrees,
            format_func=lambda d: f"{d['code']} - {d['title']}",
            key="offering_degree"
        )
        degree_code = selected_degree['code']
    
    with col2:
        selected_ay = st.selectbox(
            "Academic Year*",
            options=academic_years,
            format_func=lambda ay: ay['ay_code'],
            key="offering_ay"
        )
        ay_label = selected_ay['ay_code']
        
    with col3:
        year = st.number_input("Year (e.g., 1)", min_value=1, max_value=6, value=1, key="offering_year")
    
    # --- List offerings (Using new service with JOIN) ---
    try:
        offerings = fetch_offerings(degree_code, ay_label, year)
        
        st.subheader(f"📋 Offerings ({len(offerings)})")
        
        if offerings:
            for offering in offerings:
                status_emoji = "✅" if offering['status'] == 'published' else "📝"
                expander_title = (
                    f"{status_emoji} {offering['subject_code']} - {offering['subject_name']} "
                    f"(Y{offering['year']}-T{offering['term']})"
                )
                with st.expander(expander_title):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**AY:** {offering['ay_label']}")
                        st.write(f"**Year-Term:** {offering['year']}-{offering['term']}")
                        st.write(f"**Instructor:** {offering.get('instructor_email') or 'Not assigned'}")
                        st.write(f"**Status:** {offering['status']}")
                    with col2:
                        st.write(f"**Credits:** {offering.get('credits_total', 'N/A')}")
                        st.write(f"**Program:** {offering.get('program_code') or 'N/A'}")
                        st.write(f"**Branch:** {offering.get('branch_code') or 'N/A'}")
                        st.write(f"**Group:** {offering.get('curriculum_group_code') or 'N/A'}")
                    
                    # Actions
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        if offering['status'] == 'draft':
                            if st.button(f"Publish", key=f"publish_{offering['id']}"):
                                try:
                                    audit = get_audit_entry(f"Publishing offering {offering['id']}")
                                    app.offerings.publish_offering(offering['id'], audit)
                                    st.success("Offering published!")
                                    st.cache_data.clear()
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")
                    
                    with col2:
                        if st.button(f"View COs", key=f"view_cos_{offering['id']}"):
                            st.session_state.selected_offering_id = offering['id']
                            st.session_state.selected_tab = "Course Outcomes"
                            st.rerun()
                    
                    with col3:
                        if st.button(f"Context", key=f"context_{offering['id']}"):
                            context = app.get_complete_offering_context(offering['id'])
                            st.json(context)
        else:
            st.info("No offerings found. Create one below.")
    
    except Exception as e:
        st.error(f"Error loading offerings: {e}")
        st.code(traceback.format_exc())
    
    # --- Create new offering (Updated form) ---
    st.divider()
    st.subheader("➕ Create New Offering")
    
    # Load subjects for the selected degree
    subjects_in_catalog = fetch_all_subject_codes(degree_code)
    
    if not subjects_in_catalog:
        st.warning(f"No subjects found in catalog for degree '{degree_code}'. Please add subjects in a separate module first.")
    
    with st.form("create_offering_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            # Select subject from catalog
            selected_subject = st.selectbox(
                "Subject*",
                options=subjects_in_catalog,
                format_func=lambda s: f"{s['subject_code']} - {s['subject_name']}",
                disabled=not subjects_in_catalog,
                key="create_offering_subject"
            )
            
            ay_label_new = st.selectbox(
                "Academic Year*",
                options=[ay['ay_code'] for ay in academic_years],
                index=[ay['ay_code'] for ay in academic_years].index(ay_label) if ay_label in [ay['ay_code'] for ay in academic_years] else 0,
                key="create_offering_ay"
            )
            year_new = st.number_input("Year*", min_value=1, max_value=6, value=year, key="create_offering_year_num")
            term_new = st.number_input("Term*", min_value=1, max_value=3, value=1, key="create_offering_term_num")
        
        with col2:
            degree_code_new_option = st.selectbox(
                "Degree Code*",
                options=degrees,
                format_func=lambda d: d['code'],
                index=[d['code'] for d in degrees].index(degree_code) if degree_code in [d['code'] for d in degrees] else 0,
                key="create_offering_degree"
            )
            degree_code_new = degree_code_new_option['code']
            
            # --- Dynamic Dropdowns for Program, Branch, CG ---
            programs_new = fetch_programs_for_degree(degree_code_new)
            program_options_new = [None] + programs_new
            selected_program_new = st.selectbox(
                "Program (if applicable)",
                options=program_options_new,
                format_func=lambda p: f"{p['program_code']} - {p['program_name']}" if p else "---",
                key="create_offering_program"
            )
            program_code = selected_program_new['program_code'] if selected_program_new else None
            
            branches_new = fetch_branches_for_degree(degree_code_new)
            branch_options_new = [None] + branches_new
            selected_branch_new = st.selectbox(
                "Branch (if applicable)",
                options=branch_options_new,
                format_func=lambda b: f"{b['branch_code']} - {b['branch_name']}" if b else "---",
                key="create_offering_branch"
            )
            branch_code = selected_branch_new['branch_code'] if selected_branch_new else None

            cgs_new = fetch_cgs_for_degree(degree_code_new)
            cg_options_new = [None] + cgs_new
            selected_cg_new = st.selectbox(
                "Curriculum Group (if applicable)",
                options=cg_options_new,
                format_func=lambda cg: f"{cg['group_code']} - {cg['group_name']}" if cg else "---",
                key="create_offering_cg"
            )
            curriculum_group_code = selected_cg_new['group_code'] if selected_cg_new else None
            
            instructor_email = st.text_input("Instructor Email")
        
        reason = st.text_input("Reason", placeholder="Creating offering for new AY")
        
        submitted = st.form_submit_button("Create Offering", type="primary", disabled=not subjects_in_catalog)
        
        if submitted:
            try:
                offering = SubjectOffering(
                    subject_code=selected_subject['subject_code'],
                    degree_code=degree_code_new,
                    program_code=program_code,
                    branch_code=branch_code,
                    curriculum_group_code=curriculum_group_code,
                    ay_label=ay_label_new,
                    year=year_new,
                    term=term_new,
                    instructor_email=instructor_email or None
                )
                
                audit = get_audit_entry(reason)
                offering_id = app.offerings.create_offering(offering, audit)
                
                st.success(f"✅ Offering created successfully! (ID: {offering_id})")
                st.cache_data.clear() # Clear cache to show new offering
                st.rerun()
                
            except ValueError as e:
                st.error(f"Validation Error: {e}")
            except Exception as e:
                st.error(f"Error creating offering: {e}")
                st.code(traceback.format_exc())


# ===========================================================================
# TAB 3: COURSE OUTCOMES (Updated with new filters)
# ===========================================================================

def render_cos_tab():
    """Render course outcomes management tab."""
    st.header("🎯 Course Outcomes")
    
    app = init_subjects_app()
    
    # Offering selection
    selected_offering_id = st.session_state.get("selected_offering_id")
    
    if not selected_offering_id:
        st.info("Please select an offering from the Subject Offerings tab")
        
        # Quick offering selector
        degrees = fetch_all_degrees()
        academic_years = fetch_all_academic_years()
    
        if not degrees or not academic_years:
            st.error("No Degrees or Academic Years found.")
            return

        c1, c2, c3 = st.columns(3)
        with c1:
            degree_code = st.selectbox("Degree", options=[d['code'] for d in degrees], key="cos_degree_filter")
        with c2:
            ay_label = st.selectbox("Academic Year", options=[ay['ay_code'] for ay in academic_years], key="cos_ay_filter")
        with c3:
            year = st.number_input("Year", min_value=1, max_value=6, value=1, key="cos_year_filter")
        
        if st.button("Load Offerings"):
            try:
                offerings = fetch_offerings(degree_code, ay_label, year)
                if offerings:
                    st.session_state.cos_offering_list = offerings
                else:
                    st.warning("No offerings found for these filters")
            except Exception as e:
                st.error(f"Error: {e}")
        
        if "cos_offering_list" in st.session_state:
            st.write("Select an offering:")
            for off in st.session_state.cos_offering_list:
                label = f"{off['subject_code']} - {off.get('subject_name', 'N/A')} (Y{off['year']}-T{off['term']})"
                if st.button(label, key=f"select_{off['id']}"):
                    st.session_state.selected_offering_id = off['id']
                    del st.session_state.cos_offering_list
                    st.rerun()
        return
    
    # Get offering details
    try:
        offering_full = app.offerings._fetch_one(
            """
            SELECT so.*, sc.subject_name 
            FROM subject_offerings so 
            LEFT JOIN subjects_catalog sc ON so.subject_code = sc.subject_code AND so.degree_code = sc.degree_code
            WHERE so.id = :id
            """,
            {'id': selected_offering_id}
        )

        if not offering_full:
            st.error("Offering not found")
            del st.session_state.selected_offering_id
            st.rerun()
            return
        
        offering = offering_full
        
        st.info(f"**Offering:** {offering['subject_code']} - {offering.get('subject_name', 'N/A')} (AY: {offering['ay_label']}, Y{offering['year']}-T{offering['term']})")
        
        if st.button("← Back to Offerings"):
            del st.session_state.selected_offering_id
            st.session_state.selected_tab = "Subject Offerings"
            st.rerun()
        
        # List COs
        cos = app.cos.get_cos_for_offering(selected_offering_id, include_correlations=True)
        
        st.subheader(f"📋 Course Outcomes ({len(cos)})")
        
        if cos:
            is_valid, total_weight = app.cos.validate_co_weights(selected_offering_id)
            if is_valid:
                st.success(f"✅ CO weights are valid (Total: {total_weight:.2f})")
            else:
                st.warning(f"⚠️ CO weights sum to {total_weight:.2f}, expected ~1.0")
            
            for co in cos:
                with st.expander(f"{co['co_code']}: {co['title']}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Description:** {co['description']}")
                        st.write(f"**Bloom Level:** {co['bloom_level']}")
                        st.write(f"**Weight:** {co['weight_in_direct']}")
                        st.write(f"**Status:** {co['status']}")
                    with col2:
                        if co.get('po_correlations'):
                            st.write("**PO Correlations:**")
                            st.json(co['po_correlations'])
                        if co.get('pso_correlations'):
                            st.write("**PSO Correlations:**")
                            st.json(co['pso_correlations'])
        else:
            st.info("No COs defined yet. Create one below.")
        
        # Create new CO
        st.divider()
        st.subheader("➕ Create New CO")
        
        with st.form("create_co_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                co_code = st.text_input("CO Code*", placeholder="CO1")
                title = st.text_input("Title*", placeholder="Understand basic programming concepts")
                bloom_level = st.selectbox("Bloom Level*", [b.value for b in BloomLevel])
                weight = st.number_input("Weight in Direct*", min_value=0.0, max_value=1.0, value=0.2, step=0.05)
            
            with col2:
                description = st.text_area("Description*", placeholder="Students will be able to...")
                knowledge_type = st.text_input("Knowledge Type", placeholder="Conceptual")
                sequence = st.number_input("Sequence", min_value=0, value=len(cos) + 1)
            
            st.subheader("PO Correlations (0-3)")
            po_cols = st.columns(6)
            po_correlations = {}
            for i, col in enumerate(po_cols, 1):
                with col:
                    po_val = st.number_input(f"PO{i}", min_value=0, max_value=3, value=0, key=f"po{i}")
                    if po_val > 0:
                        po_correlations[f"PO{i}"] = po_val
            
            reason = st.text_input("Reason", placeholder="Adding CO to offering")
            
            submitted = st.form_submit_button("Create CO", type="primary")
            
            if submitted:
                if not co_code or not title or not description:
                    st.error("Please fill in all required fields")
                else:
                    try:
                        co = CourseOutcome(
                            offering_id=selected_offering_id,
                            co_code=co_code,
                            title=title,
                            description=description,
                            bloom_level=bloom_level,
                            knowledge_type=knowledge_type or None,
                            weight_in_direct=weight,
                            sequence=sequence,
                            status='draft',
                            po_correlations=po_correlations if po_correlations else None
                        )
                        
                        audit = get_audit_entry(reason)
                        co_id = app.cos.create_co(co, audit)
                        
                        st.success(f"✅ CO {co_code} created successfully! (ID: {co_id})")
                        st.rerun()
                        
                    except ValueError as e:
                        st.error(f"Validation Error: {e}")
                    except Exception as e:
                        st.error(f"Error creating CO: {e}")
    
    except Exception as e:
        st.error(f"Error: {e}")
        st.code(traceback.format_exc())


# ===========================================================================
# TAB 4: RUBRICS (Updated with new data loading)
# ===========================================================================

# --- Rubric Configuration Tab ---
def render_rubric_config_tab(app: SubjectsApplication):
    """Tab for creating and managing rubric configurations."""
    st.subheader("📝 Rubric Configuration")
    is_edit_mode = can_edit()

    # Filter: Select offering (USING NEW FUNCTION)
    offerings = fetch_published_offerings_for_rubrics()
    if not offerings:
        st.info("No published offerings found. Please publish offerings first.")
        return

    offering_id = st.selectbox(
        "Select Offering*",
        options=[o['id'] for o in offerings],
        format_func=lambda x: get_offering_label(
            next(o for o in offerings if o['id'] == x)
        ),
        key="rubric_offering"
    )

    if not offering_id:
        return

    st.markdown("---")

    # Show existing rubrics for this offering
    rubrics = app.rubrics.list_rubrics_for_offering(offering_id)

    if rubrics:
        st.markdown("#### Existing Rubrics")
        for rubric in rubrics:
            with st.expander(
                f"📋 {rubric['scope'].upper()} Rubric "
                f"(v{rubric['version']}) - {rubric['status'].upper()}"
            ):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.write(f"**Mode:** {rubric['mode']}")
                    st.write(f"**CO Linking:** {'Yes' if rubric['co_linking_enabled'] else 'No'}")
                    st.write(f"**Visible to Students:** {'Yes' if rubric['visible_to_students'] else 'No'}")
                
                with col2:
                    st.write(f"**Normalization:** {'Yes' if rubric['normalization_enabled'] else 'No'}")
                    st.write(f"**Show Before Assessment:** {'Yes' if rubric['show_before_assessment'] else 'No'}")
                    st.write(f"**Version:** {rubric['version']}")
                
                with col3:
                    st.write(f"**Locked:** {'Yes' if rubric['is_locked'] else 'No'}")
                    if rubric['locked_reason']:
                        st.write(f"**Lock Reason:** {rubric['locked_reason']}")
                    st.write(f"**Status:** {rubric['status']}")

                # Actions
                action_cols = st.columns(5)
                
                if is_edit_mode:
                    with action_cols[1]:
                        if rubric['status'] == 'draft' and not rubric['is_locked']:
                            if st.button("Publish", key=f"publish_{rubric['id']}"):
                                try:
                                    audit = get_audit_entry('publish_rubric')
                                    app.rubrics.publish_rubric(rubric['id'], audit)
                                    st.success("Rubric published successfully!")
                                    st.cache_data.clear()
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                    
                    with action_cols[2]:
                        if rubric['is_locked']:
                            if st.button("Unlock", key=f"unlock_{rubric['id']}"):
                                st.session_state[f'unlocking_{rubric["id"]}'] = True
                        else:
                            if st.button("Lock", key=f"lock_{rubric['id']}"):
                                st.session_state[f'locking_{rubric["id"]}'] = True
                    
                    with action_cols[3]:
                        if st.button("New Version", key=f"version_{rubric['id']}"):
                            try:
                                audit = get_audit_entry('version_rubric')
                                new_version = app.rubrics.create_rubric_version(rubric['id'], audit)
                                st.success(f"Created version {new_version}")
                                st.cache_data.clear()
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {str(e)}")

                # Handle lock/unlock dialogs
                if st.session_state.get(f'locking_{rubric["id"]}'):
                    reason = st.text_input("Lock Reason*", key=f"lock_reason_{rubric['id']}")
                    col_a, col_b = st.columns(2)
                    with col_a:
                        if st.button("Confirm Lock", key=f"confirm_lock_{rubric['id']}"):
                            if reason:
                                try:
                                    audit = get_audit_entry(f'lock_rubric: {reason}')
                                    app.rubrics.lock_rubric(rubric['id'], reason, audit)
                                    st.success("Rubric locked")
                                    del st.session_state[f'locking_{rubric["id"]}']
                                    st.cache_data.clear()
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                    with col_b:
                        if st.button("Cancel", key=f"cancel_lock_{rubric['id']}"):
                            del st.session_state[f'locking_{rubric["id"]}']
                            st.rerun()

                if st.session_state.get(f'unlocking_{rubric["id"]}'):
                    reason = st.text_input("Unlock Reason*", key=f"unlock_reason_{rubric['id']}")
                    st.warning("⚠️ Step-up authentication required (simulated)")
                    col_a, col_b = st.columns(2)
                    with col_a:
                        if st.button("Confirm Unlock", key=f"confirm_unlock_{rubric['id']}"):
                            if reason:
                                try:
                                    audit = get_audit_entry(f'unlock_rubric: {reason}')
                                    audit.step_up_performed = 1 # Simulate step-up
                                    app.rubrics.unlock_rubric(rubric['id'], reason, audit)
                                    st.success("Rubric unlocked")
                                    del st.session_state[f'unlocking_{rubric["id"]}']
                                    st.cache_data.clear()
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                    with col_b:
                        if st.button("Cancel", key=f"cancel_unlock_{rubric['id']}"):
                            del st.session_state[f'unlocking_{rubric["id"]}']
                            st.rerun()

    # Create new rubric
    if is_edit_mode:
        st.markdown("---")
        st.markdown("#### Create New Rubric Configuration")

        with st.form("create_rubric_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                scope = st.selectbox(
                    "Scope*",
                    options=['subject', 'component'],
                    help="Subject-level or component-level rubric"
                )
                
                component_key = None
                if scope == 'component':
                    component_key = st.text_input(
                        "Component Key*",
                        help="e.g., internal.assignment"
                    )
                
                mode = st.selectbox(
                    "Mode*",
                    options=['analytic_points', 'analytic_levels'],
                    format_func=lambda x: 'Analytic Points' if x == 'analytic_points' else 'Analytic Levels'
                )
            
            with col2:
                co_linking = st.checkbox("Enable CO Linking", value=False)
                normalization = st.checkbox("Enable Normalization", value=True)
                visible_students = st.checkbox("Visible to Students", value=True)
                show_before = st.checkbox("Show Before Assessment", value=True)

            submitted = st.form_submit_button("Create Rubric Configuration", type="primary")

            if submitted:
                try:
                    config = RubricConfig(
                        offering_id=offering_id,
                        scope=scope,
                        component_key=component_key,
                        mode=mode,
                        co_linking_enabled=1 if co_linking else 0,
                        normalization_enabled=1 if normalization else 0,
                        visible_to_students=1 if visible_students else 0,
                        show_before_assessment=1 if show_before else 0,
                        version=1,
                        is_locked=0,
                        status='draft'
                    )
                    
                    audit = get_audit_entry('create_rubric')
                    
                    config_id = app.rubrics.create_rubric_config(config, audit)
                    st.success(f"Rubric configuration created (ID: {config_id})")
                    st.cache_data.clear()
                    st.rerun()
                
                except Exception as e:
                    st.error(f"Error: {str(e)}")
                    st.code(traceback.format_exc())


# --- Assessments Manager Tab ---
def render_assessments_tab(app: SubjectsApplication):
    """Tab for managing assessments within rubrics."""
    st.subheader("📊 Assessments Manager")
    is_edit_mode = can_edit()

    # Select rubric config
    offerings = fetch_published_offerings_for_rubrics()
    if not offerings:
        st.info("No published offerings found.")
        return

    offering_id = st.selectbox(
        "Select Offering*",
        options=[o['id'] for o in offerings],
        format_func=lambda x: get_offering_label(
            next(o for o in offerings if o['id'] == x)
        ),
        key="assess_offering"
    )

    if not offering_id:
        return

    rubrics = app.rubrics.list_rubrics_for_offering(offering_id)
    
    if not rubrics:
        st.warning("No rubric configurations found for this offering.")
        st.info("Create a rubric configuration first in the 'Configuration' tab.")
        return

    rubric_id = st.selectbox(
        "Select Rubric*",
        options=[r['id'] for r in rubrics],
        format_func=lambda x: f"{next(r['scope'] for r in rubrics if r['id'] == x)} - "
                             f"v{next(r['version'] for r in rubrics if r['id'] == x)}",
        key="assess_rubric"
    )

    if not rubric_id:
        return

    rubric = app.rubrics.get_rubric_config(rubric_id)
    
    st.markdown("---")
    st.markdown(f"#### Assessments ({rubric['mode']})")

    complete_rubric = app.rubrics.get_rubric(rubric['offering_id'], rubric['scope'], 
                                        rubric['component_key'])

    if complete_rubric and complete_rubric.get('assessments'):
        for idx, assessment in enumerate(complete_rubric['assessments']):
            with st.expander(f"📝 {assessment['code']} - {assessment['title']}", expanded=idx==0):
                st.write(f"**Max Marks:** {assessment['max_marks']}")
                st.write(f"**Mode:** {assessment['mode']}")
                if assessment.get('component_key'):
                    st.write(f"**Component:** {assessment['component_key']}")

                if assessment['mode'] == 'analytic_points':
                    if assessment.get('criteria'):
                        st.markdown("##### Criteria Weights")
                        criteria_df = pd.DataFrame(assessment['criteria'])
                        if not criteria_df.empty:
                            display_cols = ['criterion_key', 'weight_pct']
                            if 'linked_cos' in criteria_df.columns:
                                display_cols.append('linked_cos')
                            st.dataframe(criteria_df[display_cols], use_container_width=True)
                            
                            total = criteria_df['weight_pct'].sum()
                            if abs(total - 100.0) < 0.01:
                                st.success(f"✓ Weights sum to 100%")
                            else:
                                st.error(f"⚠ Weights sum to {total}%, not 100%")
                    else:
                        st.info("No criteria defined yet")
                
                elif assessment['mode'] == 'analytic_levels':
                    if assessment.get('levels'):
                        st.markdown("##### Level Descriptors")
                        levels_df = pd.DataFrame(assessment['levels'])
                        if not levels_df.empty:
                            for criterion_key in levels_df['criterion_key'].unique():
                                st.markdown(f"**{criterion_key}**")
                                criterion_levels = levels_df[
                                    levels_df['criterion_key'] == criterion_key
                                ].sort_values('level_sequence')
                                
                                for _, level in criterion_levels.iterrows():
                                    st.write(
                                        f"- {level['level_label']} "
                                        f"(Score: {level['level_score']}): "
                                        f"{level.get('level_descriptor', 'N/A')}"
                                    )
                    else:
                        st.info("No levels defined yet")

                # Actions
                if is_edit_mode and not rubric['is_locked']:
                    col1, col2 = st.columns([1, 1])
                    with col1:
                        if st.button("Add/Edit Criteria/Levels", key=f"add_crit_{assessment['id']}"):
                            st.session_state.selected_rubric_id_for_criteria = rubric_id
                            st.session_state.selected_assessment_id_for_criteria = assessment['id']
                            st.warning("Please go to the 'Criteria Editor' tab to modify.")
                    
                    with col2:
                        if st.button("Delete", key=f"del_assess_{assessment['id']}"):
                            if st.session_state.get(f'confirm_del_{assessment["id"]}'):
                                try:
                                    audit = get_audit_entry('delete_assessment')
                                    app.rubrics.delete_assessment(assessment['id'], audit)
                                    st.success("Assessment deleted")
                                    st.cache_data.clear()
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                            else:
                                st.session_state[f'confirm_del_{assessment["id"]}'] = True
                                st.warning("Click again to confirm deletion")
    else:
        st.info("No assessments defined yet")

    # Add new assessment
    if is_edit_mode and not rubric['is_locked']:
        st.markdown("---")
        st.markdown("#### Add New Assessment")

        with st.form("add_assessment_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                assess_code = st.text_input("Assessment Code*", placeholder="e.g., A1, QUIZ1")
                assess_title = st.text_input("Title*", placeholder="e.g., Assignment 1 - Data Structures")
            
            with col2:
                assess_max = st.number_input("Max Marks*", min_value=0.0, value=10.0, step=0.5)
                assess_component = st.text_input("Component Key (optional)", 
                                                placeholder="e.g., internal.assignment")

            submitted = st.form_submit_button("Add Assessment", type="primary")

            if submitted:
                if not assess_code or not assess_title:
                    st.error("Code and Title are required")
                else:
                    try:
                        assessment = RubricAssessment(
                            rubric_config_id=rubric_id,
                            code=assess_code,
                            title=assess_title,
                            max_marks=assess_max,
                            mode=rubric['mode'],
                            component_key=assess_component if assess_component else None
                        )
                        
                        audit = get_audit_entry('add_assessment')
                        
                        assess_id = app.rubrics.add_assessment(assessment, audit)
                        st.success(f"Assessment added (ID: {assess_id})")
                        st.cache_data.clear()
                        st.rerun()
                    
                    except Exception as e:
                        st.error(f"Error: {str(e)}")


# --- Criteria/Levels Editor Tab ---
def render_criteria_editor_tab(app: SubjectsApplication):
    """Tab for editing criteria weights or level descriptors."""
    st.subheader("⚙️ Criteria & Levels Editor")
    is_edit_mode = can_edit()

    offerings = fetch_published_offerings_for_rubrics()
    if not offerings:
        st.info("No published offerings found.")
        return

    offering_id = st.selectbox(
        "Select Offering*",
        options=[o['id'] for o in offerings],
        format_func=lambda x: get_offering_label(
            next(o for o in offerings if o['id'] == x)
        ),
        key="crit_offering"
    )

    if not offering_id:
        return

    rubrics = app.rubrics.list_rubrics_for_offering(offering_id)
    if not rubrics:
        st.info("No rubrics found")
        return

    rubric_id = st.selectbox(
        "Select Rubric*",
        options=[r['id'] for r in rubrics],
        format_func=lambda x: f"{next(r['scope'] for r in rubrics if r['id'] == x)} - "
                             f"v{next(r['version'] for r in rubrics if r['id'] == x)}",
        key="crit_rubric"
    )

    if not rubric_id:
        return

    rubric = app.rubrics.get_rubric_config(rubric_id)
    complete_rubric = app.rubrics.get_rubric(rubric['offering_id'], rubric['scope'], 
                                        rubric['component_key'])

    if not complete_rubric or not complete_rubric.get('assessments'):
        st.info("No assessments found in this rubric")
        return

    assessment_id = st.selectbox(
        "Select Assessment*",
        options=[a['id'] for a in complete_rubric['assessments']],
        format_func=lambda x: next(
            f"{a['code']} - {a['title']}" 
            for a in complete_rubric['assessments'] if a['id'] == x
        ),
        key="crit_assessment"
    )

    if not assessment_id:
        return

    selected_assessment = next(
        a for a in complete_rubric['assessments'] if a['id'] == assessment_id
    )

    st.markdown("---")

    criteria_catalog = app.rubrics.get_criteria_catalog()

    if selected_assessment['mode'] == 'analytic_points':
        st.markdown("#### Criteria Weights (Analytic Points)")
        
        if selected_assessment.get('criteria'):
            st.markdown("##### Current Criteria")
            df = pd.DataFrame(selected_assessment['criteria'])
            st.dataframe(df[['criterion_key', 'weight_pct']], use_container_width=True)
            
            total = df['weight_pct'].sum()
            if abs(total - 100.0) < 0.01:
                st.success(f"✓ Total: {total}%")
            else:
                st.error(f"⚠ Total: {total}% (must be 100%)")

        if is_edit_mode and not rubric['is_locked']:
            st.markdown("##### Add Criteria Weights")
            
            with st.form("add_criteria_form"):
                st.write("Select criteria from catalog or add custom")
                
                num_criteria = st.number_input(
                    "Number of Criteria", min_value=1, max_value=10, value=3
                )
                
                criteria_data = {}
                for i in range(num_criteria):
                    col1, col2 = st.columns(2)
                    with col1:
                        criterion_key = st.selectbox(
                            f"Criterion {i+1}",
                            options=[c['key'] for c in criteria_catalog] + ['custom'],
                            key=f"crit_key_{i}"
                        )
                        if criterion_key == 'custom':
                            criterion_key = st.text_input(
                                "Custom Criterion Key",
                                key=f"crit_custom_{i}"
                            )
                    with col2:
                        weight = st.number_input(
                            f"Weight %",
                            min_value=0.0, max_value=100.0, value=0.0,
                            key=f"crit_weight_{i}"
                        )
                    
                    if criterion_key:
                        criteria_data[criterion_key] = weight

                submitted = st.form_submit_button("Add Criteria", type="primary")

                if submitted:
                    if not criteria_data:
                        st.error("No criteria specified")
                    else:
                        total = sum(criteria_data.values())
                        if abs(total - 100.0) > 0.01:
                            st.error(f"Weights must sum to 100%, got {total}%")
                        else:
                            try:
                                audit = get_audit_entry('add_criteria')
                                app.rubrics.add_criteria_weights(
                                    assessment_id, criteria_data, audit_entry=audit
                                )
                                st.success("Criteria added successfully")
                                st.cache_data.clear()
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {str(e)}")

    elif selected_assessment['mode'] == 'analytic_levels':
        st.markdown("#### Level Descriptors (Analytic Levels)")
        
        if selected_assessment.get('levels'):
            st.markdown("##### Current Levels")
            df = pd.DataFrame(selected_assessment['levels'])
            
            for criterion_key in df['criterion_key'].unique():
                with st.expander(f"📌 {criterion_key}"):
                    criterion_df = df[df['criterion_key'] == criterion_key].sort_values('level_sequence')
                    st.dataframe(
                        criterion_df[['level_label', 'level_score', 'level_descriptor']],
                        use_container_width=True
                    )

        if is_edit_mode and not rubric['is_locked']:
            st.markdown("##### Add Level Descriptors")
            
            with st.form("add_levels_form"):
                criterion_key = st.selectbox(
                    "Criterion*",
                    options=[c['key'] for c in criteria_catalog] + ['custom']
                )
                
                if criterion_key == 'custom':
                    criterion_key = st.text_input("Custom Criterion Key*")
                
                criterion_weight = st.number_input(
                    "Criterion Weight %*",
                    min_value=0.0, max_value=100.0, value=25.0
                )
                
                num_levels = st.number_input(
                    "Number of Levels", min_value=2, max_value=6, value=4
                )
                
                levels_data = []
                for i in range(num_levels):
                    st.markdown(f"**Level {i+1}**")
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        label = st.text_input(
                            "Label", value=f"Level {i+1}",
                            key=f"level_label_{i}"
                        )
                    with col2:
                        score = st.number_input(
                            "Score", min_value=0.0, value=float(i+1),
                            key=f"level_score_{i}"
                        )
                    with col3:
                        descriptor = st.text_area(
                            "Descriptor", height=60,
                            key=f"level_desc_{i}"
                        )
                    
                    levels_data.append({
                        'criterion_key': criterion_key,
                        'criterion_weight_pct': criterion_weight,
                        'level_label': label,
                        'level_score': score,
                        'level_descriptor': descriptor,
                        'level_sequence': i
                    })

                submitted = st.form_submit_button("Add Levels", type="primary")

                if submitted:
                    if not criterion_key:
                        st.error("Criterion key required")
                    else:
                        try:
                            audit = get_audit_entry('add_levels')
                            app.rubrics.add_assessment_levels(
                                assessment_id, levels_data, audit
                            )
                            st.success("Levels added successfully")
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {str(e)}")


# --- Criteria Catalog Tab ---
def render_criteria_catalog_tab(app: SubjectsApplication):
    """Tab for managing global criteria catalog."""
    st.subheader("📚 Criteria Catalog")
    st.caption("Global reusable criteria for all rubrics")
    is_edit_mode = can_edit()

    catalog = app.rubrics.get_criteria_catalog(active_only=False)

    if catalog:
        df = pd.DataFrame(catalog)
        st.dataframe(
            df[['key', 'label', 'description', 'active']],
            use_container_width=True
        )
    else:
        st.info("No criteria in catalog")

    if is_edit_mode:
        st.markdown("---")
        st.markdown("#### Add New Criterion")

        with st.form("add_catalog_criterion"):
            col1, col2 = st.columns(2)
            
            with col1:
                key = st.text_input("Key*", placeholder="e.g., creativity")
                label = st.text_input("Label*", placeholder="e.g., Creativity")
            
            with col2:
                description = st.text_area("Description", height=80)

            submitted = st.form_submit_button("Add Criterion", type="primary")

            if submitted:
                if not key or not label:
                    st.error("Key and Label are required")
                else:
                    try:
                        criterion_id = app.rubrics.add_catalog_criterion(key, label, description)
                        st.success(f"Criterion added (ID: {criterion_id})")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {str(e)}")


# --- Validation & Preview Tab ---
def render_validation_tab(app: SubjectsApplication):
    """Tab for validating rubrics before publishing."""
    st.subheader("✅ Validation & Preview")

    offerings = fetch_published_offerings_for_rubrics()
    if not offerings:
        st.info("No published offerings found.")
        return

    offering_id = st.selectbox(
        "Select Offering*",
        options=[o['id'] for o in offerings],
        format_func=lambda x: get_offering_label(
            next(o for o in offerings if o['id'] == x)
        ),
        key="val_offering"
    )

    if not offering_id:
        return

    rubrics = app.rubrics.list_rubrics_for_offering(offering_id)
    if not rubrics:
        st.info("No rubrics found")
        return

    rubric_id = st.selectbox(
        "Select Rubric*",
        options=[r['id'] for r in rubrics],
        format_func=lambda x: f"{next(r['scope'] for r in rubrics if r['id'] == x)} - "
                             f"v{next(r['version'] for r in rubrics if r['id'] == x)}",
        key="val_rubric"
    )

    if not rubric_id:
        return
    
    st.markdown("---")
    
    if st.button("Validate Rubric", type="primary"):
        result = app.rubrics.validate_rubric_complete(rubric_id)
        
        st.markdown("#### Validation Results")
        
        if result['is_valid']:
            st.success("✅ Rubric is valid and ready to publish!")
        else:
            st.error("❌ Validation failed")
        
        if result.get('errors'):
            st.markdown("##### Errors")
            for error in result['errors']:
                st.error(f"• {error}")
        
        if result.get('warnings'):
            st.markdown("##### Warnings")
            for warning in result['warnings']:
                st.warning(f"• {warning}")

    st.markdown("---")
    st.markdown("#### Rubric Preview")
    
    selected_rubric = next((r for r in rubrics if r['id'] == rubric_id), None)
    
    if selected_rubric:
        complete_rubric = app.rubrics.get_rubric(
            selected_rubric['offering_id'],
            selected_rubric['scope'],
            selected_rubric.get('component_key')
        )

        if complete_rubric:
            st.json(complete_rubric, expanded=False)


# --- Audit Trail Tab ---
def render_audit_tab(app: SubjectsApplication):
    """Tab for viewing rubric audit trail."""
    st.subheader("📜 Audit Trail")

    offerings = fetch_published_offerings_for_rubrics()
    if not offerings:
        st.info("No published offerings found.")
        return

    offering_id = st.selectbox(
        "Select Offering*",
        options=[o['id'] for o in offerings],
        format_func=lambda x: get_offering_label(
            next(o for o in offerings if o['id'] == x)
        ),
        key="audit_offering"
    )

    if not offering_id:
        return

    audit_records = app.rubrics._fetch_all(
        """
        SELECT * FROM rubrics_audit
        WHERE offering_id = :offering_id
        ORDER BY rowid DESC
        LIMIT 100
        """,
        {"offering_id": offering_id},
    )
    if audit_records:
        df = pd.DataFrame(audit_records)
        display_cols = [
            'occurred_at_utc', 'action', 'scope', 
            'actor_id', 'operation', 'reason'
        ]
        st.dataframe(df[display_cols], use_container_width=True)
    else:
        st.info("No audit records found")


# --- Import/Export Tab ---
def render_rubric_import_export_tab(app: SubjectsApplication):
    """Tab for importing and exporting rubrics."""
    st.subheader("🔄 Import / Export Rubrics")
    
    try:
        importer = RubricsImportExport(app.rubrics)
    except Exception as e:
        st.error(f"Failed to initialize Import/Export service: {e}")
        return

    is_edit_mode = can_edit()

    st.markdown("#### 1. Export")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Export Rubric**")
        offerings = fetch_published_offerings_for_rubrics()
        if not offerings:
            st.info("No published offerings found.")
            return

        offering_id_export = st.selectbox(
            "Select Offering*",
            options=[o['id'] for o in offerings],
            format_func=lambda x: get_offering_label(next(o for o in offerings if o['id'] == x)),
            key="export_rubric_offering"
        )
        
        if offering_id_export:
            rubrics = app.rubrics.list_rubrics_for_offering(offering_id_export)
            if rubrics:
                rubric_id_export = st.selectbox(
                    "Select Rubric to Export*",
                    options=[r['id'] for r in rubrics],
                    format_func=lambda x: f"{next(r['scope'] for r in rubrics if r['id'] == x)} - v{next(r['version'] for r in rubrics if r['id'] == x)}",
                    key="export_rubric_id"
                )
                
                if st.button("Generate Export Data"):
                    try:
                        csv_data = importer.export_rubric_to_csv(rubric_id_export)
                        st.download_button(
                            label="Download Rubric as CSV",
                            data=csv_data,
                            file_name=f"rubric_{rubric_id_export}.csv",
                            mime="text/csv"
                        )
                    except Exception as e:
                        st.error(f"Export failed: {e}")
            else:
                st.info("No rubrics for this offering.")

    with col2:
        st.markdown("**Export Criteria Catalog**")
        if st.button("Generate Catalog Data"):
            try:
                catalog_data = importer.export_criteria_catalog_csv()
                st.download_button(
                    label="Download Catalog as CSV",
                    data=catalog_data,
                    file_name="criteria_catalog.csv",
                    mime="text/csv"
                )
            except Exception as e:
                st.error(f"Catalog export failed: {e}")

    st.markdown("---")
    st.markdown("#### 2. Import")
    
    if not is_edit_mode:
        st.info("You do not have permission to import data.")
        return

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Import Rubric**")
        offering_id_import = st.selectbox(
            "Select Offering*",
            options=[o['id'] for o in offerings],
            format_func=lambda x: get_offering_label(next(o for o in offerings if o['id'] == x)),
            key="import_rubric_offering"
        )
        
        if offering_id_import:
            rubrics = app.rubrics.list_rubrics_for_offering(offering_id_import)
            if rubrics:
                rubric_id_import = st.selectbox(
                    "Select Rubric Config to Import Into*",
                    options=[r['id'] for r in rubrics],
                    format_func=lambda x: f"{next(r['scope'] for r in rubrics if r['id'] == x)} - v{next(r['version'] for r in rubrics if r['id'] == x)}",
                    key="import_rubric_id"
                )
                
                selected_rubric = app.rubrics.get_rubric_config(rubric_id_import)
                st.info(f"Import mode: `{selected_rubric['mode']}`")
                
                uploaded_file = st.file_uploader("Upload Rubric CSV", key="import_rubric_file")
                
                if uploaded_file:
                    try:
                        csv_content = StringIO(uploaded_file.getvalue().decode("utf-8")).read()
                        is_valid, errors, df = importer.preview_import(csv_content, selected_rubric['mode'])
                        
                        st.markdown("##### Import Preview")
                        st.dataframe(df, height=150)
                        
                        if not is_valid:
                            st.error("Validation failed:")
                            for err in errors:
                                st.error(f"- {err}")
                        else:
                            st.success("✅ CSV is valid and ready to import.")
                            if st.button("Confirm Import Rubric", type="primary"):
                                actor_email = get_audit_entry().actor_id
                                result = importer.import_rubric_from_csv(
                                    csv_content, rubric_id_import,
                                    selected_rubric['mode'], actor_email
                                )
                                
                                if result['success']:
                                    st.success(f"Successfully imported {result['assessments_created']} assessments!")
                                    st.cache_data.clear()
                                    st.rerun()
                                else:
                                    st.error("Import failed:")
                                    for err in result['errors']:
                                        st.error(f"- {err}")
                    except Exception as e:
                        st.error(f"Error processing file: {e}")
            else:
                st.info("No rubric configurations found for this offering.")
    
    with col2:
        st.markdown("**Import Criteria Catalog**")
        catalog_file = st.file_uploader("Upload Catalog CSV", key="import_catalog_file")
        
        if catalog_file:
            if st.button("Confirm Import Catalog", type="primary"):
                try:
                    csv_content = StringIO(catalog_file.getvalue().decode("utf-8")).read()
                    result = importer.import_criteria_catalog_csv(csv_content)
                    
                    if result['success']:
                        st.success(f"Successfully added {result['criteria_added']} criteria.")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("Catalog import failed:")
                        for err in result['errors']:
                            st.error(f"- {err}")
                except Exception as e:
                    st.error(f"Error processing file: {e}")

    st.markdown("---")
    st.markdown("#### 3. Download Templates")
    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            label="Download Analytic Points Template",
            data=importer.export_template_analytic_points(),
            file_name="template_analytic_points.csv",
            mime="text/csv"
        )
    with col2:
        st.download_button(
            label="Download Analytic Levels Template",
            data=importer.export_template_analytic_levels(),
            file_name="template_analytic_levels.csv",
            mime="text/csv"
        )


# ===========================================================================
# MAIN FUNCTION (Modified)
# ===========================================================================

def main():
    """Main page function."""
    st.title("📖 Subjects, COs & Rubrics Management")
    
    tabs = ["Subjects Catalog", "Subject Offerings", "Course Outcomes", "Rubrics Management"]
    
    if "selected_tab" not in st.session_state:
        st.session_state.selected_tab = "Subjects Catalog"
    
    try:
        default_index = tabs.index(st.session_state.selected_tab)
    except ValueError:
        default_index = 0

    tab1, tab2, tab3, tab4 = st.tabs(tabs)

    with tab1:
        st.session_state.selected_tab = "Subjects Catalog"
        render_subjects_catalog_tab()

    with tab2:
        st.session_state.selected_tab = "Subject Offerings"
        render_offerings_tab()

    with tab3:
        st.session_state.selected_tab = "Course Outcomes"
        render_cos_tab()

    with tab4:
        st.session_state.selected_tab = "Rubrics Management"
        st.header("📋 Rubrics Management")
        
        app = init_subjects_app()
        if not can_edit():
            st.info("📖 Read-only mode: You have view access but cannot modify data.")

        sub_tab1, sub_tab2, sub_tab3, sub_tab4, sub_tab5, sub_tab6, sub_tab7 = st.tabs([
            "Configuration",
            "Assessments",
            "Criteria & Levels",
            "Catalog",
            "Validation",
            "Audit Trail",
            "Import/Export"
        ])
        
        with sub_tab1:
            render_rubric_config_tab(app)
        with sub_tab2:
            render_assessments_tab(app)
        with sub_tab3:
            render_criteria_editor_tab(app)
        with sub_tab4:
            render_criteria_catalog_tab(app)
        with sub_tab5:
            render_validation_tab(app)
        with sub_tab6:
            render_audit_tab(app)
        with sub_tab7:
            render_rubric_import_export_tab(app)


if __name__ == "__main__":
    if "engine" not in st.session_state:
        st.error("No database engine. Run from main app.")
    else:
        main()
