# screens/subject_cos_rubrics/subject_catalog_tab.py
"""
Subject Catalog Tab - FIXED VERSION
Handles all degree cohort structures properly
Eliminates NaN display issues
"""

import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from typing import List, Dict, Optional
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
    """Fetch the year and term counts based on the semester binding mode."""
    default_structure = {"years": 10, "terms_per_year": 4}
    
    with engine.begin() as conn:
        binding = conn.execute(sa_text("""
            SELECT binding_mode FROM semester_binding
            WHERE degree_code = :degree_code
        """), {"degree_code": degree_code}).fetchone()
        
        binding_mode = binding._mapping['binding_mode'] if binding else 'degree'
        structure = None
        
        try:
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

def fetch_published_offerings(engine: Engine, filters: Dict) -> List[Dict]:
    """Fetch published subject offerings based on filters - FIXED."""
    query = """
        SELECT 
            so.id, so.subject_code, sc.subject_name, so.subject_type,
            so.is_elective_parent, so.ay_label, so.year, so.term,
            so.degree_code, so.program_code, so.branch_code,
            so.curriculum_group_code, so.division_code, so.applies_to_all_divisions,
            so.credits_total, so.L, so.T, so.P, so.S,
            so.internal_marks_max, so.exam_marks_max, so.jury_viva_marks_max, so.total_marks_max,
            so.pass_threshold_internal, so.pass_threshold_external, so.pass_threshold_overall,
            sc.direct_source_mode, sc.direct_internal_threshold_percent, sc.direct_external_threshold_percent,
            sc.direct_internal_weight_percent, sc.direct_external_weight_percent,
            sc.direct_target_students_percent, sc.indirect_target_students_percent,
            sc.indirect_min_response_rate_percent, sc.overall_direct_weight_percent,
            sc.overall_indirect_weight_percent,
            so.instructor_email, so.syllabus_template_id, so.status,
            so.created_at, so.updated_at
        FROM subject_offerings so
        LEFT JOIN subjects_catalog sc ON 
            so.subject_code = sc.subject_code 
            AND so.degree_code = sc.degree_code
            AND (so.program_code = sc.program_code OR sc.program_code IS NULL)
            AND (so.branch_code = sc.branch_code OR sc.branch_code IS NULL)
        WHERE so.status = 'published'
        AND so.degree_code = :degree_code
        AND so.ay_label = :ay_label
    """
    
    params = {
        "degree_code": filters["degree_code"],
        "ay_label": filters["ay_label"]
    }
    
    if filters.get("year"):
        query += " AND so.year = :year"
        params["year"] = filters["year"]
    
    if filters.get("term"):
        query += " AND so.term = :term"
        params["term"] = filters["term"]
    
    # FIXED: Handle NULL/empty properly
    program_code = filters.get("program_code")
    if is_valid(program_code):
        query += " AND (so.program_code = :program_code OR so.program_code IS NULL OR so.program_code = '')"
        params["program_code"] = program_code
    
    branch_code = filters.get("branch_code")
    if is_valid(branch_code):
        query += " AND (so.branch_code = :branch_code OR so.branch_code IS NULL OR so.branch_code = '')"
        params["branch_code"] = branch_code
    
    cg_code = filters.get("curriculum_group_code")
    if is_valid(cg_code):
        query += " AND (so.curriculum_group_code = :cg_code OR so.curriculum_group_code IS NULL OR so.curriculum_group_code = '')"
        params["cg_code"] = cg_code

    query += " ORDER BY so.year, so.term, so.subject_code"
    
    with engine.begin() as conn:
        result = conn.execute(sa_text(query), params)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df.to_dict('records')

def fetch_syllabus_template(engine: Engine, template_id: Optional[int]) -> Optional[Dict]:
    """Fetch syllabus template details."""
    if not template_id:
        return None
    
    with engine.begin() as conn:
        result = conn.execute(sa_text("""
            SELECT id, code, name, version, description, effective_from_ay, deprecated_from_ay
            FROM syllabus_templates
            WHERE id = :template_id
        """), {"template_id": template_id})
        
        row = result.fetchone()
        if row:
            template = dict(row._mapping)
            points_result = conn.execute(sa_text("""
                SELECT sequence, title, description, hours_weight, learning_outcomes, assessment_methods
                FROM syllabus_template_points
                WHERE template_id = :template_id
                ORDER BY sequence
            """), {"template_id": template_id})
            
            template["points"] = [dict(p._mapping) for p in points_result]
            return template
    
    return None

def fetch_elective_topics(engine: Engine, offering: Dict) -> List[Dict]:
    """Fetch elective topics - FIXED."""
    query = """
        SELECT topic_no, topic_code_ay, topic_name, owner_faculty_email, capacity, description
        FROM elective_topics
        WHERE subject_code = :subject_code
        AND ay_label = :ay_label
        AND year = :year
        AND term = :term
        AND degree_code = :degree_code
    """
    params = {
        "subject_code": offering['subject_code'],
        "ay_label": offering['ay_label'],
        "year": offering['year'],
        "term": offering['term'],
        "degree_code": offering['degree_code'],
    }
    
    # Handle optional scope fields
    program_code = offering.get('program_code')
    if is_valid(program_code):
        query += " AND (program_code = :program_code OR program_code IS NULL OR program_code = '')"
        params['program_code'] = program_code
    else:
        query += " AND (program_code IS NULL OR program_code = '')"

    branch_code = offering.get('branch_code')
    if is_valid(branch_code):
        query += " AND (branch_code = :branch_code OR branch_code IS NULL OR branch_code = '')"
        params['branch_code'] = branch_code
    else:
        query += " AND (branch_code IS NULL OR branch_code = '')"

    division_code = offering.get('division_code')
    if is_valid(division_code):
        query += " AND (division_code = :division_code OR division_code IS NULL OR division_code = '')"
        params['division_code'] = division_code
    else:
        query += " AND (division_code IS NULL OR division_code = '')"
    
    query += " ORDER BY topic_no"
    
    with engine.begin() as conn:
        result = conn.execute(sa_text(query), params)
        return [dict(row._mapping) for row in result]

# ===========================================================================
# UI RENDERING FUNCTIONS
# ===========================================================================

def render_filters(engine: Engine) -> Optional[Dict]:
    """Render filter controls and return selected filters."""
    st.markdown("### 🔍 Filter Subjects")
    
    col1, col2 = st.columns(2)
    
    with col1:
        degrees = fetch_degrees(engine)
        if not degrees:
            st.warning("No degrees found in the system.")
            return None
        
        degree_options = {f"{d['code']} - {d['title']}": d['code'] for d in degrees}
        selected_degree_label = st.selectbox("Degree", options=list(degree_options.keys()), key="subject_catalog_degree")
        degree_code = degree_options[selected_degree_label]
        
        academic_years = fetch_academic_years(engine)
        if not academic_years:
            st.warning("No academic years found.")
            return None
        
        ay_options = {ay['ay_code']: ay['ay_code'] for ay in academic_years}
        selected_ay = st.selectbox("Academic Year", options=list(ay_options.keys()), key="subject_catalog_ay")
    
    program_code = None
    program_id = None
    branch_code = None
    branch_id = None
    cg_code = None

    with col2:
        programs = fetch_programs_by_degree(engine, degree_code)
        if programs:
            program_options = { "All Programs": (None, None) }
            program_options.update({f"{p['code']} - {p['name']}": (p['code'], p['id']) for p in programs})
            selected_program_label = st.selectbox("Program (Optional)", options=list(program_options.keys()), key="subject_catalog_program")
            program_code, program_id = program_options[selected_program_label]
        
        if program_id:
            branches = fetch_branches_by_program(engine, degree_code, program_id)
            if branches:
                branch_options = { "All Branches": (None, None) }
                branch_options.update({f"{b['code']} - {b['name']}": (b['code'], b['id']) for b in branches})
                selected_branch_label = st.selectbox("Branch (Optional)", options=list(branch_options.keys()), key="subject_catalog_branch")
                branch_code, branch_id = branch_options[selected_branch_label]
    
    # Curriculum Groups
    cgs = fetch_curriculum_groups_by_degree(engine, degree_code)
    if cgs:
        cg_options = { "All Curriculum Groups": None }
        cg_options.update({f"{cg['code']} - {cg['name']}": cg['code'] for cg in cgs})
        selected_cg_label = st.selectbox("Curriculum Group (Optional)", options=list(cg_options.keys()), key="subject_catalog_cg")
        cg_code = cg_options[selected_cg_label]

    structure = fetch_semester_structure(engine, degree_code, program_id, branch_id)
    max_years = structure.get('years', 10)
    max_terms = structure.get('terms_per_year', 4)

    col3, col4 = st.columns(2)
    
    with col3:
        year = st.selectbox("Year (Optional)", options=["All Years"] + list(range(1, max_years + 1)), key="subject_catalog_year")
        year = year if year != "All Years" else None
    
    with col4:
        term = st.selectbox("Term (Optional)", options=["All Terms"] + list(range(1, max_terms + 1)), key="subject_catalog_term")
        term = term if term != "All Terms" else None
    
    return {
        "degree_code": degree_code,
        "ay_label": selected_ay,
        "year": year,
        "term": term,
        "program_code": program_code,
        "branch_code": branch_code,
        "curriculum_group_code": cg_code
    }

def render_offering_card(offering: Dict, engine: Engine):
    """Render offering card - FIXED to eliminate NaN."""
    
    with st.container():
        st.markdown("---")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown(f"### {offering['subject_code']} - {offering['subject_name']}")
            
            # Build scope text WITHOUT NaN
            scope_parts = [f"**Year {offering['year']}, Term {offering['term']}**"]
            secondary_parts = []
            
            if is_valid(offering.get('program_code')):
                secondary_parts.append(offering['program_code'])
            if is_valid(offering.get('branch_code')):
                secondary_parts.append(offering['branch_code'])
            
            if secondary_parts:
                scope_parts.append("| " + " / ".join(secondary_parts))
            
            if is_valid(offering.get('division_code')):
                scope_parts.append(f"/ Div {offering['division_code']}")
            elif offering.get('applies_to_all_divisions'):
                scope_parts.append("/ All Divisions")
            
            scope_text = " ".join(scope_parts)
            st.markdown(scope_text)
            
            if is_valid(offering.get('curriculum_group_code')):
                st.markdown(f"**Curriculum Group:** {offering['curriculum_group_code']}")
        
        with col2:
            st.markdown(f"**Type:** {offering['subject_type']}")
            if is_valid(offering.get('instructor_email')):
                st.markdown(f"**Instructor:** {offering['instructor_email']}")
        
        # Three column layout
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("#### 📚 Semester & Credits")
            st.markdown(f"**Semester:** {offering['year'] * 2 - (2 - offering['term'])}")
            st.markdown(f"**Total Credits:** {offering['credits_total']}")
            workload = f"{int(offering.get('L', 0))}-{int(offering.get('T', 0))}-{int(offering.get('P', 0))}-{int(offering.get('S', 0))}"
            st.markdown(f"**Workload (L/T/P/S):** {workload}")
            
            if offering.get('is_elective_parent') == 1:
                st.info("🎯 Elective Parent: All topics inherit these credits")
        
        with col2:
            st.markdown("#### 📊 Assessment (Max Marks)")
            st.markdown(f"**Maximum Internal Marks:** {offering['internal_marks_max']}")
            st.markdown(f"**Maximum External Marks (Exam):** {offering['exam_marks_max']}")
            st.markdown(f"**Maximum External Marks (Jury/Viva):** {offering['jury_viva_marks_max']}")
            st.markdown(f"**TOTAL MARKS:** {offering['total_marks_max']}")
        
        with col3:
            st.markdown("#### ✅ Passing Threshold")
            st.markdown(f"**Minimum Internal Passing %:** {offering.get('pass_threshold_internal', 0)}%")
            st.markdown(f"*Must score ≥ {offering.get('pass_threshold_internal', 0)}% of {offering.get('internal_marks_max', 0)} marks*")
            
            st.markdown(f"**Minimum External Passing %:** {offering.get('pass_threshold_external', 0)}%")
            external_marks_total = (offering.get('exam_marks_max', 0) or 0) + (offering.get('jury_viva_marks_max', 0) or 0)
            st.markdown(f"*Must score ≥ {offering.get('pass_threshold_external', 0)}% of {external_marks_total} marks*")
            
            st.markdown(f"**Minimum Overall Passing %:** {offering.get('pass_threshold_overall', 0)}%")
            st.markdown(f"*Must score ≥ {offering.get('pass_threshold_overall', 0)}% of {offering.get('total_marks_max', 0)} marks*")
        
        # Attainment Requirements
        if any([offering.get('direct_source_mode'), offering.get('overall_direct_weight_percent'), offering.get('overall_indirect_weight_percent')]):
            st.markdown("#### 🎯 Attainment Requirements (optional)")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("##### Direct Attainment")
                source_mode = offering.get('direct_source_mode', 'overall')
                st.markdown(f"**Source:** {source_mode.replace('_', ' ').title()}")
                
                if source_mode == 'split':
                    st.markdown(f"• **Internal Threshold:** {offering.get('direct_internal_threshold_percent', 50.0)}%")
                    st.markdown(f"• **External Threshold:** {offering.get('direct_external_threshold_percent', 40.0)}%")
                
                target = offering.get('direct_target_students_percent', 50.0)
                st.markdown(f"• **Target:** {target}% of students should attain")
            
            with col2:
                st.markdown("##### Overall Attainment")
                direct_weight = offering.get('overall_direct_weight_percent', 80.0)
                indirect_weight = offering.get('overall_indirect_weight_percent', 20.0)
                
                st.markdown(f"**Direct Weight:** {direct_weight}%")
                st.markdown(f"**Indirect Weight:** {indirect_weight}%")
                st.markdown(f"*Final = {direct_weight}% × Direct + {indirect_weight}% × Indirect*")
            
            st.markdown("##### Indirect Attainment")
            col1, col2 = st.columns(2)
            with col1:
                indirect_target = offering.get('indirect_target_students_percent', 50.0)
                st.markdown(f"• **Target:** {indirect_target}% of students")
            with col2:
                min_response = offering.get('indirect_min_response_rate_percent', 75.0)
                st.markdown(f"• **Min Response Rate:** {min_response}%")
        
        # Syllabus Template
        if is_valid(offering.get('syllabus_template_id')):
            with st.expander("📄 Syllabus Template"):
                template = fetch_syllabus_template(engine, int(offering['syllabus_template_id']))
                if template:
                    st.markdown(f"**Template Code:** {template['code']}")
                    st.markdown(f"**Version:** {template['version']}")
                    st.markdown(f"**Name:** {template['name']}")
                    if template.get('description'):
                        st.markdown(f"**Description:** {template['description']}")
                    
                    if template.get('points'):
                        st.markdown("**Syllabus Points:**")
                        for point in template['points']:
                            st.markdown(f"{point['sequence']}. **{point['title']}**")
                            if point.get('description'):
                                st.markdown(f"   {point['description']}")
                            if point.get('hours_weight'):
                                st.markdown(f"   *Hours: {point['hours_weight']}*")
                else:
                    st.info("Template details not found")

        # Elective Topics
        is_elective = offering.get('is_elective_parent') in (1, True, '1', 'true', 'True')
        if not is_elective and offering.get('subject_type', '').lower() == 'elective':
            is_elective = True
        
        if is_elective:
            st.markdown("---")
            st.markdown("#### 🎯 Elective Topics")
            
            try:
                with st.spinner("Loading elective topics..."):
                    topics = fetch_elective_topics(engine, offering)
                
                if not topics:
                    st.info("ℹ️ No elective topics have been created for this subject yet.")
                    st.caption("Topics should be added in the Elective Topics management section.")
                else:
                    st.success(f"✅ {len(topics)} elective topic(s) available")
                    
                    topics_df = pd.DataFrame(topics)
                    column_order = ['topic_no', 'topic_code_ay', 'topic_name', 'owner_faculty_email', 'capacity']
                    if 'description' in topics_df.columns:
                        column_order.append('description')
                    
                    display_df = topics_df[column_order]
                    display_df = display_df.rename(columns={
                        'topic_no': 'Topic #',
                        'topic_code_ay': 'Topic Code',
                        'topic_name': 'Topic Name',
                        'owner_faculty_email': 'Faculty',
                        'capacity': 'Capacity',
                        'description': 'Description'
                    })
                    
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
            except Exception as e:
                st.error(f"❌ Error loading elective topics: {e}")
                logger.error(f"Error fetching elective topics: {e}", exc_info=True)

def render_subject_catalog_tab(engine: Engine):
    """Main render function for Subject Catalog tab."""
    
    st.markdown("""
    This tab displays published subject offerings per degree cohorts per Academic Year per term.
    You can view semester information, credits, assessment configuration, passing thresholds,
    and attainment requirements for each subject.
    """)
    
    filters = render_filters(engine)
    
    if not filters:
        return
    
    if st.button("🔍 Load Subjects", type="primary", use_container_width=True):
        with st.spinner("Loading published subjects..."):
            try:
                offerings = fetch_published_offerings(engine, filters)
                
                if not offerings:
                    st.warning("No published subjects found for the selected filters.")
                    st.session_state.catalog_offerings = []
                    return
                
                st.success(f"✅ Found {len(offerings)} published subject(s)")
                st.session_state.catalog_offerings = offerings
                
            except Exception as e:
                st.error(f"❌ Error loading subjects: {e}")
                logger.error(f"Error in fetch_published_offerings: {e}", exc_info=True)
                st.session_state.catalog_offerings = []
                return
    
    if "catalog_offerings" in st.session_state:
        offerings = st.session_state.catalog_offerings
        
        if offerings:
            st.markdown(f"### 📋 Subjects ({len(offerings)} found)")
            
            # Build options WITHOUT NaN
            options = { "--- Select a Subject to View Details ---": None }
            for o in offerings:
                label_parts = [
                    f"{o['subject_code']} - {o['subject_name']}",
                    f"(Y{o['year']}, T{o['term']})"
                ]
                
                scope_parts = []
                if is_valid(o.get('program_code')):
                    scope_parts.append(o['program_code'])
                if is_valid(o.get('branch_code')):
                    scope_parts.append(o['branch_code'])
                if is_valid(o.get('curriculum_group_code')):
                    scope_parts.append(f"CG:{o['curriculum_group_code']}")
                
                if scope_parts:
                    label_parts.insert(1, " / ".join(scope_parts))
                
                label = " ".join(label_parts)
                options[label] = o['id']
            
            selected_label = st.selectbox("Select a Subject", options=list(options.keys()), index=0, key="subject_catalog_selector")
            
            if options[selected_label] is not None:
                selected_id = options[selected_label]
                try:
                    selected_offering = next(o for o in offerings if o['id'] == selected_id)
                    render_offering_card(selected_offering, engine)
                except StopIteration:
                    st.error("Selected subject not found in the loaded list. Please reload.")
            
        elif st.session_state.get('catalog_offerings') == []:
            st.info("No published subjects found for the selected filters.")
