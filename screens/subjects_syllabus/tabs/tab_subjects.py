"""
Subjects Catalog Tab - Display and create subjects at degree/program/branch level
UPDATED: Fixed UI labels, removed cancel button, improved user experience
"""

import streamlit as st
import pandas as pd
import json
from typing import Optional, List, Dict, Any

# --- 1. IMPORTS ---
from screens.subjects_syllabus.db_helpers import (
    fetch_degrees, fetch_programs, fetch_branches,
    fetch_curriculum_groups, fetch_academic_years, fetch_subjects
)
from screens.subjects_syllabus.subjects_crud import create_subject, update_subject, delete_subject
from core.forms import success
from screens.subjects_syllabus.constants import DEFAULT_SUBJECT_TYPES
from screens.subjects_syllabus.helpers import table_exists, exec_query, rows_to_dicts


# --- 2. CALLBACKS ---
def _add_workload_row():
    if 'subject_workload_components' not in st.session_state:
        st.session_state.subject_workload_components = []
    next_id = 1
    if st.session_state.subject_workload_components:
        next_id = max(item['id'] for item in st.session_state.subject_workload_components) + 1
    st.session_state.subject_workload_components.append(
        {"id": next_id, "Code": "", "Name": "", "Hours": 0.0}
    )

def _delete_workload_row(row_id: int):
    if 'subject_workload_components' in st.session_state:
        st.session_state.subject_workload_components = [
            item for item in st.session_state.subject_workload_components
            if item['id'] != row_id
        ]

def _add_edit_workload_row():
    """Add a new workload row in the edit form"""
    if 'edit_workload_components' not in st.session_state:
        st.session_state.edit_workload_components = []
    st.session_state.edit_workload_components.append(
        {"code": "", "name": "", "hours": 0.0}
    )

def _delete_edit_workload_row(idx: int):
    """Delete a workload row from the edit form"""
    if 'edit_workload_components' in st.session_state:
        if 0 <= idx < len(st.session_state.edit_workload_components):
            st.session_state.edit_workload_components.pop(idx)

# --- 3. FETCH ---
@st.cache_data(ttl=300)
def fetch_semesters_for_form(
    _engine, degree_code: str
) -> List[Dict[str, Any]]:
    with _engine.begin() as conn:
        rows = exec_query(conn, """
            SELECT id, label, semester_number
            FROM semesters
            WHERE degree_code = :dc AND program_id IS NULL AND branch_id IS NULL AND active = 1
            ORDER BY semester_number
        """, {"dc": degree_code}).fetchall()
    return rows_to_dicts(rows)

# --- 4. EDIT STATE HELPERS ---
def _set_edit_state(subject: Dict[str, Any]):
    """Loads a subject's data into session state for the edit form."""
    st.session_state.edit_subject_id = subject['id']
    st.session_state.edit_subject_code = subject['subject_code']
    st.session_state.edit_subject_name = subject['subject_name']
    st.session_state.edit_subject_type = subject['subject_type']
    st.session_state.edit_semester_id = subject['semester_id']
    
    st.session_state.edit_program_code = subject['program_code']
    st.session_state.edit_branch_code = subject['branch_code']
    st.session_state.edit_cg_code = subject['curriculum_group_code']
    
    st.session_state.edit_credits_total = subject['credits_total']
    st.session_state.edit_student_credits = subject.get('student_credits', subject['credits_total'])
    st.session_state.edit_teaching_credits = subject.get('teaching_credits', subject['credits_total'])
    
    st.session_state.edit_internal_marks = subject['internal_marks_max']
    st.session_state.edit_exam_marks = subject['exam_marks_max']
    st.session_state.edit_jury_marks = subject['jury_viva_marks_max']
    
    st.session_state.edit_min_internal = subject['min_internal_percent']
    st.session_state.edit_min_external = subject['min_external_percent']
    st.session_state.edit_min_overall = subject['min_overall_percent']
    
    st.session_state.edit_direct_source_mode = subject['direct_source_mode']
    st.session_state.edit_diw = subject['direct_internal_weight_percent']
    st.session_state.edit_dts = subject['direct_target_students_percent']
    st.session_state.edit_imr = subject['indirect_min_response_rate_percent']
    
    st.session_state.edit_description = subject.get('description', '')
    st.session_state.edit_active = bool(subject['active'])
    st.session_state.edit_sort_order = subject['sort_order']
    
    if subject.get('workload_breakup_json'):
        try:
            workload = json.loads(subject['workload_breakup_json'])
            st.session_state.edit_workload_components = [
                {**item, 'id': i+1} for i, item in enumerate(workload)
            ]
        except:
            st.session_state.edit_workload_components = []
    else:
        st.session_state.edit_workload_components = []

def _clear_edit_state():
    """Clears the edit state without touching the dropdown selection."""
    keys_to_del = [
        'edit_subject_id', 'edit_subject_code', 'edit_subject_name',
        'edit_subject_type', 'edit_semester_id', 
        'edit_program_code', 'edit_branch_code', 'edit_cg_code',
        'edit_credits_total',
        'edit_student_credits', 'edit_teaching_credits', 'edit_internal_marks',
        'edit_exam_marks', 'edit_jury_marks', 'edit_min_internal',
        'edit_min_external', 'edit_min_overall', 'edit_direct_source_mode',
        'edit_diw', 'edit_dts', 'edit_imr',
        'edit_description', 'edit_active', 'edit_sort_order',
        'edit_workload_components'
    ]
    for key in keys_to_del:
        if key in st.session_state:
            del st.session_state[key]

def _reset_subject_selection():
    """Resets the subject selection dropdown - call this before rerun."""
    if 'subject_to_edit_select' in st.session_state:
        del st.session_state['subject_to_edit_select']


# --- 5. RENDER FUNCTION ---
def render(engine, actor: str, CAN_EDIT: bool):
    """Render the Subjects Catalog tab."""

    st.subheader("Subjects Catalog")
    st.caption("Manage subjects at degree/program/branch level")

    # Initialize workload components only if not already set
    if 'subject_workload_components' not in st.session_state:
        st.session_state.subject_workload_components = [
            {"id": 1, "Code": "L", "Name": "Lectures", "Hours": 3.0},
            {"id": 2, "Code": "T", "Name": "Theory / Tutorials", "Hours": 0.0},
            {"id": 3, "Code": "P", "Name": "Practicals", "Hours": 2.0},
            {"id": 4, "Code": "S", "Name": "Studios", "Hours": 0.0},
        ]
    
    # Flag to track successful creation
    if 'subject_created_success' not in st.session_state:
        st.session_state.subject_created_success = False

    # --- PRE-FLIGHT CHECKS ---
    REQUIRED_TABLES = { "degrees": "Degrees", "programs": "Programs", "branches": "Branches", "semesters": "Semesters" }
    missing_tables_info = []
    try:
        with engine.begin() as conn:
            for table_name, page_name in REQUIRED_TABLES.items():
                if not table_exists(conn, table_name):
                    missing_tables_info.append(f"`{table_name}` (from **{page_name}** page)")
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return
    if missing_tables_info:
        error_list = "\n".join(f"* {info}" for info in missing_tables_info)
        st.error(f"**Database not fully initialized.**\n\nMissing tables:\n\n{error_list}")
        return
    # --- END OF CHECKS ---

    with engine.begin() as conn:
        degrees = fetch_degrees(engine)
    if not degrees:
        st.warning("No degrees found. Create degrees first.")
        return

    # --- TOP-LEVEL FILTERS ---
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        degree_code = st.selectbox(
            "Degree",
            options=[d["code"] for d in degrees],
            format_func=lambda x: next((d["title"] for d in degrees if d["code"] == x), x),
            key="subcat_degree"
        )
    degree = next((d for d in degrees if d["code"] == degree_code), None)
    
    top_filter_program_code, top_filter_branch_code, top_filter_cg_code = None, None, None
    programs_for_form, branches_for_form, cgs_for_form = [], [], []
    semesters_for_dropdown = []
    prog_disabled, branch_disabled, cg_disabled = True, True, True

    if degree:
        cohort_mode = degree.get("cohort_splitting_mode", "none")
        programs_for_form = fetch_programs(engine, degree_code)
        
        prog_disabled = cohort_mode not in ["both", "program_only"]
        branch_disabled = cohort_mode not in ["both", "branch_only"]
        cg_disabled = not (degree.get("cg_degree") or degree.get("cg_program") or degree.get("cg_branch"))
        
        if not prog_disabled:
            with col2:
                top_filter_program_code = st.selectbox(
                    "Filter by Program",
                    options=[None] + [p["program_code"] for p in programs_for_form],
                    format_func=lambda x: "All Programs" if x is None else next((p["program_name"] for p in programs_for_form if p["program_code"] == x), x),
                    key="subcat_program"
                )
        
        branches_for_form = fetch_branches(engine, degree_code, top_filter_program_code)
        if not branch_disabled:
            with col3:
                top_filter_branch_code = st.selectbox(
                    "Filter by Branch",
                    options=[None] + [b["branch_code"] for b in branches_for_form],
                    format_func=lambda x: "All Branches" if x is None else next((b["branch_name"] for b in branches_for_form if b["branch_code"] == x), x),
                    key="subcat_branch"
                )
        
        cgs_for_form = fetch_curriculum_groups(engine, degree_code, top_filter_program_code, top_filter_branch_code)
        if not cg_disabled:
            with col4:
                top_filter_cg_code = st.selectbox(
                    "Filter by Curric. Group",
                    options=[None] + [g["group_code"] for g in cgs_for_form],
                    format_func=lambda x: "All Groups" if x is None else next((g["group_name"] for g in cgs_for_form if g["group_code"] == x), x),
                    key="subcat_cg"
                )
    
    if degree_code:
        semesters_for_dropdown = fetch_semesters_for_form(engine, degree_code)

    st.markdown("---")

    # --- 6. CREATE SUBJECT FORM ---
    if CAN_EDIT:
        # Show success message if subject was just created
        if st.session_state.subject_created_success:
            st.success("✅ Subject created successfully!")
            st.session_state.subject_created_success = False
        
        with st.expander("📝 Create New Subject"):
            with st.form("subject_form"):
                col1, col2, col3 = st.columns(3)
                with col1: subject_code = st.text_input("Subject Code*", placeholder="e.g., CS101").upper()
                with col2: subject_name = st.text_input("Subject Name*", placeholder="e.g., Data Structures")
                with col3: subject_type = st.selectbox("Subject Type*", options=DEFAULT_SUBJECT_TYPES)

                st.markdown("**Academic Assignment**")
                col1, col2 = st.columns(2)
                with col1:
                    semester_options = {s['id']: f"Sem {s['semester_number']} - {s['label']}" for s in semesters_for_dropdown}
                    semester_id = st.selectbox(
                        "Assign to Semester*",
                        options=[None] + list(semester_options.keys()),
                        format_func=lambda x: "Select a semester..." if x is None else semester_options.get(x, f"ID {x}"),
                        key="subject_semester_id",
                    )
                with col2:
                    form_cg_code = st.selectbox(
                        "Assign to Curriculum Group (optional)",
                        options=[None] + [g["group_code"] for g in cgs_for_form],
                        format_func=lambda x: "No Group" if x is None else next((g["group_name"] for g in cgs_for_form if g["group_code"] == x), x),
                        key="form_cg_code",
                        disabled=cg_disabled
                    )
                
                col1, col2 = st.columns(2)
                with col1:
                    form_program_code = st.selectbox(
                        "Assign to Program (optional)",
                        options=[None] + [p["program_code"] for p in programs_for_form],
                        format_func=lambda x: "Degree-Level" if x is None else next((p["program_name"] for p in programs_for_form if p["program_code"] == x), x),
                        key="form_program_code",
                        disabled=prog_disabled
                    )
                with col2:
                    form_branch_code = st.selectbox(
                        "Assign to Branch (optional)",
                        options=[None] + [b["branch_code"] for b in branches_for_form],
                        format_func=lambda x: "Program-Level" if x is None else next((b["branch_name"] for b in branches_for_form if b["branch_code"] == x), x),
                        key="form_branch_code",
                        disabled=branch_disabled
                    )
                
                st.markdown("**Credits**")
                credits_total_input = st.number_input("Total Credits", 0.0, 40.0, 0.0, 0.5, key="total_c_input")
                sc_override = st.checkbox("Manually override Student Credits", key="sc_override_check")
                student_credits_input = st.number_input("Student Credits", 0.0, 40.0, 0.0, 0.5, disabled=not sc_override, key="sc_input")
                tc_override = st.checkbox("Manually override Teaching Credits", key="tc_override_check")
                teaching_credits_input = st.number_input("Teaching Credits", 0.0, 40.0, 0.0, 0.5, disabled=not tc_override, key="tc_input")
                
                st.markdown("**Assessment Marks**")
                col1, col2, col3 = st.columns(3)
                with col1: internal_marks = st.number_input("Maximum Internal Marks", 0, value=40)
                with col2: exam_marks = st.number_input("Maximum External Exam Marks", 0, value=60)
                with col3: jury_marks = st.number_input("Maximum Jury/Viva Marks", 0, value=0)
                
                st.markdown("**Passing Threshold**")
                p1, p2, p3 = st.columns(3)
                with p1: min_internal_percent = st.number_input("Minimum Internal Passing %", 0.0, 100.0, 50.0, 1.0)
                with p2: min_external_percent = st.number_input("Minimum External Passing %", 0.0, 100.0, 40.0, 1.0)
                with p3: min_overall_percent = st.number_input("Minimum Overall Passing %", 0.0, 100.0, 40.0, 1.0)
                
                with st.expander("Attainment Requirements (optional)"):
                    a1, a2 = st.columns(2)
                    with a1: 
                        direct_source_mode = st.selectbox(
                            "Direct Attainment Calculation Method", 
                            ["overall", "split_internal_external"],
                            format_func=lambda x: "Overall (Combined)" if x == "overall" else "Separate (Internal & External)",
                            help="Overall: Calculate from total marks. Separate: Calculate internal and external independently"
                        )
                    with a2: 
                        direct_internal_weight_percent = st.number_input(
                            "Direct Attainment - Internal Marks Contribution %", 
                            0.0, 100.0, 40.0, 5.0, 
                            key="diw_input_create",
                            help="Weight given to internal marks in direct attainment calculation"
                        )
                    st.caption(f"Direct Attainment - External Marks Contribution %: **{100.0 - st.session_state.diw_input_create:.1f}**")
                    st.info("Note: Weights auto-balance if marks are internal/external only.")
                    
                    a4, a5 = st.columns(2)
                    with a4: 
                        direct_target_percent = st.number_input(
                            "Direct Attainment % in Total Attainment", 
                            0.0, 100.0, 80.0, 5.0,
                            help="Weightage of direct attainment (from marks) in overall attainment"
                        )
                    with a5: 
                        st.caption(f"Indirect Attainment % in Total Attainment: **{100.0 - direct_target_percent:.1f}**")
                    
                    indirect_min_response_rate_percent = st.number_input(
                        "Minimum Indirect Attainment through Feedback Response Rate %", 
                        0.0, 100.0, 75.0, 5.0,
                        help="Minimum percentage of students who must provide feedback for indirect attainment"
                    )
                
                description = st.text_area("Description (optional)")
                col1, col2 = st.columns(2)
                with col1: active = st.checkbox("Active", value=True)
                with col2: sort_order = st.number_input("Sort Order", 1, value=100)
                
                submitted = st.form_submit_button("Create Subject", type="primary", use_container_width=True)

            # Workload UI (outside form)
            st.markdown("---")
            st.markdown("**Workload Breakdown**")
            st.caption("Edit workload components (total periods for semester).")
            for item in st.session_state.subject_workload_components:
                item_id = item['id']
                cols = st.columns([1, 3, 1, 1])
                with cols[0]: st.text_input("Code", value=item["Code"], key=f"workload_code_{item_id}", placeholder="e.g. L, T, P")
                with cols[1]: st.text_input("Name", value=item["Name"], key=f"workload_name_{item_id}", placeholder="e.g. Lectures")
                with cols[2]: st.number_input("Periods", value=float(item["Hours"]), min_value=0.0, max_value=200.0, step=1.0, key=f"workload_hours_{item_id}")
                with cols[3]: st.button("🗑️", key=f"workload_del_{item_id}", on_click=_delete_workload_row, args=(item_id,))
            st.button("Add Workload Component", on_click=_add_workload_row, type="secondary")

            # Form submit logic
            if submitted:
                try:
                    if not subject_code or not subject_name or not semester_id:
                        st.error("Subject Code, Name, and Semester are required.")
                    else:
                        workload_components, L_val, T_val, P_val, S_val = [], 0.0, 0.0, 0.0, 0.0
                        for item in st.session_state.subject_workload_components:
                            code = st.session_state.get(f"workload_code_{item['id']}", "").strip()
                            name = st.session_state.get(f"workload_name_{item['id']}", "").strip()
                            hrs = float(st.session_state.get(f"workload_hours_{item['id']}", 0) or 0)
                            if code or name or hrs:
                                workload_components.append({"code": code, "name": name, "hours": hrs})
                        L_val = sum(c["hours"] for c in workload_components if c["code"].upper() == "L")
                        T_val = sum(c["hours"] for c in workload_components if c["code"].upper() == "T")
                        P_val = sum(c["hours"] for c in workload_components if c["code"].upper() == "P")
                        S_val = sum(c["hours"] for c in workload_components if c["code"].upper() == "S")
                        credits_total_val = st.session_state.total_c_input
                        student_credits_val = st.session_state.sc_input if st.session_state.sc_override_check else credits_total_val
                        teaching_credits_val = st.session_state.tc_input if st.session_state.tc_override_check else credits_total_val
                        total_external_marks = exam_marks + jury_marks
                        is_internal_only = internal_marks > 0 and total_external_marks == 0
                        is_external_only = internal_marks == 0 and total_external_marks > 0
                        final_diw = direct_internal_weight_percent
                        final_dew = 100.0 - final_diw
                        if is_internal_only: final_diw, final_dew = 100.0, 0.0
                        elif is_external_only: final_diw, final_dew = 0.0, 100.0
                        
                        data = {
                            "subject_code": subject_code, "subject_name": subject_name,
                            "subject_type": subject_type, 
                            "degree_code": degree_code,
                            "program_code": form_program_code,
                            "branch_code": form_branch_code,
                            "curriculum_group_code": form_cg_code, 
                            "semester_id": semester_id,
                            "credits_total": credits_total_val, "student_credits": student_credits_val,
                            "teaching_credits": teaching_credits_val,
                            "L": L_val, "T": T_val, "P": P_val, "S": S_val,
                            "workload_breakup_json": json.dumps(workload_components) if workload_components else None,
                            "internal_marks_max": internal_marks, "exam_marks_max": exam_marks,
                            "jury_viva_marks_max": jury_marks, "min_internal_percent": min_internal_percent,
                            "min_external_percent": min_external_percent, "min_overall_percent": min_overall_percent,
                            "direct_source_mode": direct_source_mode,
                            "direct_internal_threshold_percent": min_internal_percent,
                            "direct_external_threshold_percent": min_external_percent,
                            "direct_internal_weight_percent": final_diw,
                            "direct_external_weight_percent": final_dew,
                            "direct_target_students_percent": direct_target_percent,
                            "indirect_target_students_percent": 100.0 - direct_target_percent,
                            "indirect_min_response_rate_percent": indirect_min_response_rate_percent,
                            "overall_direct_weight_percent": direct_target_percent,
                            "overall_indirect_weight_percent": 100.0 - direct_target_percent,
                            "description": description, "active": active, "sort_order": sort_order,
                        }
                        create_subject(engine, data, actor)
                        
                        # Set success flag and reset workload components
                        st.session_state.subject_created_success = True
                        st.session_state.subject_workload_components = [
                            {"id": 1, "Code": "L", "Name": "Lectures", "Hours": 3.0},
                            {"id": 2, "Code": "T", "Name": "Theory / Tutorials", "Hours": 0.0},
                            {"id": 3, "Code": "P", "Name": "Practicals", "Hours": 2.0},
                            {"id": 4, "Code": "S", "Name": "Studios", "Hours": 0.0},
                        ]
                        
                        st.cache_data.clear()
                        st.rerun()
                except Exception as e:
                    st.error(f"Error: {str(e)}")


    # --- 7. EDIT/DELETE UI ---
    st.markdown("---")
    st.subheader("Edit or Delete Subject")

    with engine.begin() as conn:
        subjects = fetch_subjects(conn, degree_code, top_filter_program_code, top_filter_branch_code, top_filter_cg_code, active_only=False)
    
    if not subjects:
        st.info("No subjects found for this scope.")
        return

    # --- 7a. Subject Dataframe ---
    st.markdown("##### All Subjects in Scope")
    
    # Dynamic column logic
    df = pd.DataFrame(subjects)
    PREFERRED_COLS = ["subject_code", "subject_name", "subject_type"]
    if degree:
        if not prog_disabled: PREFERRED_COLS.append("program_code")
        if not branch_disabled: PREFERRED_COLS.append("branch_code")
        if not cg_disabled: PREFERRED_COLS.append("curriculum_group_code")
    PREFERRED_COLS.extend(["credits_total", "L", "T", "P", "S", "status", "active"])
    
    existing_preferred_cols = [col for col in PREFERRED_COLS if col in df.columns]
    extra_cols = [col for col in df.columns if col not in PREFERRED_COLS and col != 'id']
    
    st.dataframe(df[existing_preferred_cols + extra_cols], use_container_width=True)

    st.markdown("---")

    # --- 7b. Filters ---
    col1, col2 = st.columns(2)
    with col1:
        sem_options = {s['id']: f"Sem {s['semester_number']} - {s['label']}" for s in semesters_for_dropdown}
        selected_semester_id = st.selectbox(
            "Filter by Semester", 
            options=[None] + list(sem_options.keys()), 
            format_func=lambda x: "All Semesters" if x is None else sem_options.get(x, f"ID {x}"), 
            key="edit_filter_sem"
        )
    with col2:
        subject_types = sorted(list(set(s['subject_type'] for s in subjects if s['subject_type'])))
        selected_subject_type = st.selectbox(
            "Filter by Type", 
            options=[None] + subject_types, 
            format_func=lambda x: "All Types" if x is None else x, 
            key="edit_filter_type"
        )

    # --- 7c. Filter the list in Python ---
    filtered_subjects = subjects
    if selected_semester_id:
        filtered_subjects = [s for s in filtered_subjects if s['semester_id'] == selected_semester_id]
    if selected_subject_type:
        filtered_subjects = [s for s in filtered_subjects if s['subject_type'] == selected_subject_type]

    # --- 7d. Selectbox for the filtered list ---
    subject_options = {s['id']: f"{s['subject_code']} - {s['subject_name']}" for s in filtered_subjects}
    selected_subject_id = st.selectbox(
        "Select Subject to Edit/Delete", 
        options=[None] + list(subject_options.keys()), 
        format_func=lambda x: "Select a subject..." if x is None else subject_options[x],
        key="subject_to_edit_select"
    )

    # --- 7e. Show Edit/Delete form for the selected subject ---
    if selected_subject_id:
        subject = next((s for s in subjects if s['id'] == selected_subject_id), None)
        
        if subject:
            if st.session_state.get('edit_subject_id') != subject['id']:
                _set_edit_state(subject)

            # --- EDIT FORM ---
            with st.form(f"edit_form_{subject['id']}"):
                st.subheader(f"Editing: {subject['subject_code']}")
                
                col1, col2, col3 = st.columns(3)
                with col1: st.text_input("Subject Code*", value=st.session_state.edit_subject_code, disabled=True)
                with col2: st.text_input("Subject Name*", key="edit_subject_name")
                with col3: st.selectbox("Subject Type*", options=DEFAULT_SUBJECT_TYPES, key="edit_subject_type")

                st.markdown("**Academic Assignment**")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.selectbox(
                        "Assign to Semester*", 
                        options=list(sem_options.keys()),
                        format_func=lambda x: sem_options.get(x, f"ID {x}"), 
                        key="edit_semester_id"
                    )
                with col2:
                    st.selectbox(
                        "Assign to Curriculum Group (optional)",
                        options=[None] + [g["group_code"] for g in cgs_for_form],
                        format_func=lambda x: "No Group" if x is None else next((g["group_name"] for g in cgs_for_form if g["group_code"] == x), x),
                        key="edit_cg_code",
                        disabled=cg_disabled
                    )
                
                col1, col2 = st.columns(2)
                with col1:
                    st.selectbox(
                        "Assign to Program (optional)",
                        options=[None] + [p["program_code"] for p in programs_for_form],
                        format_func=lambda x: "Degree-Level" if x is None else next((p["program_name"] for p in programs_for_form if p["program_code"] == x), x),
                        key="edit_program_code",
                        disabled=prog_disabled
                    )
                with col2:
                    st.selectbox(
                        "Assign to Branch (optional)",
                        options=[None] + [b["branch_code"] for b in branches_for_form],
                        format_func=lambda x: "Program-Level" if x is None else next((b["branch_name"] for b in branches_for_form if b["branch_code"] == x), x),
                        key="edit_branch_code",
                        disabled=branch_disabled
                    )

                st.markdown("**Credits**")
                st.number_input("Total Credits", min_value=0.0, max_value=40.0, step=0.5, key="edit_credits_total")
                st.number_input("Student Credits", min_value=0.0, max_value=40.0, step=0.5, key="edit_student_credits", help="Will default to Total Credits if 0")
                st.number_input("Teaching Credits", min_value=0.0, max_value=40.0, step=0.5, key="edit_teaching_credits", help="Will default to Total Credits if 0")

                st.markdown("**Assessment Marks**")
                col1, col2, col3 = st.columns(3)
                with col1: st.number_input("Maximum Internal Marks", min_value=0, key="edit_internal_marks")
                with col2: st.number_input("Maximum External Exam Marks", min_value=0, key="edit_exam_marks")
                with col3: st.number_input("Maximum Jury/Viva Marks", min_value=0, key="edit_jury_marks")
                
                st.markdown("**Passing Threshold**")
                p1, p2, p3 = st.columns(3)
                with p1: st.number_input("Minimum Internal Passing %", min_value=0.0, max_value=100.0, step=1.0, key="edit_min_internal")
                with p2: st.number_input("Minimum External Passing %", min_value=0.0, max_value=100.0, step=1.0, key="edit_min_external")
                with p3: st.number_input("Minimum Overall Passing %", min_value=0.0, max_value=100.0, step=1.0, key="edit_min_overall")

                with st.expander("Attainment Requirements (optional)"):
                    a1, a2 = st.columns(2)
                    with a1: 
                        st.selectbox(
                            "Direct Attainment Calculation Method", 
                            ["overall", "split_internal_external"],
                            format_func=lambda x: "Overall (Combined)" if x == "overall" else "Separate (Internal & External)",
                            key="edit_direct_source_mode"
                        )
                    with a2: 
                        st.number_input(
                            "Direct Attainment - Internal Marks Contribution %", 
                            min_value=0.0, max_value=100.0, step=5.0, 
                            key="edit_diw"
                        )
                    st.caption(f"Direct Attainment - External Marks Contribution %: **{100.0 - st.session_state.edit_diw:.1f}**")
                    st.info("Note: Weights auto-balance if marks are internal/external only.")
                    
                    a4, a5 = st.columns(2)
                    with a4: 
                        st.number_input(
                            "Direct Attainment % in Total Attainment", 
                            min_value=0.0, max_value=100.0, step=5.0,
                            key="edit_dts"
                        )
                    with a5: 
                        st.caption(f"Indirect Attainment % in Total Attainment: **{100.0 - st.session_state.edit_dts:.1f}**")
                    
                    st.number_input(
                        "Minimum Indirect Attainment through Feedback Response Rate %", 
                        min_value=0.0, max_value=100.0, step=5.0,
                        key="edit_imr"
                    )
                
                st.markdown("**Workload Breakdown**")
                st.caption("Edit workload components (total periods for semester).")
                
                # Initialize edit workload components if not set
                if 'edit_workload_components' not in st.session_state:
                    st.session_state.edit_workload_components = []
                
                # Display existing workload components or show empty state
                if st.session_state.edit_workload_components:
                    for idx, item in enumerate(st.session_state.edit_workload_components):
                        cols = st.columns([1, 3, 1.5, 0.5])
                        with cols[0]: 
                            st.text_input(
                                "Code", 
                                value=item.get("code", ""), 
                                key=f"edit_workload_code_{idx}", 
                                placeholder="L, T, P, S"
                            )
                        with cols[1]: 
                            st.text_input(
                                "Name", 
                                value=item.get("name", ""), 
                                key=f"edit_workload_name_{idx}", 
                                placeholder="e.g. Lectures"
                            )
                        with cols[2]: 
                            st.number_input(
                                "Hours/Periods", 
                                value=float(item.get("hours", 0)), 
                                min_value=0.0, 
                                max_value=200.0, 
                                step=1.0, 
                                key=f"edit_workload_hours_{idx}"
                            )
                        with cols[3]: 
                            st.button("🗑️", key=f"edit_workload_del_{idx}", on_click=_delete_edit_workload_row, args=(idx,), help="Delete this row")
                    
                    st.caption(f"✓ {len(st.session_state.edit_workload_components)} workload component(s)")
                else:
                    st.info("No workload breakdown set for this subject. Click 'Add Component' to add one.")
                
                st.markdown("**Note:** Click 'Add Component' below, then fill the fields, then click 'Save Changes' to update.")
                
                st.text_area("Description", key="edit_description")
                col1, col2 = st.columns(2)
                with col1: st.checkbox("Active", key="edit_active")
                with col2: st.number_input("Sort Order", min_value=1, step=1, key="edit_sort_order")
                
                if st.form_submit_button("💾 Save Changes", type="primary", use_container_width=True):
                    try:
                        total_ext = st.session_state.edit_exam_marks + st.session_state.edit_jury_marks
                        is_int_only = st.session_state.edit_internal_marks > 0 and total_ext == 0
                        is_ext_only = st.session_state.edit_internal_marks == 0 and total_ext > 0
                        final_diw = st.session_state.edit_diw
                        final_dew = 100.0 - final_diw
                        if is_int_only: final_diw, final_dew = 100.0, 0.0
                        elif is_ext_only: final_diw, final_dew = 0.0, 100.0
                        
                        # Process updated workload components
                        workload_components = []
                        L_val, T_val, P_val, S_val = 0.0, 0.0, 0.0, 0.0
                        for idx in range(len(st.session_state.edit_workload_components)):
                            code = st.session_state.get(f"edit_workload_code_{idx}", "").strip()
                            name = st.session_state.get(f"edit_workload_name_{idx}", "").strip()
                            hrs = float(st.session_state.get(f"edit_workload_hours_{idx}", 0) or 0)
                            if code or name or hrs:
                                workload_components.append({"code": code, "name": name, "hours": hrs})
                        
                        # Calculate L, T, P, S totals
                        L_val = sum(c["hours"] for c in workload_components if c["code"].upper() == "L")
                        T_val = sum(c["hours"] for c in workload_components if c["code"].upper() == "T")
                        P_val = sum(c["hours"] for c in workload_components if c["code"].upper() == "P")
                        S_val = sum(c["hours"] for c in workload_components if c["code"].upper() == "S")
                        
                        edit_data = {
                            "subject_code": st.session_state.edit_subject_code,
                            "subject_name": st.session_state.edit_subject_name,
                            "subject_type": st.session_state.edit_subject_type,
                            "degree_code": degree_code,
                            "program_code": st.session_state.edit_program_code,
                            "branch_code": st.session_state.edit_branch_code,
                            "curriculum_group_code": st.session_state.edit_cg_code,
                            "semester_id": st.session_state.edit_semester_id,
                            "credits_total": st.session_state.edit_credits_total,
                            "student_credits": st.session_state.edit_student_credits or st.session_state.edit_credits_total,
                            "teaching_credits": st.session_state.edit_teaching_credits or st.session_state.edit_credits_total,
                            "L": L_val, 
                            "T": T_val,
                            "P": P_val, 
                            "S": S_val,
                            "workload_breakup_json": json.dumps(workload_components) if workload_components else None,
                            "internal_marks_max": st.session_state.edit_internal_marks,
                            "exam_marks_max": st.session_state.edit_exam_marks,
                            "jury_viva_marks_max": st.session_state.edit_jury_marks,
                            "min_internal_percent": st.session_state.edit_min_internal,
                            "min_external_percent": st.session_state.edit_min_external,
                            "min_overall_percent": st.session_state.edit_min_overall,
                            "direct_source_mode": st.session_state.edit_direct_source_mode,
                            "direct_internal_threshold_percent": st.session_state.edit_min_internal,
                            "direct_external_threshold_percent": st.session_state.edit_min_external,
                            "direct_internal_weight_percent": final_diw,
                            "direct_external_weight_percent": final_dew,
                            "direct_target_students_percent": st.session_state.edit_dts,
                            "indirect_target_students_percent": 100.0 - st.session_state.edit_dts,
                            "indirect_min_response_rate_percent": st.session_state.edit_imr,
                            "overall_direct_weight_percent": st.session_state.edit_dts,
                            "overall_indirect_weight_percent": 100.0 - st.session_state.edit_dts,
                            "description": st.session_state.edit_description,
                            "active": st.session_state.edit_active,
                            "sort_order": st.session_state.edit_sort_order,
                        }
                        
                        update_subject(engine, selected_subject_id, edit_data, actor)
                        success(f"Subject {subject['subject_code']} updated!")
                        _clear_edit_state()
                        _reset_subject_selection()
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

            # Add workload component button (outside form to avoid nesting)
            st.button(
                "➕ Add Workload Component", 
                on_click=_add_edit_workload_row, 
                key=f"add_edit_workload_{subject['id']}",
                type="secondary",
                use_container_width=True
            )

            # --- DELETE BUTTON ---
            st.markdown("---")
            if st.session_state.get('confirm_delete_id') == selected_subject_id:
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("🚨 Confirm Delete", key=f"conf_del_{selected_subject_id}", type="primary", use_container_width=True):
                        try:
                            delete_subject(engine, selected_subject_id, actor)
                            success(f"Subject {subject['subject_code']} deleted.")
                            del st.session_state.confirm_delete_id
                            _clear_edit_state()
                            _reset_subject_selection()
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
                with col2:
                    if st.button("Cancel Delete", key=f"canc_del_{selected_subject_id}", use_container_width=True):
                        del st.session_state.confirm_delete_id
                        st.rerun()
            else:
                st.button("Delete Subject", key=f"del_{selected_subject_id}", on_click=lambda s_id=selected_subject_id: st.session_state.update({'confirm_delete_id': s_id}), use_container_width=True)
