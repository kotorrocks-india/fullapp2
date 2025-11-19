"""
Subjects Catalog Tab
UPDATED: Fixed UI labels, removed cancel button,
ensured workload UI is outside forms to prevent callback errors,
and fixed default state for workload component editing.

FIXED: Changed custom error() calls to st.error() to resolve ImportError.
FIXED: Removed 'st.hidden' call.

UPDATED (User Request):
- Removed 'Direct Attainment Target Threshold % (Internal)'
- Removed 'Direct Attainment Target Threshold % (External)'
- Added 'Direct Attainment - External Marks Contribution %' as a calculated metric.

FIXED: Changed st.button type="danger" to type="primary" to fix error.

FIXED (User Request):
- Default marks in 'Create New Subject' form are now 0 instead of 40/60.

REFACTOR (User Request):
- Changed dynamic workload UI to static L/T/P/S fields + dynamic "Other" components.
- Fixed max_value crash for marks fields (set to 500).

UPDATED (User Request):
- Added Semester and Curriculum Group filters for the Edit/Delete dropdown.
- Explained why 'Year' is not a filter in this context.
- Updated edit dropdown format_func to show semester.

FIXED (User Request):
- Refactored workload loading logic to correctly read L/T/P/S
  from their primary database columns, fixing the "all zeros" bug.
  
UPDATED (User Request):
- Replaced technical database errors (e.g., 'no such table') with 
  user-friendly messages directing them to a system administrator.
"""

from __future__ import annotations
import streamlit as st
import pandas as pd
import json
import logging  # --- *** NEW: Added for logging errors *** ---
from typing import Dict, Any, List, Optional
# Use relative imports
from ..helpers import exec_query, rows_to_dicts
from ..db_helpers import (
    fetch_degrees, fetch_programs, fetch_branches,
    fetch_curriculum_groups, fetch_subjects
)
from ..subjects_crud import create_subject, update_subject, delete_subject
from ..constants import DEFAULT_SUBJECT_TYPES
from core.forms import success  # Removed 'error'
from sqlalchemy import text as sa_text
# --- *** NEW IMPORT *** ---
# Import the specific error to catch it
from sqlalchemy.exc import OperationalError

# --- *** NEW: Set up a logger to see errors in the console/logs *** ---
logger = logging.getLogger(__name__)


# =====================================================================
# WORKLOAD STATE HELPERS (for dynamic "Other" rows)
# =====================================================================

def _init_other_workload_state(session_key: str):
    """Initialize or reset "Other" workload components in session state."""
    st.session_state[session_key] = []

def _add_other_workload_row(session_key: str):
    """Add a new empty "Other" workload component row."""
    if session_key not in st.session_state:
        st.session_state[session_key] = []
    st.session_state[session_key].append({"code": "", "name": "", "hours": 0})

def _delete_other_workload_row(session_key: str, index: int):
    """Delete an "Other" workload component row by index."""
    if session_key in st.session_state and 0 <= index < len(st.session_state[session_key]):
        st.session_state[session_key].pop(index)

def _read_other_workload_from_state(session_key: str) -> List[Dict[str, Any]]:
    """Read "Other" workload data from state, collecting data from widgets."""
    components = []
    if session_key in st.session_state:
        for i, _ in enumerate(st.session_state[session_key]):
            code = st.session_state.get(f"{session_key}_code_{i}", "").strip().upper()
            name = st.session_state.get(f"{session_key}_name_{i}", "").strip()
            hours = st.session_state.get(f"{session_key}_hours_{i}", 0.0)
            
            if code and name and hours > 0:
                # Ensure "Other" components don't use reserved codes
                if code not in ["L", "T", "P", "S"]:
                    components.append({"code": code, "name": name, "hours": hours})
                else:
                    st.warning(f"Component code '{code}' is reserved. Skipping this row.")
    return components


# --- *** MODIFIED FUNCTION (THE FIX) *** ---
def _set_workload_state_from_subject(subject: Dict[str, Any], state_prefix: str):
    """
    Set the workload state from a subject dict, partitioning
    L/T/P/S from "Other" components.
    
    Reads L/T/P/S directly from the subject's columns.
    Reads "Other" components from the workload_breakup_json.
    """
    
    # 1. Load L/T/P/S directly from the main columns.
    # The schema is NOT NULL DEFAULT 0, so we can safely cast.
    L = float(subject.get("L", 0.0) or 0.0)
    T = float(subject.get("T", 0.0) or 0.0)
    P = float(subject.get("P", 0.0) or 0.0)
    S = float(subject.get("S", 0.0) or 0.0)

    st.session_state[f"{state_prefix}_L"] = L
    st.session_state[f"{state_prefix}_T"] = T
    st.session_state[f"{state_prefix}_P"] = P
    st.session_state[f"{state_prefix}_S"] = S

    # 2. Load "Other" components from the JSON field
    other_components = []
    workload_json = subject.get("workload_breakup_json")
    if workload_json:
        try:
            components = json.loads(workload_json)
            if isinstance(components, list):
                for item in components:
                    code = item.get("code", "").upper()
                    # Only add if it's NOT a standard L/T/P/S component
                    if code not in ["L", "T", "P", "S"]:
                        other_components.append(item)
        except (json.JSONDecodeError, TypeError, ValueError):
            pass # Ignore errors in JSON

    st.session_state[f"{state_prefix}_other_workload_components"] = other_components
# --- *** END MODIFIED FUNCTION *** ---


# =====================================================================
# MAIN RENDER FUNCTION
# =====================================================================

def render(engine, actor: str, CAN_EDIT: bool):
    """Render the Subjects Catalog tab."""
    st.subheader("📚 Subjects Catalog")

    # --- *** MODIFIED: Wrap entire render logic in a try/except block *** ---
    # This will catch the "no such table" error and show a helpful message
    # instead of crashing the entire application.
    try:
        # Initialize session state for workload components if not present
        if "create_other_workload_components" not in st.session_state:
            st.session_state.create_other_workload_components = []
        if "edit_other_workload_components" not in st.session_state:
            st.session_state.edit_other_workload_components = []

        # --- 1. FILTERS ---
        st.markdown("#### Filter Subjects")
        
        # This is the line that caused the original error
        degrees = fetch_degrees(engine)
        degree_options = [d["code"] for d in degrees]
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            selected_degree = st.selectbox(
                "Degree", 
                options=degree_options, 
                key="subjects_degree"
            )
        
        with col2:
            programs = fetch_programs(engine, selected_degree)
            program_options = [p["program_code"] for p in programs]
            selected_program = st.selectbox(
                "Program", 
                options=["All"] + program_options, 
                key="subjects_program"
            )
            selected_program = None if selected_program == "All" else selected_program
        
        with col3:
            branches = fetch_branches(engine, selected_degree, selected_program)
            branch_options = [b["branch_code"] for b in branches]
            selected_branch = st.selectbox(
                "Branch", 
                options=["All"] + branch_options, 
                key="subjects_branch"
            )
            selected_branch = None if selected_branch == "All" else selected_branch
        
        with col4:
            cgs = fetch_curriculum_groups(engine, selected_degree)
            cg_options = [c["group_code"] for c in cgs]
            selected_cg = st.selectbox(
                "Curriculum Group", 
                options=["All"] + cg_options, 
                key="subjects_cg"
            )
            selected_cg = None if selected_cg == "All" else selected_cg

        # --- 2. CREATE NEW SUBJECT (if CAN_EDIT) ---
        if CAN_EDIT:
            st.markdown("---")
            with st.expander("➕ Create New Subject", expanded=False):
                
                # --- FORM: Main data ---
                with st.form("create_subject_form", clear_on_submit=True):
                    st.markdown("**Core Details**")
                    
                    c1, c2 = st.columns(2)
                    with c1:
                        subject_code = st.text_input("Subject Code*", placeholder="e.g., AD101")
                        subject_name = st.text_input("Subject Name*", placeholder="e.g., Architectural Design Studio 1")
                    
                    with c2:
                        subject_type = st.selectbox("Subject Type", options=DEFAULT_SUBJECT_TYPES)
                        # Removed st.hidden() call
                        # Optional scope
                        program_code = st.selectbox(
                            "Program (Optional)", 
                            options=[""] + program_options,
                            key="create_prog"
                        )
                        branch_code = st.selectbox(
                            "Branch (Optional)", 
                            options=[""] + branch_options,
                            key="create_branch"
                        )
                        cg_code = st.selectbox(
                            "Curriculum Group (Optional)", 
                            options=[""] + cg_options,
                            key="create_cg"
                        )
                    
                    st.markdown("**Semester & Credits**")
                    c1, c2 = st.columns(2)
                    with c1:
                        semester_id = st.number_input("Semester*", min_value=1, max_value=12, step=1)
                    with c2:
                        credits_total = st.number_input("Total Credits*", min_value=0.0, max_value=40.0, step=0.5)

                    # --- *** NEW: Static L/T/P/S fields *** ---
                    st.markdown("**Workload (L/T/P/S)**")
                    c1, c2, c3, c4 = st.columns(4)
                    with c1:
                        st.number_input("L (Lectures)", min_value=0.0, step=1.0, key="create_L")
                    with c2:
                        st.number_input("T (Tutorials)", min_value=0.0, step=1.0, key="create_T")
                    with c3:
                        st.number_input("P (Practicals)", min_value=0.0, step=1.0, key="create_P")
                    with c4:
                        st.number_input("S (Studio)", min_value=0.0, step=1.0, key="create_S")
                    # --- *** END NEW L/T/P/S *** ---


                    # --- FORM: Attainment & Passing ---
                    
                    st.markdown("**Assessment (Max Marks)**")
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        # --- *** FIX: Default value=0 *** ---
                        # --- *** FIX: max_value=500 *** ---
                        st.number_input(
                            "**Maximum Internal Marks**", 
                            min_value=0, max_value=500, value=0,
                            key="create_internal_marks_max"
                        )
                    with c2:
                        # --- *** FIX: Default value=0 *** ---
                        # --- *** FIX: max_value=500 *** ---
                        st.number_input(
                            "**Maximum External Marks (Exam)**", 
                            min_value=0, max_value=500, value=0,
                            key="create_exam_marks_max"
                        )
                    with c3:
                        # --- *** FIX: max_value=500 *** ---
                        st.number_input(
                            "**Maximum External Marks (Jury/Viva)**", 
                            min_value=0, max_value=500, value=0,
                            key="create_jury_viva_marks_max"
                        )

                    st.markdown("**Passing Threshold**")
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        st.number_input(
                            "**Minimum Internal Passing %**", 
                            min_value=0.0, max_value=100.0, value=50.0, step=1.0,
                            key="create_min_internal_percent"
                        )
                    with c2:
                        st.number_input(
                            "**Minimum External Passing %**", 
                            min_value=0.0, max_value=100.0, value=40.0, step=1.0,
                            key="create_min_external_percent"
                        )
                    with c3:
                        st.number_input(
                            "**Minimum Overall Passing %**", 
                            min_value=0.0, max_value=100.0, value=40.0, step=1.0,
                            key="create_min_overall_percent"
                        )

                    with st.expander("Attainment Requirements (optional)"):
                        st.markdown("**Direct Attainment**")
                        c1, c2 = st.columns(2)
                        with c1:
                            direct_source_mode = st.selectbox(
                                "**Direct Attainment Source**",
                                options=["overall", "separate"],
                                format_func=lambda x: "Overall (Combined)" if x == "overall" else "Separate (Internal & External)",
                                key="create_direct_source_mode"
                            )
                        with c2:
                            pass # Spacer
                        
                        c1, c2 = st.columns(2)
                        with c1:
                            internal_weight = st.number_input(
                                "**Direct Attainment - Internal Marks Contribution %**",
                                min_value=0.0, max_value=100.0, value=40.0, step=1.0,
                                key="create_direct_internal_weight_percent"
                            )
                        with c2:
                            external_weight = 100.0 - internal_weight
                            st.metric(
                                "**Direct Attainment - External Marks Contribution %**",
                                f"{external_weight:.1f} %"
                            )
                        
                        st.markdown("**Overall Attainment**")
                        c1, c2 = st.columns(2)
                        with c1:
                            direct_attainment_pct = st.number_input(
                                "**Direct Attainment % in Total Attainment**",
                                min_value=0.0, max_value=100.0, value=80.0, step=1.0,
                                key="create_direct_target_students_percent"
                            )
                        with c2:
                            indirect_attainment_pct = 100.0 - direct_attainment_pct
                            st.metric(
                                "**Indirect Attainment % in Total Attainment**",
                                f"{indirect_attainment_pct:.1f} %"
                            )
                        
                        st.number_input(
                            "**Minimum Indirect Attainment through Feedback Response Rate**",
                            min_value=0.0, max_value=100.0, value=75.0, step=1.0,
                            key="create_indirect_min_response_rate_percent"
                        )

                    # --- FORM: Misc ---
                    st.text_area("Description", key="create_description")
                    
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        status = st.selectbox("Status", options=["active", "inactive", "archived"], key="create_status")
                    with c2:
                        active = st.checkbox("Active", value=True, key="create_active")
                    with c3:
                        sort_order = st.number_input("Sort Order", value=100, key="create_sort_order")
                    
                    # --- FORM SUBMIT ---
                    submitted = st.form_submit_button("🚀 Create Subject", type="primary", use_container_width=True)

                # --- END OF FORM ---
                
                # --- WORKLOAD UI (OUTSIDE FORM) ---
                st.markdown("**Other Workload Components**")
                st.caption("Add any non-L/T/P/S components (e.g., 'Field Work').")
                
                if st.session_state.create_other_workload_components:
                    for i, item in enumerate(st.session_state.create_other_workload_components):
                        cols = st.columns([1, 3, 1.5, 0.5])
                        with cols[0]:
                            st.text_input(
                                "Code", 
                                placeholder="e.g. FW", 
                                key=f"create_other_workload_components_code_{i}"
                            )
                        with cols[1]:
                            st.text_input(
                                "Name", 
                                placeholder="e.g. Field Work", 
                                key=f"create_other_workload_components_name_{i}"
                            )
                        with cols[2]:
                            st.number_input(
                                "Hours/Periods", 
                                min_value=0.0, max_value=200.0, step=1.0, 
                                key=f"create_other_workload_components_hours_{i}"
                            )
                        with cols[3]:
                            st.button(
                                "🗑️", 
                                key=f"create_workload_del_{i}", 
                                on_click=_delete_other_workload_row, 
                                args=("create_other_workload_components", i), 
                                help="Delete this row"
                            )
                
                st.button(
                    "➕ Add Other Component", 
                    on_click=_add_other_workload_row, 
                    args=("create_other_workload_components",),
                    type="secondary",
                    use_container_width=True
                )
                # --- END WORKLOAD UI ---

                if submitted:
                    if not subject_code or not subject_name or not semester_id:
                        st.error("Subject Code, Name, and Semester are required.")
                    else:
                        # --- *** REBUILD workload_json *** ---
                        L_val = st.session_state.create_L
                        T_val = st.session_state.create_T
                        P_val = st.session_state.create_P
                        S_val = st.session_state.create_S

                        workload_components = []
                        if L_val > 0: workload_components.append({"code": "L", "name": "Lectures", "hours": L_val})
                        if T_val > 0: workload_components.append({"code": "T", "name": "Tutorials", "hours": T_val})
                        if P_val > 0: workload_components.append({"code": "P", "name": "Practicals", "hours": P_val})
                        if S_val > 0: workload_components.append({"code": "S", "name": "Studio", "hours": S_val})
                        
                        other_components = _read_other_workload_from_state("create_other_workload_components")
                        workload_components.extend(other_components)
                        workload_json = json.dumps(workload_components) if workload_components else None
                        # --- *** END REBUILD *** ---

                        
                        # Read internal weight and calculate external weight
                        internal_weight = st.session_state.create_direct_internal_weight_percent
                        external_weight = 100.0 - internal_weight
                        
                        data = {
                            "subject_code": subject_code.strip().upper(),
                            "subject_name": subject_name.strip(),
                            "subject_type": subject_type,
                            "degree_code": selected_degree,
                            "program_code": program_code or None,
                            "branch_code": branch_code or None,
                            "curriculum_group_code": cg_code or None,
                            "semester_id": semester_id,
                            "credits_total": credits_total,
                            "L": L_val,
                            "T": T_val,
                            "P": P_val,
                            "S": S_val,
                            "workload_breakup_json": workload_json,
                            "internal_marks_max": st.session_state.create_internal_marks_max,
                            "exam_marks_max": st.session_state.create_exam_marks_max,
                            "jury_viva_marks_max": st.session_state.create_jury_viva_marks_max,
                            "min_internal_percent": st.session_state.create_min_internal_percent,
                            "min_external_percent": st.session_state.create_min_external_percent,
                            "min_overall_percent": st.session_state.create_min_overall_percent,
                            "direct_source_mode": st.session_state.create_direct_source_mode,
                            "direct_internal_threshold_percent": 50.0, # Hard-coded default
                            "direct_external_threshold_percent": 40.0, # Hard-coded default
                            "direct_internal_weight_percent": internal_weight,
                            "direct_external_weight_percent": external_weight,
                            "direct_target_students_percent": direct_attainment_pct,
                            "indirect_target_students_percent": indirect_attainment_pct,
                            "indirect_min_response_rate_percent": st.session_state.create_indirect_min_response_rate_percent,
                            "overall_direct_weight_percent": direct_attainment_pct,
                            "overall_indirect_weight_percent": indirect_attainment_pct,
                            "description": st.session_state.create_description,
                            "status": status,
                            "active": active,
                            "sort_order": sort_order,
                        }

                        try:
                            create_subject(engine, data, actor)
                            success(f"Subject '{data['subject_code']}' created successfully!")
                            _init_other_workload_state("create_other_workload_components")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed to create subject: {e}")

        # --- 3. DISPLAY SUBJECTS ---
        st.markdown("---")
        st.markdown("#### Existing Subjects")

        with engine.begin() as conn:
            subjects = fetch_subjects(
                conn,
                selected_degree,
                selected_program,
                selected_branch,
                selected_cg,
                active_only=False,
            )

        if not subjects:
            st.info("No subjects found for the selected filters.")
            # Use `return` here to stop execution if no subjects are found
            # (and no error occurred)
            return

        df = pd.DataFrame(subjects)
        
        # Configure columns for display
        display_cols = [
            "subject_code", "subject_name", "subject_type", "semester_id", 
            "credits_total", "L", "T", "P", "S", "active", "status"
        ]
        # Add any missing columns (e.g., if DB schema changes)
        for col in display_cols:
            if col not in df.columns:
                df[col] = None
        
        st.dataframe(df[display_cols], use_container_width=True)
        
        st.markdown(f"Total subjects: **{len(df)}**")
        
        # --- 4. EDIT/DELETE (if CAN_EDIT) ---
        if CAN_EDIT:
            st.markdown("---")
            st.markdown("### Edit or Delete Subject")
            
            # --- PREPARE DATA FOR EDIT FORM ---
            # Create a dictionary for easy lookup by ID
            subject_dict = {s["id"]: s for s in subjects}

            # --- *** NEW FILTERS FOR EDIT/DELETE *** ---
            st.markdown("**Filter Edit/Delete List**")
            st.caption("Refine the list of subjects shown in the dropdown below.")
            
            edit_col1, edit_col2 = st.columns(2)
            
            with edit_col1:
                # Get unique semester IDs from the subjects list
                unique_semesters = sorted(list(set(
                    s['semester_id'] for s in subjects if s.get('semester_id')
                )))
                edit_filter_sem = st.selectbox(
                    "Filter by Semester",
                    options=["All"] + unique_semesters,
                    key="edit_filter_sem"
                )
            
            with edit_col2:
                # Get unique curriculum groups from the subjects list
                unique_cgs = sorted(list(set(
                    s['curriculum_group_code'] for s in subjects if s.get('curriculum_group_code')
                )))
                edit_filter_cg = st.selectbox(
                    "Filter by Curriculum Group",
                    options=["All"] + unique_cgs,
                    key="edit_filter_cg"
                )
            
            st.info("ℹ️ **Note:** 'Year' is not a filter here because the Catalog manages timeless subject definitions. Year-specific subjects ('Offerings') are managed in a different module.")

            # --- Apply filters ---
            filtered_subjects_for_edit = subjects
            if edit_filter_sem != "All":
                filtered_subjects_for_edit = [
                    s for s in filtered_subjects_for_edit 
                    if s.get('semester_id') == edit_filter_sem
                ]
            
            if edit_filter_cg != "All":
                filtered_subjects_for_edit = [
                    s for s in filtered_subjects_for_edit 
                    if s.get('curriculum_group_code') == edit_filter_cg
                ]

            # --- Use the filtered list for options ---
            subject_options = [s["id"] for s in filtered_subjects_for_edit]
            
            def format_subject_option(subject_id):
                s = subject_dict.get(subject_id)
                if not s:
                    return str(subject_id)
                return (
                    f"{s['subject_code']} - {s['subject_name']} "
                    f"(Sem: {s.get('semester_id', 'N/A')}) [ID: {s['id']}]"
                )

            selected_subject_id = st.selectbox(
                "Select Subject to Edit or Delete",
                options=subject_options,
                format_func=format_subject_option,
                index=None,
                placeholder="Select a subject...",
                key="edit_subject_select"
            )
            
            if not subject_options and (edit_filter_sem != "All" or edit_filter_cg != "All"):
                st.warning("No subjects match the selected edit filters. Adjust the filters above to find subjects.")


            if selected_subject_id:
                subject = subject_dict.get(selected_subject_id)
                
                # --- STATE MANAGEMENT FOR EDIT ---
                # Load subject data into state ONCE when selection changes
                if "current_edit_subject_id" not in st.session_state or st.session_state.current_edit_subject_id != subject["id"]:
                    st.session_state.current_edit_subject_id = subject["id"]
                    # Load subject data into session state for form defaults
                    st.session_state.edit_subject_name = subject.get("subject_name", "")
                    st.session_state.edit_subject_type = subject.get("subject_type", "Core")
                    st.session_state.edit_program_code = subject.get("program_code", "")
                    st.session_state.edit_branch_code = subject.get("branch_code", "")
                    st.session_state.edit_cg_code = subject.get("curriculum_group_code", "")
                    st.session_state.edit_semester_id = subject.get("semester_id", 1)
                    st.session_state.edit_credits_total = subject.get("credits_total", 0.0)
                    
                    st.session_state.edit_internal_marks_max = subject.get("internal_marks_max", 40)
                    st.session_state.edit_exam_marks_max = subject.get("exam_marks_max", 60)
                    st.session_state.edit_jury_viva_marks_max = subject.get("jury_viva_marks_max", 0)
                    
                    st.session_state.edit_min_internal_percent = subject.get("min_internal_percent", 50.0)
                    st.session_state.edit_min_external_percent = subject.get("min_external_percent", 40.0)
                    st.session_state.edit_min_overall_percent = subject.get("min_overall_percent", 40.0)

                    st.session_state.edit_direct_source_mode = subject.get("direct_source_mode", "overall")
                    st.session_state.edit_direct_internal_weight_percent = subject.get("direct_internal_weight_percent", 40.0)
                    st.session_state.edit_direct_target_students_percent = subject.get("direct_target_students_percent", 80.0)
                    st.session_state.edit_indirect_min_response_rate_percent = subject.get("indirect_min_response_rate_percent", 75.0)

                    st.session_state.edit_description = subject.get("description", "")
                    st.session_state.edit_status = subject.get("status", "active")
                    st.session_state.edit_active = bool(subject.get("active", 1))
                    st.session_state.edit_sort_order = subject.get("sort_order", 100)
                    
                    # --- *** MODIFIED: Call the new, fixed function *** ---
                    _set_workload_state_from_subject(subject, "edit")
                    st.rerun() # Rerun to populate form with new defaults

                st.markdown(f"### Editing: {subject['subject_code']} - {subject['subject_name']}")

                # --- EDIT FORM ---
                with st.form(f"edit_form_{subject['id']}"):
                    st.markdown("**Core Details**")
                    st.info(f"**Degree:** {subject['degree_code']} | **Subject Code:** {subject['subject_code']} (Cannot be changed)")
                    
                    c1, c2 = st.columns(2)
                    with c1:
                        st.text_input(
                            "Subject Name*", 
                            key="edit_subject_name"
                        )
                        st.selectbox(
                            "Subject Type", 
                            options=DEFAULT_SUBJECT_TYPES, 
                            key="edit_subject_type"
                        )
                    with c2:
                        st.selectbox(
                            "Program (Optional)", 
                            options=[""] + program_options,
                            key="edit_program_code"
                        )
                        st.selectbox(
                            "Branch (Optional)", 
                            options=[""] + branch_options,
                            key="edit_branch_code"
                        )
                        st.selectbox(
                            "Curriculum Group (Optional)", 
                            options=[""] + cg_options,
                            key="edit_cg_code"
                        )
                    
                    st.markdown("**Semester & Credits**")
                    c1, c2 = st.columns(2)
                    with c1:
                        st.number_input(
                            "Semester*", 
                            min_value=1, max_value=12, step=1,
                            key="edit_semester_id"
                        )
                    with c2:
                        st.number_input(
                            "Total Credits*", 
                            min_value=0.0, max_value=40.0, step=0.5,
                            key="edit_credits_total"
                        )
                    
                    # --- *** NEW: Static L/T/P/S fields *** ---
                    st.markdown("**Workload (L/T/P/S)**")
                    c1, c2, c3, c4 = st.columns(4)
                    with c1:
                        st.number_input("L (Lectures)", min_value=0.0, step=1.0, key="edit_L")
                    with c2:
                        st.number_input("T (Tutorials)", min_value=0.0, step=1.0, key="edit_T")
                    with c3:
                        st.number_input("P (Practicals)", min_value=0.0, step=1.0, key="edit_P")
                    with c4:
                        st.number_input("S (Studio)", min_value=0.0, step=1.0, key="edit_S")
                    # --- *** END NEW L/T/P/S *** ---


                    # --- FORM: Attainment & Passing ---
                    st.markdown("**Assessment (Max Marks)**")
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        # --- *** FIX: max_value=500 *** ---
                        st.number_input(
                            "**Maximum Internal Marks**", 
                            min_value=0, max_value=500,
                            key="edit_internal_marks_max"
                        )
                    with c2:
                        # --- *** FIX: max_value=500 *** ---
                        st.number_input(
                            "**Maximum External Marks (Exam)**", 
                            min_value=0, max_value=500,
                            key="edit_exam_marks_max"
                        )
                    with c3:
                        # --- *** FIX: max_value=500 *** ---
                        st.number_input(
                            "**Maximum External Marks (Jury/Viva)**", 
                            min_value=0, max_value=500,
                            key="edit_jury_viva_marks_max"
                        )

                    st.markdown("**Passing Threshold**")
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        st.number_input(
                            "**Minimum Internal Passing %**", 
                            min_value=0.0, max_value=100.0, step=1.0,
                            key="edit_min_internal_percent"
                        )
                    with c2:
                        st.number_input(
                            "**Minimum External Passing %**", 
                            min_value=0.0, max_value=100.0, step=1.0,
                            key="edit_min_external_percent"
                        )
                    with c3:
                        st.number_input(
                            "**Minimum Overall Passing %**", 
                            min_value=0.0, max_value=100.0, step=1.0,
                            key="edit_min_overall_percent"
                        )

                    with st.expander("Attainment Requirements (optional)"):
                        st.markdown("**Direct Attainment**")
                        c1, c2 = st.columns(2)
                        with c1:
                            st.selectbox(
                                "**Direct Attainment Source**",
                                options=["overall", "separate"],
                                format_func=lambda x: "Overall (Combined)" if x == "overall" else "Separate (Internal & External)",
                                key="edit_direct_source_mode"
                            )
                        with c2:
                            pass # Spacer

                        c1, c2 = st.columns(2)
                        with c1:
                            edit_internal_weight = st.number_input(
                                "**Direct Attainment - Internal Marks Contribution %**",
                                min_value=0.0, max_value=100.0, step=1.0,
                                key="edit_direct_internal_weight_percent"
                            )
                        with c2:
                            edit_external_weight = 100.0 - edit_internal_weight
                            st.metric(
                                "**Direct Attainment - External Marks Contribution %**",
                                f"{edit_external_weight:.1f} %"
                            )

                        st.markdown("**Overall Attainment**")
                        c1, c2 = st.columns(2)
                        with c1:
                            edit_direct_attainment_pct = st.number_input(
                                "**Direct Attainment % in Total Attainment**",
                                min_value=0.0, max_value=100.0, step=1.0,
                                key="edit_direct_target_students_percent"
                            )
                        with c2:
                            edit_indirect_attainment_pct = 100.0 - edit_direct_attainment_pct
                            st.metric(
                                "**Indirect Attainment % in Total Attainment**",
                                f"{edit_indirect_attainment_pct:.1f} %"
                            )
                        
                        st.number_input(
                            "**Minimum Indirect Attainment through Feedback Response Rate**",
                            min_value=0.0, max_value=100.0, step=1.0,
                            key="edit_indirect_min_response_rate_percent"
                        )

                    st.text_area("Description", key="edit_description")
                    
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        st.selectbox(
                            "Status", 
                            options=["active", "inactive", "archived"], 
                            key="edit_status"
                        )
                    with c2:
                        st.checkbox("Active", key="edit_active")
                    with c3:
                        st.number_input(
                            "Sort Order", 
                            key="edit_sort_order"
                        )
                    
                    # --- FORM SUBMIT ---
                    if st.form_submit_button("💾 Save Changes", type="primary", use_container_width=True):
                        
                        # --- *** REBUILD workload_json *** ---
                        L_val = st.session_state.edit_L
                        T_val = st.session_state.edit_T
                        P_val = st.session_state.edit_P
                        S_val = st.session_state.edit_S

                        workload_components = []
                        if L_val > 0: workload_components.append({"code": "L", "name": "Lectures", "hours": L_val})
                        if T_val > 0: workload_components.append({"code": "T", "name": "Tutorials", "hours": T_val})
                        if P_val > 0: workload_components.append({"code": "P", "name": "Practicals", "hours": P_val})
                        if S_val > 0: workload_components.append({"code": "S", "name": "Studio", "hours": S_val})

                        other_components = _read_other_workload_from_state("edit_other_workload_components")
                        workload_components.extend(other_components)
                        workload_json = json.dumps(workload_components) if workload_components else None
                        # --- *** END REBUILD *** ---

                        # Read internal weight and calculate external weight
                        internal_weight = st.session_state.edit_direct_internal_weight_percent
                        external_weight = 100.0 - internal_weight
                        
                        data = {
                            # Key fields (from subject, not form)
                            "subject_code": subject["subject_code"],
                            "degree_code": subject["degree_code"],
                            
                            # Updated fields
                            "subject_name": st.session_state.edit_subject_name.strip(),
                            "subject_type": st.session_state.edit_subject_type,
                            "program_code": st.session_state.edit_program_code or None,
                            "branch_code": st.session_state.edit_branch_code or None,
                            "curriculum_group_code": st.session_state.edit_cg_code or None,
                            "semester_id": st.session_state.edit_semester_id,
                            "credits_total": st.session_state.edit_credits_total,
                            "L": L_val,
                            "T": T_val,
                            "P": P_val,
                            "S": S_val,
                            "workload_breakup_json": workload_json,
                            "internal_marks_max": st.session_state.edit_internal_marks_max,
                            "exam_marks_max": st.session_state.edit_exam_marks_max,
                            "jury_viva_marks_max": st.session_state.edit_jury_viva_marks_max,
                            "min_internal_percent": st.session_state.edit_min_internal_percent,
                            "min_external_percent": st.session_state.edit_min_external_percent,
                            "min_overall_percent": st.session_state.edit_min_overall_percent,
                            "direct_source_mode": st.session_state.edit_direct_source_mode,
                            "direct_internal_threshold_percent": 50.0, # Hard-coded default
                            "direct_external_threshold_percent": 40.0, # Hard-coded default
                            "direct_internal_weight_percent": internal_weight,
                            "direct_external_weight_percent": external_weight,
                            "direct_target_students_percent": edit_direct_attainment_pct,
                            "indirect_target_students_percent": edit_indirect_attainment_pct,
                            "indirect_min_response_rate_percent": st.session_state.edit_indirect_min_response_rate_percent,
                            "overall_direct_weight_percent": edit_direct_attainment_pct,
                            "overall_indirect_weight_percent": edit_indirect_attainment_pct,
                            "description": st.session_state.edit_description,
                            "status": st.session_state.edit_status,
                            "active": st.session_state.edit_active,
                            "sort_order": st.session_state.edit_sort_order,
                        }
                        
                        try:
                            update_subject(engine, subject["id"], data, actor)
                            success(f"Subject '{data['subject_code']}' updated successfully!")
                            st.session_state.current_edit_subject_id = None # Clear state to force reload
                            _init_other_workload_state("edit_other_workload_components")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed to update subject: {e}")
                
                # --- END OF EDIT FORM ---
                
                # --- WORKLOAD UI (OUTSIDE FORM) ---
                st.markdown("**Other Workload Components**")
                st.caption("Edit any non-L/T/P/S components.")
                
                if st.session_state.edit_other_workload_components:
                    for idx, item in enumerate(st.session_state.edit_other_workload_components):
                        cols = st.columns([1, 3, 1.5, 0.5])
                        with cols[0]:
                            st.text_input(
                                "Code", 
                                value=item.get("code", ""), 
                                key=f"edit_other_workload_components_code_{idx}", 
                                placeholder="e.g. FW"
                            )
                        with cols[1]:
                            st.text_input(
                                "Name", 
                                value=item.get("name", ""), 
                                key=f"edit_other_workload_components_name_{idx}", 
                                placeholder="e.g. Field Work"
                            )
                        with cols[2]:
                            st.number_input(
                                "Hours/Periods", 
                                value=float(item.get("hours", 0)), 
                                min_value=0.0, 
                                max_value=200.0, 
                                step=1.0, 
                                key=f"edit_other_workload_components_hours_{idx}"
                            )
                        with cols[3]:
                            st.button(
                                "🗑️", 
                                key=f"edit_workload_del_{idx}", 
                                on_click=_delete_other_workload_row, 
                                args=("edit_other_workload_components", idx), 
                                help="Delete this row"
                            )
                else:
                    st.info("No 'Other' workload components set. Click 'Add Component' to add one.")
                
                st.markdown("**Note:** Click 'Add Component' below, fill the fields, then click 'Save Changes' to update.")

                st.button(
                    "➕ Add Other Component", 
                    on_click=_add_other_workload_row, 
                    args=("edit_other_workload_components",),
                    key=f"add_edit_workload_{subject['id']}",
                    type="secondary",
                    use_container_width=True
                )
                # --- END WORKLOAD UI ---

                # --- DELETE BUTTON ---
                st.markdown("---")
                st.markdown("### Delete Subject")
                st.warning(
                    "**Warning:** Deleting a subject is permanent and will "
                    "remove it from the catalog. This action cannot be undone."
                )
                
                if st.button(f"DELETE Subject {subject['subject_code']}", type="primary"):
                    try:
                        delete_subject(engine, subject["id"], actor)
                        success(f"Subject '{subject['subject_code']}' deleted.")
                        st.session_state.current_edit_subject_id = None # Clear state
                        _init_other_workload_state("edit_other_workload_components")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to delete subject: {e}")

    # --- *** MODIFIED: User-Friendly Error handling block *** ---
    except OperationalError as e:
        # Specifically catch the "no such table" error
        if "no such table" in str(e):
            st.error(
                "Application Not Ready",
                icon="🛠️"
            )
            st.warning(
                "**The application cannot connect to the required database tables.**"
            )
            st.info(
                """
                This module is not yet configured. The database tables 
                (e.g., `degrees`, `subjects`) appear to be missing.
                
                **Please contact your system administrator** to run the 
                initial database setup.
                """
            )
            # Log the technical error for the developer (in the console/logs)
            # but do not show it to the end-user.
            logger.error(f"Database schema missing: {e}")
        else:
            # Show a generic message for other, unexpected database operational errors
            st.error("A Database Error Occurred", icon="🔥")
            st.warning(
                "An unexpected database problem occurred. Please try again later. "
                "If the problem persists, please contact your system administrator."
            )
            logger.error(f"Caught unexpected OperationalError: {e}")
    
    except Exception as e:
        # Catch any other unexpected application errors
        st.error("An Application Error Occurred", icon="🔥")
        st.warning(
            "An unexpected application error occurred. Please try again later. "
            "If the problem persists, please contact your system administrator."
        )
        logger.error(f"Caught unexpected Exception in tab_subjects: {e}", exc_info=True)
