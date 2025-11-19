# screens/class_in_charge/main.py
"""
Class-in-Charge Management - Main Screen
FIXED: Faculty Selection moved OUTSIDE st.form to allow "Filter" toggle to work immediately.
"""

import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List, Any
import json

from sqlalchemy import text as sa_text
from sqlalchemy.exc import OperationalError

# Core imports
from core.settings import load_settings
from core.db import get_engine
from core.policy import require_page, can_edit_page, user_roles

# Service and Filters
from screens.class_in_charge import class_in_charge_service as cic_service
from screens.class_in_charge import cic_filters

# Import the term calculation logic
try:
    from screens.academic_years.db import compute_terms_with_validation
except ImportError:
    compute_terms_with_validation = None

PAGE_TITLE = "Class-in-Charge Assignments"

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def check_prerequisites(engine) -> List[str]:
    """Check if core tables (AY, Degrees, Faculty) exist."""
    missing = []
    with engine.begin() as conn:
        try:
            conn.execute(sa_text("SELECT 1 FROM academic_years LIMIT 1")).fetchone()
        except OperationalError as e:
            if "no such table" in str(e): missing.append("Academic Years")
        
        try:
            conn.execute(sa_text("SELECT 1 FROM degrees LIMIT 1")).fetchone()
        except OperationalError as e:
            if "no such table" in str(e): missing.append("Degrees")
        
        try:
            conn.execute(sa_text("SELECT 1 FROM faculty_profiles LIMIT 1")).fetchone()
        except OperationalError as e:
            if "no such table" in str(e): missing.append("Faculty Profiles")
            
    return missing

def get_ay_dates(engine, ay_code: str) -> tuple:
    """Get AY start and end dates."""
    with engine.begin() as conn:
        result = conn.execute(sa_text("""
            SELECT start_date, end_date 
            FROM academic_years 
            WHERE ay_code = :ay
        """), {"ay": ay_code}).fetchone()
        
        if result:
            start = datetime.strptime(result[0], "%Y-%m-%d").date()
            end = datetime.strptime(result[1], "%Y-%m-%d").date()
            return start, end
    return None, None

def get_batch_for_scope(engine, degree_code: str, program_code: Optional[str], branch_code: Optional[str], year: int) -> Optional[str]:
    """Get the batch code for a given academic scope."""
    query = """
        SELECT DISTINCT se.batch
        FROM student_enrollments se
        WHERE se.degree_code = :deg
          AND se.current_year = :yr
          AND se.enrollment_status = 'active'
    """
    params = {"deg": degree_code, "yr": year}
    if program_code:
        query += " AND se.program_code = :prog"
        params["prog"] = program_code
    if branch_code:
        query += " AND se.branch_code = :br"
        params["br"] = branch_code
        
    query += " LIMIT 1"
    try:
        with engine.begin() as conn:
            result = conn.execute(sa_text(query), params).fetchone()
            if result:
                return result[0]
    except Exception:
        return None
    return None

def _get_next_ay_code(ay_code: str) -> Optional[str]:
    try:
        start_year = int(ay_code.split('-')[0])
        next_start = start_year + 1
        next_end = (next_start + 1) % 100
        return f"{next_start}-{next_end:02d}"
    except Exception:
        return None

# ============================================================================
# UI COMPONENTS
# ============================================================================

def render_assignment_form(engine, actor: str, edit_mode: bool = False, assignment_id: Optional[int] = None):
    """
    Render form for creating/editing CIC assignment.
    
    STRUCTURE:
    1. Interactive Container (Outside Form): Scope Selection & Faculty Selection (Reactive)
    2. Form Container: Date Entry & Submission (Batched)
    """
    
    st.subheader("➕ Create New Assignment" if not edit_mode else "✏️ Edit Assignment")
    
    # Load existing data if editing
    existing_data = None
    if edit_mode and assignment_id:
        existing_data = cic_service.get_assignment_by_id(engine, assignment_id)
        if not existing_data:
            st.error("Assignment not found")
            return
    
    # ========================================================================
    # PART 1: INTERACTIVE SELECTION (Scope & Faculty)
    # ========================================================================
    with st.container():
        st.markdown("#### 📋 Scope & Faculty")
        col1, col2 = st.columns(2)
        
        # --- SCOPE SELECTION ---
        with col1:
            # 1. Academic Year
            academic_years = cic_filters.fetch_academic_years(engine)
            if not academic_years: st.error("No academic years found"); return
            
            default_ay_idx = 0
            if existing_data and existing_data["ay_code"] in academic_years:
                default_ay_idx = academic_years.index(existing_data["ay_code"])
            
            ay_code = st.selectbox("Academic Year*", options=academic_years, index=default_ay_idx, disabled=edit_mode)
            
            # 2. Degree (Triggers Updates)
            degrees = cic_filters.fetch_degrees(engine)
            if not degrees: st.error("No degrees found"); return
            
            degree_options = {d['code']: d for d in degrees}
            degree_codes_list = list(degree_options.keys())
            
            default_deg_idx = 0
            if existing_data and existing_data["degree_code"] in degree_codes_list:
                default_deg_idx = degree_codes_list.index(existing_data["degree_code"])
            
            degree_code = st.selectbox(
                "Degree*",
                options=degree_codes_list,
                format_func=lambda x: f"{x} - {degree_options[x]['title']}",
                index=default_deg_idx,
                disabled=edit_mode
            )

        # Variables for logic
        program_code = None; program_id = None
        branch_code = None; branch_id = None

        with col2:
            # 3. Program
            programs = cic_filters.fetch_programs_by_degree(engine, degree_code)
            if programs:
                prog_map = {f"{p['code']} - {p['name']}": (p['code'], p['id']) for p in programs}
                prog_labels = list(prog_map.keys())
                
                default_prog_idx = 0
                if existing_data and existing_data.get("program_code"):
                    for idx, p in enumerate(programs):
                        if p['code'] == existing_data['program_code']: default_prog_idx = idx; break
                
                selected_prog_label = st.selectbox("Program*", options=prog_labels, index=default_prog_idx, disabled=edit_mode, key=f"prog_{degree_code}")
                if selected_prog_label: program_code, program_id = prog_map[selected_prog_label]
            else:
                st.caption("Program is managed at the Degree level.")
            
            # 4. Branch
            if program_id:
                branches = cic_filters.fetch_branches_by_program(engine, degree_code, program_id)
                if branches:
                    branch_map = {f"{b['code']} - {b['name']}": (b['code'], b['id']) for b in branches}
                    branch_labels = list(branch_map.keys())
                    default_br_idx = 0
                    if existing_data and existing_data.get("branch_code"):
                         for idx, b in enumerate(branches):
                            if b['code'] == existing_data['branch_code']: default_br_idx = idx; break
                    selected_branch_label = st.selectbox("Branch*", options=branch_labels, index=default_br_idx, disabled=edit_mode, key=f"br_{program_id}")
                    if selected_branch_label: branch_code, branch_id = branch_map[selected_branch_label]
                else: st.caption("Branch is managed at the Program level.")
            else: st.caption("Select a Program to view Branches.")

        # --- FACULTY SELECTION (Now Reactive) ---
        st.markdown("---")
        
        # Toggle Control
        col_toggle, col_select = st.columns([1, 2])
        
        with col_toggle:
            st.markdown("###") # Spacing
            filter_by_degree = st.checkbox(
                "Strict Affiliation", 
                value=True,
                help="If checked, only shows faculty explicitly linked to this Degree in Faculty Affiliations."
            )
        
        with col_select:
            # Fetch Faculty based on Toggle
            search_degree = degree_code if filter_by_degree else None
            faculty_list = cic_filters.fetch_faculty_for_degree(engine, search_degree)
            
            # Handle Empty List Case
            if not faculty_list and filter_by_degree:
                st.warning(f"No faculty affiliated with {degree_code}. Uncheck 'Strict Affiliation' to see all.")
                faculty_id = None
            elif not faculty_list:
                st.error("No active faculty found in system.")
                faculty_id = None
            else:
                # Selection Logic
                default_fac_idx = 0
                fac_ids = [f["id"] for f in faculty_list]
                
                if existing_data and existing_data.get("faculty_id") in fac_ids:
                    default_fac_idx = fac_ids.index(existing_data["faculty_id"])
                
                faculty_id = st.selectbox(
                    "Select Faculty*",
                    options=[f["id"] for f in faculty_list],
                    format_func=lambda x: next((f"{f['name']} ({f['email']})" for f in faculty_list if f["id"] == x), "Unknown"),
                    index=default_fac_idx
                )
    
    # ========================================================================
    # PART 2: DETAILS FORM (Year, Term, Date, Submit)
    # ========================================================================
    form_key = f"cic_form_edit_{assignment_id}" if edit_mode else "cic_form_create"
    
    with st.form(form_key):
        
        # 5. Year/Term (Dynamic Structure)
        structure = cic_filters.fetch_semester_structure(engine, degree_code, program_id, branch_id)
        max_years = structure.get("years", 4)
        max_terms = structure.get("terms_per_year", 2)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            year = st.number_input("Year*", min_value=1, max_value=max_years, value=existing_data["year"] if existing_data else 1, disabled=edit_mode)
        with col2:
            term = st.number_input("Term*", min_value=1, max_value=max_terms, value=existing_data["term"] if existing_data else 1, disabled=edit_mode)
        with col3:
            division_code = st.text_input("Division (optional)", value=existing_data.get("division_code", "") if existing_data else "", disabled=edit_mode)

        # Date Logic
        ay_start, ay_end = get_ay_dates(engine, ay_code)
        default_start_date = ay_start or date.today()
        default_end_date = ay_end or date.today() + timedelta(days=180)
        
        if edit_mode and existing_data:
            default_start_date = existing_data["start_date"]
            default_end_date = existing_data["end_date"]
            if isinstance(default_start_date, str): default_start_date = date.fromisoformat(default_start_date)
            if isinstance(default_end_date, str): default_end_date = date.fromisoformat(default_end_date)
        
        # Calculate Terms (Background)
        calculated_terms = []
        if not edit_mode and compute_terms_with_validation:
            try:
                with engine.begin() as conn:
                    calculated_terms, warnings = compute_terms_with_validation(
                        conn, ay_code=ay_code, degree_code=degree_code,
                        program_code=program_code or None, branch_code=branch_code or None,
                        progression_year=year
                    )
            except Exception: pass
            
            # Auto-fill dates if Term found
            if calculated_terms and len(calculated_terms) >= term:
                try:
                    term_data = calculated_terms[term - 1]
                    default_start_date = date.fromisoformat(term_data['start_date'])
                    default_end_date = date.fromisoformat(term_data['end_date'])
                    st.caption(f"🗓️ Dates auto-filled from Calendar.")
                except: pass

        col_d1, col_d2 = st.columns(2)
        start_date = col_d1.date_input("Start Date*", value=default_start_date)
        end_date = col_d2.date_input("End Date*", value=default_end_date)
        
        status = "active"
        if edit_mode:
            status = st.selectbox("Status", options=["active", "inactive", "suspended"], 
                                index=["active", "inactive", "suspended"].index(existing_data.get("status", "active")))

        # Term Extension
        selected_extra_terms = []
        if not edit_mode and max_terms > 1 and calculated_terms:
            remaining_terms = [t for t in range(term + 1, max_terms + 1)]
            if remaining_terms:
                st.markdown("---")
                selected_extra_terms = st.multiselect("Extend assignment to next term(s):", options=remaining_terms, format_func=lambda x: f"Term {x}")

        st.markdown("---")
        col_sub, col_can = st.columns([3, 1])
        submitted = col_sub.form_submit_button("💾 Save Assignment", type="primary", use_container_width=True)
        cancelled = col_can.form_submit_button("❌ Cancel", use_container_width=True)
        
        if cancelled:
            st.session_state.pop("editing_assignment_id", None); st.session_state.pop("creating_assignment", None); st.rerun()
        
        if submitted:
            form_errors = []
            if programs and not program_code: form_errors.append("❌ Program is required for this degree.")
            if program_id and cic_filters.fetch_branches_by_program(engine, degree_code, program_id) and not branch_code:
                 form_errors.append("❌ Branch is required for this program.")
            if not faculty_id: form_errors.append("❌ Faculty selection is required.")

            if form_errors:
                for err in form_errors: st.error(err)
                return

            common_data = {
                "ay_code": ay_code, "degree_code": degree_code, "program_code": program_code,
                "branch_code": branch_code, "division_code": division_code if division_code else None,
                "faculty_id": faculty_id, "status": status
            }

            if not edit_mode and selected_extra_terms:
                # Multi-term creation
                terms_to_create = [term] + selected_extra_terms
                for t in terms_to_create:
                    t_start, t_end = start_date, end_date
                    if t != term:
                        try:
                            td = calculated_terms[t-1]
                            t_start, t_end = date.fromisoformat(td['start_date']), date.fromisoformat(td['end_date'])
                        except: st.error(f"Term {t} date error"); continue
                    
                    d = common_data.copy(); d.update({"year": year, "term": t, "start_date": t_start, "end_date": t_end})
                    cic_service.create_assignment(engine, d, actor)
                st.success("Assignments created!"); st.session_state.pop("creating_assignment", None); st.rerun()
            else:
                # Single creation / update
                common_data.update({"year": year, "term": term, "start_date": start_date, "end_date": end_date})
                if edit_mode:
                    success, _, _ = cic_service.update_assignment(engine, assignment_id, common_data, actor)
                else:
                    aid, _, _ = cic_service.create_assignment(engine, common_data, actor)
                    success = aid is not None
                
                if success:
                    st.success("Saved!"); st.session_state.pop("editing_assignment_id", None); st.session_state.pop("creating_assignment", None); st.rerun()

# ... (Rest of the file functions like render_change_cic_dialog, render_assignments_list, etc. remain exactly as they were) ...
# For brevity, I am not repeating the unchanged functions below this point. 
# Ensure you keep the existing implementations of:
# - render_change_cic_dialog
# - render_extend_form
# - render_assignments_list
# - render_expiring_soon
# - render_coverage_analysis
# - render_audit_log
# - render

def render_change_cic_dialog(engine, assignment: Dict, actor: str):
    st.subheader(f"🔄 Change CIC for {assignment['degree_code']} - Year {assignment['year']}, Term {assignment['term']}")
    st.info(f"**Current CIC:** {assignment['faculty_name']}")
    
    # Use affiliation filter here too? Yes, logic applies.
    filter_strict = st.checkbox("Strict Affiliation", value=True)
    search_degree = assignment['degree_code'] if filter_strict else None
    faculty_list = cic_filters.fetch_faculty_for_degree(engine, search_degree)
    
    if not faculty_list and filter_strict:
         st.warning("No affiliated faculty found. Uncheck to see all.")
    
    faculty_options = [""] + [f"{f['name']} ({f['email']})" for f in faculty_list]
    selected_faculty_option = st.selectbox("Select New Faculty *", options=faculty_options)
    reason = st.text_area("Reason for Change *")
    
    col1, col2 = st.columns(2)
    if col1.button("💾 Save Change", type="primary", use_container_width=True):
        if not selected_faculty_option or not reason: st.error("Fields required"); return
        selected_email = selected_faculty_option.split("(")[1].strip(")")
        new_fac = next(f for f in faculty_list if f['email'] == selected_email)
        success, errors = cic_service.change_cic(engine, assignment['id'], new_fac['id'], new_fac['email'], new_fac['name'], actor, reason)
        if success: st.success("Changed!"); del st.session_state['change_cic_id']; st.rerun()
        else: st.error(errors[0])
    if col2.button("❌ Cancel", use_container_width=True):
        del st.session_state['change_cic_id']; st.rerun()

def render_extend_form(engine, assignment_id: int, actor: str):
    # (Code identical to previous submission)
    # ... (Please use the previous implementation for render_extend_form) ...
    st.subheader("➡️ Extend / Rollover Assignment")
    source_assignment = cic_service.get_assignment_by_id(engine, assignment_id)
    if not source_assignment: st.error("Assignment not found"); return

    st.markdown(f"**Source:** {source_assignment['degree_code']} / Y{source_assignment['year']}T{source_assignment['term']} - {source_assignment['faculty_name']}")
    
    structure = cic_filters.fetch_semester_structure(engine, source_assignment['degree_code'], source_assignment.get('program_code'), source_assignment.get('branch_code'))
    max_terms = structure.get("terms_per_year", 2)
    
    target_ay = source_assignment['ay_code']
    target_year = source_assignment['year']
    target_term = source_assignment['term'] + 1
    
    if target_term > max_terms:
        target_term = 1; target_year += 1
        target_ay = _get_next_ay_code(source_assignment['ay_code'])
        st.info("Rollover to next Academic Year.")
    else:
        st.info(f"Extending to Term {target_term}.")

    if not target_ay: st.error("Next AY unknown"); return
    if target_year > structure.get("years", 4): st.error("Exceeds max years"); return

    # Target Dates
    target_start, target_end = date.today(), date.today()
    try:
        with engine.begin() as conn:
            terms, _ = compute_terms_with_validation(conn, target_ay, source_assignment['degree_code'], source_assignment.get('program_code'), source_assignment.get('branch_code'), target_year)
        if terms and len(terms) >= target_term:
             target_start = date.fromisoformat(terms[target_term-1]['start_date'])
             target_end = date.fromisoformat(terms[target_term-1]['end_date'])
    except Exception: pass

    with st.form("extend_form"):
        st.markdown(f"**New Target:** {target_ay} | Year {target_year} | Term {target_term}")
        c1, c2 = st.columns(2)
        start_date = c1.date_input("Start Date", value=target_start)
        end_date = c2.date_input("End Date", value=target_end)
        
        if st.form_submit_button("✅ Confirm", type="primary"):
             data = {
                "ay_code": target_ay, "degree_code": source_assignment['degree_code'],
                "program_code": source_assignment.get('program_code'), "branch_code": source_assignment.get('branch_code'),
                "year": target_year, "term": target_term, "division_code": source_assignment.get('division_code'),
                "faculty_id": source_assignment['faculty_id'], "start_date": start_date, "end_date": end_date, "status": "active"
             }
             aid, err, _ = cic_service.create_assignment(engine, data, actor)
             if aid:
                 st.success("Extended!"); st.session_state.pop("extending_assignment_id", None); st.rerun()
             else:
                 st.error(f"Error: {err}")
        
        if st.form_submit_button("Cancel"):
             st.session_state.pop("extending_assignment_id", None); st.rerun()

def render_assignments_list(engine, actor: str, can_edit: bool):
    st.subheader("📋 CIC Assignments")
    with st.expander("🔍 Filters", expanded=True):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            ays = cic_filters.fetch_academic_years(engine)
            filter_ay = st.selectbox("Academic Year", options=[None] + ays, format_func=lambda x: "All" if x is None else x)
        with col2:
            degs = cic_filters.fetch_degrees(engine)
            filter_deg = st.selectbox("Degree", options=[None] + [d['code'] for d in degs], 
                                    format_func=lambda x: next((d['title'] for d in degs if d['code'] == x), "All") if x else "All")
        with col3:
            filter_status = st.multiselect("Status", options=["active", "inactive", "suspended"], default=["active"])
        with col4:
            filter_expiring = st.checkbox("Expiring Soon (30 days)")
    filters = {}
    if filter_ay: filters["ay_code"] = filter_ay
    if filter_deg: filters["degree_code"] = filter_deg
    if filter_status: filters["status"] = filter_status
    if filter_expiring: filters["expiring_soon"] = True
    assignments = cic_service.list_assignments(engine, filters=filters)
    if assignments:
        st.dataframe(pd.DataFrame(assignments), use_container_width=True, hide_index=True,
                     column_config={"start_date": st.column_config.DateColumn("Start"), "end_date": st.column_config.DateColumn("End")})
    else: st.info("No assignments found.")
    if can_edit and assignments:
        st.markdown("---")
        col1, col2 = st.columns(2)
        selected_id = col1.number_input("Select Assignment ID", min_value=min(a['id'] for a in assignments), max_value=max(a['id'] for a in assignments))
        c1, c2, c3, c4 = col2.columns(4)
        if c1.button("✏️ Edit", use_container_width=True): st.session_state["editing_assignment_id"] = selected_id; st.rerun()
        if c2.button("🔄 Change CIC", use_container_width=True): st.session_state["change_cic_id"] = selected_id; st.rerun()
        if c3.button("➡️ Extend", use_container_width=True): st.session_state["extending_assignment_id"] = selected_id; st.rerun()
        if c4.button("🗑️ Delete", use_container_width=True): st.session_state[f"confirm_delete_{selected_id}"] = True
        if st.session_state.get(f"confirm_delete_{selected_id}"):
            if st.button("✅ Confirm Delete", type="primary"):
                cic_service.delete_assignment(engine, selected_id, actor, "UI Delete"); del st.session_state[f"confirm_delete_{selected_id}"]; st.rerun()

def render_expiring_soon(engine):
    st.subheader("⏰ Expiring Soon")
    expiring = cic_service.get_expiring_assignments(engine, days=30)
    if expiring: st.dataframe(pd.DataFrame(expiring), use_container_width=True, hide_index=True)
    else: st.success("No assignments expiring soon.")

def render_coverage_analysis(engine):
    st.subheader("📊 Coverage Analysis")
    with engine.begin() as conn: results = conn.execute(sa_text("SELECT * FROM v_cic_coverage_analysis")).fetchall()
    if results: st.dataframe(pd.DataFrame([dict(r._mapping) for r in results]), use_container_width=True, hide_index=True)
    else: st.info("No data.")

def render_audit_log(engine):
    st.subheader("📜 Audit Trail")
    with engine.begin() as conn: results = conn.execute(sa_text("SELECT * FROM class_in_charge_audit ORDER BY occurred_at DESC LIMIT 100")).fetchall()
    if results: st.dataframe(pd.DataFrame([dict(r._mapping) for r in results]), use_container_width=True, hide_index=True)

def render():
    settings = load_settings()
    engine = get_engine(settings.db.url)
    require_page(PAGE_TITLE)
    actor = st.session_state.get("user_email", "system")
    can_edit = can_edit_page(PAGE_TITLE, user_roles())
    st.title("📚 " + PAGE_TITLE)
    if st.button("🏠 Back to List"):
        for k in ['creating_assignment', 'editing_assignment_id', 'change_cic_id', 'extending_assignment_id']: st.session_state.pop(k, None)
        st.rerun()
    if check_prerequisites(engine): st.error("Missing prerequisites"); st.stop()
    if st.session_state.get("creating_assignment") or st.session_state.get("editing_assignment_id"):
        render_assignment_form(engine, actor, edit_mode=bool(st.session_state.get("editing_assignment_id")), assignment_id=st.session_state.get("editing_assignment_id"))
    elif st.session_state.get("change_cic_id"):
        a = cic_service.get_assignment_by_id(engine, st.session_state["change_cic_id"])
        render_change_cic_dialog(engine, a, actor)
    elif st.session_state.get("extending_assignment_id"):
        render_extend_form(engine, st.session_state["extending_assignment_id"], actor)
    else:
        tabs = st.tabs(["📋 Assignments", "➕ Create New", "⏰ Expiring", "📊 Coverage", "📜 Audit"])
        with tabs[0]: render_assignments_list(engine, actor, can_edit)
        with tabs[1]: 
            if can_edit and st.button("➕ Create New", type="primary"): st.session_state["creating_assignment"] = True; st.rerun()
        with tabs[2]: render_expiring_soon(engine)
        with tabs[3]: render_coverage_analysis(engine)
        with tabs[4]: render_audit_log(engine)

if __name__ == "__main__":
    render()
