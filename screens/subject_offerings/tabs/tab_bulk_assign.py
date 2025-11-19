# ==================================================================
# tab_bulk_assign.py
# ==================================================================
"""
Bulk Assignment Tab - Assign settings to multiple offerings at once
"""

import streamlit as st
import pandas as pd
from ..helpers import exec_query, rows_to_dicts
# Import helpers and CRUD functions
from ..db_helpers import (
    fetch_degrees, fetch_programs, fetch_branches,
    fetch_academic_years, fetch_divisions, fetch_offerings,
    fetch_semesters_for_degree  # <<< MODIFIED: Added import
)
from ..offerings_crud import bulk_update_offerings
from ..constants import STATUS_VALUES
from core.forms import success


def render(engine, actor: str, CAN_EDIT: bool):
    """Render the Bulk Assignment tab."""
    st.subheader("塘 Bulk Assignment")
    st.caption("Apply settings to multiple offerings at once")

    if not CAN_EDIT:
        st.warning("You need edit permissions to use this tool.")
        return

    # --- 1. FILTERS (copied from tab_offerings.py) ---
    degrees = fetch_degrees(engine)
    if not degrees:
        st.warning("No degrees found. Create degrees first.")
        return

    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        degree_code = st.selectbox(
            "Degree",
            options=[d["code"] for d in degrees],
            format_func=lambda x: next((d["title"] for d in degrees if d["code"] == x), x),
            key="bulk_degree"
        )
    
    degree = next((d for d in degrees if d["code"] == degree_code), None)
    cohort_mode = degree.get("cohort_splitting_mode", "none") if degree else "none"
    
    programs = fetch_programs(engine, degree_code) if degree else []
    prog_disabled = cohort_mode not in ["both", "program_only"]
    filter_program_code = None
    
    if not prog_disabled and programs:
        with col2:
            filter_program_code = st.selectbox(
                "Program (filter)",
                options=[None] + [p["program_code"] for p in programs],
                format_func=lambda x: "All Programs" if x is None else next(
                    (p["program_name"] for p in programs if p["program_code"] == x), x
                ),
                key="bulk_program"
            )

    ays = fetch_academic_years(engine)
    if not ays:
        st.warning("No academic years found. Create academic years first.")
        return
    
    # --- MODIFIED BLOCK: DYNAMIC YEAR/TERM ---
    semesters = fetch_semesters_for_degree(engine, degree_code)
    if not semesters:
        st.warning(f"No semester structure found for degree '{degree_code}'. Please configure and build semesters first.")
        return
    
    available_years = sorted(list(set(s["year_index"] for s in semesters)))
    
    with col3:
        ay_label = st.selectbox(
            "Academic Year",
            options=[ay["ay_code"] for ay in ays],
            key="bulk_ay"
        )
    
    with col4:
        year = st.selectbox(
            "Year",
            options=available_years,
            key="bulk_year",
            format_func=lambda y: f"Year {y}"
        )
    
    available_terms = sorted(list(set(
        s["term_index"] for s in semesters if s["year_index"] == year
    )))
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        term = st.selectbox(
            "Term",
            options=available_terms,
            key="bulk_term",
            format_func=lambda t: f"Term {t}"
        )
    # --- END MODIFIED BLOCK ---
    
    divisions = fetch_divisions(engine, degree_code, ay_label, year)
    filter_division_code = None
    
    with col2:
        if divisions:
            filter_division_code = st.selectbox(
                "Division (filter)",
                options=[None] + [d["division_code"] for d in divisions],
                format_func=lambda x: "All Divisions" if x is None else x,
                key="bulk_division"
            )
    
    st.markdown("---")

    # --- 2. FETCH OFFERINGS ---
    with engine.begin() as conn:
        offerings = fetch_offerings(
            conn, degree_code, ay_label, year, term,
            filter_program_code,
            None, # Branch filter not implemented in this scope
            filter_division_code
        )

    if not offerings:
        st.info("No offerings found for this scope. Cannot perform bulk actions.")
        return

    st.markdown(f"**{len(offerings)}** offerings found for this scope.")
    with st.expander("View Offerings"):
        df = pd.DataFrame(offerings)
        display_cols = [
            "id", "subject_code", "subject_name", "division_code",
            "applies_to_all_divisions", "status", "instructor_email"
        ]
        display_cols = [c for c in display_cols if c in df.columns]
        st.dataframe(df[display_cols], use_container_width=True)

    st.markdown("---")
    
    # --- 3. BULK ACTION TOOLS ---

    # Tool 1: Bulk Update Status
    with st.expander("噫 Bulk Update Status (e.g., Draft -> Published)"):
        with st.form("bulk_status_form"):
            st.markdown(
                "Select a new status and the offerings to apply it to. "
                "This is useful for publishing all 'draft' offerings at once."
            )
            
            col1, col2 = st.columns([1,2])
            with col1:
                new_status = st.selectbox(
                    "New Status",
                    options=STATUS_VALUES,
                    index=0,
                    key="bulk_new_status"
                )
                reason_status = st.text_input("Reason for change", key="bulk_status_reason", placeholder="e.g., Start of term")
            
            with col2:
                # Helper to pre-select all 'draft' if 'published' is chosen
                default_ids = []
                if new_status == 'published':
                    default_ids = [o['id'] for o in offerings if o['status'] == 'draft']
                
                status_offering_ids = st.multiselect(
                    "Select Offerings to Update",
                    options=[o["id"] for o in offerings],
                    format_func=lambda x: next(
                        (f"{o['subject_code']} - {o['subject_name']} (Current: {o['status']})" for o in offerings if o["id"] == x),
                        str(x)
                    ),
                    default=default_ids,
                    key="bulk_status_ids"
                )
            
            submitted_status = st.form_submit_button("Run Bulk Status Update", type="primary")

            if submitted_status:
                if not status_offering_ids:
                    st.error("No offerings selected.")
                elif not reason_status:
                    st.error("A reason is required for bulk updates.")
                else:
                    try:
                        with st.spinner(f"Updating {len(status_offering_ids)} offerings..."):
                            count, errors = bulk_update_offerings(
                                engine,
                                status_offering_ids,
                                {"status": new_status},
                                actor,
                                reason=reason_status
                            )
                        success(f"Successfully updated status for {count} offering(s).")
                        if errors:
                            st.warning(f"{len(errors)} error(s) occurred:")
                            for err in errors:
                                st.error(err)
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

    # Tool 2: Bulk Assign Instructor
    with st.expander("ｧ鯛 昨沛ｫ Bulk Assign Instructor"):
        with st.form("bulk_instructor_form"):
            st.markdown("Select an instructor and the offerings to assign them to.")
            
            col1, col2 = st.columns([1,2])
            with col1:
                new_instructor = st.text_input(
                    "Instructor Email",
                    key="bulk_new_instructor"
                )
                reason_instr = st.text_input("Reason for change", key="bulk_instr_reason", placeholder="e.g., Faculty assignment")

            with col2:
                instr_offering_ids = st.multiselect(
                    "Select Offerings to Update",
                    options=[o["id"] for o in offerings],
                    format_func=lambda x: next(
                        (f"{o['subject_code']} - {o['subject_name']} (Current: {o.get('instructor_email') or 'None'})" for o in offerings if o["id"] == x),
                        str(x)
                    ),
                    key="bulk_instr_ids"
                )
            
            submitted_instr = st.form_submit_button("Run Bulk Instructor Assignment", type="primary")

            if submitted_instr:
                if not instr_offering_ids:
                    st.error("No offerings selected.")
                elif not new_instructor:
                    st.error("Instructor email is required.")
                elif not reason_instr:
                    st.error("A reason is required for bulk updates.")
                else:
                    try:
                        with st.spinner(f"Updating {len(instr_offering_ids)} offerings..."):
                            count, errors = bulk_update_offerings(
                                engine,
                                instr_offering_ids,
                                {"instructor_email": new_instructor},
                                actor,
                                reason=reason_instr
                            )
                        success(f"Successfully assigned instructor for {count} offering(s).")
                        if errors:
                            st.warning(f"{len(errors)} error(s) occurred:")
                            for err in errors:
                                st.error(err)
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")
