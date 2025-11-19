# ==================================================================
# tab_customize.py
# ==================================================================
"""
Customization Tab - Per-offering overrides
"""

import streamlit as st
import pandas as pd
from ..helpers import exec_query, rows_to_dicts
from ..db_helpers import (
    fetch_degrees, fetch_programs, fetch_branches,
    fetch_academic_years, fetch_divisions, fetch_offerings,
    fetch_catalog_subject_details,
    fetch_semesters_for_degree  # <<< MODIFIED: Added import
)
from ..offerings_crud import update_offering
from core.forms import success


def _show_override_row(
    field_key: str, 
    label: str, 
    catalog_val: float, 
    offering_val: float, 
    is_disabled: bool,
    step: float = 1.0
):
    """Helper to render a comparison row in the form."""
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        st.caption(f"**{label}**")
    with col2:
        st.number_input(
            "Catalog Default",
            value=catalog_val,
            disabled=True,
            key=f"cat_{field_key}",
            label_visibility="collapsed"
        )
    with col3:
        st.number_input(
            "Override Value",
            value=offering_val,
            disabled=is_disabled,
            key=f"cust_{field_key}",  # This is the key we read on submit
            label_visibility="collapsed",
            step=step
        )


def render(engine, actor: str, CAN_EDIT: bool):
    """Render the Customization tab."""
    st.subheader("✏️ Offering-Specific Customization")
    st.caption("Override inherited credits, workload, or marks for a single offering")

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
            key="cust_degree"
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
                key="cust_program"
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
            key="cust_ay"
        )
    
    with col4:
        year = st.selectbox(
            "Year",
            options=available_years,
            key="cust_year",
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
            key="cust_term",
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
                key="cust_division"
            )
    
    st.markdown("---")

    # --- 2. FETCH OFFERINGS & SELECT ---
    with engine.begin() as conn:
        offerings = fetch_offerings(
            conn, degree_code, ay_label, year, term,
            filter_program_code,
            None, # Branch filter not implemented in this scope
            filter_division_code
        )

    if not offerings:
        st.info("No offerings found for this scope.")
        return

    # Selectbox to pick one offering
    offering_options = {o['id']: f"{o['subject_code']} - {o.get('subject_name', 'N/A')} ({'OVERRIDDEN' if o.get('override_inheritance') else 'Default'})" for o in offerings}
    selected_offering_id = st.selectbox(
        "Select Offering to Customize",
        options=[None] + list(offering_options.keys()),
        format_func=lambda x: "Select an offering..." if x is None else offering_options[x],
        key="cust_offering_select"
    )

    if not selected_offering_id:
        return

    # --- 3. FETCH DATA & RENDER FORM ---
    
    # Get the two data sources
    offering = next((o for o in offerings if o['id'] == selected_offering_id), None)
    catalog_subject = fetch_catalog_subject_details(
        engine, offering['subject_code'], offering['degree_code']
    )

    if not catalog_subject:
        st.error(
            f"Could not load original catalog data for {offering['subject_code']}. "
            "Cannot perform override."
        )
        return

    with st.form(f"customize_form_{selected_offering_id}"):
        st.subheader(f"Customizing: {offering['subject_code']} - {offering.get('subject_name', 'N/A')}")
        
        # The main override toggle
        is_overridden = st.checkbox(
            "Enable Override (Unlock Fields)", 
            value=bool(offering.get('override_inheritance', False)),
            key="cust_override_toggle",
            help="Allows you to set values different from the subject catalog."
        )
        
        reason = st.text_area(
            "**Reason for Override** (Required if enabled)",
            value=offering.get('override_reason', ''),
            key="cust_override_reason",
            height=100,
            disabled=not is_overridden
        )
        
        st.markdown("---")
        
        # --- Display Fields ---
        col_header1, col_header2, col_header3 = st.columns([1, 1, 1])
        with col_header1: st.markdown("**Field**")
        with col_header2: st.markdown("**Catalog Default**")
        with col_header3: st.markdown("**Override Value**")

        st.markdown("#### Academic & Workload")
        _show_override_row("credits_total", "Total Credits", catalog_subject.get("credits_total", 0.0), offering.get("credits_total", 0.0), not is_overridden, 0.5)
        _show_override_row("L", "L (Periods)", catalog_subject.get("L", 0.0), offering.get("L", 0.0), not is_overridden, 1.0)
        _show_override_row("T", "T (Periods)", catalog_subject.get("T", 0.0), offering.get("T", 0.0), not is_overridden, 1.0)
        _show_override_row("P", "P (Periods)", catalog_subject.get("P", 0.0), offering.get("P", 0.0), not is_overridden, 1.0)
        _show_override_row("S", "S (Periods)", catalog_subject.get("S", 0.0), offering.get("S", 0.0), not is_overridden, 1.0)

        st.markdown("#### Assessment Marks")
        _show_override_row("internal_marks_max", "Internal Max", catalog_subject.get("internal_marks_max", 0.0), offering.get("internal_marks_max", 0.0), not is_overridden, 5.0)
        _show_override_row("exam_marks_max", "Exam Max", catalog_subject.get("exam_marks_max", 0.0), offering.get("exam_marks_max", 0.0), not is_overridden, 5.0)
        _show_override_row("jury_viva_marks_max", "Jury/Viva Max", catalog_subject.get("jury_viva_marks_max", 0.0), offering.get("jury_viva_marks_max", 0.0), not is_overridden, 5.0)

        st.markdown("#### Pass Thresholds & Weights (%)")
        _show_override_row("pass_threshold_overall", "Overall Pass %", catalog_subject.get("pass_threshold_overall", 0.0), offering.get("pass_threshold_overall", 0.0), not is_overridden, 1.0)
        _show_override_row("pass_threshold_internal", "Internal Pass %", catalog_subject.get("pass_threshold_internal", 0.0), offering.get("pass_threshold_internal", 0.0), not is_overridden, 1.0)
        _show_override_row("pass_threshold_external", "External Pass %", catalog_subject.get("pass_threshold_external", 0.0), offering.get("pass_threshold_external", 0.0), not is_overridden, 1.0)
        _show_override_row("direct_weight_percent", "Direct Weight %", catalog_subject.get("direct_weight_percent", 0.0), offering.get("direct_weight_percent", 0.0), not is_overridden, 5.0)
        
        st.markdown("---")

        # --- Submission Logic ---
        submitted = st.form_submit_button("💾 Save Changes", type="primary")
        if submitted:
            if is_overridden and not reason:
                st.error("A reason is required to enable overrides.")
            else:
                try:
                    # Build the updates dictionary from session state
                    updates = {
                        "override_inheritance": is_overridden,
                        "override_reason": reason if is_overridden else None,
                        
                        "credits_total": st.session_state.cust_credits_total,
                        "L": st.session_state.cust_L,
                        "T": st.session_state.cust_T,
                        "P": st.session_state.cust_P,
                        "S": st.session_state.cust_S,
                        
                        "internal_marks_max": st.session_state.cust_internal_marks_max,
                        "exam_marks_max": st.session_state.cust_exam_marks_max,
                        "jury_viva_marks_max": st.session_state.cust_jury_viva_marks_max,
                        
                        "pass_threshold_overall": st.session_state.cust_pass_threshold_overall,
                        "pass_threshold_internal": st.session_state.cust_pass_threshold_internal,
                        "pass_threshold_external": st.session_state.cust_pass_threshold_external,
                        "direct_weight_percent": st.session_state.cust_direct_weight_percent,
                        
                        # Auto-calculate indirect weight
                        "indirect_weight_percent": 100.0 - st.session_state.cust_direct_weight_percent,
                    }
                    
                    # Call the existing CRUD function
                    update_offering(engine, selected_offering_id, updates, actor)
                    
                    success(f"Successfully updated offering {offering['subject_code']}!")
                    st.cache_data.clear()
                    st.rerun()

                except Exception as e:
                    st.error(f"Error saving changes: {e}")
