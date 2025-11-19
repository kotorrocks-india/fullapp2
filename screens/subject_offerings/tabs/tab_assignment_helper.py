# -*- coding: utf-8 -*-
"""
Tab: Assignment Helper
Auto-populate subject offerings from catalog with status indicators
"""

import streamlit as st
import pandas as pd
from typing import List, Dict, Any
from sqlalchemy.engine import Engine

from ..db_helpers import (
    fetch_degrees,
    fetch_programs,
    fetch_branches,
    fetch_academic_years,
    fetch_degree_semester_structure,
    fetch_semesters_for_degree,
    fetch_catalog_subjects_with_status
)
from ..offerings_crud import create_offering_from_catalog
from ..helpers import exec_query


def _get_status_icon(exists: bool, has_issues: bool = False) -> str:
    """Get status icon."""
    if exists and not has_issues:
        return "[OK]"
    elif exists and has_issues:
        return "[!]"
    else:
        return "[X]"


def _format_batch_status(batch_status: Dict[str, Any]) -> str:
    """Format batch status for display."""
    if not batch_status["exists"]:
        return "[X] No batch"
    
    student_count = batch_status["student_count"]
    batch_code = batch_status["batch_code"]
    
    if student_count == 0:
        return f"[!] {batch_code} (No students)"
    else:
        return f"[OK] {batch_code} ({student_count} students)"


def _format_elective_status(elective_status: Dict[str, Any]) -> str:
    """Format elective status for display."""
    if elective_status is None:
        return "N/A (Core subject)"
    
    if not elective_status["topics_exist"]:
        return "[X] No topics created"
    
    if not elective_status["allocation_complete"]:
        note = elective_status.get("note", "")
        return f"[!] {note}"
    
    return f"[OK] {elective_status['topic_count']} topics allocated"


def render(engine: Engine, actor: str, can_edit: bool):
    """Render the Assignment Helper tab."""
    
    import logging
    logger = logging.getLogger(__name__)
    
    st.markdown("### Assignment Helper")
    st.info("""
        **Auto-populate offerings from Subject Catalog**
        
        This helper shows all subjects from the catalog for the selected scope with status indicators:
        -  **Offering Status**: Whether offering already exists
        -  **Batch & Students**: Batch existence and enrollment count
        -  **Elective Status**: Topics and allocation status for elective subjects
        
        Select subjects to create offerings in bulk.
    """)
    
    # ==================== FILTERS ====================
    
    col1, col2 = st.columns(2)
    
    with col1:
        degrees = fetch_degrees(engine)
        if not degrees:
            st.warning("No degrees found. Please create degrees first.")
            return
        
        degree_options = {d["code"]: f"{d['code']} - {d['title']}" for d in degrees}
        degree_code = st.selectbox(
            "Degree *",
            options=list(degree_options.keys()),
            format_func=lambda x: degree_options[x],
            key="assign_degree"
        )
    
    with col2:
        ays = fetch_academic_years(engine)
        if not ays:
            st.warning("No academic years found. Please create AYs first.")
            return
        
        ay_options = {ay["ay_code"]: ay["ay_code"] for ay in ays}
        ay_label = st.selectbox(
            "Academic Year *",
            options=list(ay_options.keys()),
            key="assign_ay"
        )
    
    if not degree_code or not ay_label:
        return
    
    # Get semester structure
    sem_struct = fetch_degree_semester_structure(engine, degree_code)
    
    if not sem_struct:
        st.error(f"[X] No semester structure found for degree '{degree_code}'. Please configure semesters first.")
        return
    
    if sem_struct.get("requires_program"):
        st.warning("[!] This degree uses program-level semester binding. Program selection required.")
        # You would need to add program selection here
        return
    
    if sem_struct.get("requires_branch"):
        st.warning("[!] This degree uses branch-level semester binding. Branch selection required.")
        # You would need to add branch selection here
        return
    
    # Get actual semesters for this degree
    semesters = fetch_semesters_for_degree(engine, degree_code)
    
    if not semesters:
        st.error(f"[X] No semesters materialized for degree '{degree_code}'. Please rebuild semesters.")
        return
    
    # Build year and term options from actual semesters
    years = sorted(list(set(s["year_index"] for s in semesters)))
    
    col3, col4 = st.columns(2)
    
    with col3:
        year = st.selectbox(
            "Year *",
            options=years,
            format_func=lambda y: f"Year {y}",
            key="assign_year"
        )
    
    with col4:
        # Get terms for selected year
        year_semesters = [s for s in semesters if s["year_index"] == year]
        terms = sorted(list(set(s["term_index"] for s in year_semesters)))
        
        term = st.selectbox(
            "Term *",
            options=terms,
            format_func=lambda t: f"Term {t}",
            key="assign_term"
        )
    
    # Optional program/branch filters
    col5, col6 = st.columns(2)
    
    with col5:
        programs = fetch_programs(engine, degree_code)
        program_options = {"": "All Programs"} | {p["program_code"]: p["program_name"] for p in programs}
        program_code = st.selectbox(
            "Program (Optional)",
            options=list(program_options.keys()),
            format_func=lambda x: program_options[x],
            key="assign_program"
        )
        program_code = program_code if program_code else None
    
    with col6:
        branches = fetch_branches(engine, degree_code, program_code)
        branch_options = {"": "All Branches"} | {b["branch_code"]: b["branch_name"] for b in branches}
        branch_code = st.selectbox(
            "Branch (Optional)",
            options=list(branch_options.keys()),
            format_func=lambda x: branch_options[x],
            key="assign_branch"
        )
        branch_code = branch_code if branch_code else None
    
    # ==================== LOAD SUBJECTS ====================
    
    if st.button("Load Subjects from Catalog", type="primary"):
        with st.spinner("Loading subjects with status..."):
            with engine.begin() as conn:
                subjects = fetch_catalog_subjects_with_status(
                    conn=conn,
                    degree_code=degree_code,
                    ay_label=ay_label,
                    year=year,
                    term=term,
                    program_code=program_code,
                    branch_code=branch_code
                )
            
            if not subjects:
                st.warning("No subjects found in catalog for the selected scope.")
                st.session_state.pop("assign_subjects", None)
                return
            
            st.session_state["assign_subjects"] = subjects
            st.success(f"[OK] Loaded {len(subjects)} subjects from catalog")
    
    # ==================== DISPLAY SUBJECTS ====================
    
    if "assign_subjects" not in st.session_state:
        st.info("👆 Click 'Load Subjects' to see available subjects from catalog")
        return
    
    subjects = st.session_state["assign_subjects"]
    
    # Build display dataframe
    display_data = []
    
    for idx, subj in enumerate(subjects):
        offering_status = "[OK] Exists" if subj["offering_exists"] else "[X] Not created"
        batch_status = _format_batch_status(subj["batch_status"])
        elective_status = _format_elective_status(subj["elective_status"])
        
        # Determine if there are issues
        has_issues = (
            not subj["batch_status"]["exists"] or
            subj["batch_status"]["student_count"] == 0 or
            (subj["subject_type"] == "Elective" and 
             subj["elective_status"] and 
             not subj["elective_status"]["allocation_complete"])
        )
        
        display_data.append({
            "idx": idx,
            "Select": not subj["offering_exists"],  # Auto-select if offering doesn't exist
            "Subject Code": subj["subject_code"],
            "Subject Name": subj["subject_name"],
            "Type": subj["subject_type"],
            "Credits": subj["credits_total"],
            "LTPS": f"{subj['L']}-{subj['T']}-{subj['P']}-{subj['S']}",
            "Offering": offering_status,
            "Batch & Students": batch_status,
            "Elective Status": elective_status,
            "Issues": "[!]" if has_issues else "[OK]"
        })
    
    df = pd.DataFrame(display_data)
    
    # Show summary stats
    col_s1, col_s2, col_s3, col_s4 = st.columns(4)
    
    with col_s1:
        total = len(subjects)
        st.metric("Total Subjects", total)
    
    with col_s2:
        existing = sum(1 for s in subjects if s["offering_exists"])
        st.metric("Existing Offerings", existing)
    
    with col_s3:
        missing = total - existing
        st.metric("Not Created", missing)
    
    with col_s4:
        electives = sum(1 for s in subjects if s["subject_type"] == "Elective")
        st.metric("Electives", electives)
    
    st.markdown("---")
    
    # Display interactive table
    edited_df = st.data_editor(
        df.drop(columns=["idx"]),
        disabled=[col for col in df.columns if col not in ["Select"]],
        hide_index=True,
        use_container_width=True,
        column_config={
            "Select": st.column_config.CheckboxColumn(
                "Select",
                help="Select to create offering",
                default=False
            ),
            "Issues": st.column_config.TextColumn(
                "Status",
                help="[OK] Ready | [!] Has issues"
            )
        }
    )
    
    # ==================== BULK CREATE ==================== #
    
    if not can_edit:
        st.info("Read-only mode: You cannot create offerings")
        return
    
    selected_indices = [
        display_data[i]["idx"] 
        for i in range(len(display_data)) 
        if edited_df.iloc[i]["Select"]
    ]
    
    if not selected_indices:
        st.info("Select subjects above to create offerings")
        return
    
    selected_subjects = [subjects[i] for i in selected_indices]
    
    st.markdown("---")
    st.markdown(f"### Selected: {len(selected_subjects)} subject(s)")
    
    # DEBUG: Show what will be created
    with st.expander("🔍 Debug Info - View Selected Subjects", expanded=False):
        st.markdown("**Parameters that will be used for creation:**")
        params_info = {
            "degree_code": degree_code,
            "ay_label": ay_label,
            "year": year,
            "term": term,
            "actor": actor,
            "division_code": None,
            "applies_to_all_divisions": True
        }
        st.json(params_info)
        
        st.markdown("**Selected subjects details:**")
        for subj in selected_subjects:
            st.markdown(f"**{subj['subject_code']}** - {subj.get('subject_name', 'N/A')}")
            st.json({
                "catalog_id": subj.get("id"),
                "subject_type": subj.get("subject_type"),
                "credits": subj.get("credits_total"),
                "offering_exists": subj.get("offering_exists"),
                "batch_status": subj.get("batch_status"),
                "elective_status": subj.get("elective_status")
            })
    
    # Show warnings for subjects with issues
    issues_list = []
    for subj in selected_subjects:
        warnings = []
        
        if not subj["batch_status"]["exists"]:
            warnings.append(f"[X] **{subj['subject_code']}**: No batch configured")
        elif subj["batch_status"]["student_count"] == 0:
            warnings.append(f"[!] **{subj['subject_code']}**: Batch exists but no students enrolled")
        
        if subj["subject_type"] == "Elective" and subj["elective_status"]:
            if not subj["elective_status"]["topics_exist"]:
                warnings.append(f"[X] **{subj['subject_code']}**: No elective topics created")
            elif not subj["elective_status"]["allocation_complete"]:
                warnings.append(f"[!] **{subj['subject_code']}**: {subj['elective_status']['note']}")
        
        if warnings:
            issues_list.extend(warnings)
    
    if issues_list:
        with st.expander("[!] Issues Detected - Click to see details", expanded=False):
            for issue in issues_list:
                st.markdown(issue)
            st.warning("""
                **Note:** You can still create offerings, but these issues may affect:
                - Student enrollment and division assignment
                - Elective topic selection and allocation
                
                Consider resolving these issues for a complete setup.
            """)
    
    # Confirm and create
    col_action1, col_action2 = st.columns([3, 1])
    
    with col_action1:
        st.info(f"""
            This will create **{len(selected_subjects)} offering(s)** for:
            - Degree: **{degree_code}**
            - AY: **{ay_label}**
            - Year {year}, Term {term}
        """)
    
    with col_action2:
        if st.button("Create Offerings", type="primary", use_container_width=True):
            with st.spinner("Creating offerings..."):
                import traceback
                import logging
                
                # Setup logger
                logger = logging.getLogger(__name__)
                
                success_count = 0
                error_count = 0
                errors = []
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                for i, subj in enumerate(selected_subjects):
                    try:
                        status_text.text(f"Creating: {subj['subject_code']}...")
                        
                        # DEBUG: Log the parameters being used
                        logger.info(f"Creating offering for {subj['subject_code']} with catalog_id={subj['id']}")
                        
                        # Create offering from catalog
                        create_offering_from_catalog(
                            engine=engine,
                            catalog_subject_id=subj["id"],
                            ay_label=ay_label,
                            year=year,
                            term=term,
                            actor=actor,
                            division_code=None,  # For now, create degree-level offerings
                            applies_to_all_divisions=True,
                            instructor_email=None
                        )
                        
                        success_count += 1
                        logger.info(f"Successfully created offering for {subj['subject_code']}")
                    
                    except Exception as e:
                        error_count += 1
                        
                        # Get full traceback for debugging
                        tb_str = traceback.format_exc()
                        
                        # Log the full error
                        logger.error(f"Failed to create offering for {subj['subject_code']}: {e}")
                        logger.error(f"Full traceback:\n{tb_str}")
                        
                        # Store error with more details
                        errors.append({
                            "subject_code": subj["subject_code"],
                            "subject_name": subj.get("subject_name", "N/A"),
                            "catalog_id": subj.get("id"),
                            "error": str(e),
                            "traceback": tb_str,
                            "context": {
                                "degree_code": degree_code,
                                "ay_label": ay_label,
                                "year": year,
                                "term": term,
                                "subject_type": subj.get("subject_type")
                            }
                        })
                    
                    progress_bar.progress((i + 1) / len(selected_subjects))
                
                progress_bar.empty()
                status_text.empty()
                
                # Show results
                if success_count > 0:
                    st.success(f"✅ Successfully created {success_count} offering(s)")
                
                if error_count > 0:
                    st.error(f"❌ Failed to create {error_count} offering(s)")
                    
                    # Detailed error display - EXPANDED by default
                    with st.expander("🔍 View Error Details", expanded=True):
                        st.warning("**Errors occurred during offering creation. Please review below:**")
                        
                        for idx, err in enumerate(errors, 1):
                            st.markdown(f"### Error {idx}: {err['subject_code']} - {err['subject_name']}")
                            
                            # Error message
                            st.error(f"**Error:** {err['error']}")
                            
                            # Context
                            st.markdown("**Context:**")
                            st.json(err['context'])
                            
                            # Full traceback in code block
                            st.markdown("**Full Traceback:**")
                            st.code(err['traceback'], language="python")
                            
                            st.markdown("---")
                        
                        # Download errors as JSON for reporting
                        import json
                        error_json = json.dumps(errors, indent=2)
                        st.download_button(
                            label="📥 Download Error Report (JSON)",
                            data=error_json,
                            file_name=f"offering_creation_errors_{ay_label}_{degree_code}_Y{year}T{term}.json",
                            mime="application/json"
                        )
                    
                    # DO NOT RERUN if there are errors - let user see them
                    st.warning("⚠️ Fix the errors above and try again. The page will NOT reload automatically.")
                    
                else:
                    # Only reload if ALL offerings were created successfully
                    st.success("✅ All offerings created successfully! Reloading...")
                    st.session_state.pop("assign_subjects", None)
                    st.rerun()
