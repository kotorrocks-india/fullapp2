"""
Offerings Tab - List/manage offerings for specific AY & Term
COMPLETE VERSION with Mass Actions
"""

import streamlit as st
import pandas as pd
from typing import Optional
from ..helpers import exec_query, rows_to_dicts
from ..db_helpers import (
    fetch_degrees, fetch_programs, fetch_branches, fetch_curriculum_groups,
    fetch_academic_years, fetch_divisions, fetch_catalog_subjects, fetch_offerings,
    fetch_semesters_for_degree
)
from ..offerings_crud import (
    create_offering_from_catalog, update_offering, delete_offering,
    publish_offering, archive_offering, copy_offerings_forward
)
from ..constants import SUBJECT_TYPES, STATUS_VALUES
from core.forms import success


def render(engine, actor: str, CAN_EDIT: bool):
    """Render the Offerings tab."""
    st.subheader("📋 Subject Offerings")
    st.caption("Manage subject offerings for specific Academic Year & Term")

    # --- FILTERS ---
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
            key="off_degree"
        )
    
    degree = next((d for d in degrees if d["code"] == degree_code), None)
    cohort_mode = degree.get("cohort_splitting_mode", "none") if degree else "none"
    
    programs = fetch_programs(engine, degree_code) if degree else []
    branches = []
    cgs = []
    
    prog_disabled = cohort_mode not in ["both", "program_only"]
    branch_disabled = cohort_mode not in ["both", "branch_only"]
    cg_disabled = not (degree.get("cg_degree") or degree.get("cg_program") or degree.get("cg_branch")) if degree else True
    
    filter_program_code = None
    filter_branch_code = None
    filter_cg_code = None
    
    if not prog_disabled and programs:
        with col2:
            filter_program_code = st.selectbox(
                "Program (filter)",
                options=[None] + [p["program_code"] for p in programs],
                format_func=lambda x: "All Programs" if x is None else next(
                    (p["program_name"] for p in programs if p["program_code"] == x), x
                ),
                key="off_program"
            )
    
    if not branch_disabled:
        branches = fetch_branches(engine, degree_code, filter_program_code)
        with col3:
            filter_branch_code = st.selectbox(
                "Branch (filter)",
                options=[None] + [b["branch_code"] for b in branches],
                format_func=lambda x: "All Branches" if x is None else next(
                    (b["branch_name"] for b in branches if b["branch_code"] == x), x
                ),
                key="off_branch"
            )
    
    if not cg_disabled:
        cgs = fetch_curriculum_groups(engine, degree_code)
        with col4:
            filter_cg_code = st.selectbox(
                "Curriculum Group (filter)",
                options=[None] + [g["group_code"] for g in cgs],
                format_func=lambda x: "All Groups" if x is None else next(
                    (g["group_name"] for g in cgs if g["group_code"] == x), x
                ),
                key="off_cg"
            )

    # AY, Year, Term
    ays = fetch_academic_years(engine)
    if not ays:
        st.warning("No academic years found. Create academic years first.")
        return
    
    # Dynamic year/term based on semester structure
    semesters = fetch_semesters_for_degree(engine, degree_code)
    if not semesters:
        st.warning(f"No semester structure found for degree '{degree_code}'. Please configure and build semesters first.")
        return
    
    available_years = sorted(list(set(s["year_index"] for s in semesters)))
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        ay_label = st.selectbox(
            "Academic Year",
            options=[ay["ay_code"] for ay in ays],
            key="off_ay"
        )
    
    with col2:
        year = st.selectbox(
            "Year",
            options=available_years,
            key="off_year",
            format_func=lambda y: f"Year {y}"
        )
    
    available_terms = sorted(list(set(
        s["term_index"] for s in semesters if s["year_index"] == year
    )))
    
    with col3:
        term = st.selectbox(
            "Term",
            options=available_terms,
            key="off_term",
            format_func=lambda t: f"Term {t}"
        )
    
    # Division filter (optional)
    divisions = fetch_divisions(engine, degree_code, ay_label, year)
    filter_division_code = None
    
    with col4:
        if divisions:
            filter_division_code = st.selectbox(
                "Division (filter)",
                options=[None] + [d["division_code"] for d in divisions],
                format_func=lambda x: "All Divisions" if x is None else x,
                key="off_division"
            )

    st.markdown("---")

    # --- FETCH OFFERINGS ---
    with engine.begin() as conn:
        offerings = fetch_offerings(
            conn, degree_code, ay_label, year, term,
            filter_program_code, filter_branch_code, filter_division_code
        )

    # --- DISPLAY OFFERINGS ---
    if offerings:
        st.markdown(f"### {len(offerings)} Offering(s)")
        
        df = pd.DataFrame(offerings)
        display_cols = [
            "subject_code", "subject_name", "subject_type", "is_elective_parent",
            "division_code", "credits_total", "total_marks_max", "status", "instructor_email"
        ]
        display_cols = [c for c in display_cols if c in df.columns]
        
        st.dataframe(df[display_cols], use_container_width=True)
        
        # --- ACTIONS ---
        if CAN_EDIT:
            st.markdown("#### Bulk Actions")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("📋 Copy From Prior AY", key="copy_forward_btn"):
                    st.session_state.show_copy_forward = True
            
            with col2:
                selected_ids = st.multiselect(
                    "Select offerings to publish",
                    options=[o["id"] for o in offerings if o["status"] == "draft"],
                    format_func=lambda x: next(
                        (f"{o['subject_code']} - {o['subject_name']}" for o in offerings if o["id"] == x),
                        str(x)
                    ),
                    key="publish_select"
                )
                
                if selected_ids and st.button("🚀 Publish Selected", key="publish_btn"):
                    try:
                        for oid in selected_ids:
                            publish_offering(engine, oid, actor, reason="Bulk publish")
                        success(f"Published {len(selected_ids)} offering(s)")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")
            
            with col3:
                archive_ids = st.multiselect(
                    "Select offerings to archive",
                    options=[o["id"] for o in offerings],
                    format_func=lambda x: next(
                        (f"{o['subject_code']} - {o['subject_name']}" for o in offerings if o["id"] == x),
                        str(x)
                    ),
                    key="archive_select"
                )
                
                if archive_ids and st.button("📦 Archive Selected", key="archive_btn"):
                    try:
                        for oid in archive_ids:
                            archive_offering(engine, oid, actor, reason="Bulk archive")
                        success(f"Archived {len(archive_ids)} offering(s)")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")
            
            st.markdown("---")
            st.markdown("#### Mass Actions (Entire Term)")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                draft_count = len([o for o in offerings if o["status"] == "draft"])
                st.metric("Draft Offerings", draft_count)
                if st.button("🚀 Mass Publish All", key="mass_publish_btn", disabled=(draft_count == 0)):
                    st.session_state.show_mass_publish = True
            
            with col2:
                active_count = len([o for o in offerings if o["status"] != "archived"])
                st.metric("Active Offerings", active_count)
                if st.button("📦 Mass Archive All", key="mass_archive_btn", disabled=(active_count == 0)):
                    st.session_state.show_mass_archive = True
            
            with col3:
                total_count = len(offerings)
                st.metric("Total Offerings", total_count)
                if st.button("🗑️ Mass Delete All", key="mass_delete_btn", disabled=(total_count == 0)):
                    st.session_state.show_mass_delete = True

    else:
        st.info("No offerings found for this scope.")

    # --- COPY FORWARD DIALOG ---
    if CAN_EDIT and st.session_state.get("show_copy_forward"):
        with st.form("copy_forward_form"):
            st.markdown("### Copy Offerings From Prior AY")
            
            prior_ays = [ay["ay_code"] for ay in ays if ay["ay_code"] != ay_label]
            from_ay = st.selectbox("From AY", options=prior_ays, key="copy_from_ay")
            
            st.info(f"Will copy offerings from **{from_ay}** Y{year}T{term} to **{ay_label}** Y{year}T{term}")
            
            col1, col2 = st.columns(2)
            
            with col1:
                if st.form_submit_button("✅ Confirm Copy", type="primary"):
                    try:
                        count, messages = copy_offerings_forward(
                            engine, from_ay, ay_label, degree_code, year, term, actor,
                            filter_program_code, filter_branch_code
                        )
                        success(f"Copied {count} offering(s) from {from_ay}")
                        st.session_state.show_copy_forward = False
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")
            
            with col2:
                if st.form_submit_button("Cancel"):
                    st.session_state.show_copy_forward = False
                    st.rerun()
    
    # --- MASS PUBLISH DIALOG ---
    if CAN_EDIT and st.session_state.get("show_mass_publish") and offerings:
        draft_ids = [o["id"] for o in offerings if o["status"] == "draft"]
        
        with st.form("mass_publish_form"):
            st.markdown("### 🚀 Mass Publish All Draft Offerings")
            st.warning(f"This will publish **{len(draft_ids)} draft** offerings in this term.")
            
            reason = st.text_area("Reason *", placeholder="e.g., Start of term - all ready", key="mass_pub_reason")
            acknowledge = st.checkbox("I confirm this action", key="mass_pub_ack")
            
            col1, col2 = st.columns(2)
            
            with col1:
                submitted = st.form_submit_button("✅ Confirm Mass Publish", type="primary")
                if submitted:
                    if not reason:
                        st.error("Reason required")
                    elif not acknowledge:
                        st.error("Please confirm the action")
                    else:
                        try:
                            success_count = 0
                            errors = []
                            progress = st.progress(0)
                            for idx, oid in enumerate(draft_ids):
                                try:
                                    publish_offering(engine, oid, actor, reason=reason, acknowledge_no_topics=True)
                                    success_count += 1
                                except Exception as e:
                                    errors.append(f"ID {oid}: {str(e)}")
                                progress.progress((idx + 1) / len(draft_ids))
                            
                            progress.empty()
                            
                            if success_count > 0:
                                st.success(f"✅ Published {success_count} offering(s)")
                            if errors:
                                st.error(f"❌ {len(errors)} error(s):")
                                for err in errors[:5]:
                                    st.error(err)
                            
                            st.session_state.show_mass_publish = False
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
            
            with col2:
                if st.form_submit_button("Cancel"):
                    st.session_state.show_mass_publish = False
                    st.rerun()
    
    # --- MASS ARCHIVE DIALOG ---
    if CAN_EDIT and st.session_state.get("show_mass_archive") and offerings:
        active_ids = [o["id"] for o in offerings if o["status"] != "archived"]
        
        with st.form("mass_archive_form"):
            st.markdown("### 📦 Mass Archive All Active Offerings")
            st.warning(f"This will archive **{len(active_ids)} active** offerings in this term.")
            
            reason = st.text_area("Reason *", placeholder="e.g., End of academic year", key="mass_arch_reason")
            acknowledge = st.checkbox("I confirm this action", key="mass_arch_ack")
            
            col1, col2 = st.columns(2)
            
            with col1:
                submitted = st.form_submit_button("✅ Confirm Mass Archive", type="primary")
                if submitted:
                    if not reason:
                        st.error("Reason required")
                    elif not acknowledge:
                        st.error("Please confirm the action")
                    else:
                        try:
                            success_count = 0
                            errors = []
                            progress = st.progress(0)
                            for idx, oid in enumerate(active_ids):
                                try:
                                    archive_offering(engine, oid, actor, reason=reason)
                                    success_count += 1
                                except Exception as e:
                                    errors.append(f"ID {oid}: {str(e)}")
                                progress.progress((idx + 1) / len(active_ids))
                            
                            progress.empty()
                            
                            if success_count > 0:
                                st.success(f"✅ Archived {success_count} offering(s)")
                            if errors:
                                st.error(f"❌ {len(errors)} error(s):")
                                for err in errors[:5]:
                                    st.error(err)
                            
                            st.session_state.show_mass_archive = False
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
            
            with col2:
                if st.form_submit_button("Cancel"):
                    st.session_state.show_mass_archive = False
                    st.rerun()
    
    # --- MASS DELETE DIALOG ---
    if CAN_EDIT and st.session_state.get("show_mass_delete") and offerings:
        all_ids = [o["id"] for o in offerings]
        
        with st.form("mass_delete_form"):
            st.markdown("### 🗑️ Mass Delete All Offerings")
            st.error(f"⚠️ **DANGER:** This will PERMANENTLY DELETE **{len(all_ids)}** offerings. This CANNOT be undone!")
            
            reason = st.text_area("Reason *", placeholder="e.g., Incorrect data import", key="mass_del_reason")
            
            confirm_text = st.text_input(f"Type 'DELETE {len(all_ids)} OFFERINGS' to confirm", key="mass_del_confirm")
            expected = f"DELETE {len(all_ids)} OFFERINGS"
            confirm_valid = confirm_text == expected
            
            acknowledge = st.checkbox("I understand this is IRREVERSIBLE", key="mass_del_ack")
            
            col1, col2 = st.columns(2)
            
            with col1:
                submitted = st.form_submit_button("✅ Confirm Mass Delete", type="primary")
                if submitted:
                    if not reason:
                        st.error("Reason required")
                    elif not acknowledge:
                        st.error("Please acknowledge this is irreversible")
                    elif not confirm_valid:
                        st.error(f"Please type exactly: {expected}")
                    else:
                        try:
                            success_count = 0
                            errors = []
                            progress = st.progress(0)
                            for idx, oid in enumerate(all_ids):
                                try:
                                    delete_offering(engine, oid, actor, reason=reason)
                                    success_count += 1
                                except Exception as e:
                                    errors.append(f"ID {oid}: {str(e)}")
                                progress.progress((idx + 1) / len(all_ids))
                            
                            progress.empty()
                            
                            if success_count > 0:
                                st.success(f"✅ Deleted {success_count} offering(s)")
                            if errors:
                                st.error(f"❌ {len(errors)} error(s):")
                                for err in errors[:5]:
                                    st.error(err)
                            
                            st.session_state.show_mass_delete = False
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
            
            with col2:
                if st.form_submit_button("Cancel"):
                    st.session_state.show_mass_delete = False
                    st.rerun()

    st.markdown("---")

    # --- CREATE NEW OFFERING ---
    if CAN_EDIT:
        with st.expander("➕ Create New Offering"):
            with st.form("create_offering_form"):
                st.markdown("**Add Subject from Catalog**")
                
                with engine.begin() as conn:
                    catalog_subjects = fetch_catalog_subjects(
                        conn, degree_code, filter_program_code, filter_branch_code
                    )
                
                if not catalog_subjects:
                    st.warning("No subjects found in catalog for this scope.")
                    st.form_submit_button("Create Offering", disabled=True)
                else:
                    catalog_subject_id = st.selectbox(
                        "Select Subject from Catalog",
                        options=[s["id"] for s in catalog_subjects],
                        format_func=lambda x: next(
                            (f"{s['subject_code']} - {s['subject_name']}" for s in catalog_subjects if s["id"] == x),
                            str(x)
                        ),
                        key="create_catalog_subject"
                    )
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        applies_to_all = st.checkbox("Applies to All Divisions", value=True, key="create_all_div")
                    
                    with col2:
                        div_code = None
                        if not applies_to_all and divisions:
                            div_code = st.selectbox(
                                "Division",
                                options=[d["division_code"] for d in divisions],
                                key="create_div_code"
                            )
                    
                    instructor = st.text_input("Instructor Email (optional)", key="create_instructor")
                    
                    if st.form_submit_button("Create Offering", type="primary"):
                        try:
                            offering_id = create_offering_from_catalog(
                                engine, catalog_subject_id, ay_label, year, term, actor,
                                division_code=div_code if not applies_to_all else None,
                                applies_to_all_divisions=applies_to_all,
                                instructor_email=instructor or None
                            )
                            success(f"Created offering with ID {offering_id}")
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")

    st.markdown("---")

    # --- EDIT/DELETE OFFERING ---
    if offerings:
        st.markdown("### Edit/Delete Offering")
        
        selected_offering_id = st.selectbox(
            "Select Offering to Edit/Delete",
            options=[None] + [o["id"] for o in offerings],
            format_func=lambda x: "Select an offering..." if x is None else next(
                (f"{o['subject_code']} - {o['subject_name']} ({o['status']})" for o in offerings if o["id"] == x),
                str(x)
            ),
            key="edit_offering_select"
        )
        
        if selected_offering_id:
            offering = next((o for o in offerings if o["id"] == selected_offering_id), None)
            
            if offering:
                with st.form(f"edit_offering_form_{offering['id']}"):
                    st.subheader(f"Editing: {offering['subject_code']} - {offering['subject_name']}")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.text_input("Subject Code", value=offering["subject_code"], disabled=True)
                        st.selectbox(
                            "Status",
                            options=STATUS_VALUES,
                            index=STATUS_VALUES.index(offering["status"]),
                            key="edit_status"
                        )
                    
                    with col2:
                        st.text_input("Subject Name", value=offering.get("subject_name", ""), disabled=True)
                        st.text_input("Instructor Email", value=offering.get("instructor_email", "") or "", key="edit_instructor")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if st.form_submit_button("💾 Save Changes", type="primary"):
                            try:
                                updates = {
                                    "status": st.session_state.edit_status,
                                    "instructor_email": st.session_state.edit_instructor or None,
                                }
                                update_offering(engine, selected_offering_id, updates, actor)
                                success(f"Updated offering {offering['subject_code']}")
                                st.cache_data.clear()
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {e}")
                    
                    with col2:
                        if st.form_submit_button("🗑️ Delete Offering"):
                            st.session_state.confirm_delete_offering = selected_offering_id
                            st.rerun()
                
                # Confirm delete
                if st.session_state.get("confirm_delete_offering") == selected_offering_id:
                    st.warning(f"⚠️ Confirm deletion of: {offering['subject_code']} - {offering['subject_name']}")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if st.button("✅ Confirm Delete", key="confirm_del", type="primary"):
                            try:
                                delete_offering(engine, selected_offering_id, actor)
                                success(f"Deleted offering {offering['subject_code']}")
                                del st.session_state.confirm_delete_offering
                                st.cache_data.clear()
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {e}")
                    
                    with col2:
                        if st.button("Cancel", key="cancel_del"):
                            del st.session_state.confirm_delete_offering
                            st.rerun()
