"""
Template Manager Tab - Create and manage syllabus templates
""" 

import streamlit as st
import pandas as pd
# Use relative imports
from ..helpers import exec_query, rows_to_dicts
from ..templates_crud import (
    create_syllabus_template, list_templates_for_subject,
    get_template_points, clone_template
)
from core.forms import success
from sqlalchemy import text as sa_text


def render(engine, actor: str, CAN_EDIT: bool):
    """Render the Template Manager tab."""
    st.subheader("📋 Syllabus Template Manager")
    st.caption("Create reusable syllabus templates")

    with engine.begin() as conn:
        subjects = exec_query(conn, """
            SELECT DISTINCT subject_code, subject_name
            FROM subjects_catalog
            WHERE active = 1
            ORDER BY subject_code
        """).fetchall()

    if not subjects:
        st.warning("No subjects found. Create subjects first.")
        return

    subjects = rows_to_dicts(subjects)

    subject_code = st.selectbox(
        "Select Subject",
        options=[s["subject_code"] for s in subjects],
        format_func=lambda x: f"{x} - {next((s['subject_name'] for s in subjects if s['subject_code'] == x), x)}",
        key="tmpl_subject",
    )

    # List existing templates
    with engine.begin() as conn:
        templates = list_templates_for_subject(conn, subject_code)

    st.markdown("---")
    st.markdown("### Existing Templates")

    if templates:
        for tmpl in templates:
            with st.expander(
                f"{'✅' if tmpl['is_current'] else '📦'} {tmpl['name']} ({tmpl['version']})",
                expanded=tmpl['is_current'],
            ):
                col1, col2, col3 = st.columns(3)
                with col1: st.metric("Points", tmpl['point_count'])
                with col2: st.metric("Used by Offerings", tmpl['usage_count'])
                with col3: st.metric("Version #", tmpl['version_number'])

                st.caption(f"**Code:** {tmpl['code']}")
                if tmpl.get('effective_from_ay'):
                    st.caption(f"**Effective from:** {tmpl['effective_from_ay']}")

                # Show points
                with engine.begin() as conn:
                    points = get_template_points(conn, tmpl['id'])

                if points:
                    df = pd.DataFrame(points)[['sequence', 'title', 'hours_weight']]
                    st.dataframe(df, use_container_width=True)

                if CAN_EDIT:
                    # (Clone button logic - unchanged)
                    if st.button(f"Clone to New Version", key=f"clone_{tmpl['id']}"):
                        try:
                            st.session_state[f'cloning_{tmpl["id"]}'] = True
                            st.rerun()
                        except Exception:
                            pass
    else:
        st.info(f"No templates found for {subject_code}")

    # Create new template
    if CAN_EDIT:
        st.markdown("---")
        st.markdown("### Create New Template")

        # --- *** MODIFIED SECTION *** ---

        # 1. The Number of Points input is now OUTSIDE the form.
        # It uses a session_state key to remember its value.
        # Default value is set to 1 as requested.
        num_points = st.number_input(
            "Number of Points", 
            min_value=1, 
            max_value=50, 
            value=1,  # Default to 1
            key="template_num_points" # Use state to hold the value
        )

        with st.form("create_template_form"):
            col1, col2 = st.columns(2)

            with col1:
                version = st.text_input("Version*", placeholder="e.g., v1, 2024")
                name = st.text_input("Template Name*", placeholder="e.g., Standard Syllabus 2024")

            with col2:
                effective_from_ay = st.text_input("Effective From AY", placeholder="e.g., 2024-25")
                description = st.text_area("Description", height=80)

            st.markdown("**Syllabus Points**")

            points_data = []

            # 2. The loop now reads the 'num_points' variable from outside the form
            for i in range(num_points):
                with st.expander(f"Point {i+1}", expanded=i < 5): # Expand first 5
                    cols = st.columns([1, 3, 1])

                    with cols[0]:
                        seq = st.number_input(
                            "Seq", value=i + 1, key=f"pt_seq_{i}",
                            label_visibility="collapsed",
                        )
                    with cols[1]:
                        title = st.text_input("Title*", key=f"pt_title_{i}")
                    with cols[2]:
                        hours = st.number_input(
                            "Hrs", value=2.0, step=0.5,
                            key=f"pt_hrs_{i}", label_visibility="collapsed",
                        )

                    desc = st.text_area("Description", key=f"pt_desc_{i}", height=60)
                    tags = st.text_input("Tags", key=f"pt_tags_{i}")
                    resources = st.text_input("Resources", key=f"pt_res_{i}")

                    points_data.append({
                        "sequence": seq, "title": title, "description": desc,
                        "tags": tags, "resources": resources, "hours_weight": hours
                    })

            submitted = st.form_submit_button("Create Template", type="primary")

            if submitted:
                if not version or not name:
                    st.error("Version and Name are required")
                elif not any(p["title"] for p in points_data):
                    st.error("At least one point with a title is required")
                else:
                    try:
                        valid_points = [p for p in points_data if p["title"]]

                        template_id = create_syllabus_template(
                            engine, subject_code, version, name,
                            valid_points, actor,
                            description=description,
                            effective_from_ay=effective_from_ay or None,
                        )

                        success(f"Template created with ID {template_id}")
                        
                        # 3. Reset the num_points input back to 1 after success
                        if "template_num_points" in st.session_state:
                            st.session_state.template_num_points = 1
                            
                        st.cache_data.clear()
                        st.rerun()

                    except Exception as e:
                        st.error(f"Error: {str(e)}")
