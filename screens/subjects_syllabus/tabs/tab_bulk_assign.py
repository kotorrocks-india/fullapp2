"""
Bulk Assignment Tab - Assign templates to multiple offerings at once
"""

import streamlit as st
import pandas as pd
from ..helpers import exec_query, rows_to_dicts
from ..templates_crud import list_templates_for_subject, bulk_assign_template
from ..db_helpers import fetch_academic_years
from core.forms import success
from sqlalchemy import text as sa_text


def render(engine, actor: str, CAN_EDIT: bool):
    """Render the Bulk Assignment tab."""
    st.subheader("🔄 Bulk Template Assignment")
    st.caption("Apply a template to multiple offerings")

    if not CAN_EDIT:
        st.warning("You need edit permissions")
        return

    with engine.begin() as conn:
        subjects = exec_query(conn, """
            SELECT DISTINCT sc.subject_code, sc.subject_name, sc.degree_code
            FROM subjects_catalog sc
            WHERE sc.active = 1
            ORDER BY sc.subject_code
        """).fetchall()

    if not subjects:
        st.warning("No subjects found")
        return

    subjects = rows_to_dicts(subjects)

    col1, col2 = st.columns(2)

    with col1:
        subject_code = st.selectbox(
            "Subject",
            options=[s["subject_code"] for s in subjects],
            format_func=lambda x: f"{x} - {next((s['subject_name'] for s in subjects if s['subject_code'] == x), x)}",
            key="bulk_subject",
        )

    degree_code = next((s["degree_code"] for s in subjects if s["subject_code"] == subject_code), None)

    with engine.begin() as conn:
        templates = list_templates_for_subject(conn, subject_code)

    if not templates:
        st.warning(f"No templates found for {subject_code}")
        return

    with col2:
        template_id = st.selectbox(
            "Template",
            options=[t['id'] for t in templates],
            format_func=lambda x: next(
                (f"{t['name']} ({t['version']})" for t in templates if t['id'] == x),
                str(x)
            ),
            key="bulk_template",
        )

    ays = fetch_academic_years(engine)
    from_ay = st.selectbox("Apply From AY", options=ays, key="bulk_ay")

    # Preview
    with engine.begin() as conn:
        affected = exec_query(conn, """
            SELECT id, ay_label, year, term, syllabus_customized
            FROM subject_offerings
            WHERE subject_code = :sc
            AND degree_code = :dc
            AND ay_label >= :ay
            ORDER BY ay_label, year, term
        """, {"sc": subject_code, "dc": degree_code, "ay": from_ay}).fetchall()

    if affected:
        st.markdown(f"### {len(affected)} offerings will be updated")

        preview_df = pd.DataFrame([
            {
                "Offering ID": a[0], "AY": a[1], "Year": a[2],
                "Term": a[3], "Customized": "Yes" if a[4] else "No"
            }
            for a in affected
        ])

        st.dataframe(preview_df, use_container_width=True)

        customized = sum(1 for a in affected if a[4])

        if customized > 0:
            st.warning(f"⚠️ {customized} offering(s) have customizations")

        col1, col2 = st.columns([2, 1])

        with col1:
            confirm = st.checkbox(
                f"Confirm assignment to {len(affected)} offerings",
                key="bulk_confirm",
            )

        with col2:
            if confirm and st.button("Apply", type="primary", use_container_width=True):
                try:
                    count = bulk_assign_template(
                        engine, template_id, subject_code, degree_code, from_ay, actor
                    )

                    success(f"Template assigned to {count} offerings!")
                    st.rerun()

                except Exception as e:
                    st.error(f"Error: {str(e)}")

    else:
        st.info(f"No offerings found for {subject_code} from {from_ay}")
