"""
Audit Trail Tab - View audit logs for subjects and templates
"""

import streamlit as st
import pandas as pd
from ..helpers import exec_query, rows_to_dicts


def render(engine, actor: str, CAN_EDIT: bool):
    """Render the Audit Trail tab."""
    st.subheader("📜 Audit Trail")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Subject Changes")

        with engine.begin() as conn:
            logs = exec_query(conn, """
                SELECT subject_code, degree_code, action, note, actor, at
                FROM subjects_catalog_audit
                ORDER BY id DESC LIMIT 50
            """).fetchall()

        if logs:
            df = pd.DataFrame(rows_to_dicts(logs))
            st.dataframe(df, use_container_width=True)

        else:
            st.info("No audit logs")

    with col2:
        st.markdown("### Template Changes")

        with engine.begin() as conn:
            logs = exec_query(conn, """
                SELECT template_code, action, note, actor, at
                FROM syllabus_templates_audit
                ORDER BY id DESC LIMIT 50
            """).fetchall()

        if logs:
            df = pd.DataFrame(rows_to_dicts(logs))
            st.dataframe(df, use_container_width=True)

        else:
            st.info("No audit logs")
