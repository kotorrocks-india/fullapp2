"""
Offering Customization Tab - Override template points for individual offerings
"""

import streamlit as st
import pandas as pd
from ..helpers import exec_query, rows_to_dicts
from ..templates_crud import get_effective_syllabus_for_offering, create_syllabus_override
from core.forms import success
from ..constants import OVERRIDE_TYPES
from sqlalchemy import text as sa_text


def render(engine, actor: str, CAN_EDIT: bool):
    """Render the Offering Customization tab."""
    st.subheader("✏️ Offering-Specific Customization")
    st.caption("Override template points for individual offerings")

    with engine.begin() as conn:
        offerings = exec_query(conn, """
            SELECT so.id, so.subject_code, sc.subject_name,
                   so.ay_label, so.year, so.term,
                   so.syllabus_template_id, so.syllabus_customized
            FROM subject_offerings so
            LEFT JOIN subjects_catalog sc ON sc.subject_code = so.subject_code
            ORDER BY so.ay_label DESC, so.year, so.term
        """).fetchall()

    if not offerings:
        st.warning("No offerings found")
        return

    offerings = rows_to_dicts(offerings)

    offering_id = st.selectbox(
        "Select Offering",
        options=[o["id"] for o in offerings],
        format_func=lambda x: next(
            (
                f"{o['subject_code']} - {o['subject_name']} | "
                f"{o['ay_label']} Y{o['year']}T{o['term']} "
                f"{'⚙️' if o['syllabus_customized'] else ''}"
                for o in offerings if o["id"] == x
            ),
            str(x)
        ),
        key="custom_offering",
    )

    offering_data = next((o for o in offerings if o["id"] == offering_id), None)

    if not offering_data or not offering_data["syllabus_template_id"]:
        st.warning("This offering doesn't have a template assigned")
        return

    # Get effective syllabus
    with engine.begin() as conn:
        effective_points = get_effective_syllabus_for_offering(conn, offering_id)

    # Get template info
    with engine.begin() as conn:
        template = exec_query(conn, """
            SELECT code, name, version
            FROM syllabus_templates
            WHERE id = :id
        """, {"id": offering_data["syllabus_template_id"]}).fetchone()

    if template:
        st.info(f"📋 Using: **{template[1]}** ({template[2]})")

    st.metric("Total Points", len(effective_points))

    # Display points
    st.markdown("---")

    for point in effective_points:
        seq = point['sequence']
        is_custom = point.get('is_overridden', False)

        with st.expander(
            f"{'⚙️' if is_custom else '📄'} {seq}. {point['title']}",
            expanded=False,
        ):
            if is_custom:
                st.warning("This point has custom overrides")

            st.markdown(f"**Title:** {point['title']}")

            if point.get('description'):
                st.markdown(f"**Description:** {point['description']}")

            if point.get('hours_weight'):
                st.markdown(f"**Hours:** {point['hours_weight']}")

            if CAN_EDIT:
                st.markdown("---")

                with st.form(f"edit_{seq}"):
                    override_type = st.selectbox(
                        "Override Type",
                        options=OVERRIDE_TYPES,
                        help="Replace: Full override | Append: Add to template | Hide: Don't show",
                        key=f"type_{seq}",
                    )

                    new_title = st.text_input(
                        "Title", value=point['title'], key=f"title_{seq}"
                    )

                    new_desc = st.text_area(
                        "Description", value=point.get('description', ''),
                        key=f"desc_{seq}",
                    )

                    new_hours = st.number_input(
                        "Hours", value=point.get('hours_weight', 0.0) or 0.0,
                        step=0.5, key=f"hrs_{seq}",
                    )

                    reason = st.text_input("Reason", key=f"reason_{seq}")

                    if st.form_submit_button("💾 Save Override"):
                        try:
                            create_syllabus_override(
                                engine, offering_id, seq, override_type, actor,
                                title=new_title if new_title != point['title'] else None,
                                description=new_desc if new_desc != point.get('description') else None,
                                hours_weight=new_hours if new_hours != point.get('hours_weight') else None,
                                reason=reason,
                            )

                            success("Override saved!")
                            st.rerun()

                        except Exception as e:
                            st.error(f"Error: {str(e)}")
