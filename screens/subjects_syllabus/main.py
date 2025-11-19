"""
Main Subjects & Syllabus Management Screen
Entry point that orchestrates all tabs
"""

import streamlit as st
from core.settings import load_settings
from core.db import get_engine, init_db, SessionLocal
from core.forms import tagline
from core.policy import require_page, can_edit_page, user_roles
from schemas.subjects_syllabus_schema import install_subjects_offerings_schema

# Import tab renderers using relative imports
from screens.subjects_syllabus.tabs import tab_subjects, tab_templates, tab_bulk_assign
from screens.subjects_syllabus.tabs import tab_customize, tab_import_export, tab_audit


@require_page("Subjects & Syllabus")
def render():
    """Main render function for Subjects & Syllabus Management."""
    st.title("Subjects & Syllabus Management")
    tagline()

    settings = load_settings()
    engine = get_engine(settings.db.url)

    # Install schema
    install_subjects_offerings_schema(engine)
    init_db(engine)
    SessionLocal.configure(bind=engine)

    user = st.session_state.get("user") or {}
    actor = user.get("email", "system")
    roles = user_roles()

    CAN_EDIT = can_edit_page("Subjects & Syllabus", roles)

    if not CAN_EDIT:
        st.info("📖 Read-only mode: You have view access but cannot modify data.")

    # Create tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "Subjects Catalog",
        "Template Manager",
        "Bulk Assignment",
        "Offering Customization",
        "Import/Export",
        "Audit Trail"
    ])

    # Render each tab
    with tab1:
        tab_subjects.render(engine, actor, CAN_EDIT)

    with tab2:
        tab_templates.render(engine, actor, CAN_EDIT)

    with tab3:
        tab_bulk_assign.render(engine, actor, CAN_EDIT)

    with tab4:
        tab_customize.render(engine, actor, CAN_EDIT)

    with tab5:
        tab_import_export.render(engine, actor, CAN_EDIT)

    with tab6:
        tab_audit.render(engine, actor, CAN_EDIT)


if __name__ == "__main__":
    render()
