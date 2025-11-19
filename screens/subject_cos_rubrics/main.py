# screens/subject_cos_rubrics/main.py
"""
Main entry point for Subject COs and Rubrics Management - UPDATED

Provides tabs:
1. Subject Catalog - View published subjects per degree/AY/term with parameters (has own filters)
2. Course Outcomes - Manage COs for published subjects (uses shared filters)
3. Rubrics - Manage assessment rubrics (uses shared filters)
4. CO Import/Export - Bulk manage COs (uses shared filters)
5. CO Audit Trail - View change history for COs (uses shared filters)
"""

import streamlit as st
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def check_permissions() -> bool:
    """Check if user has permissions to access this module."""
    user = st.session_state.get("user", {})
    roles = user.get("roles", set())
    
    # Allow roles as per page_access_schema.py
    allowed_roles = {"superadmin", "principal", "director", "academic_admin", "faculty", "tech_admin"}
    return bool(roles & allowed_roles) or not roles  # Allow if no roles set


def render_subject_cos_rubrics_page():
    """Main render function for Subject COs and Rubrics page."""
    
    st.title("📚 Subject Course Outcomes & Rubrics")
    
    # Check permissions
    if not check_permissions():
        st.error("⛔ You don't have permission to access this module.")
        st.info("Required roles: superadmin, principal, director, academic_admin, faculty, or tech_admin")
        return
    
    # Check database engine
    engine = st.session_state.get("engine")
    if not engine:
        st.error("❌ Database engine not initialized")
        return
    
    # Import tab modules dynamically
    try:
        from screens.subject_cos_rubrics.subject_catalog_tab import render_subject_catalog_tab
        from screens.subject_cos_rubrics.course_outcomes_tab import render_course_outcomes_tab
        from screens.subject_cos_rubrics.rubrics_tab import render_rubrics_tab
        from screens.subject_cos_rubrics.shared_filters import render_co_filters
        from screens.subject_cos_rubrics.course_outcomes_audit_tab import render_co_audit_tab
        from screens.subject_cos_rubrics.course_outcomes_import_export_tab import render_co_import_export_tab
        from screens.subject_cos_rubrics.rubrics_import_export_tab import render_rubrics_import_export_tab

    except ImportError as e:
        st.error(f"❌ Failed to import tab modules: {e}")
        logger.error(f"Import error in subject_cos_rubrics main: {e}", exc_info=True)
        return
    
    # Subject Catalog tab has its own filters, so render it separately
    st.markdown("---")
    with st.expander("📋 View Subject Catalog Details", expanded=False):
        render_subject_catalog_tab(engine)
    st.markdown("---")

    # Render shared filters for CO and Rubric management
    st.markdown("## 🎯 Course Outcomes & Rubrics Management")
    st.info("Select a subject offering below to manage its Course Outcomes and Rubrics")
    
    offering_id, offering_info = render_co_filters(engine)
    
    # Show selected offering info
    if offering_id and offering_info:
        st.success(f"✅ Selected: **{offering_info['subject_code']}** - Year {offering_info['year']}, Term {offering_info['term']}")
    
    st.markdown("---")
    
    # Create tabs for CO and Rubric management (all use the same offering from shared filters)
    tab1, tab2, tab3, tab4 = st.tabs([
        "🎯 Course Outcomes",
        "📊 Rubrics",
        "⬆️ CO Import/Export",
        "📜 CO Audit Trail"
    ])
    
    # Render tabs - all receive the selected offering from shared filters
    with tab1:
        render_course_outcomes_tab(engine, offering_id, offering_info)
    
    with tab2:
        render_rubrics_tab(engine, offering_id, offering_info)

    with tab3:
        render_co_import_export_tab(engine, offering_id, offering_info)

    with tab4:
        render_co_audit_tab(engine, offering_id, offering_info)


if __name__ == "__main__":
    # For testing purposes
    render_subject_cos_rubrics_page()
