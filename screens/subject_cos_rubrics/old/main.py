# screens/subject_cos_rubrics/main.py
"""
Main entry point for Subject COs and Rubrics Management

Provides three tabs:
1. Subject Catalog - View published subjects per degree/AY/term with parameters
2. Course Outcomes - Manage COs for published subjects
3. Rubrics - Manage assessment rubrics
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
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs([
        "📋 Subject Catalog",
        "🎯 Course Outcomes",
        "📊 Rubrics"
    ])
    
    # Import tab modules dynamically to avoid circular imports
    try:
        from screens.subject_cos_rubrics.subject_catalog_tab import render_subject_catalog_tab
        from screens.subject_cos_rubrics.course_outcomes_tab import render_course_outcomes_tab
        from screens.subject_cos_rubrics.rubrics_tab import render_rubrics_tab
    except ImportError as e:
        st.error(f"❌ Failed to import tab modules: {e}")
        logger.error(f"Import error in subject_cos_rubrics main: {e}", exc_info=True)
        return
    
    # Render tabs
    with tab1:
        render_subject_catalog_tab(engine)
    
    with tab2:
        render_course_outcomes_tab(engine)
    
    with tab3:
        render_rubrics_tab(engine)


if __name__ == "__main__":
    # For testing purposes
    render_subject_cos_rubrics_page()
