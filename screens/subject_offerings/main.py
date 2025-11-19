# -*- coding: utf-8 -*-
"""
Subject Offerings Management - Main Entry Point
Orchestrates all tabs for AY-specific subject offerings

FIXED: Corrected emoji encoding issues
"""

import streamlit as st
import logging
from sqlalchemy.exc import OperationalError
from core.settings import load_settings
from core.db import get_engine
from core.forms import tagline
from core.policy import require_page, can_edit_page, user_roles

# Import tab modules directly to avoid circular imports
from screens.subject_offerings.tabs import tab_offerings
from screens.subject_offerings.tabs import tab_assignment_helper
from screens.subject_offerings.tabs import tab_bulk_assign
from screens.subject_offerings.tabs import tab_customize
from screens.subject_offerings.tabs import tab_audit

# ---- Import/Export tab: guard import so a bug there doesn't kill the whole page ----
try:
    from screens.subject_offerings.tabs import tab_import_export
    _TAB_IMPORT_EXPORT_ERROR = None
except Exception as e:
    tab_import_export = None
    _TAB_IMPORT_EXPORT_ERROR = e

# Set up logger
logger = logging.getLogger(__name__)


@require_page("Subjects Offerings")
def render():
    """Main render function for Subject Offerings Management."""
    st.title("Subject Offerings by AY & Term")
    tagline()

    try:
        settings = load_settings()
        engine = get_engine(settings.db.url)

        user = st.session_state.get("user") or {}
        actor = user.get("email", "system")
        roles = user_roles()

        CAN_EDIT = can_edit_page("Subjects Offerings", roles)

        if not CAN_EDIT:
            st.info("Read-only mode: You have view access but cannot modify data.")

        # Create tabs
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
            [
                "Offerings",
                "Assignment Helper",
                "Bulk Assignment",
                "Customization",
                "Import/Export",
                "Audit Trail",
            ]
        )

        # Render each tab
        with tab1:
            tab_offerings.render(engine, actor, CAN_EDIT)

        with tab2:
            tab_assignment_helper.render(engine, actor, CAN_EDIT)

        with tab3:
            tab_bulk_assign.render(engine, actor, CAN_EDIT)

        with tab4:
            tab_customize.render(engine, actor, CAN_EDIT)

        with tab5:
            if tab_import_export is None:
                st.error("Import/Export tab failed to load.")
                st.code(str(_TAB_IMPORT_EXPORT_ERROR or ""), language="text")
                st.info(
                    "Please open `screens/subject_offerings/tabs/tab_import_export.py` "
                    "and check for:\n"
                    "- Missing file or wrong filename\n"
                    "- Syntax errors (often from bad emoji/unicode)\n"
                    "- Circular imports like `from screens.subject_offerings.tabs import ...`"
                )
            else:
                tab_import_export.render(engine, actor, CAN_EDIT)

        with tab6:
            tab_audit.render(engine, actor, CAN_EDIT)

    except OperationalError as e:
        # Catch database table errors
        if "no such table" in str(e):
            st.error("Application Not Ready", icon="🛠️")
            st.warning(
                "**The application cannot connect to the required database tables.**"
            )
            st.info(
                """
                This module is not yet configured. The database tables 
                (e.g., `degrees`, `subjects`, `offerings`) appear to be missing.
                
                **Please contact your system administrator** to run the 
                initial database setup.
                """
            )
            logger.error(f"Database schema missing in Subject Offerings: {e}")
        else:
            st.error("A Database Error Occurred", icon="🔥")
            st.warning(
                "An unexpected database problem occurred. Please try again later. "
                "If the problem persists, please contact your system administrator."
            )
            logger.error(f"Caught unexpected OperationalError in Subject Offerings: {e}")
    
    except Exception as e:
        # Catch any other unexpected application errors
        st.error("An Application Error Occurred", icon="🔥")
        st.warning(
            "An unexpected application error occurred. Please try again later. "
            "If the problem persists, please contact your system administrator."
        )
        
        # TEMPORARY: Show full traceback for debugging
        import traceback
        st.code(traceback.format_exc(), language="python")
        
        logger.error(f"Caught unexpected Exception in Subject Offerings: {e}", exc_info=True)


# Entry point for Streamlit navigation
if __name__ == "__main__":
    render()
else:
    # When imported as a module by st.Page
    render()
