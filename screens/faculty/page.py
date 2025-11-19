# screens/faculty/page.py
from __future__ import annotations

import sys
import traceback
from pathlib import Path
from typing import Set

import streamlit as st
from sqlalchemy import text as sa_text

# Ensure project root
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Core app imports
from core.settings import load_settings
from core.db import get_engine
# --- MODIFIED IMPORTS ---
from core.policy import (
    require_page, 
    user_roles, 
    render_policy_aware_tabs  # <-- IMPORT OUR NEW FUNCTION
)
# --- END MODIFIED IMPORTS ---

# Faculty module tabs
from screens.faculty.ui_tabs import (
    _tab_designations,
    _tab_designation_removal,
    _tab_custom_types,
    _tab_profiles,
    _tab_affiliations,
    _tab_export_credentials,
    _tab_bulk_ops,
)
from screens.faculty.tabs.credits_policy import render as _tab_credits_policy
from screens.faculty.tabs.positions import render as _tab_positions
from screens.faculty.db import (
    _active_degrees,
    _add_fixed_role_admins_to_degree,
)

PAGE_KEY = "Faculty"


@require_page(PAGE_KEY)
def render():
    """
    Main Faculty page renderer.
    - Uses the universal 'render_policy_aware_tabs' helper.
    """
    try:
        st.title("Faculty")

        settings = load_settings()
        engine = get_engine(settings.db.url)

        # --- Read active degrees ---
        degrees = []
        try:
            with engine.begin() as conn:
                degrees = _active_degrees(conn)
        except Exception as e:
            st.error(f"❌ Error accessing core tables: {e}")
            return

        # --- Session/user permissions ---
        user = st.session_state.get("user") or {}
        roles: Set[str] = user_roles(engine, user.get("email"))
        user_email = user.get("email") or "anonymous"

        if not degrees:
            st.warning("⚠️ No **active** degrees found.")
            st.info("💡 Go to **Degrees & Programs** to create/activate a degree.")
            return

        # --- Header controls ---
        col_left, col_right = st.columns([2, 1])
        with col_left:
            degree = st.selectbox("Degree", degrees, key=f"{PAGE_KEY.lower()}_degree_select")
        with col_right:
            st.caption(f"Signed in as **{user_email}**")

        if not degree:
            st.info("Please select a degree.")
            return

        # --- Sync admin affiliations ---
        try:
            with engine.begin() as conn:
                _add_fixed_role_admins_to_degree(conn, degree)
        except Exception as e:
            st.warning(f"Note: Could not sync admin affiliations for {degree}: {e}")

        # =================================================================
        # --- DYNAMIC TAB RENDERING (NOW UNIVERSAL) ---
        # =================================================================
        
        # 1. Define all possible tabs for THIS page
        ALL_FACULTY_TABS = [
            ("Credits Policy", _tab_credits_policy),
            ("Designation Catalog", _tab_designations),
            ("Designation Removal", _tab_designation_removal),
            ("Custom Types", _tab_custom_types),
            ("Profiles", _tab_profiles),
            ("Affiliations", _tab_affiliations),
            ("Manage Positions", _tab_positions),
            ("Bulk Operations", _tab_bulk_ops),
            ("Export Credentials", _tab_export_credentials),
        ]
        
        # 2. Call the universal helper
        # It handles all filtering, rendering, and permission checks.
        render_policy_aware_tabs(
            all_tabs=ALL_FACULTY_TABS,
            engine=engine,
            roles=roles,
            # --- Pass down page-specific arguments ---
            
            # --------------------- FIX ---------------------
            # The argument must be named 'degree' to match the
            # function signatures in your tab files.
            degree=degree, 
            # ------------------- END FIX -------------------
            
            key_prefix=f"{PAGE_KEY.lower()}"
        )
        # =================================================================
        # --- END DYNAMIC TAB RENDERING ---
        # =================================================================

    except Exception as e:
        st.error("An unexpected error occurred while rendering the Faculty page.")
        st.exception(e)
        st.code("".join(traceback.format_exc()))


# Keep this call if your navigation expects pages to render on import
render()
