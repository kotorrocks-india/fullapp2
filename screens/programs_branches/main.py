"""
Main render function for Programs/Branches page
"""
import streamlit as st
import pandas as pd
from core.settings import load_settings
from core.db import get_engine, init_db
from core.policy import require_page, can_edit_page, user_roles
from schemas.degrees_schema import migrate_degrees

# Use absolute imports instead of relative imports
from screens.programs_branches.constants import allow_programs_for, allow_branches_for
from screens.programs_branches.db_helpers import (
    _ensure_curriculum_columns, _fetch_degree, _degrees_df,
    _programs_df, _branches_df, _curriculum_groups_df,
    _curriculum_group_links_df, _get_approvals_df,
    _get_semester_binding, _get_degree_struct,
    _get_program_structs_for_degree, _get_branch_structs_for_degree,
    _table_cols
)
from screens.programs_branches.ui_components import (
    render_degree_structure_map,
    render_import_export_section
)


@require_page("Programs / Branches")
def render():
    """Main render function for the Programs/Branches page."""
    settings = load_settings()
    engine = get_engine(settings.db.url)
    
    # Ensure schema is up to date
    migrate_degrees(engine)
    _ensure_curriculum_columns(engine)
    init_db(engine)
    
    # Get user info and permissions
    user = st.session_state.get("user") or {}
    actor = (user.get("email") or user.get("full_name") or "system")
    roles = user_roles()
    CAN_EDIT = can_edit_page("Programs / Branches", roles)
    
    # Initialize error states
    if "prog_create_error" not in st.session_state:
        st.session_state.prog_create_error = None
    if "branch_create_error" not in st.session_state:
        st.session_state.branch_create_error = None
    if "cg_create_error" not in st.session_state:
        st.session_state.cg_create_error = None
    
    # Display permissions info
    if not CAN_EDIT:
        st.info("📖 Read-only mode: You have view access but cannot modify data.")
    
    st.title("📚 Programs, Branches & Curriculum")
    
    # Load degrees
    try:
        ddf = _degrees_df(engine)
    except Exception as e:
        st.error(f"Failed to load degrees. Has the database been initialized? Error: {e}")
        st.warning("If this is a new setup, please visit the 'Degrees' page first to create the necessary tables.")
        return

    if ddf.empty:
        st.info("No degrees found. Please create a degree on the 'Degrees' page first.")
        return
    
    # Degree selection
    deg_codes = ddf["code"].tolist()
    degree_sel = st.selectbox("Degree", options=deg_codes, key="pb_deg_sel")
    
    # Load degree-specific data
    with engine.begin() as conn:
        deg = _fetch_degree(conn, degree_sel)
        dfp = _programs_df(engine, degree_sel)
        dfb_all = _branches_df(engine, degree_sel, program_id=None)
        
        SHOW_CG = bool(deg.cg_degree or deg.cg_program or deg.cg_branch)
        df_cg = _curriculum_groups_df(engine, degree_sel) if SHOW_CG else pd.DataFrame()
        df_cgl = _curriculum_group_links_df(engine, degree_sel) if SHOW_CG else pd.DataFrame()
        df_approvals = _get_approvals_df(engine, ["program", "branch", "curriculum_group"])
        
        sem_binding = _get_semester_binding(conn, degree_sel) or 'degree'
        deg_struct = _get_degree_struct(conn, degree_sel)
        prog_structs = _get_program_structs_for_degree(conn, degree_sel)
        branch_structs = _get_branch_structs_for_degree(conn, degree_sel)
        
        bcols = _table_cols(engine, "branches")
        BR_HAS_PID = "program_id" in bcols
        BR_HAS_DEG = "degree_code" in bcols
    
    mode = str(deg.cohort_splitting_mode or "both").lower()
    
    # Display degree info
    st.caption(f"Degree: **{deg.title}** • Cohort mode: `{mode}` • Active: `{bool(deg.active)}`")
    st.markdown("---")
    
    # Render degree structure map
    render_degree_structure_map(
        deg, degree_sel, sem_binding, deg_struct, prog_structs, branch_structs,
        dfp, dfb_all, df_cg, df_cgl, mode
    )
    
    st.markdown("---")
    
    # Determine which tabs to show
    allow_programs = allow_programs_for(mode)
    allow_branches = allow_branches_for(mode)
    supports_degree_level_branches = BR_HAS_DEG
    
    if not supports_degree_level_branches:
        st.info("Schema note: your 'branches' table has no degree_code column, so all branches must be attached to a Program.")
    
    # Define tab labels
    labels = []
    if allow_programs:
        labels.append("Programs")
    if allow_branches:
        labels.append("Branches")
    if SHOW_CG:
        labels.append("Curriculum Groups")
    if not labels:
        labels.append("View")
    
    # Tab navigation
    page_tab_key = f"pb_active_tab_{degree_sel}"
    if page_tab_key not in st.session_state:
        st.session_state[page_tab_key] = labels[0]
    
    if st.session_state[page_tab_key] not in labels:
        st.session_state[page_tab_key] = labels[0]
    
    try:
        active_tab_index = labels.index(st.session_state[page_tab_key])
    except ValueError:
        active_tab_index = 0
    
    active_tab = st.radio(
        "Navigation",
        options=labels,
        index=active_tab_index,
        key=page_tab_key,
        horizontal=True,
        label_visibility="collapsed"
    )
    
    # Render import/export section
    if active_tab in ["Programs", "Branches", "Curriculum Groups"]:
        render_import_export_section(
            engine, degree_sel, deg, dfp, dfb_all, df_cg, df_cgl,
            actor, active_tab, BR_HAS_PID, SHOW_CG
        )
        st.markdown("---")
    
    # Render active tab content
    if active_tab == "Programs":
        from screens.programs_branches.programs_tab import render_programs_tab
        render_programs_tab(
            engine, degree_sel, deg, dfp, df_approvals,
            actor, CAN_EDIT, mode
        )
    
    elif active_tab == "Branches":
        from screens.programs_branches.branches_tab import render_branches_tab
        render_branches_tab(
            engine, degree_sel, deg, dfp, dfb_all, df_approvals,
            actor, CAN_EDIT, mode, BR_HAS_PID, BR_HAS_DEG,
            supports_degree_level_branches
        )
    
    elif active_tab == "Curriculum Groups":
        from screens.programs_branches.curriculum_groups_tab import render_curriculum_groups_tab
        render_curriculum_groups_tab(
            engine, degree_sel, deg, df_cg, df_cgl, df_approvals,
            actor, CAN_EDIT
        )
    
    elif active_tab == "View":
        st.info("This degree's cohort mode does not allow Programs or Branches.")


# Entry point with error handling
try:
    render()
except Exception as e:
    import traceback
    st.error(f"An unexpected error occurred on this page: {e}")
    st.warning("If you just created a new database, please visit the 'Degrees' page *first* to initialize the application schema.")
    with st.expander("Show Error Details"):
        st.code(traceback.format_exc())
