"""
Reusable UI components for Programs/Branches module
"""
import io
import csv
import pandas as pd
import streamlit as st
from sqlalchemy import text as sa_text

from .constants import (
    PROGRAM_IMPORT_COLS, BRANCH_IMPORT_COLS,
    CG_IMPORT_COLS, CGL_IMPORT_COLS
)
from screens.programs_branches.import_export import (
    import_programs, import_branches,
    import_cgs, import_cg_links
)


def _df_to_csv(df_to_conv: pd.DataFrame):
    """Convert DataFrame to CSV bytes."""
    with io.StringIO() as buffer:
        df_to_conv.to_csv(buffer, index=False, quoting=csv.QUOTE_ALL)
        return buffer.getvalue().encode('utf-8')


def _keep_import_open():
    """Callback to keep the import expander open."""
    st.session_state["pb_import_open"] = True


def render_degree_structure_map(deg, degree_sel, sem_binding, deg_struct, 
                                prog_structs, branch_structs, dfp, dfb_all, 
                                df_cg, df_cgl, mode):
    """Renders the collapsible degree structure map."""
    with st.expander("Show full degree structure map", expanded=True):
        map_md = f"**Degree:** {deg.title} (`{degree_sel}`)\n"
        if sem_binding == 'degree' and deg_struct:
            map_md += f"- *Semester Structure: {deg_struct[0]} Years, {deg_struct[1]} Terms/Year*\n"
        
        if deg.cg_degree:
            linked_cgs_deg = df_cgl[df_cgl['program_code'].isnull() & df_cgl['branch_code'].isnull()] if not df_cgl.empty else pd.DataFrame()
            for _, cg_link_row in linked_cgs_deg.iterrows():
                map_md += f"- *Curriculum Group:* `{cg_link_row['group_code']}`\n"
        
        map_md += "\n"
        
        if mode == 'both':
            map_md += "**Hierarchy:** `Degree → Program → Branch`\n"
            if not dfp.empty:
                for _, prog_row in dfp.iterrows():
                    prog_code = prog_row['program_code']
                    map_md += f"- **Program:** {prog_row['program_name']} (`{prog_code}`)\n"
                    if sem_binding == 'program' and prog_code in prog_structs:
                        p_struct = prog_structs[prog_code]
                        map_md += f"  - *Semester Structure: {p_struct[0]} Years, {p_struct[1]} Terms/Year*\n"
                    
                    if deg.cg_program:
                        linked_cgs_prog = df_cgl[
                            (df_cgl['program_code'] == prog_code) & (df_cgl['branch_code'].isnull())
                        ] if not df_cgl.empty else pd.DataFrame()
                        for _, cg_link_row in linked_cgs_prog.iterrows():
                            map_md += f"  - *Curriculum Group:* `{cg_link_row['group_code']}`\n"
                    
                    child_branches = dfb_all[dfb_all['program_code'] == prog_code] if not dfb_all.empty else pd.DataFrame()
                    if not child_branches.empty:
                        for _, branch_row in child_branches.iterrows():
                            branch_code = branch_row['branch_code']
                            map_md += f"  - **Branch:** {branch_row['branch_name']} (`{branch_code}`)\n"
                            if sem_binding == 'branch' and branch_code in branch_structs:
                                b_struct = branch_structs[branch_code]
                                map_md += f"    - *Semester Structure: {b_struct[0]} Years, {b_struct[1]} Terms/Year*\n"
                            
                            if deg.cg_branch:
                                linked_cgs_branch = df_cgl[df_cgl['branch_code'] == branch_code] if not df_cgl.empty else pd.DataFrame()
                                for _, cg_link_row in linked_cgs_branch.iterrows():
                                    map_md += f"    - *Curriculum Group:* `{cg_link_row['group_code']}`\n"
                    else:
                        map_md += "  - *(No branches defined for this program)*\n"
            else:
                map_md += "*(No programs defined for this degree)*\n"
        
        SHOW_CG = bool(deg.cg_degree or deg.cg_program or deg.cg_branch)
        if SHOW_CG:
            map_md += "\n---\n"
            cg_list = df_cg["group_name"].tolist() if not df_cg.empty else []
            map_md += f"**All Defined Curriculum Groups (for this degree):** {', '.join(cg_list) if cg_list else 'None'}"
        
        st.markdown(map_md)


def render_import_export_section(
    engine, degree_sel, deg, dfp, dfb_all, df_cg, df_cgl,
    actor, active_tab, BR_HAS_PID, SHOW_CG
):
    """Renders the import/export UI section."""
    
    # --- FIX START: Use generic expander title and move specific degree info inside ---
    if "pb_import_open" not in st.session_state:
        st.session_state["pb_import_open"] = False
        
    with st.expander("📥 Import / Export Data"): 
        st.markdown(f"""
        ### Import / Export
        Import **{active_tab}** for the **{deg.title} ({degree_sel})** degree.
        """)
    # --- FIX END ---
        
        st.markdown("### 🛠️ Import Controls")
        
        col1, col2 = st.columns(2)
        with col1:
            dry_run_mode = st.checkbox(
                "🔍 Dry-Run Mode",
                value=False,
                help="Simulate import without saving to database. Shows what WOULD be imported."
            )
        with col2:
            debug_mode = st.checkbox(
                "🐛 Debug Mode", 
                value=False,
                help="Show detailed step-by-step information about the import process."
            )
        
        if dry_run_mode:
            st.warning("⚠️ **DRY-RUN MODE ENABLED**: No changes will be saved to the database. This is a simulation only.")
        
        if debug_mode:
            st.info("ℹ️ **DEBUG MODE ENABLED**: Detailed information will be displayed for each row processed.")
        
        st.markdown("---")
        
        # Import tabs based on active tab
        if active_tab in ["Programs", "Branches"]:
            _render_programs_branches_import(
                engine, degree_sel, actor, dry_run_mode, debug_mode, BR_HAS_PID
            )
        elif active_tab == "Curriculum Groups":
            _render_curriculum_groups_import(
                engine, degree_sel, actor, dry_run_mode, debug_mode,
                SHOW_CG, df_cg, dfp, dfb_all
            )
        
        st.markdown("---")
        st.subheader("Export")
        st.info(f"Download all {active_tab} currently associated with the **{deg.title} ({degree_sel})** degree.")
        
        _render_export_section(active_tab, dfp, dfb_all, df_cg, df_cgl, degree_sel, SHOW_CG)


def _render_programs_branches_import(engine, degree_sel, actor, dry_run_mode, debug_mode, BR_HAS_PID):
    """Renders import UI for programs and branches."""
    im_tab1, im_tab2 = st.tabs(["Import Programs", "Import Branches"])
    
    with im_tab1:
        df_prog_template = pd.DataFrame(columns=PROGRAM_IMPORT_COLS)
        st.download_button(
            label="📄 Download Program Template (CSV)",
            data=_df_to_csv(df_prog_template),
            file_name=f"{degree_sel}_programs_template.csv",
            mime="text/csv",
            key="dload_prog_template"
        )
        
        prog_file = st.file_uploader(
            "Upload Program CSV", 
            type=["csv"], 
            key="prog_uploader",
            help="Upload a CSV file with columns: " + ", ".join(PROGRAM_IMPORT_COLS)
        )
        
        if prog_file:
            with st.expander("👀 Preview Uploaded File", expanded=False):
                try:
                    df_preview = pd.read_csv(prog_file, dtype=str).fillna("")
                    st.write(f"**Rows:** {len(df_preview)}")
                    st.write(f"**Columns:** {list(df_preview.columns)}")
                    st.dataframe(df_preview.head(10), use_container_width=True)
                    prog_file.seek(0)
                except Exception as e:
                    st.error(f"Error reading CSV: {e}")
        
        button_label = "🔍 Preview Import (Dry-Run)" if dry_run_mode else "📥 Import Programs"
        
        if st.button(button_label, key="import_prog_btn", disabled=not prog_file, on_click=_keep_import_open):
            _handle_program_import(
                engine, prog_file, degree_sel, actor,
                dry_run_mode, debug_mode
            )
    
    with im_tab2:
        df_br_template = pd.DataFrame(columns=BRANCH_IMPORT_COLS)
        st.download_button(
            label="📄 Download Branch Template (CSV)",
            data=_df_to_csv(df_br_template),
            file_name=f"{degree_sel}_branches_template.csv",
            mime="text/csv",
            key="dload_br_template"
        )
        
        branch_file = st.file_uploader(
            "Upload Branch CSV", 
            type=["csv"], 
            key="branch_uploader",
            help="Upload a CSV file with columns: " + ", ".join(BRANCH_IMPORT_COLS)
        )
        
        if branch_file:
            with st.expander("👀 Preview Uploaded File", expanded=False):
                try:
                    df_preview = pd.read_csv(branch_file, dtype=str).fillna("")
                    st.write(f"**Rows:** {len(df_preview)}")
                    st.write(f"**Columns:** {list(df_preview.columns)}")
                    st.dataframe(df_preview.head(10), use_container_width=True)
                    branch_file.seek(0)
                except Exception as e:
                    st.error(f"Error reading CSV: {e}")
        
        button_label = "🔍 Preview Import (Dry-Run)" if dry_run_mode else "📥 Import Branches"
        
        if st.button(button_label, key="import_br_btn", disabled=not branch_file, on_click=_keep_import_open):
            _handle_branch_import(
                engine, branch_file, degree_sel, actor,
                BR_HAS_PID, dry_run_mode, debug_mode
            )


def _render_curriculum_groups_import(engine, degree_sel, actor, dry_run_mode, 
                                    debug_mode, SHOW_CG, df_cg, dfp, dfb_all):
    """Renders import UI for curriculum groups."""
    im_tab1, im_tab2 = st.tabs(["Import Curriculum Groups", "Import Group Links"])
    
    with im_tab1:
        df_cg_template = pd.DataFrame(columns=CG_IMPORT_COLS)
        st.download_button(
            label="📄 Download Curriculum Groups Template (CSV)",
            data=_df_to_csv(df_cg_template),
            file_name=f"{degree_sel}_curriculum_groups_template.csv",
            mime="text/csv",
            key="dload_cg_template",
            disabled=not SHOW_CG
        )
        
        cg_file = st.file_uploader(
            "Upload Curriculum Groups CSV", 
            type=["csv"], 
            key="cg_uploader",
            disabled=not SHOW_CG,
            help="Upload a CSV file with columns: " + ", ".join(CG_IMPORT_COLS)
        )
        
        if cg_file:
            with st.expander("👀 Preview Uploaded File", expanded=False):
                try:
                    df_preview = pd.read_csv(cg_file, dtype=str).fillna("")
                    st.write(f"**Rows:** {len(df_preview)}")
                    st.write(f"**Columns:** {list(df_preview.columns)}")
                    st.dataframe(df_preview.head(10), use_container_width=True)
                    cg_file.seek(0)
                except Exception as e:
                    st.error(f"Error reading CSV: {e}")
        
        button_label = "🔍 Preview Import (Dry-Run)" if dry_run_mode else "📥 Import Curriculum Groups"
        
        if st.button(button_label, key="import_cg_btn", disabled=not cg_file or not SHOW_CG, on_click=_keep_import_open):
            _handle_cg_import(engine, cg_file, degree_sel, actor, SHOW_CG, dry_run_mode, debug_mode)
    
    with im_tab2:
        df_cgl_template = pd.DataFrame(columns=CGL_IMPORT_COLS)
        st.download_button(
            label="📄 Download Group Links Template (CSV)",
            data=_df_to_csv(df_cgl_template),
            file_name=f"{degree_sel}_curriculum_group_links_template.csv",
            mime="text/csv",
            key="dload_cgl_template",
            disabled=not SHOW_CG
        )
        
        cgl_file = st.file_uploader(
            "Upload Group Links CSV", 
            type=["csv"], 
            key="cgl_uploader",
            disabled=not SHOW_CG,
            help="Upload a CSV file with columns: " + ", ".join(CGL_IMPORT_COLS)
        )
        
        if cgl_file:
            with st.expander("👀 Preview Uploaded File", expanded=False):
                try:
                    df_preview = pd.read_csv(cgl_file, dtype=str).fillna("")
                    st.write(f"**Rows:** {len(df_preview)}")
                    st.write(f"**Columns:** {list(df_preview.columns)}")
                    st.dataframe(df_preview.head(10), use_container_width=True)
                    cgl_file.seek(0)
                except Exception as e:
                    st.error(f"Error reading CSV: {e}")
        
        button_label = "🔍 Preview Import (Dry-Run)" if dry_run_mode else "📥 Import Group Links"
        
        if st.button(button_label, key="import_cgl_btn", disabled=not cgl_file or not SHOW_CG, on_click=_keep_import_open):
            group_codes = df_cg["group_code"].tolist() if not df_cg.empty else []
            program_codes = dfp["program_code"].tolist() if not dfp.empty else []
            branch_codes = dfb_all["branch_code"].tolist() if not dfb_all.empty else []
            _handle_cgl_import(
                engine, cgl_file, degree_sel, actor, SHOW_CG,
                group_codes, program_codes, branch_codes,
                dry_run_mode, debug_mode
            )


def _render_export_section(active_tab, dfp, dfb_all, df_cg, df_cgl, degree_sel, SHOW_CG):
    """Renders export buttons."""
    if active_tab in ["Programs", "Branches"]:
        exp_col1, exp_col2 = st.columns(2)
        with exp_col1:
            if not dfp.empty:
                export_dfp = dfp[PROGRAM_IMPORT_COLS] if all(col in dfp.columns for col in PROGRAM_IMPORT_COLS) else dfp
                st.download_button(
                    label=f"📥 Export {len(export_dfp)} Programs (CSV)",
                    data=_df_to_csv(export_dfp),
                    file_name=f"{degree_sel}_programs_export.csv",
                    mime="text/csv",
                    key="export_prog_btn"
                )
            else:
                st.caption("No programs to export.")
        
        with exp_col2:
            if not dfb_all.empty:
                export_dfb = dfb_all[BRANCH_IMPORT_COLS] if all(col in dfb_all.columns for col in BRANCH_IMPORT_COLS) else dfb_all
                st.download_button(
                    label=f"📥 Export {len(export_dfb)} Branches (CSV)",
                    data=_df_to_csv(export_dfb),
                    file_name=f"{degree_sel}_branches_export.csv",
                    mime="text/csv",
                    key="export_br_btn"
                )
            else:
                st.caption("No branches to export.")
    
    elif active_tab == "Curriculum Groups":
        exp_col1, exp_col2 = st.columns(2)
        with exp_col1:
            if not df_cg.empty and SHOW_CG:
                export_dfcg = df_cg[CG_IMPORT_COLS] if all(col in df_cg.columns for col in CG_IMPORT_COLS) else df_cg
                st.download_button(
                    label=f"📥 Export {len(export_dfcg)} Curriculum Groups (CSV)",
                    data=_df_to_csv(export_dfcg),
                    file_name=f"{degree_sel}_curriculum_groups_export.csv",
                    mime="text/csv",
                    key="export_cg_btn"
                )
            else:
                st.caption("No curriculum groups to export.")
        
        with exp_col2:
            if not df_cgl.empty and SHOW_CG:
                export_dfcgl = df_cgl[CGL_IMPORT_COLS] if all(col in df_cgl.columns for col in CGL_IMPORT_COLS) else df_cgl
                st.download_button(
                    label=f"📥 Export {len(export_dfcgl)} Group Links (CSV)",
                    data=_df_to_csv(export_dfcgl),
                    file_name=f"{degree_sel}_curriculum_group_links_export.csv",
                    mime="text/csv",
                    key="export_cgl_btn"
                )
            else:
                st.caption("No curriculum group links to export.")


def _handle_program_import(engine, prog_file, degree_sel, actor, dry_run_mode, debug_mode):
    """Handles program import."""
    try:
        df_import = pd.read_csv(prog_file, dtype=str).fillna("")
        
        with engine.begin() as conn:
            c_count, u_count, errors = import_programs(
                conn, df_import, degree_sel, actor, engine,
                dry_run=dry_run_mode,
                debug=debug_mode
            )
        
        if dry_run_mode:
            st.success(f"🔍 DRY-RUN: Would create {c_count}, would update {u_count}")
            st.info("No changes saved (dry-run mode)")
        else:
            if c_count > 0 or u_count > 0:
                st.success(f"✅ Import complete: {c_count} created, {u_count} updated")
                
                with engine.begin() as v:
                    cnt = v.execute(sa_text("SELECT COUNT(*) FROM programs WHERE degree_code=:d"), 
                                   {"d": degree_sel}).fetchone()[0]
                    st.info(f"✅ Verified: {cnt} programs in database for {degree_sel}")
                
                st.cache_data.clear()
                st.rerun()
            else:
                st.info("Import complete: No changes (data identical)")
        
        if errors:
            st.error(f"Errors: {len(errors)}")
            for e in errors:
                st.error(f"• {e}")
                
    except Exception as e:
        st.error(f"Import failed: {e}")
        import traceback
        st.code(traceback.format_exc())


def _handle_branch_import(engine, branch_file, degree_sel, actor, BR_HAS_PID, dry_run_mode, debug_mode):
    """Handles branch import."""
    try:
        df_import = pd.read_csv(branch_file, dtype=str).fillna("")
        
        with engine.begin() as conn:
            c_count, u_count, errors = import_branches(
                conn, df_import, degree_sel, actor, BR_HAS_PID, engine,
                dry_run=dry_run_mode,
                debug=debug_mode
            )
        
        if dry_run_mode:
            st.success(f"🔍 DRY-RUN: Would create {c_count}, would update {u_count}")
            st.info("No changes saved (dry-run mode)")
        else:
            if c_count > 0 or u_count > 0:
                st.success(f"✅ Import complete: {c_count} created, {u_count} updated")
                
                with engine.begin() as v:
                    if BR_HAS_PID:
                        cnt = v.execute(sa_text("""
                            SELECT COUNT(*) FROM branches b
                            JOIN programs p ON b.program_id = p.id
                            WHERE p.degree_code = :dc
                        """), {"dc": degree_sel}).fetchone()[0]
                    else:
                        cnt = v.execute(sa_text("""
                            SELECT COUNT(*) FROM branches WHERE degree_code = :dc
                        """), {"dc": degree_sel}).fetchone()[0]
                    st.info(f"✅ Verified: {cnt} branches in database for {degree_sel}")
                
                st.cache_data.clear()
                st.rerun()
            else:
                st.info("Import complete: No changes (data identical)")
        
        if errors:
            st.error(f"Errors: {len(errors)}")
            for e in errors:
                st.error(f"• {e}")
                
    except Exception as e:
        st.error(f"Import failed: {e}")
        import traceback
        st.code(traceback.format_exc())


def _handle_cg_import(engine, cg_file, degree_sel, actor, SHOW_CG, dry_run_mode, debug_mode):
    """Handles curriculum group import."""
    try:
        df_import = pd.read_csv(cg_file, dtype=str).fillna("")
        
        with engine.begin() as conn:
            c_count, u_count, errors = import_cgs(
                conn, df_import, degree_sel, actor, SHOW_CG, engine,
                dry_run=dry_run_mode,
                debug=debug_mode
            )
        
        if dry_run_mode:
            st.success(f"🔍 DRY-RUN: Would create {c_count}, would update {u_count}")
            st.info("No changes saved (dry-run mode)")
        else:
            if c_count > 0 or u_count > 0:
                st.success(f"✅ Import complete: {c_count} created, {u_count} updated")
                
                with engine.begin() as v:
                    cnt = v.execute(sa_text("""
                        SELECT COUNT(*) FROM curriculum_groups WHERE degree_code = :dc
                    """), {"dc": degree_sel}).fetchone()[0]
                    st.info(f"✅ Verified: {cnt} curriculum groups in database for {degree_sel}")
                
                st.cache_data.clear()
                st.rerun()
            else:
                st.info("Import complete: No changes (data identical)")
        
        if errors:
            st.error(f"Errors: {len(errors)}")
            for e in errors:
                st.error(f"• {e}")
                
    except Exception as e:
        st.error(f"Import failed: {e}")
        import traceback
        st.code(traceback.format_exc())


def _handle_cgl_import(engine, cgl_file, degree_sel, actor, SHOW_CG, 
                      group_codes, program_codes, branch_codes,
                      dry_run_mode, debug_mode):
    """Handles curriculum group links import."""
    try:
        df_import = pd.read_csv(cgl_file, dtype=str).fillna("")
        
        with engine.begin() as conn:
            c_count, u_count, errors = import_cg_links(
                conn, df_import, degree_sel, actor, SHOW_CG, 
                group_codes, program_codes, branch_codes, engine,
                dry_run=dry_run_mode,
                debug=debug_mode
            )
        
        if dry_run_mode:
            st.success(f"🔍 DRY-RUN: Would create {c_count} links")
            st.info("No changes saved (dry-run mode)")
        else:
            if c_count > 0:
                st.success(f"✅ Import complete: {c_count} links created")
                
                with engine.begin() as v:
                    cnt = v.execute(sa_text("""
                        SELECT COUNT(*) FROM curriculum_group_links WHERE degree_code = :dc
                    """), {"dc": degree_sel}).fetchone()[0]
                    st.info(f"✅ Verified: {cnt} curriculum group links in database for {degree_sel}")
                
                if st.button("🔄 Refresh to see changes", key="refresh_cgl"):
                    st.cache_data.clear()
                    st.rerun()
            else:
                st.info("Import complete: No new links created (all links already exist)")
        
        if errors:
            st.error(f"Errors: {len(errors)}")
            for e in errors:
                st.error(f"• {e}")
                
    except Exception as e:
        st.error(f"Import failed: {e}")
        import traceback
        st.code(traceback.format_exc())
