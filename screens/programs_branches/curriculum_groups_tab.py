"""
Curriculum Groups tab rendering for Programs/Branches module
"""
import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.exc import IntegrityError

from screens.programs_branches.audit_helpers import (
    _audit_curriculum_group, 
    _audit_curriculum_group_link,
    _request_deletion
)


def render_curriculum_groups_tab(engine, degree_sel, deg, df_cg, df_cgl, 
                                 df_approvals, actor, CAN_EDIT):
    """
    Renders the Curriculum Groups tab with groups, links, and management.
    """
    st.subheader("Curriculum Groups")
    st.markdown("**Existing Groups**")
    st.dataframe(df_cg, use_container_width=True, hide_index=True)
    
    # Approval status for curriculum groups
    if not df_approvals.empty:
        group_ids = df_cg["id"].astype(str).tolist() if "id" in df_cg.columns else []
        cg_approvals = df_approvals[
            (df_approvals["object_type"] == "curriculum_group") &
            (df_approvals["object_id"].isin(group_ids)) &
            (df_approvals["status"].isin(["pending", "under_review"]))
        ]
        
        if not cg_approvals.empty:
            st.markdown("#### Group Approval Status")
            st.dataframe(cg_approvals, use_container_width=True, hide_index=True)
    
    st.markdown("**Existing Links**")
    st.dataframe(df_cgl, use_container_width=True, hide_index=True)
    
    # Delete Curriculum Group Link
    if CAN_EDIT and not df_cgl.empty:
        st.markdown("### Delete Link")
        link_options_map = {}
        for _, row in df_cgl.iterrows():
            if row['program_code'] and row['branch_code']:
                label = f"Link ID {row['id']} (Complex Link)"
            elif row['program_code']:
                label = f"Group '{row['group_code']}' → Program '{row['program_code']}' (ID: {row['id']})"
            elif row['branch_code']:
                label = f"Group '{row['group_code']}' → Branch '{row['branch_code']}' (ID: {row['id']})"
            else:
                label = f"Group '{row['group_code']}' → Degree '{degree_sel}' (ID: {row['id']})"
            link_options_map[label] = row['id']
        
        link_to_delete_label = st.selectbox(
            "Select a link to delete",
            options=[""] + list(link_options_map.keys()),
            key="cg_link_delete_pick"
        )
        
        if st.button("Delete Selected Link", disabled=(not link_to_delete_label or not CAN_EDIT)):
            try:
                link_id_to_delete = link_options_map[link_to_delete_label]
                link_row_details = df_cgl[df_cgl['id'] == link_id_to_delete].to_dict('records')[0]
                with engine.begin() as conn:
                    conn.execute(sa_text("DELETE FROM curriculum_group_links WHERE id = :id"), {"id": link_id_to_delete})
                    _audit_curriculum_group_link(conn, "delete", actor, link_row_details, note="Link deleted")
                st.success(f"Successfully deleted link: {link_to_delete_label}")
                st.cache_data.clear()
                st.rerun()
            except Exception as ex:
                st.error(f"Could not delete link: {ex}")
    
    if not CAN_EDIT:
        st.info("You don't have permissions to create or edit Curriculum Groups.")
    else:
        st.markdown("---")
        st.markdown("### Create Curriculum Group")
        with st.form(key="cg_create_form"):
            c1, c2 = st.columns(2)
            with c1:
                gc = st.text_input("Group code").strip()
                gn = st.text_input("Group name").strip()
                gkind = st.selectbox("Group Kind", ["pseudo", "cohort"])
            with c2:
                gactive = st.checkbox("Active", value=True)
                gsort = st.number_input("Sort order", 1, 10000, 100, step=1)
                gdesc = st.text_area("Description", "")
            
            submitted = st.form_submit_button("Create Group", disabled=not CAN_EDIT)
            
            if submitted:
                st.session_state.cg_create_error = None
                success = False
                
                if not gc or not gn:
                    st.error("Group code and name are required.")
                else:
                    try:
                        with engine.begin() as conn:
                            conn.execute(sa_text("""
                                INSERT INTO curriculum_groups(degree_code, group_code, group_name, kind, active, sort_order, description)
                                VALUES(:deg, :gc, :gn, :kind, :act, :so, :desc)
                            """), {
                                "deg": degree_sel, "gc": gc, "gn": gn, "kind": gkind,
                                "act": 1 if gactive else 0, "so": int(gsort), "desc": (gdesc or None)
                            })
                            
                            _audit_curriculum_group(conn, "create", actor, {
                                "degree_code": degree_sel, "group_code": gc, "group_name": gn, "kind": gkind,
                                "active": 1 if gactive else 0, "sort_order": int(gsort), "description": (gdesc or None)
                            })
                            
                            st.success("Curriculum Group created.")
                            success = True
                    except IntegrityError:
                        st.error(f"Error: A group with the code '{gc}' already exists for this degree.")
                    except Exception as ex:
                        st.session_state.cg_create_error = ex
                        import traceback
                        print("--- ERROR: FAILED TO CREATE CURRICULUM GROUP ---")
                        traceback.print_exc()
                        print("------------------------------------------")
                
                if success:
                    st.cache_data.clear()
                    st.rerun()
        
        # Display errors if any
        if st.session_state.cg_create_error:
            st.error("An error occurred during creation. See details below:")
            st.exception(st.session_state.cg_create_error)
            st.session_state.cg_create_error = None
        
        st.markdown("---")
        st.markdown("### Edit / Delete Group")
        group_codes = df_cg["group_code"].tolist() if "group_code" in df_cg.columns else []
        sel_gc = st.selectbox("Select group_code", [""] + group_codes, key="cg_edit_pick")
        
        if sel_gc:
            with engine.begin() as conn:
                grow = conn.execute(sa_text("""
                    SELECT id, group_code, group_name, kind, active, sort_order, description
                      FROM curriculum_groups
                     WHERE degree_code=:d AND lower(group_code)=lower(:gc)
                     LIMIT 1
                """), {"d": degree_sel, "gc": sel_gc}).fetchone()
            
            if grow:
                with st.form(key=f"cg_edit_form_{sel_gc}"):
                    e1, e2 = st.columns(2)
                    with e1:
                        editable_name = st.text_input("Group name", grow.group_name or "", key=f"cg_edit_name_{sel_gc}")
                        editable_kind = st.selectbox("Group Kind", ["pseudo", "cohort"], index=["pseudo", "cohort"].index(grow.kind), key=f"cg_edit_kind_{sel_gc}")
                    with e2:
                        editable_active = st.checkbox("Active", value=bool(grow.active), key=f"cg_edit_active_{sel_gc}")
                        editable_so = st.number_input("Sort order", 1, 10000, int(grow.sort_order), step=1, key=f"cg_edit_sort_{sel_gc}")
                        editable_desc = st.text_area("Description", grow.description or "", key=f"cg_edit_desc_{sel_gc}")
                    
                    save_submitted = st.form_submit_button("Save changes", disabled=not CAN_EDIT)
                    
                    if save_submitted:
                        success = False
                        try:
                            with engine.begin() as conn:
                                conn.execute(sa_text("""
                                    UPDATE curriculum_groups
                                       SET group_name=:gn, kind=:kind, active=:act, sort_order=:so, description=:desc,
                                           updated_at=CURRENT_TIMESTAMP
                                     WHERE id=:id
                                """), {
                                    "gn": (editable_name or None),
                                    "kind": editable_kind,
                                    "act": 1 if editable_active else 0,
                                    "so": int(editable_so),
                                    "desc": (editable_desc or None),
                                    "id": int(grow.id)
                                })
                                
                                _audit_curriculum_group(conn, "edit", actor, {
                                    "degree_code": degree_sel, "group_code": grow.group_code,
                                    "group_name": (editable_name or None), "kind": editable_kind,
                                    "active": 1 if editable_active else 0, "sort_order": int(editable_so),
                                    "description": (editable_desc or None)
                                })
                                
                                st.success("Saved.")
                                success = True
                        except Exception as ex:
                            st.error(str(ex))
                        
                        if success:
                            st.cache_data.clear()
                            st.rerun()
                
                # Delete button
                if st.button("Request Delete Group", disabled=not CAN_EDIT, key=f"cg_delete_req_{sel_gc}"):
                    try:
                        with engine.begin() as conn: 
                            success, error = _request_deletion(
                                conn,
                                object_type="curriculum_group",
                                object_id=grow.id,
                                actor=actor,
                                audit_function=_audit_curriculum_group,
                                audit_row=dict(grow._mapping),
                                reason_note="Curriculum Group delete (requires approval)"
                            )
                            
                            if not success:
                                raise error
                        
                        st.success("Delete request submitted.")
                        st.cache_data.clear()
                        st.rerun()
                        
                    except Exception as ex:
                        st.error(str(ex))
        
        st.markdown("---")
        
        # Link creation logic
        _render_link_creation_section(engine, degree_sel, deg, df_cg, df_cgl, actor, CAN_EDIT)


def _keep_linking_open():
    """Callback to keep the linking expander open."""
    st.session_state["pb_linking_open"] = True


def _render_link_creation_section(engine, degree_sel, deg, df_cg, df_cgl, actor, CAN_EDIT):
    """Renders the link creation section for curriculum groups."""
    from .db_helpers import _programs_df, _branches_df
    
    can_link_degree = bool(deg.cg_degree)
    can_link_program = bool(deg.cg_program)
    can_link_branch = bool(deg.cg_branch)
    
    # Load programs and branches for linking
    dfp = _programs_df(engine, degree_sel)
    dfb_all = _branches_df(engine, degree_sel, program_id=None)
    
    can_link_program = can_link_program and not dfp.empty
    can_link_branch = can_link_branch and not dfb_all.empty
    
    if can_link_degree or can_link_program or can_link_branch:
        
        # --- FIX START: Manage Expander State ---
        if "pb_linking_open" not in st.session_state:
            st.session_state["pb_linking_open"] = True # Default to open since it's at bottom
            
        with st.expander("🔗 Link Group to Degree/Program/Branch", expanded=st.session_state["pb_linking_open"]):
        # --- FIX END ---

            # Get all defined groups
            all_groups = df_cg["group_code"].tolist() if not df_cg.empty else []
            
            # Get all possible targets
            all_targets = []
            if can_link_degree:
                all_targets.append({"type": "Degree", "code": degree_sel, "pc": None, "bc": None})
            if can_link_program:
                prog_codes = dfp["program_code"].tolist() if not dfp.empty else []
                all_targets.extend([{"type": "Program", "code": pc, "pc": pc, "bc": None} for pc in prog_codes])
            if can_link_branch:
                branch_codes = dfb_all["branch_code"].tolist() if not dfb_all.empty else []
                all_targets.extend([{"type": "Branch", "code": bc, "pc": None, "bc": bc} for bc in branch_codes])
            
            # Get all existing links as a set of tuples for easy lookup
            existing_links_set = set()
            if not df_cgl.empty:
                for _, row in df_cgl.iterrows():
                    gc = row['group_code']
                    pc = row['program_code']
                    bc = row['branch_code']
                    pc_key = pc if pd.notna(pc) else None
                    bc_key = bc if pd.notna(bc) else None
                    existing_links_set.add((gc, pc_key, bc_key))
            
            # Create the list of *available new links*
            available_links_map = {}
            if all_groups and all_targets:
                for group_code in all_groups:
                    for target in all_targets:
                        target_pc = target["pc"]
                        target_bc = target["bc"]
                        link_tuple = (group_code, target_pc, target_bc)
                        
                        if link_tuple not in existing_links_set:
                            label = f"Group '{group_code}' → {target['type']} '{target['code']}'"
                            payload = {
                                "group_code": group_code,
                                "program_code": target_pc,
                                "branch_code": target_bc,
                                "label": label
                            }
                            available_links_map[label] = payload
            
            # Show the form OR the "all done" message
            if not all_groups:
                st.info("Linking is available, but no Curriculum Groups exist yet. Please create one first.")
            elif not all_targets:
                st.info("Linking is enabled, but no Degrees, Programs, or Branches are available to link to.")
            elif not available_links_map:
                st.success("✅ All possible group links have been created.")
            else:
                with st.form(key="cg_link_form_new"):
                    sel_link_label = st.selectbox(
                        "Select new link to create",
                        options=[""] + list(available_links_map.keys())
                    )
                    
                    # Add callback to keep expander open
                    link_submitted = st.form_submit_button("Link Group", disabled=(not CAN_EDIT), on_click=_keep_linking_open)
                    
                    if link_submitted:
                        success = False
                        if not sel_link_label:
                            st.error("You must select a link to create.")
                        else:
                            try:
                                link_payload_data = available_links_map[sel_link_label]
                                sel_group = link_payload_data["group_code"]
                                prog_code_to_link = link_payload_data["program_code"]
                                branch_code_to_link = link_payload_data["branch_code"]
                                
                                with engine.begin() as conn:
                                    group_id_row = conn.execute(sa_text(
                                        "SELECT id FROM curriculum_groups WHERE degree_code=:d AND group_code=:gc"
                                    ), {"d": degree_sel, "gc": sel_group}).fetchone()
                                    
                                    if not group_id_row:
                                        st.error(f"Selected group '{sel_group}' not found.")
                                        raise RuntimeError("Group missing")
                                    
                                    link_insert_payload = {
                                        "gid": group_id_row.id,
                                        "deg": degree_sel,
                                        "pc": prog_code_to_link,
                                        "bc": branch_code_to_link
                                    }
                                    
                                    conn.execute(sa_text("""
                                        INSERT INTO curriculum_group_links(group_id, degree_code, program_code, branch_code)
                                        VALUES(:gid, :deg, :pc, :bc)
                                    """), link_insert_payload)
                                    
                                    audit_link_payload = {
                                        "group_id": group_id_row.id,
                                        "degree_code": degree_sel,
                                        "program_code": prog_code_to_link,
                                        "branch_code": branch_code_to_link
                                    }
                                    
                                    _audit_curriculum_group_link(conn, "create", actor, audit_link_payload, note="Link created")
                                    
                                    st.success(f"Successfully created link: {sel_link_label}")
                                    success = True
                            except Exception as ex:
                                st.error(f"Failed to create link. Details: {ex}")
                        
                        if success:
                            st.cache_data.clear()
                            st.rerun()
    else:
        st.info("Linking is not available. Enable curriculum groups at the Degree, Program, or Branch level on the Degrees page.")
