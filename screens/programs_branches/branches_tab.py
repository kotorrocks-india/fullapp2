"""
Branches tab rendering for Programs/Branches module
"""
import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.exc import IntegrityError

from screens.programs_branches.db_helpers import _programs_df, _branches_df, _program_id_by_code, _table_cols
from screens.programs_branches.audit_helpers import _audit_branch, _request_deletion


def render_branches_tab(engine, degree_sel, deg, dfp, dfb_all, df_approvals,
                       actor, CAN_EDIT, mode, BR_HAS_PID, BR_HAS_DEG,
                       supports_degree_level_branches):
    """
    Renders the Branches tab with listing, create, edit, and delete functionality.
    
    Args:
        engine: Database engine
        degree_sel: Selected degree code
        deg: Degree row object
        dfp: Programs DataFrame
        dfb_all: Branches DataFrame (all for degree)
        df_approvals: Approvals DataFrame
        actor: Current user email/name
        CAN_EDIT: Boolean indicating edit permissions
        mode: Cohort splitting mode
        BR_HAS_PID: Boolean - branches table has program_id column
        BR_HAS_DEG: Boolean - branches table has degree_code column
        supports_degree_level_branches: Boolean - schema supports degree-level branches
    """
    st.subheader("Branches")
    
    with engine.begin() as conn:
        dfp2 = _programs_df(engine, degree_sel)
    
    if mode == 'both' and dfp2.empty:
        st.warning("This degree requires Program → Branch structure. Create a Program first.")
        st.markdown("---")
        st.markdown("**Existing Branches**")
        st.dataframe(pd.DataFrame(columns=['id', 'branch_code', 'branch_name']), use_container_width=True, hide_index=True)
    else:
        prog_pick_codes = dfp2["program_code"].tolist() if "program_code" in dfp2.columns else []
        filter_pc = st.selectbox(
            "Filter branches by program_code (optional)", [""] + prog_pick_codes, key="branch_filter_prog"
        )
        
        filter_pid = None
        if filter_pc:
            with engine.begin() as conn:
                filter_pid = _program_id_by_code(conn, degree_sel, filter_pc)
        
        dfb = _branches_df(engine, degree_sel, program_id=filter_pid)
        
        st.markdown("**Existing Branches**")
        st.dataframe(dfb, use_container_width=True, hide_index=True)
        
        # Approval table
        if not df_approvals.empty:
            branch_ids = dfb_all["id"].astype(str).tolist() if "id" in dfb_all.columns else []
            branch_approvals = df_approvals[
                (df_approvals["object_type"] == "branch") &
                (df_approvals["object_id"].isin(branch_ids)) &
                (df_approvals["status"].isin(["pending", "under_review"]))
            ]
            
            if not branch_approvals.empty:
                st.markdown("---")
                st.markdown("#### Branch Approval Status")
                st.dataframe(branch_approvals, use_container_width=True, hide_index=True)
        
        if not CAN_EDIT:
            st.info("You don't have permissions to create or edit Branches.")
        else:
            st.markdown("### Create Branch")
            with st.form(key="branch_create_form"):
                c1, c2 = st.columns(2)
                with c1:
                    parent_pc = ""
                    if mode == 'both' or (mode == 'program_or_branch' and not dfp2.empty) or not supports_degree_level_branches:
                        parent_pc = st.selectbox(
                            "Parent program_code",
                            options=([""] + prog_pick_codes)
                        )
                    
                    bc = st.text_input("Branch code").strip()
                    bn = st.text_input("Branch name").strip()
                    bactive = st.checkbox("Active", value=True)
                    bsort = st.number_input("Sort order", 1, 10000, 100, step=1)
                with c2:
                    blogo = st.text_input("Logo file name (optional)")
                    bdesc = st.text_area("Description", "")
                
                submitted = st.form_submit_button("Create Branch", disabled=not CAN_EDIT)
                parent_required = (mode == 'both') or (not supports_degree_level_branches)
                
                if submitted:
                    st.session_state.branch_create_error = None
                    success = False
                    
                    if parent_required and not parent_pc:
                        st.error("Select a parent program.")
                    elif not bc or not bn:
                        st.error("Branch code and name are required.")
                    else:
                        try:
                            with engine.begin() as conn:
                                pid = _program_id_by_code(conn, degree_sel, parent_pc) if parent_pc else None
                                if parent_pc and not pid:
                                    st.error("Parent program not found.")
                                    raise RuntimeError("parent program missing")
                                
                                if pid is not None:
                                    base_payload = {
                                        "bc": bc, "bn": bn, "pid": int(pid), "act": 1 if bactive else 0,
                                        "so": int(bsort), "logo": (blogo or None), "desc": (bdesc or None)
                                    }
                                    audit_payload = {
                                        "branch_code": bc, "branch_name": bn, "program_id": int(pid),
                                        "active": 1 if bactive else 0, "sort_order": int(bsort),
                                        "logo_file_name": (blogo or None), "description": (bdesc or None)
                                    }
                                    
                                    if BR_HAS_DEG:
                                        sql = """
                                            INSERT INTO branches(branch_code, branch_name, program_id, degree_code, active, sort_order, logo_file_name, description)
                                            VALUES(:bc, :bn, :pid, :deg, :act, :so, :logo, :desc)
                                        """
                                        base_payload["deg"] = degree_sel
                                        audit_payload["degree_code"] = degree_sel
                                    else:
                                        sql = """
                                            INSERT INTO branches(branch_code, branch_name, program_id, active, sort_order, logo_file_name, description)
                                            VALUES(:bc, :bn, :pid, :act, :so, :logo, :desc)
                                        """
                                    
                                    conn.execute(sa_text(sql), base_payload)
                                    _audit_branch(conn, "create", actor, audit_payload)
                                
                                elif BR_HAS_DEG:
                                    conn.execute(sa_text("""
                                        INSERT INTO branches(branch_code, branch_name, degree_code, active, sort_order, logo_file_name, description)
                                        VALUES(:bc, :bn, :deg, :act, :so, :logo, :desc)
                                    """), {
                                        "bc": bc, "bn": bn, "deg": degree_sel, "act": 1 if bactive else 0,
                                        "so": int(bsort), "logo": (blogo or None), "desc": (bdesc or None)
                                    })
                                    _audit_branch(conn, "create", actor, {
                                        "degree_code": degree_sel, "branch_code": bc, "branch_name": bn,
                                        "active": 1 if bactive else 0, "sort_order": int(bsort),
                                        "logo_file_name": (blogo or None), "description": (bdesc or None)
                                    })
                                else:
                                    raise ValueError("Schema requires branches to be attached to a Program.")
                                
                                st.success("Branch created.")
                                success = True
                        except IntegrityError:
                            st.error(f"Error: A branch with the code '{bc}' already exists.")
                        except Exception as ex:
                            st.session_state.branch_create_error = ex
                            import traceback
                            print("--- ERROR: FAILED TO CREATE BRANCH ---")
                            traceback.print_exc()
                            print("------------------------------------------")
                    
                    if success:
                        st.cache_data.clear()
                        st.rerun()
            
            # Display errors if any
            if st.session_state.branch_create_error:
                st.error("An error occurred during creation. See details below:")
                st.exception(st.session_state.branch_create_error)
                st.session_state.branch_create_error = None

            st.markdown("---")
            st.markdown("### Edit / Delete Branch")
            br_codes = dfb["branch_code"].tolist() if "branch_code" in dfb.columns else []
            sel_bc = st.selectbox("Select branch_code", [""] + br_codes, key="branch_edit_pick")
            
            if sel_bc:
                with engine.begin() as conn:
                    params = {"deg": degree_sel, "bc": sel_bc}
                    
                    if BR_HAS_PID and BR_HAS_DEG:
                        sql = """
                            SELECT b.id, b.branch_code, b.branch_name, b.active, b.sort_order, b.logo_file_name, b.description,
                                   p.program_code, p.degree_code, b.program_id
                              FROM branches b
                              LEFT JOIN programs p ON p.id=b.program_id
                             WHERE (p.degree_code=:deg OR b.degree_code=:deg) AND lower(b.branch_code)=lower(:bc)
                             LIMIT 1
                        """
                    elif BR_HAS_PID:
                        sql = """
                            SELECT b.id, b.branch_code, b.branch_name, b.active, b.sort_order, b.logo_file_name, b.description,
                                   p.program_code, p.degree_code, b.program_id
                              FROM branches b
                              LEFT JOIN programs p ON p.id=b.program_id
                             WHERE p.degree_code=:deg AND lower(b.branch_code)=lower(:bc)
                             LIMIT 1
                        """
                    elif BR_HAS_DEG:
                        sql = """
                            SELECT id, branch_code, branch_name, active, sort_order, logo_file_name, description,
                                   degree_code, NULL as program_code, NULL as program_id
                              FROM branches
                             WHERE degree_code=:deg AND lower(branch_code)=lower(:bc)
                             LIMIT 1
                        """
                    
                    brow = conn.execute(sa_text(sql), params).fetchone()
                
                if brow:
                    with st.form(key=f"branch_edit_form_{sel_bc}"):
                        e1, e2 = st.columns(2)
                        with e1:
                            editable_name = st.text_input("Branch name", brow.branch_name or "", key=f"branch_edit_name_{sel_bc}")
                            editable_active = st.checkbox("Active", value=bool(brow.active), key=f"branch_edit_active_{sel_bc}")
                            editable_so = st.number_input("Sort order", 1, 10000, int(brow.sort_order), step=1, key=f"branch_edit_sort_{sel_bc}")
                        with e2:
                            editable_logo = st.text_input("Logo file name (optional)", brow.logo_file_name or "", key=f"branch_edit_logo_{sel_bc}")
                            editable_desc = st.text_area("Description", brow.description or "", key=f"branch_edit_desc_{sel_bc}")
                        
                        save_submitted = st.form_submit_button("Save changes", disabled=not CAN_EDIT)
                        
                        if save_submitted:
                            success = False
                            try:
                                with engine.begin() as conn:
                                    conn.execute(sa_text("""
                                        UPDATE branches
                                           SET branch_name=:bn, active=:act, sort_order=:so, logo_file_name=:logo, description=:desc,
                                               updated_at=CURRENT_TIMESTAMP
                                         WHERE id=:id
                                    """), {
                                        "bn": (editable_name or None), "act": 1 if editable_active else 0, "so": int(editable_so),
                                        "logo": (editable_logo or None), "desc": (editable_desc or None), "id": int(brow.id)
                                    })
                                    
                                    audit_row = {
                                        "program_id": brow.program_id, "degree_code": brow.degree_code,
                                        "branch_code": brow.branch_code, "branch_name": editable_name,
                                        "active": 1 if editable_active else 0, "sort_order": int(editable_so),
                                        "logo_file_name": (editable_logo or None), "description": (editable_desc or None)
                                    }
                                    _audit_branch(conn, "edit", actor, audit_row)
                                    
                                    st.success("Saved.")
                                    success = True
                            except Exception as ex:
                                st.error(str(ex))
                            
                            if success:
                                st.cache_data.clear()
                                st.rerun()
                    
                    # Delete button
                    if st.button("Request Delete", disabled=not CAN_EDIT, key=f"branch_delete_req_{sel_bc}"):
                        try:
                            with engine.begin() as conn: 
                                success, error = _request_deletion(
                                    conn,
                                    object_type="branch",
                                    object_id=brow.id,
                                    actor=actor,
                                    audit_function=_audit_branch,
                                    audit_row=dict(brow._mapping),
                                    reason_note="Branch delete (requires approval)"
                                )
                                
                                if not success:
                                    raise error
                            
                            st.success("Delete request submitted.")
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as ex:
                            st.error(str(ex))
