"""
Programs tab rendering for Programs/Branches module
"""
import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.exc import IntegrityError

from screens.programs_branches.db_helpers import _program_id_by_code
from screens.programs_branches.audit_helpers import _audit_program, _request_deletion


def render_programs_tab(engine, degree_sel, deg, dfp, df_approvals, actor, CAN_EDIT, mode):
    """
    Renders the Programs tab with listing, create, edit, and delete functionality.
    
    Args:
        engine: Database engine
        degree_sel: Selected degree code
        deg: Degree row object
        dfp: Programs DataFrame
        df_approvals: Approvals DataFrame
        actor: Current user email/name
        CAN_EDIT: Boolean indicating edit permissions
        mode: Cohort splitting mode
    """
    st.subheader("Programs (per degree)")
    st.markdown("**Existing Programs**")
    st.dataframe(dfp, use_container_width=True, hide_index=True)
    
    # Approval table
    if not df_approvals.empty:
        program_ids = dfp["id"].astype(str).tolist() if "id" in dfp.columns else []
        prog_approvals = df_approvals[
            (df_approvals["object_type"] == "program") &
            (df_approvals["object_id"].isin(program_ids)) &
            (df_approvals["status"].isin(["pending", "under_review"]))
        ]
        
        if not prog_approvals.empty:
            st.markdown("---")
            st.markdown("#### Program Approval Status")
            st.dataframe(prog_approvals, use_container_width=True, hide_index=True)
    
    if not CAN_EDIT:
        st.info("You don't have permissions to create or edit Programs.")
    else:
        st.markdown("### Create Program")
        with st.form(key="prog_create_form"):
            c1, c2 = st.columns(2)
            with c1:
                pc = st.text_input("Program code").strip()
                pn = st.text_input("Program name").strip()
                pactive = st.checkbox("Active", value=True)
                psort = st.number_input("Sort order", 1, 10000, 100, step=1)
            with c2:
                plogo = st.text_input("Logo file name (optional)")
                pdesc = st.text_area("Description", "")
            
            submitted = st.form_submit_button("Create Program", disabled=not CAN_EDIT)
            
            if submitted:
                st.session_state.prog_create_error = None
                success = False
                
                if not pc or not pn:
                    st.error("Program code and name are required.")
                else:
                    try:
                        with engine.begin() as conn:
                            conn.execute(sa_text("""
                                INSERT INTO programs(program_code, program_name, degree_code, active, sort_order, logo_file_name, description)
                                VALUES(:pc, :pn, :deg, :act, :so, :logo, :desc)
                            """), {
                                "pc": pc, "pn": pn, "deg": degree_sel,
                                "act": 1 if pactive else 0, "so": int(psort),
                                "logo": (plogo or None), "desc": (pdesc or None)
                            })
                            
                            _audit_program(conn, "create", actor, {
                                "degree_code": degree_sel, "program_code": pc, "program_name": pn,
                                "active": 1 if pactive else 0, "sort_order": int(psort),
                                "logo_file_name": (plogo or None), "description": (pdesc or None)
                            })
                            
                            st.success("Program created.")
                            success = True
                    except IntegrityError:
                        st.error(f"Error: A program with the code '{pc}' already exists.")
                    except Exception as ex:
                        st.session_state.prog_create_error = ex
                        import traceback
                        print("--- ERROR: FAILED TO CREATE PROGRAM ---")
                        traceback.print_exc()
                        print("------------------------------------------")
                
                if success:
                    st.cache_data.clear()
                    st.rerun()

        # Display errors if any
        if st.session_state.prog_create_error:
            st.error("An error occurred during creation. See details below:")
            st.exception(st.session_state.prog_create_error)
            st.session_state.prog_create_error = None
        
        st.markdown("---")
        st.markdown("### Edit / Delete Program")
        prog_codes = dfp["program_code"].tolist() if "program_code" in dfp.columns else []
        sel_pc = st.selectbox("Select program_code", [""] + prog_codes, key="prog_edit_pick")
        
        if sel_pc:
            with engine.begin() as conn:
                prow = conn.execute(sa_text("""
                    SELECT id, program_code, program_name, degree_code, active, sort_order, logo_file_name, description
                      FROM programs
                     WHERE degree_code=:d AND lower(program_code)=lower(:pc)
                     LIMIT 1
                """), {"d": degree_sel, "pc": sel_pc}).fetchone()
            
            if prow:
                with st.form(key=f"prog_edit_form_{sel_pc}"):
                    e1, e2 = st.columns(2)
                    with e1:
                        editable_name = st.text_input("Program name", prow.program_name or "", key=f"prog_edit_name_{sel_pc}")
                        editable_active = st.checkbox("Active", value=bool(prow.active), key=f"prog_edit_active_{sel_pc}")
                        editable_so = st.number_input("Sort order", 1, 10000, int(prow.sort_order), step=1, key=f"prog_edit_sort_{sel_pc}")
                    with e2:
                        editable_logo = st.text_input("Logo file name (optional)", prow.logo_file_name or "", key=f"prog_edit_logo_{sel_pc}")
                        editable_desc = st.text_area("Description", prow.description or "", key=f"prog_edit_desc_{sel_pc}")
                    
                    save_submitted = st.form_submit_button("Save changes", disabled=not CAN_EDIT)
                    
                    if save_submitted:
                        success = False
                        try:
                            with engine.begin() as conn:
                                conn.execute(sa_text("""
                                    UPDATE programs
                                       SET program_name=:pn, active=:act, sort_order=:so, logo_file_name=:logo, description=:desc,
                                           updated_at=CURRENT_TIMESTAMP
                                     WHERE id=:id
                                """), {
                                    "pn": (editable_name or None),
                                    "act": 1 if editable_active else 0,
                                    "so": int(editable_so),
                                    "logo": (editable_logo or None),
                                    "desc": (editable_desc or None),
                                    "id": int(prow.id)
                                })
                                
                                _audit_program(conn, "edit", actor, {
                                    "degree_code": degree_sel, "program_code": prow.program_code,
                                    "program_name": (editable_name or None), "active": 1 if editable_active else 0,
                                    "sort_order": int(editable_so), "logo_file_name": (editable_logo or None),
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
                if st.button("Request Delete", disabled=not CAN_EDIT, key=f"prog_delete_req_{sel_pc}"):
                    try:
                        with engine.begin() as conn: 
                            success, error = _request_deletion(
                                conn,
                                object_type="program",
                                object_id=prow.id,
                                actor=actor,
                                audit_function=_audit_program,
                                audit_row=dict(prow._mapping),
                                reason_note="Program delete (requires approval)"
                            )
                            
                            if not success:
                                raise error

                        st.success("Delete request submitted.")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))
