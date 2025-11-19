# screens/office_admin/ui.py
from __future__ import annotations
import io
import csv
import secrets
import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text  # <<< ADDED THIS IMPORT

from screens.office_admin import db as odb
from screens.office_admin.utils import (
    is_valid_email,
    generate_initial_password,
    hash_password,
    validate_username,
)
from core.approval_handler_enhanced import ApprovalHandler


def _get_engine():
    return st.session_state.get("engine")


def _current_user():
    return st.session_state.get("user", {}).get("email", "")


def _load_degrees(conn):
    """Load available degrees."""
    try:
        # from sqlalchemy import text as sa_text (already imported at top)
        # degrees table: code, title, ...
        rows = conn.execute(
            sa_text(
                "SELECT code, title FROM degrees "
                "WHERE active=1 ORDER BY sort_order, title"
            )
        ).fetchall()
        return [{"code": r[0], "name": r[1]} for r in rows]
    except Exception:
        return []


def _load_programs(conn, degree_code: str | None = None):
    """Load programs, optionally filtered by degree."""
    try:
        # from sqlalchemy import text as sa_text (already imported at top)
        if degree_code:
            sql = sa_text(
                "SELECT id, program_name, degree_code FROM programs "
                "WHERE degree_code=:deg AND active=1 "
                "ORDER BY sort_order, program_name"
            )
            rows = conn.execute(sql, {"deg": degree_code}).fetchall()
        else:
            rows = conn.execute(
                sa_text(
                    "SELECT id, program_name, degree_code FROM programs "
                    "WHERE active=1 ORDER BY sort_order, program_name"
                )
            ).fetchall()
        return [{"id": r[0], "name": r[1], "degree_code": r[2]} for r in rows]
    except Exception:
        return []


def _load_branches(conn, program_id: int | None = None):
    """Load branches, optionally filtered by program."""
    try:
        # from sqlalchemy import text as sa_text (already imported at top)
        if program_id:
            sql = sa_text(
                "SELECT id, branch_name, program_id FROM branches "
                "WHERE program_id=:pid AND active=1 "
                "ORDER BY sort_order, branch_name"
            )
            rows = conn.execute(sql, {"pid": program_id}).fetchall()
        else:
            rows = conn.execute(
                sa_text(
                    "SELECT id, branch_name, program_id FROM branches "
                    "WHERE active=1 ORDER BY sort_order, branch_name"
                )
            ).fetchall()
        return [{"id": r[0], "name": r[1], "program_id": r[2]} for r in rows]
    except Exception:
        return []


def _suggest_username(email: str, full_name: str) -> str:
    """
    Suggest a username aligned with your faculty policy (enforced by validate_username):
    - lowercase
    - 6–30 chars
    - only [a-z0-9._-]
    - starts with a letter
    """
    local = (email or "").split("@")[0].lower()
    if not local and full_name:
        parts = full_name.strip().lower().split()
        if parts:
            given = parts[0]
            family = parts[-1] if len(parts) > 1 else parts[0]
            local = given + family[0]

    if not local:
        local = "officeuser"

    allowed = "abcdefghijklmnopqrstuvwxyz0123456789._-"
    local = "".join(c for c in local if c in allowed) or "officeuser"

    # must start with a letter
    if not local[0].isalpha():
        local = "o" + local

    # length 6–30
    if len(local) < 6:
        local = (local + "officeadmin")[:6]
    if len(local) > 30:
        local = local[:30]

    ok, _ = validate_username(local)
    if ok:
        return local

    # safe fallback
    return "officeuser01"


# ---------- ACCOUNT MANAGEMENT ----------
def render_accounts():
    st.subheader("Office Admin Accounts")
    eng = _get_engine()
    if not eng:
        st.info("No engine configured.")
        return

    status_filter = st.selectbox("Filter by Status", ["(all)", "active", "disabled"])

    with eng.begin() as conn:
        rows = odb.list_office_admins(
            conn, None if status_filter == "(all)" else status_filter
        )

    if rows:
        df = pd.DataFrame(rows)
        display_cols = [
            "full_name",
            "designation",
            "email",
            "username",
            "employee_id", # <<< ADDED THIS
            "status",
            "scopes",
            "last_login",
            "created_at",
        ]
        available_cols = [c for c in display_cols if c in df.columns]
        st.dataframe(df[available_cols], use_container_width=True)
    else:
        st.info("No office admin accounts found.")

    # ----- create new admin -----
    with st.expander("➕ Create New Office Admin"):
        st.markdown(
            "**Policy:** Only superadmin and tech_admin can create office admin accounts."
        )
        st.caption(
            "Usernames follow the same validation rules as faculty (length, allowed characters, etc.)."
        )

        # Primary scope widgets OUTSIDE the form so they re-render immediately
        st.markdown(
            "**Primary scope** (you can add more scopes later in the *Scope Assignments* tab)"
        )
        primary_scope_type = st.selectbox(
            "Primary Scope Type",
            ["global", "degree", "program", "branch"],
            index=0,
            key="oa_primary_scope_type",
        )

        primary_scope_value = None
        primary_degree_code = None
        primary_program_id = None
        primary_branch_id = None

        if primary_scope_type == "degree":
            with eng.begin() as conn:
                degrees = _load_degrees(conn)
            if degrees:
                selected_degree = st.selectbox(
                    "Select Degree",
                    [d["code"] for d in degrees],
                    format_func=lambda c: next(
                        (d["name"] for d in degrees if d["code"] == c), c
                    ),
                    key="oa_primary_degree",
                )
                primary_degree_code = selected_degree
                primary_scope_value = selected_degree
            else:
                st.warning("No degrees available; scope will not be assigned.")

        elif primary_scope_type == "program":
            with eng.begin() as conn:
                degrees = _load_degrees(conn)
                if degrees:
                    degree_filter = st.selectbox(
                        "Filter Programs by Degree",
                        ["(all)"] + [d["code"] for d in degrees],
                        format_func=lambda c: (
                            next((d["name"] for d in degrees if d["code"] == c), c)
                            if c != "(all)"
                            else c
                        ),
                        key="oa_primary_program_degree_filter",
                    )
                    programs = _load_programs(
                        conn, None if degree_filter == "(all)" else degree_filter
                    )
                else:
                    programs = []

            if programs:
                selected_program = st.selectbox(
                    "Select Program",
                    [p["id"] for p in programs],
                    format_func=lambda pid: next(
                        (p["name"] for p in programs if p["id"] == pid), str(pid)
                    ),
                    key="oa_primary_program",
                )
                primary_program_id = selected_program
                primary_scope_value = str(selected_program)
                primary_degree_code = next(
                    (p["degree_code"] for p in programs if p["id"] == selected_program),
                    None,
                )
            else:
                if primary_scope_type == "program":
                    st.warning("No programs available; scope will not be assigned.")

        elif primary_scope_type == "branch":
            with eng.begin() as conn:
                programs = _load_programs(conn)
            if programs:
                program_filter = st.selectbox(
                    "Filter Branches by Program",
                    [p["id"] for p in programs],
                    format_func=lambda pid: next(
                        (p["name"] for p in programs if p["id"] == pid), str(pid)
                    ),
                    key="oa_primary_branch_program_filter",
                )
                with eng.begin() as conn:
                    branches = _load_branches(conn, program_filter)
            else:
                branches = []

            if programs and branches:
                selected_branch = st.selectbox(
                    "Select Branch",
                    [b["id"] for b in branches],
                    format_func=lambda bid: next(
                        (b["name"] for b in branches if b["id"] == bid), str(bid)
                    ),
                    key="oa_primary_branch",
                )
                primary_branch_id = selected_branch
                primary_scope_value = str(selected_branch)
                primary_program_id = next(
                    (b["program_id"] for b in branches if b["id"] == selected_branch),
                    None,
                )
                primary_degree_code = next(
                    (p["degree_code"] for p in programs if p["id"] == primary_program_id),
                    None,
                )
            else:
                if primary_scope_type == "branch":
                    st.warning("No branches available; scope will not be assigned.")

        # Form only for user details + submit
        with st.form(key="create_admin_form"):
            full_name = st.text_input("Full Name*")
            email = st.text_input("Email*")
            # VVV ADDED THIS INPUT VVV
            employee_id = st.text_input("Employee ID (optional)")
            designation = st.text_input(
                "Designation (optional, e.g., 'Office Assistant', 'Sr. Clerk')"
            )
            username = st.text_input(
                "Username (optional – will be suggested if left blank)"
            )

            submitted = st.form_submit_button("Create Account")

        if submitted:
            if not full_name or not is_valid_email(email):
                st.error("Full name and valid email are required.")
                return

            # Username generation + validation
            if not username:
                username = _suggest_username(email, full_name)

            is_valid_user, user_err = validate_username(username)
            if not is_valid_user:
                st.error(f"Username invalid: {user_err}")
                return

            # VVV ADDED THIS VALIDATION BLOCK VVV
            if employee_id:
                try:
                    with eng.begin() as conn:
                        # Check office admins
                        oa_taken = conn.execute(sa_text("SELECT 1 FROM office_admin_accounts WHERE employee_id = :eid"), {"eid": employee_id}).scalar()
                        # Check faculty profiles
                        fp_taken = conn.execute(sa_text("SELECT 1 FROM faculty_profiles WHERE employee_id = :eid"), {"eid": employee_id}).scalar()
                    
                    if oa_taken:
                        st.error(f"Employee ID '{employee_id}' is already in use by another office admin.")
                        return
                    if fp_taken:
                         st.error(f"Employee ID '{employee_id}' is already in use by a faculty member.")
                         return
                         
                except Exception as e:
                    # Handle case where faculty_profiles table might not exist
                    if "no such table" in str(e):
                         with eng.begin() as conn:
                            oa_taken = conn.execute(sa_text("SELECT 1 FROM office_admin_accounts WHERE employee_id = :eid"), {"eid": employee_id}).scalar()
                         if oa_taken:
                            st.error(f"Employee ID '{employee_id}' is already in use by another office admin (faculty table not found).")
                            return
                    else:
                        st.error(f"Error validating Employee ID: {e}")
                        return
            # ^^^ END OF VALIDATION BLOCK ^^^

            # If a non-global scope is chosen, ensure a concrete selection was made
            if primary_scope_type != "global" and primary_scope_value is None:
                st.error(
                    f"Please select a concrete {primary_scope_type} "
                    "for the primary scope (or choose Global)."
                )
                return

            temp_password = generate_initial_password(full_name)
            password_hash = hash_password(temp_password)

            try:
                with eng.begin() as conn:
                    # 1) create the account
                    admin_id = odb.create_office_admin(
                        conn,
                        {
                            "email": email,
                            "username": username,
                            "full_name": full_name,
                            "designation": designation,
                            "password_hash": password_hash,
                            "employee_id": employee_id or None, # <<< ADDED THIS
                        },
                        created_by=_current_user(),
                    )

                    # 2) create a primary scope row (including global)
                    scope_id = odb.assign_scope(
                        conn,
                        {
                            "admin_email": email,
                            "scope_type": primary_scope_type,
                            "scope_value": primary_scope_value,
                            "degree_code": primary_degree_code,
                            "program_id": primary_program_id,
                            "branch_id": primary_branch_id,
                            "notes": "Primary scope at account creation",
                        },
                        created_by=_current_user(),
                    )

                    # 3) audit log
                    odb.log_audit(
                        conn,
                        {
                            "actor_email": _current_user(),
                            "actor_role": "superadmin",
                            "action": "create_office_admin",
                            "target_type": "office_admin",
                            "target_id": admin_id,
                            "reason": "Created new office admin account",
                        },
                    )
                    odb.log_audit(
                        conn,
                        {
                            "actor_email": _current_user(),
                            "action": "assign_scope",
                            "target_type": "office_admin_scope",
                            "target_id": scope_id,
                            "scope_type": primary_scope_type,
                            "scope_value": primary_scope_value,
                        },
                    )

                # Cache credentials in session for one-time export
                creds = st.session_state.get("office_admin_new_credentials", [])
                creds.append(
                    {
                        "full_name": full_name,
                        "email": email,
                        "username": username,
                        "temporary_password": temp_password,
                    }
                )
                st.session_state["office_admin_new_credentials"] = creds

                st.success(f"✅ Account created for {full_name}")
                st.info(f"**Username:** `{username}`")
                st.info(
                    f"**Temporary Password:** `{temp_password}` "
                    "(show this once, user must change on first login)"
                )
                st.info(
                    "Primary scope assigned; you can add more scopes from the *Scope Assignments* tab."
                )
                st.rerun()
            except Exception as e:
                st.error(f"Failed to create account: {e}")

    with st.expander("🔒 Disable Account"):
        with st.form(key="disable_form"):
            email_to_disable = st.text_input("Email to Disable")
            reason = st.text_area("Reason for Disabling*")
            submitted = st.form_submit_button("Disable Account")

        if submitted:
            if not email_to_disable or not reason:
                st.error("Email and reason are required.")
            else:
                with eng.begin() as conn:
                    try:
                        odb.disable_office_admin(
                            conn, email_to_disable, reason, _current_user()
                        )
                        odb.log_audit(
                            conn,
                            {
                                "actor_email": _current_user(),
                                "action": "disable_office_admin",
                                "target_type": "office_admin",
                                "reason": reason,
                            },
                        )
                        st.success(f"Account {email_to_disable} has been disabled.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed: {e}")

    with st.expander("✅ Enable Account"):
        with st.form(key="enable_form"):
            email_to_enable = st.text_input("Email to Enable")
            submitted = st.form_submit_button("Enable Account")

        if submitted:
            if not email_to_enable:
                st.error("Email is required.")
            else:
                with eng.begin() as conn:
                    try:
                        odb.enable_office_admin(conn, email_to_enable)
                        odb.log_audit(
                            conn,
                            {
                                "actor_email": _current_user(),
                                "action": "enable_office_admin",
                                "target_type": "office_admin",
                            },
                        )
                        st.success(f"Account {email_to_enable} has been enabled.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed: {e}")

    # ----- one-time credentials export for this session -----
    creds = st.session_state.get("office_admin_new_credentials", [])
    if creds:
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow(["full_name", "email", "username", "temporary_password"])
        for c in creds:
            writer.writerow(
                [
                    c.get("full_name", ""),
                    c.get("email", ""),
                    c.get("username", ""),
                    c.get("temporary_password", ""),
                ]
            )
        st.download_button(
            "⬇️ Download new office admin credentials (this session only)",
            data=csv_buffer.getvalue(),
            file_name="office_admin_initial_credentials.csv",
            mime="text/csv",
        )


# ---------- SCOPE MANAGEMENT ----------
def render_scopes():
    st.subheader("Scope Assignments")
    st.markdown(
        """
    **Organizational Scoping:**
    - **Global**: Access to all degrees, programs, and branches
    - **Degree**: Access to all programs/branches within a specific degree
    - **Program**: Access to all branches within a specific program  
    - **Branch**: Access to only that specific branch
    """
    )

    eng = _get_engine()
    if not eng:
        st.info("No engine configured.")
        return

    # Select admin to manage
    with eng.begin() as conn:
        admins = odb.list_office_admins(conn, "active")

    if not admins:
        st.warning("No active office admins found. Create an account first.")
        return

    admin_email = st.selectbox(
        "Select Office Admin",
        options=[a["email"] for a in admins],
        format_func=lambda e: f"{next((a['full_name'] for a in admins if a['email'] == e), e)} ({e})",
    )

    # Show current scopes
    with eng.begin() as conn:
        current_scopes = odb.get_admin_scopes(conn, admin_email)

    if current_scopes:
        st.write("**Current Scopes:**")
        scope_df = pd.DataFrame(current_scopes)
        display_cols = [
            "scope_type",
            "scope_value",
            "degree_code",
            "program_id",
            "branch_id",
            "created_at",
            "notes",
        ]
        available_cols = [c for c in display_cols if c in scope_df.columns]
        st.dataframe(scope_df[available_cols], use_container_width=True)

        # Revoke scope
        with st.expander("❌ Revoke Scope"):
            scope_to_revoke = st.selectbox(
                "Select Scope to Revoke",
                [s["id"] for s in current_scopes],
                format_func=lambda sid: next(
                    (
                        f"{s['scope_type']}: {s['scope_value']}"
                        for s in current_scopes
                        if s["id"] == sid
                    ),
                    str(sid),
                ),
            )
            if st.button("Revoke Selected Scope"):
                with eng.begin() as conn:
                    odb.revoke_scope(conn, scope_to_revoke)
                    odb.log_audit(
                        conn,
                        {
                            "actor_email": _current_user(),
                            "action": "revoke_scope",
                            "target_type": "office_admin_scope",
                            "target_id": scope_to_revoke,
                        },
                    )
                st.success("Scope revoked.")
                st.rerun()
    else:
        st.info("No scopes assigned yet.")

    # Assign new scope (widgets outside form so dependent dropdowns update)
    with st.expander("➕ Assign New Scope"):
        scope_type = st.selectbox(
            "Scope Type", ["global", "degree", "program", "branch"], key="oa_scope_type"
        )

        scope_value = None
        degree_code = None
        program_id = None
        branch_id = None

        if scope_type == "global":
            st.info("Global scope grants access to all organizational units.")

        elif scope_type == "degree":
            with eng.begin() as conn:
                degrees = _load_degrees(conn)
            if degrees:
                selected_degree = st.selectbox(
                    "Select Degree",
                    [d["code"] for d in degrees],
                    format_func=lambda c: next(
                        (d["name"] for d in degrees if d["code"] == c), c
                    ),
                    key="oa_scope_degree",
                )
                degree_code = selected_degree
                scope_value = selected_degree
            else:
                st.warning("No degrees available.")

        elif scope_type == "program":
            with eng.begin() as conn:
                degrees = _load_degrees(conn)
                if degrees:
                    degree_filter = st.selectbox(
                        "Filter by Degree",
                        ["(all)"] + [d["code"] for d in degrees],
                        format_func=lambda c: (
                            next((d["name"] for d in degrees if d["code"] == c), c)
                            if c != "(all)"
                            else c
                        ),
                        key="oa_scope_program_degree_filter",
                    )
                    programs = _load_programs(
                        conn, None if degree_filter == "(all)" else degree_filter
                    )
                else:
                    programs = []

            if programs:
                selected_program = st.selectbox(
                    "Select Program",
                    [p["id"] for p in programs],
                    format_func=lambda pid: next(
                        (p["name"] for p in programs if p["id"] == pid),
                        str(pid),
                    ),
                    key="oa_scope_program",
                )
                program_id = selected_program
                scope_value = str(selected_program)
                degree_code = next(
                    (p["degree_code"] for p in programs if p["id"] == selected_program),
                    None,
                )
            else:
                if scope_type == "program":
                    st.warning("No programs available.")

        elif scope_type == "branch":
            with eng.begin() as conn:
                programs = _load_programs(conn)
            if programs:
                program_filter = st.selectbox(
                    "Filter by Program",
                    [p["id"] for p in programs],
                    format_func=lambda pid: next(
                        (p["name"] for p in programs if p["id"] == pid),
                        str(pid),
                    ),
                    key="oa_scope_branch_program_filter",
                )
                with eng.begin() as conn:
                    branches = _load_branches(conn, program_filter)
            else:
                branches = []

            if programs and branches:
                selected_branch = st.selectbox(
                    "Select Branch",
                    [b["id"] for b in branches],
                    format_func=lambda bid: next(
                        (b["name"] for b in branches if b["id"] == bid),
                        str(bid),
                    ),
                    key="oa_scope_branch",
                )
                branch_id = selected_branch
                scope_value = str(selected_branch)
                program_id = next(
                    (b["program_id"] for b in branches if b["id"] == selected_branch),
                    None,
                )
                degree_code = next(
                    (p["degree_code"] for p in programs if p["id"] == program_id),
                    None,
                )
            else:
                if scope_type == "branch":
                    st.warning("No branches available for this program.")

        notes = st.text_area("Notes (optional)", key="oa_scope_notes")

        if st.button("Assign Scope"):
            if scope_type != "global" and scope_value is None:
                st.error(
                    f"Please select a concrete {scope_type} (degree/program/branch) before assigning."
                )
            else:
                with eng.begin() as conn:
                    try:
                        scope_id = odb.assign_scope(
                            conn,
                            {
                                "admin_email": admin_email,
                                "scope_type": scope_type,
                                "scope_value": scope_value,
                                "degree_code": degree_code,
                                "program_id": program_id,
                                "branch_id": branch_id,
                                "notes": notes,
                            },
                            created_by=_current_user(),
                        )

                        odb.log_audit(
                            conn,
                            {
                                "actor_email": _current_user(),
                                "action": "assign_scope",
                                "target_type": "office_admin_scope",
                                "target_id": scope_id,
                                "scope_type": scope_type,
                                "scope_value": scope_value,
                            },
                        )

                        st.success(
                            f"Scope assigned: {scope_type} - {scope_value or 'global'}"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to assign scope: {e}")


# ---------- PII ACCESS LOG ----------
def render_pii_access():
    st.subheader("PII Access Log")
    st.caption("Tracks when office admins unmask sensitive student information")

    eng = _get_engine()
    if not eng:
        st.info("No engine configured.")
        return

    with eng.begin() as conn:
        rows = odb.list_pii_access_log(conn, limit=100)

    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No PII access events logged yet.")


# ---------- EXPORT REQUESTS ----------
def render_export_requests():
    st.subheader("Data Export Requests")
    st.caption(
        "Office admins can request exports (students roster, attendance, marks, login credentials). "
        "Requires principal/director approval."
    )

    eng = _get_engine()
    if not eng:
        st.info("No engine configured.")
        return

    status_filter = st.selectbox(
        "Filter by Status", ["(all)", "pending", "approved", "rejected", "completed"]
    )

    with eng.begin() as conn:
        rows = odb.list_export_requests(
            conn, None if status_filter == "(all)" else status_filter
        )

    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No export requests.")

    with st.expander("📤 New Export Request"):
        with st.form(key="export_request_form"):
            entity_type = st.selectbox(
                "Data Type",
                [
                    "students_roster",
                    "attendance_summary",
                    "marks_summary",
                    "initial_credentials",
                ],
            )

            scope_type = st.selectbox("Scope", ["degree", "program", "branch"])

            scope_value = None

            # Load scope options based on type
            with eng.begin() as conn:
                if scope_type == "degree":
                    degrees = _load_degrees(conn)
                    if degrees:
                        scope_value = st.selectbox(
                            "Select Degree",
                            [d["code"] for d in degrees],
                            format_func=lambda c: next(
                                (d["name"] for d in degrees if d["code"] == c), c
                            ),
                        )
                elif scope_type == "program":
                    programs = _load_programs(conn)
                    if programs:
                        p_id = st.selectbox(
                            "Select Program",
                            [p["id"] for p in programs],
                            format_func=lambda pid: next(
                                (p["name"] for p in programs if p["id"] == pid),
                                str(pid),
                            ),
                        )
                        scope_value = str(p_id)
                elif scope_type == "branch":
                    branches = _load_branches(conn)
                    if branches:
                        b_id = st.selectbox(
                            "Select Branch",
                            [b["id"] for b in branches],
                            format_func=lambda bid: next(
                                (b["name"] for b in branches if b["id"] == bid),
                                str(bid),
                            ),
                        )
                        scope_value = str(b_id)

            reason = st.text_area("Reason for Export*")
            submitted = st.form_submit_button("Submit Request")

        if submitted:
            if not reason:
                st.error("Reason is required.")
            else:
                try:
                    # 1. Create the request row in the office_admin table
                    with eng.begin() as conn:
                        request_code = odb.create_export_request(
                            conn,
                            {
                                "admin_email": _current_user(),
                                "entity_type": entity_type,
                                "scope_type": scope_type,
                                "scope_value": scope_value,
                                "reason": reason,
                            },
                        )

                    # 2. Hook into central Approvals handler
                    handler = ApprovalHandler(eng, object_type="office_admin")

                    # 3. Create the approval request
                    approval_id = handler.request_approval(
                        object_id=request_code,
                        action="export_data",
                        requester_email=_current_user(),
                        reason=reason,
                        payload={
                            "entity_type": entity_type,
                            "scope_type": scope_type,
                            "scope_value": scope_value,
                            "request_code": request_code,
                        },
                    )

                    st.success(f"Export request {request_code} submitted!")
                    st.info(
                        f"Your request (Approval ID #{approval_id}) is now pending in the Approvals Inbox."
                    )
                    st.rerun()

                except Exception as e:
                    st.error(f"Error submitting request: {e}")


# ---------- AUDIT LOG ----------
def render_audit_log():
    st.subheader("Audit Trail")
    st.caption("All sensitive actions performed by office admins")

    eng = _get_engine()
    if not eng:
        st.info("No engine configured.")
        return

    with eng.begin() as conn:
        rows = odb.list_audit_log(conn, limit=200)

    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No audit events logged yet.")


# ---------- MAIN RENDER ----------
def render_office_admin():
    st.title("👥 Office Administration")
    st.markdown(
        """
    **Office Admins** can manage students and run reports within their assigned scope (degree/program/branch).
    
    - **Hierarchical Access**: Admins can be assigned global, degree, program, or branch-level access  
    - **PII Protection**: PII is masked by default (requires step-up + approval to unmask)  
    - **Export Approval**: Data exports require principal/director approval  
    - **Full Audit Trail**: All sensitive actions are logged
    """
    )

    tabs = st.tabs(
        [
            "Accounts",
            "Scope Assignments",
            "Export Requests",
            "PII Access Log",
            "Audit Trail",
        ]
    )

    with tabs[0]:
        render_accounts()

    with tabs[1]:
        render_scopes()

    with tabs[2]:
        render_export_requests()

    with tabs[3]:
        render_pii_access()

    with tabs[4]:
        render_audit_log()
