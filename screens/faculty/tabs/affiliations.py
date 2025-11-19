# screens/faculty/tabs/affiliations.py
from __future__ import annotations
from typing import Set, List, Optional, Tuple, Any, Dict

import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

from screens.faculty.utils import _handle_error
from screens.faculty.db import (
    _designation_catalog,
    _degree_enabled_map,
    # _branches_for_degree removed - implemented locally for schema-awareness
    _affiliation_types,
    _is_academic_admin,
)

# ==============================================================================
# Helpers (robust to Row/RowMapping/dict/positional rows)
# ==============================================================================

def _row_get(row: Any, key: str, pos_fallback: Optional[int] = None):
    if row is None:
        return None
    try:
        return row._mapping.get(key)  # type: ignore
    except Exception:
        pass
    try:
        return getattr(row, key)
    except Exception:
        pass
    try:
        return row.get(key)
    except Exception:
        pass
    if pos_fallback is not None:
        try:
            return row[pos_fallback]
        except Exception:
            return None
    return None


def _as_code_list(rows: List[Any], code_key: str) -> List[str]:
    out: List[str] = []
    for r in rows or []:
        val = _row_get(r, code_key, pos_fallback=0)
        if val not in (None, ""):
            out.append(str(val))
    return out


def _has_column(conn, table: str, column: str) -> bool:
    try:
        rows = conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()
        cols = {str(r[1]).lower() for r in rows}
        return column.lower() in cols
    except Exception:
        return False


def _table_exists(conn, table_name: str) -> bool:
    try:
        row = conn.execute(sa_text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=:t"
        ), {"t": table_name}).fetchone()
        return bool(row)
    except Exception:
        return False


def _table_cols(conn, table: str) -> set[str]:
    try:
        return {c[1] for c in conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()}
    except Exception:
        return set()


def _ensure_affiliation_audit_table(conn) -> None:
    conn.execute(sa_text(
        """
        CREATE TABLE IF NOT EXISTS faculty_affiliation_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            affiliation_id INTEGER,
            email TEXT NOT NULL,
            degree_code TEXT NOT NULL,
            program_code TEXT,
            branch_code TEXT,
            group_code TEXT,
            old_designation TEXT,
            new_designation TEXT,
            old_type TEXT,
            new_type TEXT,
            old_override INTEGER,
            new_override INTEGER,
            actor_email TEXT,
            reason TEXT,
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    ))


def _ensure_affiliation_audit_primary_columns(conn) -> None:
    try:
        if not _has_column(conn, "faculty_affiliation_audit", "old_primary"):
            conn.execute(sa_text("ALTER TABLE faculty_affiliation_audit ADD COLUMN old_primary INTEGER"))
    except Exception:
        pass
    try:
        if not _has_column(conn, "faculty_affiliation_audit", "new_primary"):
            conn.execute(sa_text("ALTER TABLE faculty_affiliation_audit ADD COLUMN new_primary INTEGER"))
    except Exception:
        pass


def _insert_affiliation_audit(conn, *, affiliation_id, email, degree_code,
                              program_code, branch_code, group_code,
                              old_desg, new_desg, old_type, new_type,
                              old_ovr, new_ovr, actor_email, reason,
                              old_primary: Optional[int]=None, new_primary: Optional[int]=None):
    try:
        _ensure_affiliation_audit_table(conn)
        _ensure_affiliation_audit_primary_columns(conn)
    except Exception:
        pass

    has_primary_cols = (
        _has_column(conn, "faculty_affiliation_audit", "old_primary") and
        _has_column(conn, "faculty_affiliation_audit", "new_primary")
    )

    params = {
        "aff_id": affiliation_id,
        "em": email,
        "deg": degree_code,
        "prog": (program_code or None),
        "br": (branch_code or None),
        "grp": (group_code or None),
        "od": old_desg, "nd": new_desg,
        "ot": old_type, "nt": new_type,
        "oo": int(old_ovr) if old_ovr is not None else None,
        "no": int(new_ovr) if new_ovr is not None else None,
        "actor": actor_email, "reason": (reason or None),
        "op": old_primary if old_primary is not None else 0,
        "np": new_primary if new_primary is not None else 0,
    }

    if has_primary_cols:
        conn.execute(sa_text("""
            INSERT INTO faculty_affiliation_audit(
                affiliation_id, email, degree_code, program_code, branch_code, group_code,
                old_designation, new_designation, old_type, new_type,
                old_override, new_override, actor_email, reason,
                old_primary, new_primary
            ) VALUES (
                :aff_id, :em, :deg, :prog, :br, :grp,
                :od, :nd, :ot, :nt, :oo, :no, :actor, :reason, :op, :np
            )
        """), params)
    else:
        conn.execute(sa_text("""
            INSERT INTO faculty_affiliation_audit(
                affiliation_id, email, degree_code, program_code, branch_code, group_code,
                old_designation, new_designation, old_type, new_type,
                old_override, new_override, actor_email, reason
            ) VALUES (
                :aff_id, :em, :deg, :prog, :br, :grp,
                :od, :nd, :ot, :nt, :oo, :no, :actor, :reason
            )
        """), params)


def _ensure_unique_scope_index(conn) -> None:
    try:
        conn.execute(sa_text(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_faculty_aff_scope
            ON faculty_affiliations(
              lower(email),
              degree_code,
              COALESCE(program_code,''),
              COALESCE(branch_code,''),
              COALESCE(group_code,'')
            )
            """
        ))
    except Exception:
        pass


def _find_existing_affiliation(conn, *, email: str, degree: str,
                               program: Optional[str], branch: Optional[str], group_: Optional[str]):
    # Exact match but case-insensitive on program/branch/group
    row = conn.execute(sa_text(
        """
        SELECT id, designation, type, allowed_credit_override
          FROM faculty_affiliations
         WHERE lower(email)=lower(:e)
           AND lower(degree_code)=lower(:d)
           AND COALESCE(lower(program_code),'') = lower(COALESCE(:p,''))
           AND COALESCE(lower(branch_code),'')  = lower(COALESCE(:b,''))
           AND COALESCE(lower(group_code),'')   = lower(COALESCE(:g,''))
         LIMIT 1
        """
    ), {
        "e": email, "d": degree,
        "p": (program or ""), "b": (branch or ""), "g": (group_ or "")
    }).fetchone()

    if row:
        return row

    # Fallback: if there is exactly ONE active row for this email+degree, treat it as the target
    # (Useful if scope changed slightly or user is switching between 'general' and specific)
    rows = conn.execute(sa_text(
        """
        SELECT id, designation, type, allowed_credit_override
          FROM faculty_affiliations
         WHERE lower(email)=lower(:e) AND lower(degree_code)=lower(:d) AND active=1
         ORDER BY updated_at DESC, id DESC
        """
    ), {"e": email, "d": degree}).fetchall()

    if len(rows) == 1:
        return rows[0]

    return None


def _run_dedupe(conn) -> int:
    conn.execute(sa_text(
        """
        CREATE TEMP TABLE tmp_keep AS
        SELECT a.rowid AS rid
          FROM faculty_affiliations a
          LEFT JOIN faculty_affiliations b
            ON lower(a.email)=lower(b.email)
           AND a.degree_code=b.degree_code
           AND COALESCE(a.program_code,'')=COALESCE(b.program_code,'')
           AND COALESCE(a.branch_code,'') =COALESCE(b.branch_code,'')
           AND COALESCE(a.group_code,'')  =COALESCE(b.group_code,'')
           AND (
                COALESCE(b.updated_at,'') > COALESCE(a.updated_at,'')
                OR (COALESCE(b.updated_at,'') = COALESCE(a.updated_at,'') AND b.rowid > a.rowid)
           )
        WHERE b.rowid IS NULL
        """
    ))
    before = conn.execute(sa_text("SELECT COUNT(1) FROM faculty_affiliations")).fetchone()[0]
    conn.execute(sa_text("DELETE FROM faculty_affiliations WHERE rowid NOT IN (SELECT rid FROM tmp_keep)"))
    after = conn.execute(sa_text("SELECT COUNT(1) FROM faculty_affiliations")).fetchone()[0]
    conn.execute(sa_text("DROP TABLE IF EXISTS tmp_keep"))
    return int(before - after)


# ---------------- Primary support ----------------

def _ensure_primary_schema(conn) -> None:
    try:
        if not _has_column(conn, "faculty_affiliations", "is_primary"):
            conn.execute(sa_text("ALTER TABLE faculty_affiliations ADD COLUMN is_primary INTEGER DEFAULT 0"))
    except Exception:
        pass
    try:
        conn.execute(sa_text(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_aff_primary_per_degree
              ON faculty_affiliations(lower(email), lower(degree_code))
             WHERE is_primary = 1 AND active = 1
            """
        ))
    except Exception:
        pass


def _current_primary_id(conn, *, email: str, degree: str) -> Optional[int]:
    if not _has_column(conn, "faculty_affiliations", "is_primary"):
        return None
    row = conn.execute(sa_text(
        """
        SELECT id FROM faculty_affiliations
         WHERE lower(email)=lower(:e) AND lower(degree_code)=lower(:d)
           AND is_primary=1 AND active=1
         LIMIT 1
        """
    ), {"e": email, "d": degree}).fetchone()
    return int(row[0]) if row else None


def set_primary(conn, *, email: str, degree: str, target_id: int,
                actor_email: Optional[str] = None, reason: Optional[str] = None):
    if not _has_column(conn, "faculty_affiliations", "is_primary"):
        return

    prev = conn.execute(sa_text(
        """
        SELECT id, program_code, branch_code, group_code, designation, type,
               COALESCE(allowed_credit_override,0)
          FROM faculty_affiliations
         WHERE lower(email)=lower(:e) AND lower(degree_code)=lower(:d)
           AND is_primary=1 AND active=1
         LIMIT 1
        """
    ), {"e": email, "d": degree}).fetchone()

    if prev and int(prev[0]) == int(target_id):
        return  # already primary

    if prev:
        conn.execute(sa_text(
            "UPDATE faculty_affiliations SET is_primary=0, updated_at=CURRENT_TIMESTAMP WHERE id=:id"
        ), {"id": int(prev[0])})
        try:
            _insert_affiliation_audit(
                conn,
                affiliation_id=int(prev[0]), email=email, degree_code=degree,
                program_code=prev[1], branch_code=prev[2], group_code=prev[3],
                old_desg=prev[4], new_desg=prev[4],
                old_type=prev[5], new_type=prev[5],
                old_ovr=int(prev[6]), new_ovr=int(prev[6]),
                actor_email=actor_email, reason=reason or "Unset primary",
                old_primary=1, new_primary=0
            )
        except Exception:
            pass

    conn.execute(sa_text(
        "UPDATE faculty_affiliations SET is_primary=1, active=1, updated_at=CURRENT_TIMESTAMP WHERE id=:id"
    ), {"id": int(target_id)})

    try:
        tgt = conn.execute(sa_text(
            """
            SELECT program_code, branch_code, group_code, designation, type,
                   COALESCE(allowed_credit_override,0)
              FROM faculty_affiliations
             WHERE id=:id
            """
        ), {"id": int(target_id)}).fetchone()
        _insert_affiliation_audit(
            conn,
            affiliation_id=int(target_id), email=email, degree_code=degree,
            program_code=tgt[0], branch_code=tgt[1], group_code=tgt[2],
            old_desg=tgt[3], new_desg=tgt[3],
            old_type=tgt[4], new_type=tgt[4],
            old_ovr=int(tgt[5]), new_ovr=int(tgt[5]),
            actor_email=actor_email, reason=reason or "Set as primary",
            old_primary=0, new_primary=1
        )
    except Exception:
        pass


# ==============================================================================
# Schema-aware branch querying (handles multiple schema types)
# ==============================================================================

def _branches_for_degree(conn, degree_code: str) -> List[Any]:
    if not _table_exists(conn, "branches"):
        return []
    
    bcols = _table_cols(conn, "branches")
    has_pid = "program_id" in bcols
    has_deg = "degree_code" in bcols
    params = {"deg": degree_code}

    if has_pid and has_deg:
        rows = conn.execute(sa_text("""
            SELECT b.id, b.branch_code, b.branch_name, 
                   COALESCE(p.program_code, '') as program_code,
                   COALESCE(p.degree_code, b.degree_code) as degree_code,
                   b.active, b.sort_order
              FROM branches b
              LEFT JOIN programs p ON p.id = b.program_id
             WHERE (p.degree_code = :deg OR b.degree_code = :deg)
             ORDER BY p.degree_code, p.program_code, b.sort_order, lower(b.branch_code)
        """), params).fetchall()
    elif has_pid:
        rows = conn.execute(sa_text("""
            SELECT b.id, b.branch_code, b.branch_name, 
                   p.program_code, p.degree_code,
                   b.active, b.sort_order
              FROM branches b
              LEFT JOIN programs p ON p.id = b.program_id
             WHERE p.degree_code = :deg
             ORDER BY p.degree_code, p.program_code, b.sort_order, lower(b.branch_code)
        """), params).fetchall()
    elif has_deg:
        rows = conn.execute(sa_text("""
            SELECT id, branch_code, branch_name, 
                   '' as program_code, degree_code,
                   active, sort_order
              FROM branches
             WHERE degree_code = :deg
             ORDER BY degree_code, sort_order, lower(branch_code)
        """), params).fetchall()
    else:
        return []

    return list(rows)


# --- Scoped designation propagation ------------------------------------------
def _propagate_designation_scoped(
    conn,
    *,
    email: str,
    degree: str,
    new_desg: str,
    scope: str,              # "degree" | "program"
    program: str | None,     # required when scope="program"
    actor_email: str | None = None,
    reason: str | None = None,
):
    where = ["lower(email)=lower(:e)", "lower(degree_code)=lower(:d)"]
    params = {"e": email, "d": degree}
    if scope == "program":
        where.append("COALESCE(lower(program_code),'') = lower(:p)")
        params["p"] = (program or "")

    rows = conn.execute(sa_text(f"""
        SELECT id,
               designation,
               COALESCE(program_code,''), COALESCE(branch_code,''), COALESCE(group_code,''),
               COALESCE(allowed_credit_override,0)
          FROM faculty_affiliations
         WHERE {' AND '.join(where)}
    """), params).fetchall()

    for r in rows:
        rid, old_desg = int(r[0]), (r[1] or "")
        if old_desg != new_desg:
            conn.execute(sa_text("""
                UPDATE faculty_affiliations
                   SET designation=:g, updated_at=CURRENT_TIMESTAMP
                 WHERE id=:id
            """), {"g": new_desg, "id": rid})
            try:
                _insert_affiliation_audit(
                    conn,
                    affiliation_id=rid, email=email, degree_code=degree,
                    program_code=r[2] or None, branch_code=r[3] or None, group_code=r[4] or None,
                    old_desg=old_desg, new_desg=new_desg,
                    old_type=None, new_type=None,
                    old_ovr=int(r[5]), new_ovr=int(r[5]),
                    actor_email=actor_email, reason=(reason or f"Designation propagation ({scope})"),
                )
            except Exception:
                pass


# ----------------------------- Main UI -----------------------------

def render(engine: Engine, degree: str, roles: Set[str], can_edit: bool, key_prefix: str):
    st.subheader("Affiliations")

    # DB guardrails
    try:
        with engine.begin() as guard:
            _ensure_unique_scope_index(guard)
            _ensure_affiliation_audit_table(guard)
            _ensure_primary_schema(guard)
            _ensure_affiliation_audit_primary_columns(guard)
    except Exception:
        pass

    adf = pd.DataFrame()
    editable_emails, desgs, type_codes = [], [], []
    programs: List[str] = []
    groups: List[str] = []
    branches: List[str] = []
    degree_uses_programs = False
    degree_uses_groups = False
    degree_uses_branches = False

    # -------- Data preload (modern schema) --------
    try:
        with engine.begin() as conn:
            # Programs
            if _table_exists(conn, "programs"):
                programs_data = conn.execute(sa_text(
                    """
                    SELECT DISTINCT program_code
                      FROM programs
                     WHERE lower(degree_code)=lower(:d)
                     ORDER BY program_code
                    """
                ), {"d": degree}).fetchall()
                programs = _as_code_list(programs_data, "program_code")

            # Curriculum Groups
            if _table_exists(conn, "curriculum_groups"):
                groups_data = conn.execute(sa_text(
                    """
                    SELECT DISTINCT group_code
                      FROM curriculum_groups
                     WHERE lower(degree_code)=lower(:d)
                     ORDER BY group_code
                    """
                ), {"d": degree}).fetchall()
                groups = _as_code_list(groups_data, "group_code")

            # Branches
            if _table_exists(conn, "branches"):
                b_rows = _branches_for_degree(conn, degree) or []
                seen = set()
                for r in b_rows:
                    bc = _row_get(r, "branch_code", pos_fallback=1)
                    if bc and str(bc) not in seen:
                        seen.add(str(bc))
                        branches.append(str(bc))

            degree_uses_programs = len(programs) > 0
            degree_uses_groups = len(groups) > 0
            degree_uses_branches = len(branches) > 0

            # include name and primary (if present)
            has_primary = _has_column(conn, "faculty_affiliations", "is_primary")
            select_cols = (
                "a.id, a.email, COALESCE(fp.name,''), a.degree_code,"
                " COALESCE(a.program_code,''), COALESCE(a.branch_code,''), COALESCE(a.group_code,''),"
                " a.designation, a.type, a.allowed_credit_override, a.active"
                + (", COALESCE(a.is_primary,0)" if has_primary else "")
            )
            afrows = conn.execute(
                sa_text(f"""
                    SELECT {select_cols}
                      FROM faculty_affiliations a
                      LEFT JOIN faculty_profiles fp ON lower(fp.email)=lower(a.email)
                     WHERE lower(a.degree_code)=lower(:d)
                     ORDER BY a.email,
                              COALESCE(a.program_code,''), COALESCE(a.branch_code,''), COALESCE(a.group_code,'')
                """),
                {"d": degree},
            ).fetchall()

            data = []
            for r in afrows:
                is_admin, _, fixed_role = _is_academic_admin(conn, r[1])
                is_immutable = (is_admin and fixed_role in ("principal", "director"))
                email_display = f"🔒 {r[1]}" if is_immutable else r[1]
                is_primary_val = int(r[11]) if len(r) >= 12 else 0
                data.append(
                    {
                        "ID": r[0],
                        "Email": email_display,
                        "Name": r[2],
                        "Degree Code": r[3],
                        "Program Code": r[4],
                        "Branch Code": r[5],
                        "Group Code": r[6],
                        "Designation": r[7],
                        "Type": r[8],
                        "Override": r[9],
                        "Active": r[10],
                        "Primary": "⭐" if is_primary_val == 1 else "",
                        "_is_primary": is_primary_val,
                        "_is_immutable": is_immutable,
                        "_raw_email": r[1],
                    }
                )
            if data:
                adf = pd.DataFrame(data)

            editable_emails = [
                row[0]
                for row in conn.execute(
                    sa_text(
                        """SELECT fp.email
                             FROM faculty_profiles fp
                            WHERE NOT EXISTS (
                                   SELECT 1
                                     FROM academic_admins aa
                                     JOIN users u ON aa.user_id = u.id
                                    WHERE lower(u.email) = lower(fp.email)
                                      AND aa.fixed_role IN ('principal', 'director', 'management_representative')
                                      AND u.active = 1
                                  )
                            ORDER BY fp.name"""
                    )
                ).fetchall()
            ]

            catalog = _designation_catalog(conn)
            enabled_map = _degree_enabled_map(conn, degree)
            desgs = [d for d in catalog if enabled_map.get(d.lower(), False)]

            type_rows = _affiliation_types(conn)
            type_codes = [t[0] for t in type_rows if t[0] in ("core", "visiting")]

    except Exception as e:
        _handle_error(e, "Could not load affiliations.")
        st.exception(e)
        return

    # -------- Table view (COLLAPSED VIEW) --------
    if adf.empty:
        st.info(f"No affiliations yet for '{degree}'. Use the form below to add.")
    else:
        # 1. Define columns to Group By (Everything EXCEPT Group Code and IDs)
        group_cols = [
            "Email", "Name", "Degree Code", "Program Code", "Branch Code", 
            "Designation", "Type", "Override", "Active", "Primary", 
            "_is_primary", "_is_immutable", "_raw_email"
        ]
        
        # 2. Perform the Grouping - aggregate CGs into tags
        collapsed_df = adf.groupby(group_cols, as_index=False).agg({
            "Group Code": lambda x: ", ".join(sorted(filter(None, set(x)))), # Join unique CGs: "ALD, DES"
            "ID": 'first' # Just grab one ID for reference
        })

        # 3. Rename for clarity
        collapsed_df.rename(columns={"Group Code": "CG Tags"}, inplace=True)

        # 4. Sorting controls
        sort_by = st.selectbox("Sort by", ["Name", "Email", "Designation"], index=0, key=f"{key_prefix}_af_sort_by")
        sort_dir = st.radio("Order", ["Ascending", "Descending"], index=0, horizontal=True, key=f"{key_prefix}_af_sort_dir")
        
        try:
            collapsed_df = collapsed_df.sort_values(
                by=sort_by if sort_by in collapsed_df.columns else "Email",
                ascending=(sort_dir == "Ascending")
            )
        except Exception:
            pass

        # 5. Dynamic Column Display
        display_cols = []
        if "Primary" in collapsed_df.columns: display_cols.append("Primary")
        display_cols.extend(["Name", "Email", "Degree Code"])
        
        # Only show Program/Branch if the degree actually uses them
        if degree_uses_programs: display_cols.append("Program Code")
        if degree_uses_branches: display_cols.append("Branch Code")
        
        # Always show Tags
        display_cols.extend(["CG Tags", "Designation", "Type", "Override", "Active"])

        st.dataframe(collapsed_df[display_cols], use_container_width=True, hide_index=True)

        # ==============================================================================
        # Unassigned Faculty Alert
        # ==============================================================================
        try:
            with engine.begin() as conn:
                all_faculty_rows = conn.execute(sa_text("""
                    SELECT email, name, employee_id 
                    FROM faculty_profiles 
                    WHERE status = 'active'
                    AND NOT EXISTS (
                        SELECT 1 FROM academic_admins aa 
                        JOIN users u ON aa.user_id = u.id 
                        WHERE lower(u.email) = lower(faculty_profiles.email) 
                        AND aa.fixed_role IN ('principal', 'director')
                        AND u.active = 1
                    )
                """)).fetchall()
                
            affiliated_emails_in_degree = set()
            if not adf.empty and "Email" in adf.columns:
                 affiliated_emails_in_degree = {
                     str(e).replace("🔒 ", "").strip().lower() 
                     for e in adf["Email"].unique()
                 }

            unassigned_list = []
            for row in all_faculty_rows:
                email = str(row[0]).lower()
                if email not in affiliated_emails_in_degree:
                    unassigned_list.append({
                        "Name": row[1],
                        "Email": row[0],
                        "Employee ID": row[2] or "N/A"
                    })

            if unassigned_list:
                with st.expander(f"⚠️ Found {len(unassigned_list)} Unassigned Faculty Profiles", expanded=True):
                    st.info(
                        f"These faculty profiles exist in the database but are **not yet assigned** to **{degree}**. "
                        "Select them in the 'Edit / Create' form below to assign them."
                    )
                    st.dataframe(pd.DataFrame(unassigned_list), use_container_width=True, hide_index=True)
        except Exception as e:
            st.warning(f"Could not calculate unassigned faculty: {e}")
        # ==============================================================================


        colx, coly = st.columns([1, 1])
        with colx:
            if st.button("🧹 Dedupe exact duplicates", use_container_width=True):
                try:
                    with engine.begin() as conn:
                        removed = _run_dedupe(conn)
                    st.success(f"Removed {removed} duplicate row(s).")
                    st.rerun()
                except Exception as e:
                    _handle_error(e, "Dedupe failed.")
        with coly:
            limit = st.number_input("Show last N audit entries", min_value=5, max_value=500, value=25, step=5)
            try:
                with engine.begin() as conn:
                    _ensure_affiliation_audit_table(conn)
                    has_primary_audit = _has_column(conn, "faculty_affiliation_audit", "old_primary")
                    select_cols = (
                        "changed_at, email, degree_code,"
                        " COALESCE(program_code,''), COALESCE(branch_code,''), COALESCE(group_code,''),"
                        " old_designation, new_designation, old_type, new_type,"
                        " COALESCE(old_override,''), COALESCE(new_override,''),"
                        " COALESCE(actor_email,''), COALESCE(reason,'')"
                        + (", COALESCE(old_primary,''), COALESCE(new_primary,'')" if has_primary_audit else "")
                    )
                    audit = conn.execute(sa_text(f"""
                        SELECT {select_cols}
                          FROM faculty_affiliation_audit
                         WHERE lower(degree_code)=lower(:d)
                         ORDER BY changed_at DESC, id DESC
                         LIMIT :lim
                    """), {"d": degree, "lim": int(limit)}).fetchall()
                if audit:
                    cols = ["When","Email","Degree","Program","Branch","Group",
                            "Old Desg","New Desg","Old Type","New Type",
                            "Old Ovr","New Ovr","Actor","Reason"]
                    if has_primary_audit:
                        cols += ["Old Primary","New Primary"]
                    adf_a = pd.DataFrame(audit, columns=cols)
                    st.dataframe(adf_a, use_container_width=True, hide_index=True)
                else:
                    st.caption("No audit entries yet.")
            except Exception as e:
                _handle_error(e, "Could not load audit trail.")

    if not can_edit:
        st.caption("View-only.")
        return

    st.divider()

    # -------- Editor --------
    st.markdown("### Edit / Create an affiliation")

    try:
        with engine.begin() as conn:
            emails = [r[0] for r in conn.execute(
                sa_text(
                    """SELECT fp.email
                         FROM faculty_profiles fp
                        WHERE NOT EXISTS (
                               SELECT 1
                                 FROM academic_admins aa
                                 JOIN users u ON aa.user_id = u.id
                                WHERE lower(u.email) = lower(fp.email)
                                  AND aa.fixed_role IN ('principal', 'director', 'management_representative')
                                  AND u.active = 1
                              )
                        ORDER BY fp.name, fp.email"""
                )
            ).fetchall()]
    except Exception:
        emails = editable_emails

    em = st.selectbox("Faculty (email)", options=[""] + emails, key=f"{key_prefix}_af_email")

    cols1 = st.columns(3)
    i = 0
    prog = ""
    if degree_uses_programs:
        with cols1[i]:
            prog = st.selectbox("Program (optional)", options=[""] + programs,
                                key=f"{key_prefix}_af_program",
                                help="Leave blank to assign at degree-only scope.")
        i += 1

    br = ""
    if degree_uses_branches:
        with cols1[i]:
            br = st.selectbox("Branch (optional)", options=[""] + branches,
                              key=f"{key_prefix}_af_branch",
                              help="Requires a Program to be selected.")
        i += 1

    # --- MULTI-SELECT FOR CGS ---
    selected_cgs = []
    existing_affiliation_id: Optional[int] = None
    old_row = None
    is_immutable_affiliation = False

    if degree_uses_groups:
        with cols1[i]:
            # Pre-fetch logic if editing
            current_cgs = []
            if em:
                try:
                    with engine.begin() as conn:
                        cg_rows = conn.execute(sa_text("""
                            SELECT group_code FROM faculty_affiliations
                            WHERE lower(email)=lower(:e) 
                            AND degree_code=:d 
                            AND COALESCE(program_code,'')=COALESCE(:p,'') 
                            AND COALESCE(branch_code,'')=COALESCE(:b,'')
                            AND active=1
                        """), {"e": em, "d": degree, "p": prog or "", "b": br or ""}).fetchall()
                        current_cgs = [r[0] for r in cg_rows if r[0]]
                        
                        # Also fetch basic info for diff display
                        row = conn.execute(sa_text("""
                            SELECT id, designation, type, allowed_credit_override
                            FROM faculty_affiliations
                            WHERE lower(email)=lower(:e) AND degree_code=:d
                            AND COALESCE(program_code,'')=COALESCE(:p,'')
                            AND COALESCE(branch_code,'')=COALESCE(:b,'')
                            LIMIT 1
                        """), {"e": em, "d": degree, "p": prog or "", "b": br or ""}).fetchone()
                        if row:
                            existing_affiliation_id = int(row[0])
                            old_row = (row[1], row[2], int(row[3] or 0))
                except Exception:
                    pass

            selected_cgs = st.multiselect(
                "Curriculum Groups (Tags)", 
                options=groups,
                default=current_cgs,
                key=f"{key_prefix}_af_cg_tags",
                help="Select all CGs this faculty belongs to for this role."
            )
        i += 1
    else:
        # Logic for non-CG degrees (just fetch general row)
        if em:
            try:
                with engine.begin() as conn:
                    row = _find_existing_affiliation(conn, email=em, degree=degree,
                                                     program=(prog or None), branch=(br or None), group_=None)
                    if row:
                        existing_affiliation_id = int(row[0])
                        old_row = (row[1], row[2], int(row[3] or 0))
            except Exception:
                pass

    with cols1[i] if i < 3 else st.container(): # Fallback if cols full
        dg = st.selectbox("Designation", options=desgs, key=f"{key_prefix}_af_desg")

    cols2 = st.columns([2, 2])
    with cols2[0]:
        type_codes = type_codes or ["core", "visiting"]
        tp = st.selectbox("Type", options=type_codes, key=f"{key_prefix}_af_type")
    with cols2[1]:
        ovr = st.number_input("Allowed credit override", min_value=0, max_value=10000, value=0,
                              key=f"{key_prefix}_af_ovr")

    if br and not prog:
        st.warning("Selecting a Branch requires selecting a Program.")
    
    st.caption(f"Editing scope → Degree={degree} • Program={prog or '—'} • Branch={br or '—'}")

    # Immutable check
    try:
        with engine.begin() as conn:
            imm = conn.execute(sa_text(
                "SELECT 1 FROM academic_admins aa JOIN users u ON aa.user_id = u.id "
                "WHERE lower(u.email) = lower(:e) AND aa.fixed_role IN ('principal','director') AND u.active = 1"
            ), {"e": em}).fetchone()
            is_immutable_affiliation = bool(imm)
    except Exception:
        pass

    if is_immutable_affiliation:
        st.error("🔒 Cannot edit affiliations for Fixed Role Designations.")
        return

    # -------- Save / Remove Buttons --------
    col_save, col_remove = st.columns([1, 4])

    with col_save:
        save_clicked = st.button("💾 Save", key=f"{key_prefix}_af_save", type="primary")
    
    with col_remove:
        # Only show Remove if we found ANY row for this Base Scope (regardless of CG tags)
        if existing_affiliation_id:
            if st.button("🗑️ Remove this Affiliation", key=f"{key_prefix}_af_remove", type="secondary"):
                try:
                    with engine.begin() as conn:
                        # Delete ALL rows for this base scope (Degree+Program+Branch) for this user
                        conn.execute(sa_text("""
                            DELETE FROM faculty_affiliations 
                            WHERE lower(email)=lower(:e) 
                            AND degree_code=:d
                            AND COALESCE(program_code,'') = COALESCE(:p,'')
                            AND COALESCE(branch_code,'')  = COALESCE(:b,'')
                        """), {
                            "e": em, "d": degree, "p": prog or "", "b": br or ""
                        })
                        
                        _insert_affiliation_audit(
                            conn,
                            affiliation_id=existing_affiliation_id, email=em.lower(), degree_code=degree,
                            program_code=prog, branch_code=br, group_code=None,
                            old_desg="VARIOUS", new_desg="DELETED", old_type="VARIOUS", new_type="DELETED",
                            old_ovr=0, new_ovr=0,
                            actor_email=st.session_state.get("current_user_email", "Admin"), 
                            reason="Explicit removal via UI (Base Scope)"
                        )
                    st.success("✅ Affiliation removed successfully.")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as ex:
                    _handle_error(ex, "Failed to remove affiliation.")

    if save_clicked:
        if not em or not dg:
            st.error("Faculty and Designation are required.")
            st.stop()
        
        try:
            with engine.begin() as conn:
                base_params = {
                    "e": em.lower(), "d": degree, 
                    "p": prog or None, "b": br or None,
                    "des": dg, "t": tp, "o": int(ovr)
                }

                # Get existing CGs
                existing_db_rows = conn.execute(sa_text("""
                    SELECT id, group_code FROM faculty_affiliations
                    WHERE lower(email)=:e AND degree_code=:d
                    AND COALESCE(program_code,'') = COALESCE(:p,'')
                    AND COALESCE(branch_code,'')  = COALESCE(:b,'')
                """), base_params).fetchall()
                
                existing_map = {r[1]: r[0] for r in existing_db_rows} # code -> id
                
                target_cgs = set(selected_cgs)
                # If NO tags selected, we create one row with NULL group (General Entry)
                if not target_cgs and not degree_uses_groups:
                     target_cgs = {None} 
                elif not target_cgs and degree_uses_groups:
                     # For degrees with CGs, empty selection implies we might want a general entry OR user cleared tags.
                     # Let's default to a general entry (None) so the affiliation persists without tags.
                     target_cgs = {None}

                current_db_cgs = set(existing_map.keys())
                
                to_add = target_cgs - current_db_cgs
                to_remove = current_db_cgs - target_cgs
                to_update = target_cgs.intersection(current_db_cgs)

                # Execute Changes
                for cg in to_add:
                    conn.execute(sa_text("""
                        INSERT INTO faculty_affiliations
                        (email, degree_code, program_code, branch_code, group_code, designation, type, allowed_credit_override, active)
                        VALUES (:e, :d, :p, :b, :g, :des, :t, :o, 1)
                    """), {**base_params, "g": cg})
                    
                for cg in to_remove:
                    if cg in existing_map:
                        conn.execute(sa_text("DELETE FROM faculty_affiliations WHERE id=:id"), {"id": existing_map[cg]})
                
                for cg in to_update:
                    if cg in existing_map:
                        conn.execute(sa_text("""
                            UPDATE faculty_affiliations 
                            SET designation=:des, type=:t, allowed_credit_override=:o, active=1, updated_at=CURRENT_TIMESTAMP
                            WHERE id=:id
                        """), {**base_params, "id": existing_map[cg]})

            st.success("✅ Affiliations updated successfully.")
            st.cache_data.clear()
            st.rerun()

        except Exception as ex:
            _handle_error(ex, "Failed to save affiliations.")
