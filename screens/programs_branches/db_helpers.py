"""
Database helper functions for Programs/Branches module
"""
import pandas as pd
import streamlit as st
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine


def _ensure_curriculum_columns(engine: Engine):
    """Ensure the curriculum group columns exist in the degrees table."""
    try:
        with engine.begin() as conn:
            columns = conn.execute(sa_text("PRAGMA table_info(degrees)")).fetchall()
            column_names = [col[1] for col in columns]

            if 'cg_degree' not in column_names:
                conn.execute(sa_text("ALTER TABLE degrees ADD COLUMN cg_degree INTEGER NOT NULL DEFAULT 0"))
            if 'cg_program' not in column_names:
                conn.execute(sa_text("ALTER TABLE degrees ADD COLUMN cg_program INTEGER NOT NULL DEFAULT 0"))
            if 'cg_branch' not in column_names:
                conn.execute(sa_text("ALTER TABLE degrees ADD COLUMN cg_branch INTEGER NOT NULL DEFAULT 0"))
    except Exception:
        pass


def _fetch_degree(conn, degree_code: str):
    return conn.execute(sa_text("""
        SELECT code, title, cohort_splitting_mode, roll_number_scope, active, sort_order,
               logo_file_name, cg_degree, cg_program, cg_branch
          FROM degrees
         WHERE code = :c
    """), {"c": degree_code}).fetchone()


@st.cache_data
def _degrees_df(_engine: Engine):
    cols = ["code","title","cohort_splitting_mode","roll_number_scope","active","sort_order","logo_file_name"]
    with _engine.begin() as conn:
        rows = conn.execute(sa_text("""
            SELECT code, title, cohort_splitting_mode, roll_number_scope, active, sort_order, logo_file_name
              FROM degrees
             ORDER BY sort_order, code
        """)).fetchall()
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame([dict(r._mapping) for r in rows], columns=cols)


@st.cache_data
def _programs_df(_engine: Engine, degree_filter: str | None = None):
    cols = ["id","program_code","program_name","degree_code","active","sort_order","logo_file_name","description"]
    q = f"SELECT {', '.join(cols)} FROM programs"
    params = {}
    if degree_filter:
        q += " WHERE degree_code=:d"
        params["d"] = degree_filter
    q += " ORDER BY degree_code, sort_order, lower(program_code)"
    with _engine.begin() as conn:
        rows = conn.execute(sa_text(q), params).fetchall()
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame([dict(r._mapping) for r in rows], columns=cols)


@st.cache_data
def _table_cols(_engine: Engine, table: str) -> set[str]:
    try:
        with _engine.begin() as conn:
            return {c[1] for c in conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()}
    except:
        return set()


@st.cache_data
def _branches_df(_engine: Engine, degree_filter: str | None = None, program_id: int | None = None):
    """List branches; supports schemas with or without degree_code on branches."""
    bcols = _table_cols(_engine, "branches")
    has_pid = "program_id" in bcols
    has_deg = "degree_code" in bcols
    params = {}
    
    if has_pid and has_deg:
        wh = []
        if degree_filter:
            params["deg"] = degree_filter
            if program_id:
                wh.append("b.program_id = :pid")
                params["pid"] = program_id
                wh.append("p.degree_code = :deg")
            elif degree_filter:
                wh.append("(p.degree_code = :deg OR b.degree_code = :deg)")
        where = (" WHERE " + " AND ".join(wh)) if wh else ""
        with _engine.begin() as conn:
            rows = conn.execute(sa_text(f"""
                SELECT b.id, b.branch_code, b.branch_name, p.program_code, p.degree_code,
                       b.active, b.sort_order, b.logo_file_name, b.description
                  FROM branches b
                  LEFT JOIN programs p ON p.id=b.program_id
                {where}
                 ORDER BY p.degree_code, p.program_code, b.sort_order, lower(b.branch_code)
            """), params).fetchall()
        cols = ["id","branch_code","branch_name","program_code","degree_code",
                "active","sort_order","logo_file_name","description"]
    
    elif has_pid:
        wh = []
        if program_id:
            wh.append("b.program_id=:pid"); params["pid"] = program_id
        if degree_filter:
            wh.append("p.degree_code=:deg"); params["deg"] = degree_filter
        where = (" WHERE " + " AND ".join(wh)) if wh else ""
        with _engine.begin() as conn:
            rows = conn.execute(sa_text(f"""
                SELECT b.id, b.branch_code, b.branch_name, p.program_code, p.degree_code,
                       b.active, b.sort_order, b.logo_file_name, b.description
                  FROM branches b
                  LEFT JOIN programs p ON p.id=b.program_id
                {where}
                 ORDER BY p.degree_code, p.program_code, b.sort_order, lower(b.branch_code)
            """), params).fetchall()
        cols = ["id","branch_code","branch_name","program_code","degree_code",
                "active","sort_order","logo_file_name","description"]
    
    elif has_deg:
        wh = []
        if degree_filter:
            wh.append("degree_code=:deg"); params["deg"] = degree_filter
        where = (" WHERE " + " AND ".join(wh)) if wh else ""
        with _engine.begin() as conn:
            rows = conn.execute(sa_text(f"""
                SELECT id, branch_code, branch_name, degree_code,
                       active, sort_order, logo_file_name, description
                  FROM branches
                {where}
                 ORDER BY degree_code, sort_order, lower(branch_code)
            """), params).fetchall()
        cols = ["id","branch_code","branch_name","degree_code",
                "active","sort_order","logo_file_name","description"]
    else:
        return pd.DataFrame(columns=["id","branch_code","branch_name","active","sort_order","logo_file_name","description"])
    
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame([dict(r._mapping) for r in rows], columns=cols)


def _fetch_program_by_code(conn, degree_code: str, program_code: str):
    """Fetches a single program by its degree and code."""
    return conn.execute(sa_text("""
        SELECT * FROM programs WHERE degree_code = :dc AND program_code = :pc
    """), {"dc": degree_code, "pc": program_code}).fetchone()


def _fetch_branch_by_code(conn, degree_code: str, branch_code: str):
    """Fetches a single branch by its degree and code."""
    bcols = _table_cols(conn.engine, "branches")
    if "degree_code" in bcols:
        return conn.execute(sa_text("""
            SELECT * FROM branches WHERE degree_code = :dc AND branch_code = :bc
        """), {"dc": degree_code, "bc": branch_code}).fetchone()
    else:
        return conn.execute(sa_text("""
            SELECT b.* FROM branches b
            LEFT JOIN programs p ON p.id = b.program_id
            WHERE p.degree_code = :dc AND b.branch_code = :bc
        """), {"dc": degree_code, "bc": branch_code}).fetchone()


def _program_id_by_code(conn, degree_code: str, program_code: str) -> int | None:
    """Finds a program's primary key (id) from its code and degree."""
    row = conn.execute(sa_text("""
        SELECT id FROM programs
         WHERE degree_code=:d AND lower(program_code)=lower(:pc)
         LIMIT 1
    """), {"d": degree_code, "pc": program_code}).fetchone()
    return int(row.id) if row else None


@st.cache_data
def _curriculum_groups_df(_engine: Engine, degree_filter: str):
    with _engine.begin() as conn:
        rows = conn.execute(sa_text("""
            SELECT id, group_code, group_name, kind, active, sort_order, description
              FROM curriculum_groups
             WHERE degree_code=:d
             ORDER BY sort_order, group_code
        """), {"d": degree_filter}).fetchall()
    return pd.DataFrame([dict(r._mapping) for r in rows]) if rows else pd.DataFrame()


@st.cache_data
def _curriculum_group_links_df(_engine: Engine, degree_filter: str):
    with _engine.begin() as conn:
        rows = conn.execute(sa_text("""
            SELECT cgl.id, cg.group_code, cgl.program_code, cgl.branch_code
              FROM curriculum_group_links cgl
              JOIN curriculum_groups cg ON cg.id = cgl.group_id
             WHERE cgl.degree_code = :d
             ORDER BY cg.group_code, cgl.program_code, cgl.branch_code
        """), {"d": degree_filter}).fetchall()
    return pd.DataFrame([dict(r._mapping) for r in rows]) if rows else pd.DataFrame()


@st.cache_data
def _get_approvals_df(_engine: Engine, object_types: list[str]):
    """Fetches approval requests for specific object types."""
    cols = _table_cols(_engine, "approvals")
    select_cols = ["id", "object_type", "object_id", "action", "status"]
    if "requester_email" in cols:
        select_cols.append("requester_email")
    elif "requester" in cols:
        select_cols.append("requester AS requester_email")
    if "reason_note" in cols:
        select_cols.append("reason_note")
    if "requested_at" in cols:
        select_cols.append("requested_at")
    if "decided_at" in cols:
        select_cols.append("decided_at")
    if "decider_email" in cols:
        select_cols.append("decider_email")
    
    placeholders = ", ".join([f"'{t}'" for t in object_types])
    order_by = "ORDER BY id DESC"
    if "requested_at" in cols:
        order_by = "ORDER BY requested_at DESC"
    
    with _engine.begin() as conn:
        rows = conn.execute(sa_text(f"""
            SELECT {', '.join(select_cols)}
              FROM approvals
             WHERE object_type IN ({placeholders})
            {order_by}
        """)).fetchall()
    
    selected_final_cols = [c.split(" AS ")[-1] for c in select_cols]
    return pd.DataFrame([dict(r._mapping) for r in rows], columns=selected_final_cols) if rows else pd.DataFrame(columns=selected_final_cols)


def _get_semester_binding(conn, degree_code: str) -> str | None:
    row = conn.execute(sa_text("SELECT binding_mode FROM semester_binding WHERE degree_code=:dc"), {"dc": degree_code}).fetchone()
    return row.binding_mode if row else None


def _get_degree_struct(conn, degree_code: str) -> tuple | None:
    row = conn.execute(sa_text("SELECT years, terms_per_year FROM degree_semester_struct WHERE degree_code=:k"), {"k": degree_code}).fetchone()
    return (row.years, row.terms_per_year) if row else None


def _get_program_structs_for_degree(conn, degree_code: str) -> dict:
    rows = conn.execute(sa_text("""
        SELECT p.program_code, s.years, s.terms_per_year
          FROM programs p
          JOIN program_semester_struct s ON p.id = s.program_id
         WHERE p.degree_code = :dc
    """), {"dc": degree_code}).fetchall()
    return {r.program_code: (r.years, r.terms_per_year) for r in rows}


def _get_branch_structs_for_degree(conn, degree_code: str) -> dict:
    q = """
        SELECT b.branch_code, s.years, s.terms_per_year
          FROM branches b
          JOIN branch_semester_struct s ON b.id = s.branch_id
    """
    if 'degree_code' in _table_cols(conn.engine, 'branches'):
        q += " WHERE b.degree_code = :dc"
    else:
        q += " JOIN programs p ON p.id = b.program_id WHERE p.degree_code = :dc"
    
    rows = conn.execute(sa_text(q), {"dc": degree_code}).fetchall()
    return {r.branch_code: (r.years, r.terms_per_year) for r in rows}
