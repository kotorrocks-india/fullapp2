# In screens/approvals/data_loader.py

import pandas as pd
from sqlalchemy import text as sa_text
from .schema_helpers import _has_col, _cols  # <-- MODIFIED IMPORT (added _cols)

def _fetch_open_approvals(engine) -> pd.DataFrame: #
    with engine.begin() as conn: #
        # Detect optional columns
        has_payload = _has_col(conn, "approvals", "payload") #
        db_cols = _cols(conn, "approvals") #
        
        # Decide which column to treat as the "note" for UI purposes
        if "note" in db_cols:
            note_expr = "note"
        elif "reason_note" in db_cols:
            note_expr = "reason_note"
        elif "decision_note" in db_cols:
            note_expr = "decision_note"
        else:
            note_expr = "''"
        
        select_cols = [
            "id",
            "object_type",
            "object_id",
            "action",
            "status",
            "requester",
            f"{note_expr} AS note",
            "created_at",
        ]
        if has_payload: #
            select_cols.append("payload") #
        
        base_cols = ["id", "object_type", "object_id", "action", "status", "requester", "note", "created_at"] #
        if has_payload:
            base_cols.append("payload") #
        
        sql = f"""
            SELECT {', '.join(select_cols)}
              FROM approvals
             WHERE status IN ('pending','under_review')
             ORDER BY created_at DESC, id DESC
        """
        rows = conn.execute(sa_text(sql)).fetchall() #
    
    if not rows: #
        return pd.DataFrame(columns=base_cols) #
    
    return pd.DataFrame([dict(r._mapping) for r in rows]) #

# --- 2. NEW FUNCTION TO ADD ---
def _fetch_completed_approvals(engine) -> pd.DataFrame:
    """Fetch all 'approved' and 'rejected' approvals for the audit log."""
    with engine.begin() as conn:
        db_cols = _cols(conn, "approvals")
        
        # Decide which column to treat as the "note" for UI purposes
        if "note" in db_cols:
            note_expr = "note"
        elif "reason_note" in db_cols:
            note_expr = "reason_note"
        elif "decision_note" in db_cols:
            note_expr = "decision_note"
        else:
            note_expr = "''"
        
        select_cols = [
            "id",
            "object_type",
            "object_id",
            "action",
            "status",
            "requester",
            f"{note_expr} AS note",
            "created_at",
        ]
        
        # Add optional columns if they exist in the database
        if "approver" in db_cols:
            select_cols.append("approver")
        if "decided_at" in db_cols:
            select_cols.append("decided_at")
        
        # Choose a safe ORDER BY depending on schema
        if "decided_at" in db_cols:
            order_by = "decided_at DESC, id DESC"
        else:
            order_by = "created_at DESC, id DESC"
            
        sql = f"""
            SELECT {', '.join(select_cols)}
              FROM approvals
             WHERE status IN ('approved', 'rejected')
             ORDER BY {order_by}
        """
        rows = conn.execute(sa_text(sql)).fetchall()
    
    if not rows:
        # Strip any " AS note" parts back to simple column labels
        return pd.DataFrame(columns=[c.split(" AS ")[-1] for c in select_cols])
    
    return pd.DataFrame([dict(r._mapping) for r in rows])
# --- END OF NEW FUNCTION ---

def get_affiliation_details(engine, affiliation_id: int) -> dict: #
    """Get details about an affiliation for display in approval UI.""" #
    with engine.begin() as conn: #
        row = conn.execute(sa_text("""
            SELECT fa.email, fa.degree_code, fa.branch_code, fa.designation, fa.type,
                   fp.name as faculty_name, d.name as degree_name
            FROM faculty_affiliations fa
            LEFT JOIN faculty_profiles fp ON fp.email = fa.email
            LEFT JOIN degrees d ON d.code = fa.degree_code
            WHERE fa.id = :aff_id
        """), {"aff_id": affiliation_id}).fetchone() #
        
        if row: #
            return dict(row._mapping) #
        return {} #
