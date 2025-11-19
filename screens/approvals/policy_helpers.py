# screens/approvals/policy_helpers.py
import json
import streamlit as st
from sqlalchemy import text as sa_text
from .schema_helpers import _table_exists, _cols

# --- MODIFIED IMPORTS ---
# Import from the NEW enhanced policy engine (from Batch 1)
from core.approvals_policy import (
    can_user_approve,
    approver_roles as get_approver_set, # Renamed for clarity
    rule as get_approval_rule
)
# --- END MODIFICATIONS ---

def _allowed_to_act(
    engine, 
    user_email: str, 
    roles_set: set[str], 
    row: dict
) -> tuple[bool, set[str], str]:
    """
    Ask the NEW enhanced policy if this user can approve this item.
    """
    object_type = row["object_type"]
    action = row["action"]
    
    # Try to get scope from payload for more specific checks
    payload = {}
    if row.get("payload"):
        try:
            payload = json.loads(row["payload"]) or {}
        except json.JSONDecodeError:
            pass
            
    degree_code = payload.get("degree_code")
    program_code = payload.get("program_code")
    branch_code = payload.get("branch_code")

    # 1. Check eligibility using the NEW policy function
    eligible = can_user_approve(
        engine,
        user_email,
        roles_set,
        object_type,
        action,
        degree=degree_code,
        program=program_code,
        branch=branch_code
    )
    
    # 2. Get the set of approvers for display
    # (This will return user emails OR roles)
    approver_set = get_approver_set(
        engine, 
        object_type, 
        action,
        degree=degree_code,
        program=program_code,
        branch=branch_code
    )
    
    # 3. Get the rule for display
    rule = get_approval_rule(
        engine,
        object_type,
        action,
        degree=degree_code
    ) or "either_one"
    
    return eligible, approver_set, rule


def _record_vote_and_finalize(engine, approval_id: int, decision: str, actor_email: str, note: str):
    """
    Record a vote and finalize approval status.
    (This function was already correct and compatible)
    """
    d_norm = (decision or "").strip().lower()
    vote_val = "approve" if d_norm in ("approve", "approved") else "reject"
    status_val = "approved" if vote_val == "approve" else "rejected"

    with engine.begin() as conn:
        # record the vote if table/cols exist
        cols = _cols(conn, "approvals_votes") if _table_exists(conn, "approvals_votes") else set()
        if {"approval_id","voter_email","decision","note"}.issubset(cols):
            conn.execute(sa_text("""
                INSERT INTO approvals_votes(approval_id, voter_email, decision, note)
                VALUES (:aid, :actor, :dec, :note)
            """), {"aid": approval_id, "actor": actor_email, "dec": vote_val, "note": note})

        # Update the main approval record
        cols = _cols(conn, "approvals")
        update_clauses = ["status=:st", "approver=:actor", "decided_at=CURRENT_TIMESTAMP"]
        params = {"st": status_val, "actor": actor_email, "id": approval_id}

        if "decision_note" in cols:
            update_clauses.append("decision_note=:note")
            params["note"] = note
        
        conn.execute(sa_text(f"""
            UPDATE approvals
               SET {', '.join(update_clauses)}
             WHERE id=:id
        """), params)
