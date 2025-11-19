"""
Audit and approval helper functions for Programs/Branches module
"""
import json
from typing import Tuple
from sqlalchemy import text as sa_text
from .db_helpers import _table_cols


def _audit_program(conn, action: str, actor: str, row: dict, note: str = ""):
    cols = _table_cols(conn.engine, "programs_audit")
    audit_row = {k: v for k, v in row.items() if k != 'id'}
    payload = { "action": action, "actor": actor, "note": note, **audit_row }
    fields = [k for k in payload.keys() if k in cols]
    params = {k: payload[k] for k in fields}
    conn.execute(sa_text(
        f"INSERT INTO programs_audit({', '.join(fields)}) VALUES({', '.join(':'+f for f in fields)})"
    ), params)


def _audit_branch(conn, action: str, actor: str, row: dict, note: str = ""):
    cols = _table_cols(conn.engine, "branches_audit")
    audit_row = {k: v for k, v in row.items() if k != 'id'}
    payload = { "action": action, "actor": actor, "note": note, **audit_row }
    fields = [k for k in payload.keys() if k in cols]
    params = {k: payload[k] for k in fields}
    conn.execute(sa_text(
        f"INSERT INTO branches_audit({', '.join(fields)}) VALUES({', '.join(':'+f for f in fields)})"
    ), params)


def _audit_curriculum_group(conn, action: str, actor: str, row: dict, note: str = ""):
    cols = _table_cols(conn.engine, "curriculum_groups_audit")
    audit_row = {k: v for k, v in row.items() if k != 'id'}
    payload = { "action": action, "actor": actor, "note": note, **audit_row }
    fields = [k for k in payload.keys() if k in cols]
    params = {k: payload[k] for k in fields}
    conn.execute(sa_text(
        f"INSERT INTO curriculum_groups_audit({', '.join(fields)}) VALUES({', '.join(':'+f for f in fields)})"
    ), params)


def _audit_curriculum_group_link(conn, action: str, actor: str, row: dict, note: str = ""):
    cols = _table_cols(conn.engine, "curriculum_group_links_audit")
    if not cols: return
    audit_row = {k: v for k, v in row.items() if k != 'id'}
    payload = { "action": action, "actor": actor, "note": note, **audit_row }
    fields = [k for k in payload.keys() if k in cols]
    params = {k: payload[k] for k in fields}
    conn.execute(sa_text(
        f"INSERT INTO curriculum_group_links_audit({', '.join(fields)}) VALUES({', '.join(':'+f for f in fields)})"
    ), params)


def _approvals_columns(conn) -> set[str]:
    return _table_cols(conn.engine, "approvals")


def _queue_approval(conn, *, object_type: str, object_id: str, action: str,
                    requester_email: str | None, reason_note: str, 
                    rule_value: str | None = None, payload: dict | None = None):
    """Queue an approval request in the approvals table."""
    cols = _approvals_columns(conn)
    fields = ["object_type", "object_id", "action", "status"]
    params = {
        "object_type": object_type, 
        "object_id": object_id, 
        "action": action, 
        "status": "pending"
    }
    
    if "requester_email" in cols and requester_email:
        fields.append("requester_email")
        params["requester_email"] = requester_email
    elif "requester" in cols and requester_email:
        fields.append("requester")
        params["requester"] = requester_email
    
    if "rule" in cols and rule_value:
        fields.append("rule")
        params["rule"] = rule_value
    
    if "reason_note" in cols:
        fields.append("reason_note")
        params["reason_note"] = reason_note
    
    if "payload" in cols and payload:
        fields.append("payload")
        params["payload"] = json.dumps(payload)
    
    conn.execute(sa_text(
        f"INSERT INTO approvals({', '.join(fields)}) VALUES({', '.join(':'+f for f in fields)})"
    ), params)


def _request_deletion(
    conn,
    *,
    object_type: str,
    object_id: str | int,
    actor: str,
    audit_function: callable,
    audit_row: dict,
    reason_note: str,
    rule_value: str | None = "either_one",
    additional_payload: dict | None = None
) -> Tuple[bool, Exception | None]:
    """Universal handler to queue an item for deletion via the approvals table."""
    try:
        payload = {}
        
        if object_type == "program":
            payload["program_code"] = audit_row.get("program_code")
            payload["cascade"] = True
            payload["degree_code"] = audit_row.get("degree_code")
        
        elif object_type == "branch":
            payload["branch_code"] = audit_row.get("branch_code")
            payload["branch_id"] = object_id
            payload["degree_code"] = audit_row.get("degree_code")
            payload["program_code"] = audit_row.get("program_code")
        
        elif object_type == "curriculum_group":
            payload["group_code"] = audit_row.get("group_code")
            payload["degree_code"] = audit_row.get("degree_code")
            payload["group_id"] = object_id
        
        if additional_payload:
            payload.update(additional_payload)
        
        _queue_approval(
            conn,
            object_type=object_type,
            object_id=str(object_id),
            action="delete",
            requester_email=actor,
            reason_note=reason_note,
            rule_value=rule_value,
            payload=payload
        )
        
        audit_payload = {k: v for k, v in audit_row.items() if k != 'id'}
        
        audit_function(
            conn,
            action="delete_request",
            actor=actor,
            row=audit_payload,
            note="Approval requested"
        )
        
        return True, None
        
    except Exception as e:
        return False, e
