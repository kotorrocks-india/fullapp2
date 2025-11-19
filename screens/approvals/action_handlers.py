# action_handlers.py

import json
from typing import Dict, Any, Optional

from sqlalchemy import text as sa_text


from .action_registry import register_action_handler
from .cascade_handlers import (
    _program_children_counts,
    _program_delete_cascade,
    _degree_delete_cascade,
    _rebuild_semesters_for_approval,
    _faculty_delete_cascade,  # NEW: cascade helper for faculty
)
from .schema_helpers import _table_exists, _has_col
from . import data_loader as dl
from screens.office_admin import db as odb
from screens.outcomes.helpers import update_outcome_item as _update_outcome_item


# Academic Year helpers (soft import: don't break if module missing)
try:
    from screens.academic_years.db import (
        update_ay_status as _ay_update_status,
        delete_ay as _ay_delete,
    )
except Exception:
    def _ay_update_status(conn, ay_code, new_status, actor="system", reason=None):
        # Minimal fallback if the AY module can't be imported
        conn.execute(
            sa_text(
                "UPDATE academic_years "
                "SET status=:st, updated_at=CURRENT_TIMESTAMP "
                "WHERE ay_code=:c"
            ),
            {"st": new_status, "c": ay_code},
        )

    def _ay_delete(conn, ay_code, actor="system"):
        conn.execute(sa_text("DELETE FROM academic_years WHERE ay_code=:c"), {"c": ay_code})



# ───────────────────────────────────────────────────────────────────────────────
# Utility: look up handlers
# ───────────────────────────────────────────────────────────────────────────────

_HANDLER_REGISTRY: Dict[str, Dict[str, Any]] = {}


def get_action_handler(object_type: str, action: str):
    otype = (object_type or "").strip().lower()
    act = (action or "").strip().lower()
    if otype not in _HANDLER_REGISTRY or act not in _HANDLER_REGISTRY[otype]:
        raise ValueError(f"No handler registered for {otype}.{act}")
    return _HANDLER_REGISTRY[otype][act]


def register_action_handler_key(otype: str, action: str, fn):
    o = (otype or "").strip().lower()
    a = (action or "").strip().lower()
    _HANDLER_REGISTRY.setdefault(o, {})[a] = fn


# This is a wrapper over the imported decorator so that we also fill the local
# registry.
def register_action_handler(otype: str, action: str):
    def _decorator(fn):
        register_action_handler_key(otype, action, fn)
        return fn

    return _decorator


# ───────────────────────────────────────────────────────────────────────────────
# DEGREE delete
# ───────────────────────────────────────────────────────────────────────────────


@register_action_handler("degree", "delete")
def handle_degree_delete(conn, object_id: str, payload: dict) -> None:
    """
    Handle a degree deletion approval.
    payload may include:
      - cascade: bool
      - allow_delete_if_children: bool (legacy)
    """
    degree_code = str(object_id).strip()
    if not degree_code:
        raise ValueError("Degree delete requires degree_code as object_id")

    # If we want to check child entities before deleting:
    cascade = bool((payload or {}).get("cascade", False))
    allow_delete_if_children = bool((payload or {}).get("allow_delete_if_children", False))

    if cascade:
        _degree_delete_cascade(conn, degree_code)
    else:
        # Optionally, we can check for children here; for now, we do a direct delete.
        conn.execute(
            sa_text("DELETE FROM degrees WHERE LOWER(code)=LOWER(:dc)"),
            {"dc": degree_code},
        )


# ───────────────────────────────────────────────────────────────────────────────
# PROGRAM delete
# ───────────────────────────────────────────────────────────────────────────────


@register_action_handler("program", "delete")
def handle_program_delete(conn, object_id: str, payload: dict) -> None:
    """
    Handle program deletion approvals.
    object_id may be either the numeric programs.id or the program_code string.
    payload may include:
      - cascade: bool
      - allow_delete_if_children: bool (legacy)
    """
    # Determine how to interpret object_id:
    # 1. If it's purely numeric, treat as programs.id
    # 2. Otherwise, treat as program_code
    cascade = bool((payload or {}).get("cascade", False))
    allow_delete_if_children = bool((payload or {}).get("allow_delete_if_children", False))

    # First, figure out if we have a numeric primary key
    has_pk = _has_col(conn, "programs", "id")
    program_id = None
    program_code = None

    oid_str = str(object_id).strip()

    if has_pk and oid_str.isdigit():
        program_id = int(oid_str)
        row = conn.execute(
            sa_text("SELECT program_code FROM programs WHERE id=:id"), {"id": program_id}
        ).fetchone()
        if not row:
            raise ValueError(f"Program with id={program_id} not found")
        program_code = row[0]
    else:
        program_code = oid_str

    # If we need to check children before deleting:
    if not allow_delete_if_children:
        counts = _program_children_counts(conn, program_code)
        total_children = sum(counts.values())
    else:
        total_children = 0

    if total_children > 0 and allow_delete_if_children is False:
        # If we disallow deletion when children exist, we can raise:
        raise ValueError(
            f"Cannot delete program '{program_code}' because it has dependent records: {counts}"
        )

    # At this point we either have no children or we allow cascade.
    if total_children > 0 and allow_delete_if_children:
        _program_delete_cascade(conn, program_code)
    else:
        conn.execute(
            sa_text("DELETE FROM programs WHERE LOWER(program_code)=LOWER(:pc)"),
            {"pc": program_code},
        )


@register_action_handler("branch", "delete")
def handle_branch_delete(conn, object_id: str, payload: dict) -> None:
    """
    Handle branch deletion, by numeric ID (if branches.id exists) or by branch_code
    (if only that is available).
    """
    oid_str = str(object_id).strip()
    if not oid_str:
        raise ValueError("Branch delete requires a non-empty object_id")

    # We will attempt to discover if there is a numeric PK.
    has_pk = _has_col(conn, "branches", "id")

    if has_pk and oid_str.isdigit():
        bid = int(oid_str)
        conn.execute(
            sa_text("DELETE FROM branches WHERE id=:id"),
            {"id": bid},
        )
    else:
        # fallback: treat as branch_code
        conn.execute(
            sa_text("DELETE FROM branches WHERE LOWER(branch_code)=LOWER(:bc)"),
            {"bc": oid_str},
        )

@register_action_handler("curriculum_group", "delete")
def handle_curriculum_group_delete(conn, object_id: str, payload: dict) -> None:
    """Handle curriculum group delete via approvals.

    We delete the curriculum_groups row and any curriculum_group_links
    that reference it. object_id is usually curriculum_groups.id (numeric),
    but we also support group_code as a fallback.
    """
    oid = str(object_id).strip()
    if not oid:
        raise ValueError("Curriculum group delete requires a non-empty object_id")

    # Prefer numeric primary key if present
    has_pk = _has_col(conn, "curriculum_groups", "id")

    if has_pk and oid.isdigit():
        gid = int(oid)

        # Delete links first (if the links table exists)
        if _table_exists(conn, "curriculum_group_links"):
            conn.execute(
                sa_text("DELETE FROM curriculum_group_links WHERE group_id = :gid"),
                {"gid": gid},
            )

        # Delete the group itself
        conn.execute(
            sa_text("DELETE FROM curriculum_groups WHERE id = :gid"),
            {"gid": gid},
        )

    else:
        # Fallback: treat object_id as group_code
        group_code = oid

        # Resolve group IDs matching this code
        rows = conn.execute(
            sa_text(
                "SELECT id FROM curriculum_groups "
                "WHERE LOWER(group_code)=LOWER(:gc)"
            ),
            {"gc": group_code},
        ).fetchall()
        ids = [r[0] for r in rows] if rows else []

        # Delete links for those groups (if links table exists)
        if ids and _table_exists(conn, "curriculum_group_links"):
            conn.execute(
                sa_text(
                    "DELETE FROM curriculum_group_links "
                    f"WHERE group_id IN ({', '.join(str(i) for i in ids)})"
                )
            )

        # Delete the groups by code
        conn.execute(
            sa_text(
                "DELETE FROM curriculum_groups "
                "WHERE LOWER(group_code)=LOWER(:gc)"
            ),
            {"gc": group_code},
        )




# ───────────────────────────────────────────────────────────────────────────────
# SUBJECT delete
# ───────────────────────────────────────────────────────────────────────────────


@register_action_handler("subject", "delete")
def handle_subject_delete(conn, object_id: str, payload: dict) -> None:
    """
    Handle subject delete by subject_code. If the table has a numeric ID as well,
    we only use subject_code to match existing code paths.
    """
    subject_code = str(object_id).strip()
    if not subject_code:
        raise ValueError("Subject delete requires subject_code as object_id")

    conn.execute(
        sa_text("DELETE FROM subjects WHERE LOWER(subject_code)=LOWER(:sc)"),
        {"sc": subject_code},
    )


# ───────────────────────────────────────────────────────────────────────────────
# OFFICE ADMIN: delete student, export
# ───────────────────────────────────────────────────────────────────────────────


@register_action_handler("office_admin", "delete_student")
def handle_office_delete_student(conn, object_id: str, payload: dict) -> None:
    """
    Handle deletes for student records.
    object_id is assumed to be the student_id (numeric ID or unique code).
    """
    student_id = str(object_id).strip()
    if not student_id:
        raise ValueError("Student delete requires a student identifier")

    # We'll try numeric ID first, else fallback to student_code.
    if student_id.isdigit() and _has_col(conn, "students", "id"):
        conn.execute(
            sa_text("DELETE FROM students WHERE id=:id"),
            {"id": int(student_id)},
        )
    else:
        # fallback: treat as student_code
        conn.execute(
            sa_text("DELETE FROM students WHERE LOWER(student_code)=LOWER(:sc)"),
            {"sc": student_id.lower()},
        )


@register_action_handler("office_admin", "export_data")
def handle_office_export_data(conn, object_id: str, payload: dict) -> None:
    """
    Handle an office data export request once it is approved.
    object_id is the export request ID or code.
    """
    # We assume the export request is already created in some table,
    # e.g., "office_exports", and that "office_data.approve_export"
    # will handle both status updates and any further side-effects.
    odb.approve_export(conn, object_id)


@register_action_handler("office_admin", "export_request")
def handle_office_export_request(conn, object_id: str, payload: dict) -> None:
    """
    Handle an export request approval when the export was requested via an
    "export_requests" table.
    This function illustrates the pattern for hooking into your own logic.
    """
    # Example: mark export as approved in your own office_data module
    odb.mark_export_request_approved(conn, object_id)


@register_action_handler("academic_year", "status_change")
def handle_academic_year_status_change(conn, object_id: str, payload: dict) -> None:
    """
    On approval: actually change the status of an Academic Year.

    object_id: ay_code
    payload:  {"from": "...", "to": "...", "reason": "...", "requested_by": "...", ...}
    """
    ay_code = (object_id or "").strip()
    if not ay_code:
        raise ValueError("academic_year.status_change requires ay_code as object_id")

    new_status = (payload or {}).get("to") or (payload or {}).get("new_status")
    if not new_status:
        raise ValueError("Payload must include 'to' or 'new_status' for academic_year.status_change")

    actor = (payload or {}).get("requested_by") or (payload or {}).get("requester_email") or "system"
    reason = (payload or {}).get("reason")

    _ay_update_status(conn, ay_code, new_status, actor=actor, reason=reason)


@register_action_handler("academic_year", "delete")
def handle_academic_year_delete(conn, object_id: str, payload: dict) -> None:
    """
    On approval: delete the Academic Year record.

    object_id: ay_code
    payload: can optionally include {"requested_by": "..."}
    """
    ay_code = (object_id or "").strip()
    if not ay_code:
        raise ValueError("academic_year.delete requires ay_code as object_id")

    actor = (payload or {}).get("requested_by") or (payload or {}).get("requester_email") or "system"
    _ay_delete(conn, ay_code, actor=actor)


# ───────────────────────────────────────────────────────────────────────────────
# SEMESTERS: binding and structure changes
# ───────────────────────────────────────────────────────────────────────────────


@register_action_handler("semesters", "binding_change")
def handle_binding_change(conn, degree_code: str, payload: dict) -> None:
    """
    Handle semester binding mode changes at the degree level and rebuild if requested.
    payload may contain: {from, to|new_binding|binding_mode, auto_rebuild}
    """
    from_binding = (payload or {}).get("from")
    to_binding = (payload or {}).get("to")
    if to_binding is None:
        to_binding = (payload or {}).get("new_binding") or (payload or {}).get(
            "binding_mode"
        ) or "degree"

    if to_binding not in ["degree", "program", "branch"]:
        raise ValueError(f"Invalid binding mode: {to_binding}")

    conn.execute(
        sa_text(
            """
            INSERT INTO semester_binding(degree_code, binding_mode, label_mode)
            VALUES(:dc, :bm, COALESCE(
                (SELECT label_mode FROM semester_binding WHERE degree_code=:dc),
                'year_term'
            ))
            ON CONFLICT(degree_code) DO UPDATE SET
                binding_mode=excluded.binding_mode,
                updated_at=CURRENT_TIMESTAMP
            """
        ),
        {"dc": degree_code, "bm": to_binding},
    )

    # optional auto-rebuild
    auto_rebuild = bool((payload or {}).get("auto_rebuild", False))
    if auto_rebuild:
        row = conn.execute(
            sa_text(
                "SELECT binding_mode, label_mode FROM semester_binding WHERE degree_code=:dc"
            ),
            {"dc": degree_code},
        ).fetchone()
        if row:
            binding_mode, label_mode = row
            _rebuild_semesters_for_approval(conn, degree_code, binding_mode, label_mode)


@register_action_handler("semesters", "edit_structure")
def handle_structure_edit(conn, target_key: str, payload: dict) -> None:
    """
    Handle semester structure edits against degree/program/branch structure tables,
    then rebuild the semesters for the affected degree.

    target_key format: "degree:DEGREE_CODE" | "program:PROGRAM_ID" | "branch:BRANCH_ID"
    payload: {years_to, tpy_to}
    """
    if ":" not in target_key:
        return

    target, key = target_key.split(":", 1)
    table_map = {
        "degree": "degree_semester_struct",
        "program": "program_semester_struct",
        "branch": "branch_semester_struct",
    }

    if target not in table_map:
        return

    years_to = (payload or {}).get("years_to")
    tpy_to = (payload or {}).get("tpy_to")
    if not years_to or not tpy_to:
        return

    table = table_map[target]
    key_col = "degree_code" if target == "degree" else f"{target}_id"

    conn.execute(
        sa_text(
            f"""
            INSERT INTO {table}({key_col}, years, terms_per_year, active)
            VALUES(:k, :y, :t, 1)
            ON CONFLICT({key_col}) DO UPDATE SET
                years=excluded.years,
                terms_per_year=excluded.terms_per_year,
                active=1,
                updated_at=CURRENT_TIMESTAMP
            """
        ),
        {"k": key, "y": int(years_to), "t": int(tpy_to)},
    )

    # Figure out which degree we are under, then rebuild semesters for that degree.
    degree_code = None
    if target == "degree":
        degree_code = key
    else:
        if target == "program":
            row = conn.execute(
                sa_text("SELECT degree_code FROM programs WHERE id=:id"), {"id": key}
            ).fetchone()
            degree_code = row[0] if row else None
        elif target == "branch":
            row = conn.execute(
                sa_text("SELECT degree_code FROM branches WHERE id=:id"), {"id": key}
            ).fetchone()
            degree_code = row[0] if row else None

    if degree_code:
        binding_row = conn.execute(
            sa_text(
                "SELECT binding_mode, label_mode FROM semester_binding WHERE degree_code=:dc"
            ),
            {"dc": degree_code},
        ).fetchone()
        if binding_row:
            binding_mode, label_mode = binding_row
            _rebuild_semesters_for_approval(conn, degree_code, binding_mode, label_mode)


@register_action_handler("semesters", "rebuild_semesters")
def handle_semesters_rebuild(conn, target_key: str, payload: dict) -> None:
    """
    Handle a targeted semester rebuild for a single degree/program/branch.

    This is triggered when a structure change for a specific target has *no*
    existing semesters and the user explicitly requests a rebuild, but the
    request goes through the approvals engine.

    We rebuild semesters for the **whole degree**, which is consistent with
    the existing `_rebuild_semesters_for_approval` helper semantics.
    """
    if ":" not in target_key:
        return

    target, key = target_key.split(":", 1)

    # Resolve degree_code from the target/key pair
    degree_code = None
    if target == "degree":
        degree_code = key
    elif target == "program":
        row = conn.execute(
            sa_text("SELECT degree_code FROM programs WHERE id=:id"),
            {"id": key},
        ).fetchone()
        degree_code = row[0] if row else None
    elif target == "branch":
        # branches may or may not have a direct degree_code column
        if _has_col(conn, "branches", "degree_code"):
            row = conn.execute(
                sa_text("SELECT degree_code FROM branches WHERE id=:id"),
                {"id": key},
            ).fetchone()
            degree_code = row[0] if row else None
        else:
            row = conn.execute(
                sa_text(
                    "SELECT p.degree_code "
                    "FROM branches b JOIN programs p ON p.id = b.program_id "
                    "WHERE b.id=:id"
                ),
                {"id": key},
            ).fetchone()
            degree_code = row[0] if row else None

    if not degree_code:
        return

    # Look up current binding + label mode and rebuild via shared helper
    binding_row = conn.execute(
        sa_text(
            "SELECT binding_mode, label_mode "
            "FROM semester_binding WHERE degree_code=:dc"
        ),
        {"dc": degree_code},
    ).fetchone()
    if not binding_row:
        return

    binding_mode, label_mode = binding_row
    _rebuild_semesters_for_approval(conn, degree_code, binding_mode, label_mode)


@register_action_handler("semesters", "rebuild_all_semesters")
def handle_semesters_rebuild_all(conn, degree_code: str, payload: dict) -> None:
    """
    Handle a full rebuild of all semesters for a degree
    when the structure has changed and there are existing semesters.
    """
    # Prefer explicit values from the payload, fall back to the current binding row
    binding_mode = (payload or {}).get("binding_mode")
    label_mode = (payload or {}).get("label_mode")

    if not binding_mode or not label_mode:
        row = conn.execute(
            sa_text(
                "SELECT binding_mode, label_mode "
                "FROM semester_binding WHERE degree_code=:dc"
            ),
            {"dc": degree_code},
        ).fetchone()
        if not row:
            return
        binding_mode, label_mode = row

    _rebuild_semesters_for_approval(conn, degree_code, binding_mode, label_mode)


@register_action_handler("semesters", "clear_all_semesters")
def handle_semesters_clear_all(conn, degree_code: str, payload: dict) -> None:
    """
    Handle clearing all semesters for a degree.

    This is only invoked when there *are* existing semesters and the user
    submitted a clear-all request that required approval.
    """
    conn.execute(
        sa_text("DELETE FROM semesters WHERE degree_code=:dc"),
        {"dc": degree_code},
    )


# ───────────────────────────────────────────────────────────────────────────────
# AFFILIATION edit_in_use
# ───────────────────────────────────────────────────────────────────────────────


@register_action_handler("affiliation", "edit_in_use")
def handle_affiliation_edit(conn, affiliation_id: str, payload: dict) -> None:
    """
    Handle faculty affiliation edits while the affiliation is in use elsewhere.
    payload: {"updates": {field: value, ...}}
    """
    updates = (payload or {}).get("updates", {})
    if updates:
        set_clauses = []
        params = {"aff_id": int(affiliation_id)}
        for col, val in updates.items():
            set_clauses.append(f"{col} = :{col}")
            params[col] = val

        sql = f"""
            UPDATE faculty_affiliations
               SET {", ".join(set_clauses)},
                   updated_at = CURRENT_TIMESTAMP
             WHERE id = :aff_id
        """
        conn.execute(sa_text(sql), params)


# ───────────────────────────────────────────────────────────────────────────────
# SUBJECT edit (example)
# ───────────────────────────────────────────────────────────────────────────────


@register_action_handler("subject", "edit")
def handle_subject_edit(conn, object_id: str, payload: dict) -> None:
    """
    Example handler for subject edits. Not fully wired, but demonstrates pattern.
    payload may contain updated fields.
    """
    subject_code = str(object_id).strip()
    if not subject_code:
        raise ValueError("Subject edit requires subject_code as object_id")

    updates = (payload or {}).get("updates", {})
    if not updates:
        return

    set_clauses = []
    params = {"sc": subject_code}
    for col, val in updates.items():
        set_clauses.append(f"{col} = :{col}")
        params[col] = val

    sql = f"""
        UPDATE subjects
           SET {", ".join(set_clauses)},
               updated_at=CURRENT_TIMESTAMP
         WHERE LOWER(subject_code)=LOWER(:sc)
    """
    conn.execute(sa_text(sql), params)

@register_action_handler("outcome", "edit")
def handle_outcome_edit(conn, object_id: str, payload: dict) -> None:
    """
    Handle major outcome edits via approvals.

    - object_id: outcomes_items.id (item_id)
    - payload: {
        "set_id": ...,
        "degree_code": ...,
        "program_code": ...,
        "branch_code": ...,
        "code": ...,
        "before": {...},
        "after": {
            "title": ...,
            "description": ...,
            "bloom_level": ...,
            "timeline_years": ...,
            "tags": "...",
        },
        "change_type": "major",
        "reason": "...",
        "requested_by": "email@domain"
      }
    """
    oid = (object_id or "").strip()
    if not oid.isdigit():
        raise ValueError("Outcome edit approval requires numeric outcomes_items.id as object_id")

    item_id = int(oid)
    after = (payload or {}).get("after") or {}
    actor = (payload or {}).get("requested_by") or "system"

    title = after.get("title")
    description = after.get("description")
    bloom_level = after.get("bloom_level")
    years = after.get("timeline_years")
    tags = after.get("tags", "")

    # Reuse existing outcome update logic
    _update_outcome_item(
        conn,
        item_id,
        title,
        description,
        bloom_level,
        years,
        tags,
        actor,
    )





# ───────────────────────────────────────────────────────────────────────────────
# GENERIC: subject details update (example with payload filtering)
# ───────────────────────────────────────────────────────────────────────────────


@register_action_handler("semester", "edit_details")
def handle_semester_edit_details(conn, object_id: str, payload: dict) -> None:
    """
    Example: editing fields on the semesters table.
    object_id may be numeric ID or composite key; adjust logic as needed.
    payload => {title, start_date, end_date, status, active, sort_order, description}
    """
    oid = str(object_id).strip()
    if not oid:
        raise ValueError("Semester edit_details requires object_id")

    has_pk = _has_col(conn, "semesters", "id")

    # We will do a simple approach: if numeric and we have 'id', use that;
    # otherwise, if we have (degree_code, semester_no), we can use that.
    updates = (payload or {}).get("updates", {})
    if not updates:
        return

    allowed = {
        "title",
        "start_date",
        "end_date",
        "status",
        "active",
        "sort_order",
        "description",
    }
    safe = {k: v for k, v in (updates or {}).items() if k in allowed}

    if not safe:
        return

    set_clauses = [f"{col} = :{col}" for col in safe.keys()]
    params = dict(safe)

    if has_pk and oid.isdigit():
        params["id"] = int(oid)
        sql = f"""
            UPDATE semesters
               SET {", ".join(set_clauses)},
                   updated_at=CURRENT_TIMESTAMP
             WHERE id=:id
        """
        conn.execute(sa_text(sql), params)
    else:
        # Fallback: if we have degree_code + semester_no in payload:
        degree_code = (payload or {}).get("degree_code")
        semester_no = (payload or {}).get("semester_no")
        if not degree_code or not semester_no:
            raise ValueError(
                "Semester edit_details requires numeric id or (degree_code, semester_no)"
            )
        params["degree_code"] = degree_code
        params["semester_no"] = int(semester_no)
        sql = f"""
            UPDATE semesters
               SET {", ".join(set_clauses)},
                   updated_at=CURRENT_TIMESTAMP
             WHERE degree_code=:degree_code
               AND semester_no=:semester_no
        """
        conn.execute(sa_text(sql), params)


# ───────────────────────────────────────────────────────────────────────────────
# OFFICE ADMIN: export approval status update
# ───────────────────────────────────────────────────────────────────────────────


@register_action_handler("office_admin", "export_status_update")
def handle_office_export_status_update(conn, object_id: str, payload: dict) -> None:
    """
    Example handler to update export request status when an office_admin export request is approved in the central inbox.
    object_id will be the 'request_code'.
    """
    # Get the approver email from the payload (or use system)
    approver_email = payload.get("approved_by", "system_approved")

    # Call the existing db function to update the status
    odb.approve_export_request(conn, object_id, approver_email)


# ───────────────────────────────────────────────────────────────────────────────
# NEW: Faculty delete via Approvals
# ───────────────────────────────────────────────────────────────────────────────

def _resolve_faculty_id(conn, object_id: str, payload: dict) -> int:
    """
...
    """

    # Attempt direct numeric ID first
    if faculty_id_str.isdigit() and _has_col(conn, "faculty", "id"):
        return int(faculty_id_str)

    # Otherwise fall back to faculty_code
    row = conn.execute(
        sa_text("SELECT id FROM faculty WHERE faculty_code=:fc"),
        {"fc": faculty_id_str},
    ).fetchone()
    if not row:
        raise ValueError(f"Faculty with code {faculty_id_str} not found")
    return int(row[0])


@register_action_handler("faculty", "delete")
def handle_faculty_delete(conn, object_id: str, payload: dict) -> None:
    """
    Handle faculty delete via approvals, cascading to workloads, affiliations,
    and other dependent tables using the shared cascade helper.
    """
    faculty_id = _resolve_faculty_id(conn, object_id, payload)
    cascade = bool((payload or {}).get("cascade", True))

    if cascade:
        _faculty_delete_cascade(conn, faculty_id)
    else:
        conn.execute(sa_text("DELETE FROM faculty WHERE id=:id"), {"id": faculty_id})


# ───────────────────────────────────────────────────────────────────────────────
# MAIN dispatcher
# ───────────────────────────────────────────────────────────────────────────────


def perform_action(conn, approval_row: Dict[str, Any]) -> None:
    """
    Main entry point called by the approvals engine when an approval is approved.
    approval_row is the row dict from the approvals table.
    """
    row = approval_row
    otype = (row.get("object_type") or "").strip().lower()
    action = (row.get("action") or "").strip().lower()
    object_id = str(row.get("object_id") or "")

    raw = row.get("payload")
    payload = {}
    if raw:
        try:
            payload = json.loads(raw) or {}
        except Exception:
            payload = {}

    # Get and execute the appropriate handler
    handler = get_action_handler(otype, action)
    handler(conn, object_id, payload)

        # ✅ CRITICAL: Clear Streamlit cache after any database modification
    # This ensures all pages see fresh data after approvals
    try:
        import streamlit as st
        st.cache_data.clear()
    except Exception:
        # If streamlit not available (non-UI context), ignore
        pass
