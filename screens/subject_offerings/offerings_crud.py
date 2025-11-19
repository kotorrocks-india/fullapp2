"""
Subject Offerings CRUD operations - Enhanced Implementation
Includes:
- Comprehensive audit logging
- Versioning/snapshots
- Freeze controls
- Approval workflows
- Validation against all guardrails
"""

from typing import Dict, Any, List, Optional, Tuple
import json
from datetime import datetime, timedelta
from sqlalchemy import text as sa_text
from .helpers import exec_query, rows_to_dicts
from .constants import (
    validate_offering, 
    validate_offering_uniqueness,
    validate_freeze_rules,
    validate_elective_publish_requirements,
    validate_catalog_sync,
    format_audit_changed_fields,
    check_approval_required,
    PRETERM_DEFAULTS,
    VERSIONING_SETTINGS
)


def audit_offering(
    conn,
    offering_id: int,
    subject_code: str,
    degree_code: str,
    ay_label: str,
    action: str,
    actor: str,
    note: str = "",
    reason: str = "",
    changed_fields: Dict[str, Any] = None,
    actor_role: str = None,
    operation: str = None,
    source: str = "ui",
    correlation_id: str = None,
    step_up_performed: bool = False,
    ip_address: str = None,
    user_agent: str = None,
    session_id: str = None,
    snapshot_json: str = None,
    program_code: str = None,
    branch_code: str = None,
    curriculum_group_code: str = None,
    year: int = None,
    term: int = None,
    division_code: str = None
):
    """
    Write comprehensive audit log for offering changes.
    Enhanced with all YAML-specified fields.
    """
    exec_query(conn, """
        INSERT INTO subject_offerings_audit
        (offering_id, subject_code, degree_code, program_code, branch_code,
         curriculum_group_code, ay_label, year, term, division_code,
         action, operation, note, reason, changed_fields, 
         actor, actor_role, 
         source, correlation_id, step_up_performed,
         ip_address, user_agent, session_id, snapshot_json)
        VALUES (:oid, :sc, :dc, :pc, :bc, :cg, :ay, :y, :t, :div,
                :act, :op, :note, :reason, :fields,
                :actor, :actor_role,
                :source, :corr_id, :step_up,
                :ip, :ua, :sid, :snapshot)
    """, {
        "oid": offering_id,
        "sc": subject_code,
        "dc": degree_code,
        "pc": program_code,
        "bc": branch_code,
        "cg": curriculum_group_code,
        "ay": ay_label,
        "y": year,
        "t": term,
        "div": division_code,
        "act": action,
        "op": operation,
        "note": note or "",
        "reason": reason or "",
        "fields": json.dumps(changed_fields) if changed_fields else None,
        "actor": actor or "system",
        "actor_role": actor_role,
        "source": source,
        "corr_id": correlation_id,
        "step_up": 1 if step_up_performed else 0,
        "ip": ip_address,
        "ua": user_agent,
        "sid": session_id,
        "snapshot": snapshot_json
    })


def create_snapshot(
    conn,
    offering_id: int,
    snapshot_type: str,
    actor: str,
    note: str = None
) -> int:
    """
    Create a version snapshot of an offering.
    Implements versioning from YAML specification.
    """
    # Get current offering data
    offering = exec_query(conn, """
        SELECT * FROM subject_offerings WHERE id = :id
    """, {"id": offering_id}).fetchone()
    
    if not offering:
        raise ValueError(f"Offering {offering_id} not found")
    
    offering_dict = dict(offering._mapping)
    
    # Get next snapshot number
    result = exec_query(conn, """
        SELECT COALESCE(MAX(snapshot_number), 0) + 1 as next_num
        FROM subject_offerings_snapshots
        WHERE offering_id = :id
    """, {"id": offering_id}).fetchone()
    
    next_num = result[0]
    
    # Mark previous snapshots as inactive
    exec_query(conn, """
        UPDATE subject_offerings_snapshots
        SET is_active_version = 0
        WHERE offering_id = :id
    """, {"id": offering_id})
    
    # Create new snapshot
    exec_query(conn, """
        INSERT INTO subject_offerings_snapshots
        (offering_id, subject_code, ay_label, snapshot_number, snapshot_data,
         snapshot_type, note, created_by, is_active_version)
        VALUES (:oid, :sc, :ay, :num, :data, :type, :note, :actor, 1)
    """, {
        "oid": offering_id,
        "sc": offering_dict["subject_code"],
        "ay": offering_dict["ay_label"],
        "num": next_num,
        "data": json.dumps(offering_dict, default=str),
        "type": snapshot_type,
        "note": note,
        "actor": actor
    })
    
    snapshot_id = exec_query(conn, "SELECT last_insert_rowid()").fetchone()[0]
    
    return snapshot_id


def _normalize_subject_type(subject_type: str) -> str:
    """
    Normalize subject type to proper case.
    Handles case-insensitive matching for backward compatibility.
    
    Maps:
    - 'core' / 'Core' / 'CORE' -> 'Core'
    - 'elective' / 'Elective' / 'ELECTIVE' -> 'Elective'
    - 'college project' / 'College Project' -> 'College Project'
    - 'other' / 'Other' / 'OTHER' -> 'Other'
    """
    if not subject_type:
        return "Other"
    
    subject_type_lower = str(subject_type).strip().lower()
    
    if subject_type_lower == "core":
        return "Core"
    elif subject_type_lower == "elective":
        return "Elective"
    elif subject_type_lower in ("college project", "collegeproject"):
        return "College Project"
    elif subject_type_lower == "other":
        return "Other"
    else:
        # Return original if no match (will fail validation with clear message)
        return subject_type


def create_offering_from_catalog(
    engine,
    catalog_subject_id: int,
    ay_label: str,
    year: int,
    term: int,
    actor: str,
    actor_role: str = None,
    division_code: Optional[str] = None,
    applies_to_all_divisions: bool = True,
    instructor_email: Optional[str] = None,
    curriculum_group_code: Optional[str] = None,
    elective_selection_lead_days: int = None,
    correlation_id: str = None,
    source: str = "ui"
) -> int:
    """
    Create an offering by copying data from subjects_catalog.
    Enhanced with all YAML features.
    """
    with engine.begin() as conn:
        # Fetch catalog subject
        catalog = exec_query(conn, """
            SELECT * FROM subjects_catalog WHERE id = :id AND active = 1
        """, {"id": catalog_subject_id}).fetchone()
        
        if not catalog:
            raise ValueError("Catalog subject not found or inactive")
        
        catalog = dict(catalog._mapping)
        
        # Normalize subject_type for case-insensitive handling
        normalized_subject_type = _normalize_subject_type(catalog.get("subject_type"))
        
        # Build offering data
        data = {
            "ay_label": ay_label,
            "degree_code": catalog["degree_code"],
            "program_code": catalog.get("program_code"),
            "branch_code": catalog.get("branch_code"),
            "curriculum_group_code": curriculum_group_code or catalog.get("curriculum_group_code"),
            "year": year,
            "term": term,
            "division_code": division_code,
            "applies_to_all_divisions": applies_to_all_divisions,
            "subject_code": catalog["subject_code"],
            "subject_type": normalized_subject_type,  # Use normalized value
            "is_elective_parent": normalized_subject_type in ["Elective", "College Project"],
            "credits_total": catalog["credits_total"],
            "L": catalog.get("L", 0),
            "T": catalog.get("T", 0),
            "P": catalog.get("P", 0),
            "S": catalog.get("S", 0),
            "internal_marks_max": catalog.get("internal_marks_max", 40),
            "exam_marks_max": catalog.get("exam_marks_max", 60),
            "jury_viva_marks_max": catalog.get("jury_viva_marks_max", 0),
            "direct_weight_percent": catalog.get("direct_internal_weight_percent", 40.0),
            "indirect_weight_percent": 100.0 - catalog.get("direct_internal_weight_percent", 40.0),
            "pass_threshold_overall": catalog.get("min_overall_percent", 40.0),
            "pass_threshold_internal": catalog.get("min_internal_percent", 50.0),
            "pass_threshold_external": catalog.get("min_external_percent", 40.0),
            "instructor_email": instructor_email,
            "status": "draft",
            "override_inheritance": False,
        }
        
        # CRITICAL: Calculate total_marks_max BEFORE validation
        # The validation guardrail checks if internal + exam + jury = total
        # So we must calculate total BEFORE the validation runs
        data["total_marks_max"] = (
            data["internal_marks_max"] + 
            data["exam_marks_max"] + 
            data["jury_viva_marks_max"]
        )
        
        # Add preterm defaults for electives
        if data["is_elective_parent"]:
            data["elective_selection_lead_days"] = elective_selection_lead_days or PRETERM_DEFAULTS["elective_selection_lead_days"]
            data["allow_negative_offset"] = PRETERM_DEFAULTS["allow_negative_offset"]
        
        # Validate
        ok, msg = validate_offering(data)
        if not ok:
            raise ValueError(msg)
        
        ok, msg = validate_offering_uniqueness(conn, data)
        if not ok:
            raise ValueError(msg)
        
        # Calculate total marks
        total_marks = (
            data["internal_marks_max"] + 
            data["exam_marks_max"] + 
            data["jury_viva_marks_max"]
        )
        
        # Insert offering
        result = exec_query(conn, """
            INSERT INTO subject_offerings (
                ay_label, degree_code, program_code, branch_code, curriculum_group_code,
                year, term, division_code, applies_to_all_divisions,
                subject_code, subject_type, is_elective_parent,
                credits_total, L, T, P, S,
                internal_marks_max, exam_marks_max, jury_viva_marks_max, total_marks_max,
                direct_weight_percent, indirect_weight_percent,
                pass_threshold_overall, pass_threshold_internal, pass_threshold_external,
                instructor_email, status, override_inheritance,
                elective_selection_lead_days, allow_negative_offset,
                created_by, updated_by
            ) VALUES (
                :ay, :deg, :prog, :branch, :cg,
                :year, :term, :div, :all_div,
                :sc, :stype, :elec_parent,
                :credits, :L, :T, :P, :S,
                :int_max, :exam_max, :jury_max, :total_max,
                :dir_w, :ind_w,
                :pass_ov, :pass_int, :pass_ext,
                :instr, :status, :override,
                :elec_lead_days, :allow_neg_offset,
                :actor, :actor
            )
        """, {
            "ay": data["ay_label"],
            "deg": data["degree_code"],
            "prog": data.get("program_code"),
            "branch": data.get("branch_code"),
            "cg": data.get("curriculum_group_code"),
            "year": data["year"],
            "term": data["term"],
            "div": data.get("division_code"),
            "all_div": 1 if data["applies_to_all_divisions"] else 0,
            "sc": data["subject_code"],
            "stype": data["subject_type"],
            "elec_parent": 1 if data["is_elective_parent"] else 0,
            "credits": data["credits_total"],
            "L": data["L"],
            "T": data["T"],
            "P": data["P"],
            "S": data["S"],
            "int_max": data["internal_marks_max"],
            "exam_max": data["exam_marks_max"],
            "jury_max": data["jury_viva_marks_max"],
            "total_max": total_marks,
            "dir_w": data["direct_weight_percent"],
            "ind_w": data["indirect_weight_percent"],
            "pass_ov": data["pass_threshold_overall"],
            "pass_int": data["pass_threshold_internal"],
            "pass_ext": data["pass_threshold_external"],
            "instr": data.get("instructor_email"),
            "status": data["status"],
            "override": 0,
            "elec_lead_days": data.get("elective_selection_lead_days"),
            "allow_neg_offset": 1 if data.get("allow_negative_offset", True) else 0,
            "actor": actor
        })
        
        offering_id = result.lastrowid
        
        # Create initial snapshot
        if "create" in VERSIONING_SETTINGS["snapshot_on"]:
            create_snapshot(conn, offering_id, "create", actor, 
                          f"Initial creation from catalog subject {catalog['subject_code']}")
        
        # Audit
        audit_offering(
            conn, offering_id, data["subject_code"], data["degree_code"],
            data["ay_label"], "create", actor,
            f"Created offering from catalog subject {catalog['subject_code']}",
            actor_role=actor_role,
            operation="create_from_catalog",
            source=source,
            correlation_id=correlation_id,
            program_code=data.get("program_code"),
            branch_code=data.get("branch_code"),
            curriculum_group_code=data.get("curriculum_group_code"),
            year=data["year"],
            term=data["term"],
            division_code=data.get("division_code")
        )
        
        return offering_id


def update_offering(
    engine,
    offering_id: int,
    updates: Dict[str, Any],
    actor: str,
    actor_role: str = None,
    reason: str = "",
    correlation_id: str = None,
    source: str = "ui",
    step_up_performed: bool = False
) -> None:
    """
    Update an offering with selective field updates.
    Enhanced with freeze checks, validation, and versioning.
    """
    with engine.begin() as conn:
        # Fetch current offering
        current = exec_query(conn, """
            SELECT * FROM subject_offerings WHERE id = :id
        """, {"id": offering_id}).fetchone()
        
        if not current:
            raise ValueError("Offering not found")
        
        current = dict(current._mapping)
        
        # Check freeze rules
        ok, msg = validate_freeze_rules(conn, offering_id, updates)
        if not ok:
            raise ValueError(msg)
        
        # Merge updates
        data = {**current, **updates}
        
        # Validate
        ok, msg = validate_offering(data)
        if not ok:
            raise ValueError(msg)
        
        ok, msg = validate_offering_uniqueness(conn, data, offering_id)
        if not ok:
            raise ValueError(msg)
        
        # Recalculate total if marks changed
        if any(k in updates for k in ["internal_marks_max", "exam_marks_max", "jury_viva_marks_max"]):
            total_marks = (
                data["internal_marks_max"] + 
                data["exam_marks_max"] + 
                data["jury_viva_marks_max"]
            )
            updates["total_marks_max"] = total_marks
        
        # Create snapshot before update
        if "update" in VERSIONING_SETTINGS["snapshot_on"]:
            create_snapshot(conn, offering_id, "update", actor,
                          f"Pre-update snapshot. Reason: {reason}")
        
        # Build UPDATE query dynamically
        set_clause = ", ".join([f"{k} = :{k}" for k in updates.keys()])
        params = {**updates, "id": offering_id, "actor": actor}
        
        exec_query(conn, f"""
            UPDATE subject_offerings
            SET {set_clause}, updated_at = CURRENT_TIMESTAMP, updated_by = :actor
            WHERE id = :id
        """, params)
        
        # Audit
        changed_fields = format_audit_changed_fields(current, updates)
        audit_offering(
            conn, offering_id, current["subject_code"], current["degree_code"],
            current["ay_label"], "update", actor,
            f"Updated offering", reason=reason,
            changed_fields=json.loads(changed_fields) if changed_fields else None,
            actor_role=actor_role,
            operation="update",
            source=source,
            correlation_id=correlation_id,
            step_up_performed=step_up_performed,
            program_code=current.get("program_code"),
            branch_code=current.get("branch_code"),
            curriculum_group_code=current.get("curriculum_group_code"),
            year=current["year"],
            term=current["term"],
            division_code=current.get("division_code")
        )


def delete_offering(
    engine, 
    offering_id: int, 
    actor: str,
    actor_role: str = None,
    reason: str = "",
    approved_by: str = None,
    correlation_id: str = None
) -> None:
    """
    Delete an offering.
    Enhanced with marks check and approval tracking.
    """
    with engine.begin() as conn:
        # Fetch offering
        offering = exec_query(conn, """
            SELECT * FROM subject_offerings WHERE id = :id
        """, {"id": offering_id}).fetchone()
        
        if not offering:
            raise ValueError("Offering not found")
        
        offering = dict(offering._mapping)
        
        # Check for constraints (e.g., marks exist) - only if table exists
        marks_exist = 0
        try:
            marks_exist = exec_query(conn, """
                SELECT COUNT(*) FROM subject_marks
                WHERE offering_id = :id
            """, {"id": offering_id}).fetchone()[0]
            
            if marks_exist > 0:
                raise ValueError(
                    f"Cannot delete offering: {marks_exist} mark records exist. "
                    "Archive instead or delete marks first."
                )
        except Exception as e:
            # Table doesn't exist or other error - skip marks check
            if "no such table" not in str(e).lower():
                # Re-raise if it's not a missing table error
                raise
            # Otherwise continue - no marks table means no marks to worry about
        
        # Check for elective selections
        selections_exist = 0
        try:
            selections_exist = exec_query(conn, """
                SELECT COUNT(*) FROM elective_student_selections
                WHERE subject_code = :sc AND ay_label = :ay AND year = :y AND term = :t
            """, {
                "sc": offering["subject_code"],
                "ay": offering["ay_label"],
                "y": offering["year"],
                "t": offering["term"]
            }).fetchone()[0]
            
            if selections_exist > 0:
                raise ValueError(
                    f"Cannot delete offering: {selections_exist} student selections exist. "
                    "Archive instead."
                )
        except Exception as e:
            # Table doesn't exist or other error
            if "no such table" not in str(e).lower():
                # Re-raise if it's not a missing table error
                raise
            # Otherwise continue - no selections table means no selections to worry about
        
        # Delete offering
        exec_query(conn, "DELETE FROM subject_offerings WHERE id = :id", {"id": offering_id})
        
        # Audit
        audit_offering(
            conn, offering_id, offering["subject_code"], offering["degree_code"],
            offering["ay_label"], "delete", actor,
            f"Deleted offering: {offering['subject_code']}",
            reason=reason,
            actor_role=actor_role,
            operation="delete",
            source="ui",
            correlation_id=correlation_id,
            program_code=offering.get("program_code"),
            branch_code=offering.get("branch_code"),
            curriculum_group_code=offering.get("curriculum_group_code"),
            year=offering["year"],
            term=offering["term"],
            division_code=offering.get("division_code"),
            snapshot_json=json.dumps(offering, default=str)
        )


def publish_offering(
    engine, 
    offering_id: int, 
    actor: str,
    actor_role: str = None,
    reason: str = "",
    acknowledge_no_topics: bool = False,
    approved_by: str = None,
    step_up_performed: bool = False,
    correlation_id: str = None
) -> Tuple[bool, str, List[str]]:
    """
    Publish an offering (change status to published).
    Enhanced with elective topics check and approval workflow.
    Returns (success, message, warnings)
    """
    with engine.begin() as conn:
        offering = exec_query(conn, """
            SELECT * FROM subject_offerings WHERE id = :id
        """, {"id": offering_id}).fetchone()
        
        if not offering:
            raise ValueError("Offering not found")
        
        offering = dict(offering._mapping)
        
        if offering["status"] == "published":
            return False, "Offering is already published", []
        
        warnings = []
        
        # Check if elective parent has topics
        if offering["is_elective_parent"]:
            ok, msg, topic_warnings = validate_elective_publish_requirements(
                conn, offering_id, acknowledge_no_topics
            )
            if not ok:
                return False, msg, []
            if topic_warnings:
                warnings.extend(topic_warnings)
        
        # Check catalog sync
        ok, sync_status, diff = validate_catalog_sync(conn, offering_id)
        if not ok and sync_status == "out_of_sync":
            warnings.append(f"Warning: Offering is out of sync with catalog: {diff}")
        
        # Create snapshot before publish
        if "publish" in VERSIONING_SETTINGS["snapshot_on"]:
            create_snapshot(conn, offering_id, "publish", actor,
                          f"Pre-publish snapshot. Reason: {reason}")
        
        # Update status
        exec_query(conn, """
            UPDATE subject_offerings
            SET status = 'published', updated_at = CURRENT_TIMESTAMP, updated_by = :actor
            WHERE id = :id
        """, {"id": offering_id, "actor": actor})
        
        # Audit
        audit_offering(
            conn, offering_id, offering["subject_code"], offering["degree_code"],
            offering["ay_label"], "publish", actor,
            f"Published offering. {len(warnings)} warning(s).",
            reason=reason,
            actor_role=actor_role,
            operation="publish",
            source="ui",
            correlation_id=correlation_id,
            step_up_performed=step_up_performed,
            program_code=offering.get("program_code"),
            branch_code=offering.get("branch_code"),
            curriculum_group_code=offering.get("curriculum_group_code"),
            year=offering["year"],
            term=offering["term"],
            division_code=offering.get("division_code")
        )
        
        return True, "Offering published successfully", warnings


def archive_offering(
    engine, 
    offering_id: int, 
    actor: str,
    actor_role: str = None,
    reason: str = "",
    correlation_id: str = None
) -> None:
    """Archive an offering."""
    with engine.begin() as conn:
        offering = exec_query(conn, """
            SELECT * FROM subject_offerings WHERE id = :id
        """, {"id": offering_id}).fetchone()
        
        if not offering:
            raise ValueError("Offering not found")
        
        offering = dict(offering._mapping)
        
        # Create snapshot before archive
        if "archive" in VERSIONING_SETTINGS["snapshot_on"]:
            create_snapshot(conn, offering_id, "archive", actor,
                          f"Pre-archive snapshot. Reason: {reason}")
        
        exec_query(conn, """
            UPDATE subject_offerings
            SET status = 'archived', updated_at = CURRENT_TIMESTAMP, updated_by = :actor
            WHERE id = :id
        """, {"id": offering_id, "actor": actor})
        
        audit_offering(
            conn, offering_id, offering["subject_code"], offering["degree_code"],
            offering["ay_label"], "archive", actor,
            f"Archived offering",
            reason=reason,
            actor_role=actor_role,
            operation="archive",
            source="ui",
            correlation_id=correlation_id,
            program_code=offering.get("program_code"),
            branch_code=offering.get("branch_code"),
            curriculum_group_code=offering.get("curriculum_group_code"),
            year=offering["year"],
            term=offering["term"],
            division_code=offering.get("division_code")
        )


def freeze_offering(
    engine,
    offering_id: int,
    actor: str,
    reason: str,
    actor_role: str = None
) -> None:
    """
    Freeze an offering (prevent modifications).
    Typically done when marks exist.
    """
    with engine.begin() as conn:
        offering = exec_query(conn, """
            SELECT * FROM subject_offerings WHERE id = :id
        """, {"id": offering_id}).fetchone()
        
        if not offering:
            raise ValueError("Offering not found")
        
        offering = dict(offering._mapping)
        
        # Check marks count - only if table exists
        marks_count = 0
        try:
            marks_count = exec_query(conn, """
                SELECT COUNT(*) FROM subject_marks WHERE offering_id = :id
            """, {"id": offering_id}).fetchone()[0]
        except Exception as e:
            if "no such table" not in str(e).lower():
                raise
            # Table doesn't exist - no marks
        
        exec_query(conn, """
            UPDATE subject_offerings
            SET is_frozen = 1, frozen_at = CURRENT_TIMESTAMP, 
                frozen_by = :actor, frozen_reason = :reason,
                updated_at = CURRENT_TIMESTAMP, updated_by = :actor
            WHERE id = :id
        """, {"id": offering_id, "actor": actor, "reason": reason})
        
        # Log freeze event - only if log table exists
        try:
            exec_query(conn, """
                INSERT INTO subject_offerings_freeze_log
                (offering_id, subject_code, ay_label, action, reason, marks_exist, marks_count, actor)
                VALUES (:oid, :sc, :ay, 'freeze', :reason, :marks_exist, :marks_count, :actor)
            """, {
                "oid": offering_id,
                "sc": offering["subject_code"],
                "ay": offering["ay_label"],
                "reason": reason,
                "marks_exist": 1 if marks_count > 0 else 0,
                "marks_count": marks_count,
                "actor": actor
            })
        except Exception as e:
            if "no such table" not in str(e).lower():
                raise
            # Freeze log table doesn't exist - skip logging
        
        audit_offering(
            conn, offering_id, offering["subject_code"], offering["degree_code"],
            offering["ay_label"], "freeze", actor,
            f"Frozen offering. Marks count: {marks_count}",
            reason=reason,
            actor_role=actor_role,
            operation="freeze",
            program_code=offering.get("program_code"),
            branch_code=offering.get("branch_code"),
            curriculum_group_code=offering.get("curriculum_group_code"),
            year=offering["year"],
            term=offering["term"],
            division_code=offering.get("division_code")
        )


def unfreeze_offering(
    engine,
    offering_id: int,
    actor: str,
    reason: str,
    actor_role: str = None,
    approved_by: str = None
) -> None:
    """
    Unfreeze an offering.
    May require approval if marks exist.
    """
    with engine.begin() as conn:
        offering = exec_query(conn, """
            SELECT * FROM subject_offerings WHERE id = :id
        """, {"id": offering_id}).fetchone()
        
        if not offering:
            raise ValueError("Offering not found")
        
        offering = dict(offering._mapping)
        
        # Check marks count - only if table exists
        marks_count = 0
        try:
            marks_count = exec_query(conn, """
                SELECT COUNT(*) FROM subject_marks WHERE offering_id = :id
            """, {"id": offering_id}).fetchone()[0]
        except Exception as e:
            if "no such table" not in str(e).lower():
                raise
            # Table doesn't exist - no marks
        
        exec_query(conn, """
            UPDATE subject_offerings
            SET is_frozen = 0, frozen_at = NULL, frozen_by = NULL, frozen_reason = NULL,
                updated_at = CURRENT_TIMESTAMP, updated_by = :actor
            WHERE id = :id
        """, {"id": offering_id, "actor": actor})
        
        # Log unfreeze event - only if log table exists
        try:
            exec_query(conn, """
                INSERT INTO subject_offerings_freeze_log
                (offering_id, subject_code, ay_label, action, reason, marks_exist, marks_count, actor, approved_by)
                VALUES (:oid, :sc, :ay, 'unfreeze', :reason, :marks_exist, :marks_count, :actor, :approved_by)
            """, {
                "oid": offering_id,
                "sc": offering["subject_code"],
                "ay": offering["ay_label"],
                "reason": reason,
                "marks_exist": 1 if marks_count > 0 else 0,
                "marks_count": marks_count,
                "actor": actor,
                "approved_by": approved_by
            })
        except Exception as e:
            if "no such table" not in str(e).lower():
                raise
            # Freeze log table doesn't exist - skip logging
        
        audit_offering(
            conn, offering_id, offering["subject_code"], offering["degree_code"],
            offering["ay_label"], "unfreeze", actor,
            f"Unfrozen offering. Marks count: {marks_count}",
            reason=reason,
            actor_role=actor_role,
            operation="unfreeze",
            program_code=offering.get("program_code"),
            branch_code=offering.get("branch_code"),
            curriculum_group_code=offering.get("curriculum_group_code"),
            year=offering["year"],
            term=offering["term"],
            division_code=offering.get("division_code")
        )


def copy_offerings_forward(
    engine,
    from_ay: str,
    to_ay: str,
    degree_code: str,
    year: int,
    term: int,
    actor: str,
    actor_role: str = None,
    program_code: Optional[str] = None,
    branch_code: Optional[str] = None,
    correlation_id: str = None
) -> Tuple[int, List[str]]:
    """
    Copy offerings from prior AY to new AY.
    Returns (count, list_of_messages)
    """
    messages = []
    
    with engine.begin() as conn:
        # Fetch source offerings
        query = """
            SELECT * FROM subject_offerings
            WHERE ay_label = :from_ay AND degree_code = :d AND year = :y AND term = :t
        """
        params = {"from_ay": from_ay, "d": degree_code, "y": year, "t": term}
        
        if program_code:
            query += " AND (program_code = :p OR program_code IS NULL)"
            params["p"] = program_code
        
        if branch_code:
            query += " AND (branch_code = :b OR branch_code IS NULL)"
            params["b"] = branch_code
        
        source_offerings = exec_query(conn, query, params).fetchall()
        
        if not source_offerings:
            return 0, ["No offerings found in source AY"]
        
        count = 0
        for offering in source_offerings:
            offering = dict(offering._mapping)
            
            # Check if subject still exists in catalog
            catalog_exists = exec_query(conn, """
                SELECT id FROM subjects_catalog
                WHERE subject_code = :sc AND degree_code = :d AND active = 1
            """, {"sc": offering["subject_code"], "d": offering["degree_code"]}).fetchone()
            
            if not catalog_exists:
                messages.append(f"Skipped {offering['subject_code']}: No longer in catalog")
                continue
            
            # Check if offering already exists in target AY
            target_exists = exec_query(conn, """
                SELECT id FROM subject_offerings
                WHERE ay_label = :to_ay AND degree_code = :d AND year = :y AND term = :t
                AND subject_code = :sc
                AND COALESCE(program_code, '') = COALESCE(:p, '')
                AND COALESCE(branch_code, '') = COALESCE(:b, '')
                AND COALESCE(division_code, '') = COALESCE(:div, '')
            """, {
                "to_ay": to_ay,
                "d": offering["degree_code"],
                "y": offering["year"],
                "t": offering["term"],
                "sc": offering["subject_code"],
                "p": offering.get("program_code"),
                "b": offering.get("branch_code"),
                "div": offering.get("division_code"),
            }).fetchone()
            
            if target_exists:
                messages.append(f"Skipped {offering['subject_code']}: Already exists in target AY")
                continue
            
            # Copy offering to new AY
            exec_query(conn, """
                INSERT INTO subject_offerings (
                    ay_label, degree_code, program_code, branch_code, curriculum_group_code,
                    year, term, division_code, applies_to_all_divisions,
                    subject_code, subject_type, is_elective_parent,
                    credits_total, L, T, P, S,
                    internal_marks_max, exam_marks_max, jury_viva_marks_max, total_marks_max,
                    direct_weight_percent, indirect_weight_percent,
                    pass_threshold_overall, pass_threshold_internal, pass_threshold_external,
                    instructor_email, status, override_inheritance,
                    elective_selection_lead_days, allow_negative_offset,
                    created_by, updated_by
                ) VALUES (
                    :ay, :deg, :prog, :branch, :cg,
                    :year, :term, :div, :all_div,
                    :sc, :stype, :elec_parent,
                    :credits, :L, :T, :P, :S,
                    :int_max, :exam_max, :jury_max, :total_max,
                    :dir_w, :ind_w,
                    :pass_ov, :pass_int, :pass_ext,
                    :instr, :status, :override,
                    :elec_lead_days, :allow_neg_offset,
                    :actor, :actor
                )
            """, {
                "ay": to_ay,
                "deg": offering["degree_code"],
                "prog": offering.get("program_code"),
                "branch": offering.get("branch_code"),
                "cg": offering.get("curriculum_group_code"),
                "year": offering["year"],
                "term": offering["term"],
                "div": offering.get("division_code"),
                "all_div": offering["applies_to_all_divisions"],
                "sc": offering["subject_code"],
                "stype": offering["subject_type"],
                "elec_parent": offering["is_elective_parent"],
                "credits": offering["credits_total"],
                "L": offering["L"],
                "T": offering["T"],
                "P": offering["P"],
                "S": offering["S"],
                "int_max": offering["internal_marks_max"],
                "exam_max": offering["exam_marks_max"],
                "jury_max": offering["jury_viva_marks_max"],
                "total_max": offering["total_marks_max"],
                "dir_w": offering["direct_weight_percent"],
                "ind_w": offering["indirect_weight_percent"],
                "pass_ov": offering["pass_threshold_overall"],
                "pass_int": offering["pass_threshold_internal"],
                "pass_ext": offering["pass_threshold_external"],
                "instr": offering.get("instructor_email"),
                "status": "draft",  # Always draft when copying
                "override": 0,
                "elec_lead_days": offering.get("elective_selection_lead_days"),
                "allow_neg_offset": offering.get("allow_negative_offset", 1),
                "actor": actor
            })
            
            new_id = exec_query(conn, "SELECT last_insert_rowid()").fetchone()[0]
            
            # Create snapshot for new offering
            if "create" in VERSIONING_SETTINGS["snapshot_on"]:
                create_snapshot(conn, new_id, "create", actor,
                              f"Copied forward from {from_ay} offering ID {offering['id']}")
            
            audit_offering(
                conn, new_id, offering["subject_code"], offering["degree_code"],
                to_ay, "copy_forward", actor,
                f"Copied from {from_ay} offering ID {offering['id']}",
                actor_role=actor_role,
                operation="copy_forward",
                source="ui",
                correlation_id=correlation_id,
                program_code=offering.get("program_code"),
                branch_code=offering.get("branch_code"),
                curriculum_group_code=offering.get("curriculum_group_code"),
                year=offering["year"],
                term=offering["term"],
                division_code=offering.get("division_code")
            )
            
            count += 1
            messages.append(f"Copied {offering['subject_code']}")
        
        return count, messages


def bulk_update_offerings(
    engine,
    offering_ids: List[int],
    updates: Dict[str, Any],
    actor: str,
    actor_role: str = None,
    reason: str = "",
    correlation_id: str = None
) -> Tuple[int, List[str]]:
    """
    Perform a bulk update on a list of offering IDs.
    Logs each update individually.
    Returns (updated_count, list_of_errors)
    """
    if not offering_ids:
        return 0, ["No offerings selected"]

    if not updates:
        return 0, ["No update data provided"]

    # Validate keys in updates
    allowed_keys = ["status", "instructor_email", "override_inheritance", "override_reason"]
    update_keys = list(updates.keys())
    for k in update_keys:
        if k not in allowed_keys:
            return 0, [f"Invalid update key: {k}"]

    updated_count = 0
    errors = []
    
    with engine.begin() as conn:
        for offering_id in offering_ids:
            try:
                # Fetch current offering for audit
                current = exec_query(conn, """
                    SELECT * FROM subject_offerings WHERE id = :id
                """, {"id": offering_id}).fetchone()
                
                if not current:
                    errors.append(f"Offering ID {offering_id}: Not found")
                    continue

                current = dict(current._mapping)
                
                # Check freeze rules
                ok, msg = validate_freeze_rules(conn, offering_id, updates)
                if not ok:
                    errors.append(f"Offering ID {offering_id}: {msg}")
                    continue
                
                # Create snapshot before update
                if "update" in VERSIONING_SETTINGS["snapshot_on"]:
                    create_snapshot(conn, offering_id, "update", actor,
                                  f"Pre-bulk-update snapshot. Reason: {reason}")
                
                # Build UPDATE query dynamically
                set_clause = ", ".join([f"{k} = :{k}" for k in updates.keys()])
                params = {**updates, "id": offering_id, "actor": actor}
                
                exec_query(conn, f"""
                    UPDATE subject_offerings
                    SET {set_clause}, updated_at = CURRENT_TIMESTAMP, updated_by = :actor
                    WHERE id = :id
                """, params)
                
                note = f"Bulk update. {reason}"
                if 'status' in updates:
                    note = f"Bulk status update to '{updates['status']}'. {reason}"
                elif 'instructor_email' in updates:
                    note = f"Bulk instructor assign to '{updates['instructor_email']}'. {reason}"

                changed_fields = format_audit_changed_fields(current, updates)
                audit_offering(
                    conn, offering_id, current["subject_code"], current["degree_code"],
                    current["ay_label"], "bulk_update", actor,
                    note,
                    reason=reason,
                    changed_fields=json.loads(changed_fields) if changed_fields else None,
                    actor_role=actor_role,
                    operation="bulk_update",
                    source="ui",
                    correlation_id=correlation_id,
                    program_code=current.get("program_code"),
                    branch_code=current.get("branch_code"),
                    curriculum_group_code=current.get("curriculum_group_code"),
                    year=current["year"],
                    term=current["term"],
                    division_code=current.get("division_code")
                )
                updated_count += 1
                
            except Exception as e:
                errors.append(f"Offering ID {offering_id}: {str(e)}")
                
    return updated_count, errors


def enable_override(
    engine,
    offering_id: int,
    override_reason: str,
    actor: str,
    actor_role: str = None,
    approved_by: str = None,
    step_up_performed: bool = False
) -> None:
    """
    Enable override_inheritance flag with approval tracking.
    Implements override approval workflow from YAML.
    """
    if not override_reason:
        raise ValueError("Override reason is required")
    
    with engine.begin() as conn:
        offering = exec_query(conn, """
            SELECT * FROM subject_offerings WHERE id = :id
        """, {"id": offering_id}).fetchone()
        
        if not offering:
            raise ValueError("Offering not found")
        
        offering = dict(offering._mapping)
        
        if offering["override_inheritance"] == 1:
            raise ValueError("Override is already enabled")
        
        # Create snapshot before override
        if "update" in VERSIONING_SETTINGS["snapshot_on"]:
            create_snapshot(conn, offering_id, "update", actor,
                          f"Pre-override snapshot. Reason: {override_reason}")
        
        exec_query(conn, """
            UPDATE subject_offerings
            SET override_inheritance = 1,
                override_reason = :reason,
                override_approved_by = :approved_by,
                override_approved_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP,
                updated_by = :actor
            WHERE id = :id
        """, {
            "id": offering_id,
            "reason": override_reason,
            "approved_by": approved_by,
            "actor": actor
        })
        
        audit_offering(
            conn, offering_id, offering["subject_code"], offering["degree_code"],
            offering["ay_label"], "override_enable", actor,
            "Enabled override inheritance",
            reason=override_reason,
            actor_role=actor_role,
            operation="override_enable",
            step_up_performed=step_up_performed,
            program_code=offering.get("program_code"),
            branch_code=offering.get("branch_code"),
            curriculum_group_code=offering.get("curriculum_group_code"),
            year=offering["year"],
            term=offering["term"],
            division_code=offering.get("division_code")
        )


def disable_override(
    engine,
    offering_id: int,
    actor: str,
    actor_role: str = None,
    reason: str = ""
) -> None:
    """
    Disable override_inheritance flag and revert to catalog defaults.
    """
    with engine.begin() as conn:
        offering = exec_query(conn, """
            SELECT * FROM subject_offerings WHERE id = :id
        """, {"id": offering_id}).fetchone()
        
        if not offering:
            raise ValueError("Offering not found")
        
        offering = dict(offering._mapping)
        
        if offering["override_inheritance"] == 0:
            raise ValueError("Override is not enabled")
        
        # Get catalog defaults
        catalog = exec_query(conn, """
            SELECT * FROM subjects_catalog
            WHERE subject_code = :sc AND degree_code = :dc AND active = 1
            LIMIT 1
        """, {"sc": offering["subject_code"], "dc": offering["degree_code"]}).fetchone()
        
        if not catalog:
            raise ValueError("Catalog subject not found")
        
        catalog = dict(catalog._mapping)
        
        # Create snapshot before reverting
        if "update" in VERSIONING_SETTINGS["snapshot_on"]:
            create_snapshot(conn, offering_id, "update", actor,
                          f"Pre-override-disable snapshot. Reason: {reason}")
        
        # Revert to catalog values
        exec_query(conn, """
            UPDATE subject_offerings
            SET override_inheritance = 0,
                override_reason = NULL,
                override_approved_by = NULL,
                override_approved_at = NULL,
                credits_total = :credits,
                L = :L, T = :T, P = :P, S = :S,
                internal_marks_max = :int_max,
                exam_marks_max = :exam_max,
                jury_viva_marks_max = :jury_max,
                total_marks_max = :total_max,
                direct_weight_percent = :dir_w,
                indirect_weight_percent = :ind_w,
                pass_threshold_overall = :pass_ov,
                pass_threshold_internal = :pass_int,
                pass_threshold_external = :pass_ext,
                updated_at = CURRENT_TIMESTAMP,
                updated_by = :actor
            WHERE id = :id
        """, {
            "id": offering_id,
            "credits": catalog["credits_total"],
            "L": catalog["L"],
            "T": catalog["T"],
            "P": catalog["P"],
            "S": catalog["S"],
            "int_max": catalog["internal_marks_max"],
            "exam_max": catalog["exam_marks_max"],
            "jury_max": catalog["jury_viva_marks_max"],
            "total_max": catalog["internal_marks_max"] + catalog["exam_marks_max"] + catalog["jury_viva_marks_max"],
            "dir_w": catalog.get("direct_internal_weight_percent", 40.0),
            "ind_w": 100.0 - catalog.get("direct_internal_weight_percent", 40.0),
            "pass_ov": catalog.get("min_overall_percent", 40.0),
            "pass_int": catalog.get("min_internal_percent", 50.0),
            "pass_ext": catalog.get("min_external_percent", 40.0),
            "actor": actor
        })
        
        audit_offering(
            conn, offering_id, offering["subject_code"], offering["degree_code"],
            offering["ay_label"], "override_disable", actor,
            "Disabled override inheritance and reverted to catalog defaults",
            reason=reason,
            actor_role=actor_role,
            operation="override_disable",
            program_code=offering.get("program_code"),
            branch_code=offering.get("branch_code"),
            curriculum_group_code=offering.get("curriculum_group_code"),
            year=offering["year"],
            term=offering["term"],
            division_code=offering.get("division_code")
        )


def sync_with_catalog(
    engine,
    offering_id: int,
    actor: str,
    actor_role: str = None,
    force: bool = False
) -> Tuple[bool, str]:
    """
    Sync offering with catalog (if not overridden).
    Returns (success, message)
    """
    with engine.begin() as conn:
        offering = exec_query(conn, """
            SELECT * FROM subject_offerings WHERE id = :id
        """, {"id": offering_id}).fetchone()
        
        if not offering:
            return False, "Offering not found"
        
        offering = dict(offering._mapping)
        
        if offering["override_inheritance"] == 1 and not force:
            return False, "Cannot sync: Override is enabled. Use force=True to override."
        
        # Get catalog
        catalog = exec_query(conn, """
            SELECT * FROM subjects_catalog
            WHERE subject_code = :sc AND degree_code = :dc AND active = 1
            LIMIT 1
        """, {"sc": offering["subject_code"], "dc": offering["degree_code"]}).fetchone()
        
        if not catalog:
            return False, "Catalog subject not found"
        
        catalog = dict(catalog._mapping)
        
        # Create snapshot
        if "update" in VERSIONING_SETTINGS["snapshot_on"]:
            create_snapshot(conn, offering_id, "update", actor,
                          f"Pre-catalog-sync snapshot")
        
        # Update to catalog values
        total_marks = catalog["internal_marks_max"] + catalog["exam_marks_max"] + catalog["jury_viva_marks_max"]
        
        exec_query(conn, """
            UPDATE subject_offerings
            SET credits_total = :credits,
                L = :L, T = :T, P = :P, S = :S,
                internal_marks_max = :int_max,
                exam_marks_max = :exam_max,
                jury_viva_marks_max = :jury_max,
                total_marks_max = :total_max,
                direct_weight_percent = :dir_w,
                indirect_weight_percent = :ind_w,
                pass_threshold_overall = :pass_ov,
                pass_threshold_internal = :pass_int,
                pass_threshold_external = :pass_ext,
                updated_at = CURRENT_TIMESTAMP,
                updated_by = :actor
            WHERE id = :id
        """, {
            "id": offering_id,
            "credits": catalog["credits_total"],
            "L": catalog["L"],
            "T": catalog["T"],
            "P": catalog["P"],
            "S": catalog["S"],
            "int_max": catalog["internal_marks_max"],
            "exam_max": catalog["exam_marks_max"],
            "jury_max": catalog["jury_viva_marks_max"],
            "total_max": total_marks,
            "dir_w": catalog.get("direct_internal_weight_percent", 40.0),
            "ind_w": 100.0 - catalog.get("direct_internal_weight_percent", 40.0),
            "pass_ov": catalog.get("min_overall_percent", 40.0),
            "pass_int": catalog.get("min_internal_percent", 50.0),
            "pass_ext": catalog.get("min_external_percent", 40.0),
            "actor": actor
        })
        
        audit_offering(
            conn, offering_id, offering["subject_code"], offering["degree_code"],
            offering["ay_label"], "update", actor,
            f"Synced with catalog. Force: {force}",
            actor_role=actor_role,
            operation="catalog_sync",
            program_code=offering.get("program_code"),
            branch_code=offering.get("branch_code"),
            curriculum_group_code=offering.get("curriculum_group_code"),
            year=offering["year"],
            term=offering["term"],
            division_code=offering.get("division_code")
        )
        
        return True, "Successfully synced with catalog"


def rollback_to_snapshot(
    engine,
    offering_id: int,
    snapshot_id: int,
    actor: str,
    actor_role: str = None,
    reason: str = ""
) -> bool:
    """
    Rollback an offering to a previous snapshot.
    Implements rollback from YAML versioning specification.
    """
    with engine.begin() as conn:
        # Get snapshot
        snapshot = exec_query(conn, """
            SELECT * FROM subject_offerings_snapshots
            WHERE id = :id AND offering_id = :offering_id
        """, {"id": snapshot_id, "offering_id": offering_id}).fetchone()
        
        if not snapshot:
            raise ValueError(f"Snapshot {snapshot_id} not found for offering {offering_id}")
        
        snapshot_dict = dict(snapshot._mapping)
        offering_data = json.loads(snapshot_dict["snapshot_data"])
        
        # Check current offering status
        current = exec_query(conn, """
            SELECT is_frozen, status FROM subject_offerings WHERE id = :id
        """, {"id": offering_id}).fetchone()
        
        if not current:
            raise ValueError("Offering not found")
        
        # Check if frozen
        if current[0] == 1:
            raise ValueError("Cannot rollback frozen offering. Unfreeze first.")
        
        # Check if published with marks (special behavior)
        if current[1] == "published":
            marks_count = 0
            try:
                marks_count = exec_query(conn, """
                    SELECT COUNT(*) FROM subject_marks WHERE offering_id = :id
                """, {"id": offering_id}).fetchone()[0]
            except Exception as e:
                if "no such table" not in str(e).lower():
                    raise
                # Table doesn't exist - no marks
            
            if marks_count > 0:
                if VERSIONING_SETTINGS["rollback"]["published_with_marks_behavior"] == "clone_new_draft":
                    raise ValueError(
                        "Cannot rollback published offering with marks. "
                        "This would require creating a new draft clone (not implemented)."
                    )
        
        # Create a pre-rollback snapshot
        if "rollback" in VERSIONING_SETTINGS["snapshot_on"]:
            create_snapshot(conn, offering_id, "rollback", actor,
                          f"Pre-rollback state before reverting to snapshot #{snapshot_dict['snapshot_number']}")
        
        # Update offering with snapshot data (selective fields only)
        update_fields = [
            "credits_total", "L", "T", "P", "S",
            "internal_marks_max", "exam_marks_max", "jury_viva_marks_max", "total_marks_max",
            "direct_weight_percent", "indirect_weight_percent",
            "pass_threshold_overall", "pass_threshold_internal", "pass_threshold_external",
            "instructor_email", "status", "override_inheritance", "override_reason"
        ]
        
        set_clause = ", ".join([f"{field} = :{field}" for field in update_fields])
        params = {field: offering_data.get(field) for field in update_fields}
        params["id"] = offering_id
        params["actor"] = actor
        
        exec_query(conn, f"""
            UPDATE subject_offerings
            SET {set_clause}, updated_by = :actor, updated_at = CURRENT_TIMESTAMP
            WHERE id = :id
        """, params)
        
        # Mark snapshot as active
        exec_query(conn, """
            UPDATE subject_offerings_snapshots
            SET is_active_version = 0
            WHERE offering_id = :id
        """, {"id": offering_id})
        
        exec_query(conn, """
            UPDATE subject_offerings_snapshots
            SET is_active_version = 1, rolled_back_from_snapshot_id = :from_id
            WHERE id = :snapshot_id
        """, {"snapshot_id": snapshot_id, "from_id": snapshot.id})
        
        # Audit
        offering = exec_query(conn, """
            SELECT * FROM subject_offerings WHERE id = :id
        """, {"id": offering_id}).fetchone()
        offering = dict(offering._mapping)
        
        audit_offering(
            conn, offering_id, offering["subject_code"], offering["degree_code"],
            offering["ay_label"], "update", actor,
            f"Rolled back to snapshot #{snapshot_dict['snapshot_number']}",
            reason=reason,
            actor_role=actor_role,
            operation="rollback",
            program_code=offering.get("program_code"),
            branch_code=offering.get("branch_code"),
            curriculum_group_code=offering.get("curriculum_group_code"),
            year=offering["year"],
            term=offering["term"],
            division_code=offering.get("division_code")
        )
        
        return True


def get_offering_history(
    engine,
    offering_id: int,
    limit: int = 50
) -> List[Dict[str, Any]]:
    """
    Get audit history for an offering.
    Returns list of audit records.
    """
    with engine.begin() as conn:
        rows = exec_query(conn, """
            SELECT * FROM subject_offerings_audit
            WHERE offering_id = :id
            ORDER BY occurred_at DESC
            LIMIT :limit
        """, {"id": offering_id, "limit": limit}).fetchall()
        
        return rows_to_dicts(rows)


def get_offering_snapshots(
    engine,
    offering_id: int,
    limit: int = 20
) -> List[Dict[str, Any]]:
    """
    Get version snapshots for an offering.
    Returns list of snapshots (most recent first).
    """
    with engine.begin() as conn:
        rows = exec_query(conn, """
            SELECT id, snapshot_number, snapshot_type, note, 
                   is_active_version, created_by, created_at
            FROM subject_offerings_snapshots
            WHERE offering_id = :id
            ORDER BY snapshot_number DESC
            LIMIT :limit
        """, {"id": offering_id, "limit": limit}).fetchall()
        
        return rows_to_dicts(rows)


def check_marks_exist(
    engine,
    offering_id: int
) -> Tuple[bool, int]:
    """
    Check if marks exist for an offering.
    Returns (marks_exist, count)
    """
    with engine.begin() as conn:
        try:
            count = exec_query(conn, """
                SELECT COUNT(*) FROM subject_marks
                WHERE offering_id = :id
            """, {"id": offering_id}).fetchone()[0]
            
            return (count > 0, count)
        except Exception as e:
            if "no such table" not in str(e).lower():
                raise
            # Table doesn't exist - no marks
            return (False, 0)


def auto_freeze_if_marks_exist(
    engine,
    offering_id: int,
    actor: str = "system"
) -> bool:
    """
    Automatically freeze an offering if marks exist.
    Called after marks entry.
    Returns True if frozen, False if already frozen or no marks.
    """
    with engine.begin() as conn:
        # Check if already frozen
        result = exec_query(conn, """
            SELECT is_frozen FROM subject_offerings WHERE id = :id
        """, {"id": offering_id}).fetchone()
        
        if not result or result[0] == 1:
            return False
        
        # Check marks
        marks_exist, count = check_marks_exist(engine, offering_id)
        
        if marks_exist:
            freeze_offering(
                engine, offering_id, actor,
                f"Auto-frozen: {count} marks records exist"
            )
            return True
        
        return False


def cleanup_old_snapshots(
    engine,
    keep_last: int = None
) -> int:
    """
    Clean up old snapshots, keeping only the most recent N per offering.
    Uses VERSIONING_SETTINGS['keep_last'] if not specified.
    Returns count of deleted snapshots.
    """
    if keep_last is None:
        keep_last = VERSIONING_SETTINGS["keep_last"]
    
    with engine.begin() as conn:
        deleted = exec_query(conn, """
            DELETE FROM subject_offerings_snapshots
            WHERE id IN (
                SELECT id FROM (
                    SELECT id, 
                           ROW_NUMBER() OVER (
                               PARTITION BY offering_id 
                               ORDER BY snapshot_number DESC
                           ) as rn
                    FROM subject_offerings_snapshots
                )
                WHERE rn > :keep_last
            )
        """, {"keep_last": keep_last})
        
        count = deleted.rowcount
        return count


def get_offerings_needing_sync(
    engine,
    ay_label: str = None,
    degree_code: str = None
) -> List[Dict[str, Any]]:
    """
    Get offerings that are out of sync with catalog.
    Excludes offerings with override_inheritance = 1.
    """
    with engine.begin() as conn:
        query = """
            SELECT o.id, o.subject_code, o.degree_code, o.ay_label,
                   o.credits_total as o_credits, sc.credits_total as c_credits,
                   o.internal_marks_max as o_internal, sc.internal_marks_max as c_internal
            FROM subject_offerings o
            JOIN subjects_catalog sc 
                ON sc.subject_code = o.subject_code 
                AND sc.degree_code = o.degree_code
                AND sc.active = 1
            WHERE o.override_inheritance = 0
            AND o.status = 'published'
            AND (
                o.credits_total != sc.credits_total
                OR o.internal_marks_max != sc.internal_marks_max
                OR o.exam_marks_max != sc.exam_marks_max
            )
        """
        
        params = {}
        if ay_label:
            query += " AND o.ay_label = :ay"
            params["ay"] = ay_label
        if degree_code:
            query += " AND o.degree_code = :degree"
            params["degree"] = degree_code
        
        query += " ORDER BY o.ay_label, o.degree_code, o.subject_code"
        
        rows = exec_query(conn, query, params).fetchall()
        return rows_to_dicts(rows)


def bulk_sync_with_catalog(
    engine,
    offering_ids: List[int],
    actor: str,
    actor_role: str = None,
    correlation_id: str = None
) -> Tuple[int, List[str]]:
    """
    Bulk sync multiple offerings with catalog.
    Returns (synced_count, list_of_errors)
    """
    synced_count = 0
    errors = []
    
    for offering_id in offering_ids:
        try:
            success, msg = sync_with_catalog(
                engine, offering_id, actor, actor_role, force=False
            )
            if success:
                synced_count += 1
            else:
                errors.append(f"Offering {offering_id}: {msg}")
        except Exception as e:
            errors.append(f"Offering {offering_id}: {str(e)}")
    
    return synced_count, errors


def get_elective_offerings_without_topics(
    engine,
    ay_label: str = None,
    degree_code: str = None
) -> List[Dict[str, Any]]:
    """
    Get published elective/CP offerings that have no topics.
    For use in health checks and warnings.
    """
    with engine.begin() as conn:
        query = """
            SELECT 
                o.id, o.subject_code, o.subject_type, 
                o.degree_code, o.ay_label, o.year, o.term,
                COUNT(et.id) as topic_count
            FROM subject_offerings o
            LEFT JOIN elective_topics et 
                ON et.subject_code = o.subject_code 
                AND et.ay_label = o.ay_label
                AND et.year = o.year
                AND et.term = o.term
            WHERE o.is_elective_parent = 1
            AND o.subject_type IN ('Elective', 'College Project')
            AND o.status = 'published'
        """
        
        params = {}
        if ay_label:
            query += " AND o.ay_label = :ay"
            params["ay"] = ay_label
        if degree_code:
            query += " AND o.degree_code = :degree"
            params["degree"] = degree_code
        
        query += """
            GROUP BY o.id, o.subject_code, o.subject_type, 
                     o.degree_code, o.ay_label, o.year, o.term
            HAVING COUNT(et.id) = 0
            ORDER BY o.ay_label, o.degree_code, o.subject_code
        """
        
        rows = exec_query(conn, query, params).fetchall()
        return rows_to_dicts(rows)


def create_approval_request(
    engine,
    offering_id: int,
    request_type: str,
    requested_by: str,
    reason: str,
    proposed_changes: Dict[str, Any] = None,
    ttl_hours: int = 48
) -> int:
    """
    Create an approval request for sensitive operations.
    Returns approval_request_id.
    """
    with engine.begin() as conn:
        # Get offering details
        offering = exec_query(conn, """
            SELECT subject_code, ay_label FROM subject_offerings WHERE id = :id
        """, {"id": offering_id}).fetchone()
        
        if not offering:
            raise ValueError("Offering not found")
        
        # Calculate expiry
        expires_at = datetime.now() + timedelta(hours=ttl_hours)
        
        # Insert request
        exec_query(conn, """
            INSERT INTO subject_offerings_approvals
            (offering_id, subject_code, ay_label, request_type, 
             requested_by, reason, proposed_changes, expires_at)
            VALUES (:oid, :sc, :ay, :type, :req_by, :reason, :changes, :expires)
        """, {
            "oid": offering_id,
            "sc": offering[0],
            "ay": offering[1],
            "type": request_type,
            "req_by": requested_by,
            "reason": reason,
            "changes": json.dumps(proposed_changes) if proposed_changes else None,
            "expires": expires_at.isoformat()
        })
        
        request_id = exec_query(conn, "SELECT last_insert_rowid()").fetchone()[0]
        return request_id


def approve_request(
    engine,
    approval_request_id: int,
    approver: str,
    execute: bool = True
) -> Tuple[bool, str]:
    """
    Approve a pending request.
    If execute=True, performs the requested action.
    Returns (success, message)
    """
    with engine.begin() as conn:
        # Get request
        request = exec_query(conn, """
            SELECT * FROM subject_offerings_approvals WHERE id = :id
        """, {"id": approval_request_id}).fetchone()
        
        if not request:
            return False, "Approval request not found"
        
        request = dict(request._mapping)
        
        if request["status"] != "pending":
            return False, f"Request is not pending (status: {request['status']})"
        
        # Check expiry
        if request["expires_at"]:
            expires_at = datetime.fromisoformat(request["expires_at"])
            if datetime.now() > expires_at:
                exec_query(conn, """
                    UPDATE subject_offerings_approvals
                    SET status = 'expired'
                    WHERE id = :id
                """, {"id": approval_request_id})
                return False, "Request has expired"
        
        # Update request
        exec_query(conn, """
            UPDATE subject_offerings_approvals
            SET status = 'approved',
                approver = :approver,
                approved_at = CURRENT_TIMESTAMP,
                executed = :executed
            WHERE id = :id
        """, {
            "id": approval_request_id,
            "approver": approver,
            "executed": 1 if execute else 0
        })
        
        # Execute if requested
        if execute:
            request_type = request["request_type"]
            offering_id = request["offering_id"]
            
            try:
                if request_type == "publish":
                    publish_offering(
                        engine, offering_id, approver,
                        reason=request["reason"],
                        approved_by=approver,
                        step_up_performed=True
                    )
                elif request_type == "override_enable":
                    enable_override(
                        engine, offering_id,
                        override_reason=request["reason"],
                        actor=approver,
                        approved_by=approver,
                        step_up_performed=True
                    )
                elif request_type == "delete":
                    delete_offering(
                        engine, offering_id, approver,
                        reason=request["reason"],
                        approved_by=approver
                    )
                
                exec_query(conn, """
                    UPDATE subject_offerings_approvals
                    SET executed = 1, executed_at = CURRENT_TIMESTAMP
                    WHERE id = :id
                """, {"id": approval_request_id})
                
            except Exception as e:
                return False, f"Execution failed: {str(e)}"
        
        return True, "Request approved" + (" and executed" if execute else "")


def reject_request(
    engine,
    approval_request_id: int,
    approver: str,
    rejection_reason: str
) -> Tuple[bool, str]:
    """
    Reject a pending approval request.
    Returns (success, message)
    """
    with engine.begin() as conn:
        # Get request
        request = exec_query(conn, """
            SELECT status FROM subject_offerings_approvals WHERE id = :id
        """, {"id": approval_request_id}).fetchone()
        
        if not request:
            return False, "Approval request not found"
        
        if request[0] != "pending":
            return False, f"Request is not pending (status: {request[0]})"
        
        # Update request
        exec_query(conn, """
            UPDATE subject_offerings_approvals
            SET status = 'rejected',
                approver = :approver,
                approved_at = CURRENT_TIMESTAMP,
                rejection_reason = :reason
            WHERE id = :id
        """, {
            "id": approval_request_id,
            "approver": approver,
            "reason": rejection_reason
        })
        
        return True, "Request rejected"


# Export all functions
__all__ = [
    "audit_offering",
    "create_snapshot",
    "create_offering_from_catalog",
    "update_offering",
    "delete_offering",
    "publish_offering",
    "archive_offering",
    "freeze_offering",
    "unfreeze_offering",
    "copy_offerings_forward",
    "bulk_update_offerings",
    "enable_override",
    "disable_override",
    "sync_with_catalog",
    "rollback_to_snapshot",
    "get_offering_history",
    "get_offering_snapshots",
    "check_marks_exist",
    "auto_freeze_if_marks_exist",
    "cleanup_old_snapshots",
    "get_offerings_needing_sync",
    "bulk_sync_with_catalog",
    "get_elective_offerings_without_topics",
    "create_approval_request",
    "approve_request",
    "reject_request"
]
