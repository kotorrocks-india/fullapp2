"""
Subject catalog CRUD operations with audit logging
"""

from typing import Dict, Any, List, Optional, Tuple
import json
from sqlalchemy import text as sa_text
# Use relative imports
from .helpers import exec_query, rows_to_dicts, safe_int, safe_float, to_bool
from .constants import validate_subject


def audit_subject(
    conn,
    subject_id: int,
    subject_code: str,
    degree_code: str,
    program_code: str,
    branch_code: str,
    action: str,
    actor: str,
    note: str = "",
    changed_fields: Dict[str, Any] = None,
):
    """Write audit log for subject catalog changes."""
    exec_query(conn, """
        INSERT INTO subjects_catalog_audit
        (subject_id, subject_code, degree_code, program_code, branch_code,
         action, note, changed_fields, actor)
        VALUES (:sid, :sc, :dc, :pc, :bc, :act, :note, :fields, :actor)
    """, {
        "sid": subject_id,
        "sc": subject_code,
        "dc": degree_code,
        "pc": program_code,
        "bc": branch_code,
        "act": action,
        "note": note or "",
        "fields": json.dumps(changed_fields) if changed_fields else None,
        "actor": actor or "system"
    })


def create_subject_in_conn(
    conn,
    data: Dict[str, Any],
    actor: str,
    validated: bool = False,
) -> int:
    """
    Internal helper to create a subject using an existing connection.
    NOTE: This function no longer checks for duplicates, assuming
    the calling function (e.g., importer) has already done so.
    """
    if not validated:
        ok, msg = validate_subject(data)
        if not ok:
            raise ValueError(msg)
    
    credits_total_default = data.get("credits_total", 0.0)
    student_credits_default = data.get("student_credits", credits_total_default)
    teaching_credits_default = data.get("teaching_credits", credits_total_default)

    # Insert subject
    result = exec_query(conn, """
        INSERT INTO subjects_catalog (
            subject_code, subject_name, subject_type,
            degree_code, program_code, branch_code, curriculum_group_code,
            semester_id,
            credits_total, L, T, P, S,
            student_credits, teaching_credits, workload_breakup_json,
            internal_marks_max, exam_marks_max, jury_viva_marks_max,
            min_internal_percent, min_external_percent, min_overall_percent,
            direct_source_mode,
            direct_internal_threshold_percent, direct_external_threshold_percent,
            direct_internal_weight_percent, direct_external_weight_percent,
            direct_target_students_percent, indirect_target_students_percent,
            indirect_min_response_rate_percent,
            overall_direct_weight_percent, overall_indirect_weight_percent,
            description, status, active, sort_order
        ) VALUES (
            :code, :name, :type,
            :deg, :prog, :branch, :cg, :sem_id,
            :credits, :L, :T, :P, :S,
            :sc, :tc, :workload_json,
            :int_max, :exam_max, :jury_max,
            :min_int, :min_ext, :min_overall,
            :dsm,
            :dit, :det, :diw, :dew,
            :dts, :its, :imr,
            :odw, :oiw,
            :desc, :status, :active, :sort
        )
    """, {
        "code": data["subject_code"],
        "name": data["subject_name"],
        "type": data.get("subject_type", "Core"),
        "deg": data["degree_code"],
        "prog": data.get("program_code"),
        "branch": data.get("branch_code"),
        "cg": data.get("curriculum_group_code"),
        "sem_id": data.get("semester_id"),
        "credits": credits_total_default,
        "sc": student_credits_default,
        "tc": teaching_credits_default,
        "L": data.get("L", 0),
        "T": data.get("T", 0),
        "P": data.get("P", 0),
        "S": data.get("S", 0),
        "workload_json": data.get("workload_breakup_json"),
        "int_max": data.get("internal_marks_max", 40),
        "exam_max": data.get("exam_marks_max", 60),
        "jury_max": data.get("jury_viva_marks_max", 0),
        "min_int": data.get("min_internal_percent", 50.0),
        "min_ext": data.get("min_external_percent", 40.0),
        "min_overall": data.get("min_overall_percent", 40.0),
        "dsm": data.get("direct_source_mode", "overall"),
        "dit": data.get("direct_internal_threshold_percent", 50.0),
        "det": data.get("direct_external_threshold_percent", 40.0),
        "diw": data.get("direct_internal_weight_percent", 40.0),
        "dew": data.get("direct_external_weight_percent", 60.0),
        "dts": data.get("direct_target_students_percent", 50.0),
        "its": data.get("indirect_target_students_percent", 50.0),
        "imr": data.get("indirect_min_response_rate_percent", 75.0),
        "odw": data.get("overall_direct_weight_percent", 80.0),
        "oiw": data.get("overall_indirect_weight_percent", 20.0),
        "desc": data.get("description"),
        "status": data.get("status", "active"),
        "active": 1 if data.get("active", True) else 0,
        "sort": data.get("sort_order", 100),
    })

    subject_id = result.lastrowid

    audit_subject(
        conn,
        subject_id,
        data["subject_code"],
        data["degree_code"],
        data.get("program_code"),
        data.get("branch_code"),
        "create",
        actor,
        f"Created subject: {data['subject_name']}",
    )

    return subject_id


def create_subject(engine, data: Dict[str, Any], actor: str) -> int:
    """Create a new subject in catalog (public entry point)."""
    ok, msg = validate_subject(data)
    if not ok:
        raise ValueError(msg)

    with engine.begin() as conn:
        existing = exec_query(conn, """
            SELECT id FROM subjects_catalog
            WHERE subject_code = :code
            AND degree_code = :deg
            AND COALESCE(program_code, '') = COALESCE(:prog, '')
            AND COALESCE(branch_code, '') = COALESCE(:branch, '')
            AND COALESCE(curriculum_group_code, '') = COALESCE(:cg, '')
        """, {
            "code": data["subject_code"],
            "deg": data["degree_code"],
            "prog": data.get("program_code"),
            "branch": data.get("branch_code"),
            "cg": data.get("curriculum_group_code"),
        }).fetchone()

        if existing:
            raise ValueError(
                "Subject already exists in this scope "
                "(degree/program/branch/curriculum group)"
            )
            
        return create_subject_in_conn(conn, data, actor, validated=True)


def update_subject_in_conn(
    conn,
    subject_id: int,
    data: Dict[str, Any],
    actor: str,
    validated: bool = False,
):
    """Internal helper to update a subject using an existing connection."""
    if not validated:
        ok, msg = validate_subject(data)
        if not ok:
            raise ValueError(msg)
            
    credits_total_default = data.get("credits_total", 0.0)
    student_credits_default = data.get("student_credits", credits_total_default)
    teaching_credits_default = data.get("teaching_credits", credits_total_default)

    exec_query(conn, """
        UPDATE subjects_catalog SET
            subject_name = :name,
            subject_type = :type,
            program_code = :prog,
            branch_code = :branch,
            curriculum_group_code = :cg,
            semester_id = :sem_id,
            credits_total = :credits,
            L = :L, T = :T, P = :P, S = :S,
            student_credits = :sc,
            teaching_credits = :tc,
            workload_breakup_json = :workload_json,
            internal_marks_max = :int_max,
            exam_marks_max = :exam_max,
            jury_viva_marks_max = :jury_max,
            min_internal_percent = :min_int,
            min_external_percent = :min_ext,
            min_overall_percent = :min_overall,
            direct_source_mode = :dsm,
            direct_internal_threshold_percent = :dit,
            direct_external_threshold_percent = :det,
            direct_internal_weight_percent = :diw,
            direct_external_weight_percent = :dew,
            direct_target_students_percent = :dts,
            indirect_target_students_percent = :its,
            indirect_min_response_rate_percent = :imr,
            overall_direct_weight_percent = :odw,
            overall_indirect_weight_percent = :oiw,
            description = :desc,
            status = :status,
            active = :active,
            sort_order = :sort
        WHERE id = :id
    """, {
        "id": subject_id,
        "name": data["subject_name"],
        "type": data.get("subject_type", "Core"),
        "prog": data.get("program_code"),
        "branch": data.get("branch_code"),
        "cg": data.get("curriculum_group_code"),
        "sem_id": data.get("semester_id"),
        "credits": credits_total_default,
        "sc": student_credits_default,
        "tc": teaching_credits_default,
        "L": data.get("L", 0),
        "T": data.get("T", 0),
        "P": data.get("P", 0),
        "S": data.get("S", 0),
        "workload_json": data.get("workload_json"),
        "int_max": data.get("internal_marks_max", 40),
        "exam_max": data.get("exam_marks_max", 60),
        "jury_max": data.get("jury_viva_marks_max", 0),
        "min_int": data.get("min_internal_percent", 50.0),
        "min_ext": data.get("min_external_percent", 40.0),
        "min_overall": data.get("min_overall_percent", 40.0),
        "dsm": data.get("direct_source_mode", "overall"),
        "dit": data.get("direct_internal_threshold_percent", 50.0),
        "det": data.get("direct_external_threshold_percent", 40.0),
        "diw": data.get("direct_internal_weight_percent", 40.0),
        "dew": data.get("direct_external_weight_percent", 60.0),
        "dts": data.get("direct_target_students_percent", 50.0),
        "its": data.get("indirect_target_students_percent", 50.0),
        "imr": data.get("indirect_min_response_rate_percent", 75.0),
        "odw": data.get("overall_direct_weight_percent", 80.0),
        "oiw": data.get("overall_indirect_weight_percent", 20.0),
        "desc": data.get("description"),
        "status": data.get("status", "active"),
        "active": 1 if data.get("active", True) else 0,
        "sort": data.get("sort_order", 100),
    })

    audit_subject(
        conn,
        subject_id,
        data["subject_code"],
        data["degree_code"],
        data.get("program_code"),
        data.get("branch_code"),
        "update",
        actor,
        f"Updated subject: {data['subject_name']}",
    )


def delete_subject_in_conn(
    conn,
    subject_id: int,
    actor: str,
):
    """Internal helper to delete a subject."""
    
    subject_data = exec_query(conn, """
        SELECT subject_code, subject_name, degree_code, program_code, branch_code
        FROM subjects_catalog WHERE id = :id
    """, {"id": subject_id}).fetchone()

    if not subject_data:
        raise ValueError("Subject not found")

    exec_query(conn, "DELETE FROM subjects_catalog WHERE id = :id", {"id": subject_id})
    
    audit_subject(
        conn,
        subject_id,
        subject_data[0], # subject_code
        subject_data[2], # degree_code
        subject_data[3], # program_code
        subject_data[4], # branch_code
        "delete",
        actor,
        f"Deleted subject: {subject_data[1]}",
    )

# --- NEW PUBLIC FUNCTION ---
def update_subject(engine, subject_id: int, data: Dict[str, Any], actor: str):
    """
    Public entry point to update a subject.
    Handles transaction and validation.
    """
    ok, msg = validate_subject(data)
    if not ok:
        raise ValueError(msg)
        
    with engine.begin() as conn:
        update_subject_in_conn(conn, subject_id, data, actor, validated=True)

# --- NEW PUBLIC FUNCTION ---
def delete_subject(engine, subject_id: int, actor: str):
    """
    Public entry point to delete a subject.
    Handles transaction.
    """
    with engine.begin() as conn:
        delete_subject_in_conn(conn, subject_id, actor)
