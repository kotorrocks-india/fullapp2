"""
Import functions for bulk subject catalog import
FIXED: Properly handles exam_marks_max defaulting to 0 when not provided,
and uses updated field names for attainment parameters
"""

from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
import json

# Use relative imports for modules in the same package
from .helpers import exec_query, to_bool, safe_float as helper_safe_float
from .subjects_crud import (
    create_subject_in_conn, 
    update_subject_in_conn
)
from .constants import validate_subject, SUBJECT_IMPORT_TEMPLATE_COLUMNS


# --- LOCAL HELPERS ---
def _get_int(val: Any) -> Optional[int]:
    if val is None or pd.isna(val): return None
    try: return int(val)
    except (ValueError, TypeError): return None

def _get_float(val: Any) -> Optional[float]:
    if val is None or pd.isna(val): return None
    try: return float(val)
    except (ValueError, TypeError): return None

def _find_semester_id(conn, degree_code: str, semester_number: int) -> Optional[int]:
    row = exec_query(conn, """
        SELECT id FROM semesters
        WHERE degree_code = :dc AND semester_number = :sn
        AND program_id IS NULL AND branch_id IS NULL LIMIT 1
    """, {"dc": degree_code, "sn": semester_number}).fetchone()
    return row[0] if row else None

def _find_existing_subject_id(conn, code: str, degree: str) -> Optional[int]:
    """Finds a subject by its core key (code + degree)."""
    row = exec_query(conn, """
        SELECT id FROM subjects_catalog
        WHERE subject_code = :code
        AND degree_code = :deg
        LIMIT 1
    """, {"code": code, "deg": degree}).fetchone()
    return row[0] if row else None


def import_subjects_from_df(engine, df: pd.DataFrame, dry_run: bool,
                            actor: str) -> Tuple[List[Dict[str, Any]], int]:
    """
    Import subjects from a DataFrame with UPSERT (update-or-create) logic.
    - Matches subjects on (subject_code, degree_code).
    - If found, updates the subject.
    - If not found, creates a new subject.
    """
    if df is None or df.empty:
        return [{"row": None, "subject_code": "", "error": "Empty DataFrame"}], 0

    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    with engine.begin() as meta_conn:
        deg_rows = exec_query(meta_conn, "SELECT code FROM degrees WHERE active = 1").fetchall()
        valid_degrees = {r[0].upper() for r in deg_rows}

    errors: List[Dict[str, Any]] = []
    success_create_count = 0
    success_update_count = 0

    conn = engine.connect()
    trans = conn.begin()  # manual transaction

    try:
        for idx, row in df.iterrows():
            row_num = idx + 2  # assuming row 1 is header
            
            # --- Initialize L/T/P/S variables ---
            L_periods, T_periods, P_periods, S_periods = 0.0, 0.0, 0.0, 0.0
            
            try:
                raw_code = row.get("subject_code", "")
                raw_name = row.get("subject_name", "")
                raw_degree = row.get("degree_code", "")
                raw_sem_num = row.get("semester_number")

                code = str(raw_code or "").strip().upper()
                name = str(raw_name or "").strip()
                degree_code = str(raw_degree or "").strip().upper()
                semester_number = _get_int(raw_sem_num)

                if not code or not name or not degree_code:
                    raise ValueError(
                        "Missing required fields: subject_code, subject_name, degree_code"
                    )
                
                if not semester_number or semester_number <= 0:
                    raise ValueError("semester_number is required and must be > 0")

                if valid_degrees and degree_code not in valid_degrees:
                    raise ValueError(f"Degree '{degree_code}' not found or inactive")
                
                semester_id = _find_semester_id(conn, degree_code, semester_number)
                if not semester_id:
                    raise ValueError(
                        f"No degree-level semester found for {degree_code} "
                        f"with semester_number {semester_number}"
                    )
                
                # --- WORKLOAD ---
                is_advanced_template = "workload_breakup_json" in df.columns
                raw_workload = row.get("workload_breakup_json") if is_advanced_template else None
                
                # Check if workload_breakup_json has actual content
                has_workload_json = (
                    is_advanced_template 
                    and not pd.isna(raw_workload) 
                    and str(raw_workload).strip() 
                    and str(raw_workload).strip().lower() not in ['', 'nan', 'none']
                )
                
                if has_workload_json:
                    # Advanced path: parse provided JSON
                    workload_json = str(raw_workload).strip()
                    try:
                        workload_components = json.loads(workload_json)
                        if not isinstance(workload_components, list):
                             raise ValueError("JSON must be a list of objects")
                        # Set L/T/P/S from JSON
                        L_periods = sum(c.get("hours", 0) for c in workload_components if c.get("code", "").upper() == "L")
                        T_periods = sum(c.get("hours", 0) for c in workload_components if c.get("code", "").upper() == "T")
                        P_periods = sum(c.get("hours", 0) for c in workload_components if c.get("code", "").upper() == "P")
                        S_periods = sum(c.get("hours", 0) for c in workload_components if c.get("code", "").upper() == "S")
                    except Exception as json_e:
                        raise ValueError(f"Invalid workload_breakup_json: {json_e}")
                else:
                    # Simple path: build workload from L/T/P/S columns IF they exist
                    L_periods = helper_safe_float(row.get("l"), 0.0) if "l" in df.columns else 0.0
                    T_periods = helper_safe_float(row.get("t"), 0.0) if "t" in df.columns else 0.0
                    P_periods = helper_safe_float(row.get("p"), 0.0) if "p" in df.columns else 0.0
                    S_periods = helper_safe_float(row.get("s"), 0.0) if "s" in df.columns else 0.0

                    workload_components = []
                    if L_periods: workload_components.append({"code": "L", "name": "Lectures", "hours": L_periods})
                    if T_periods: workload_components.append({"code": "T", "name": "Tutorials", "hours": T_periods})
                    if P_periods: workload_components.append({"code": "P", "name": "Practical", "hours": P_periods})
                    if S_periods: workload_components.append({"code": "S", "name": "Studio", "hours": S_periods})
                    
                    workload_json = json.dumps(workload_components) if workload_components else None

                # --- CREDITS ---
                credits_total_val = _get_float(row.get("credits_total"))
                student_credits_val = _get_float(row.get("student_credits"))
                teaching_credits_val = _get_float(row.get("teaching_credits"))

                # --- ATTAINMENT (FIXED: Default exam_marks to 0 if not provided) ---
                internal_val = _get_int(row.get("internal_marks_max"))
                exam_val = _get_int(row.get("exam_marks_max"))
                jury_val = _get_int(row.get("jury_viva_marks_max"))
                
                # FIX: Default to 0 for exam and jury if not provided
                final_internal_marks = internal_val if internal_val is not None else 40
                final_exam_marks = exam_val if exam_val is not None else 0
                final_jury_marks = jury_val if jury_val is not None else 0
                
                total_external_marks = final_exam_marks + final_jury_marks
                is_internal_only = final_internal_marks > 0 and total_external_marks == 0
                is_external_only = final_internal_marks == 0 and total_external_marks > 0
                
                # Use updated CSV column names for attainment
                internal_weight_csv = _get_float(row.get("direct_internal_weight_percent"))
                final_internal_weight = internal_weight_csv if internal_weight_csv is not None else 40.0
                final_external_weight = 100.0 - final_internal_weight
                
                if is_internal_only:
                    final_internal_weight = 100.0
                    final_external_weight = 0.0
                elif is_external_only:
                    final_internal_weight = 0.0
                    final_external_weight = 100.0
                
                # New: Handle direct/indirect attainment percentages
                direct_attainment_percent = _get_float(row.get("direct_target_students_percent"))
                if direct_attainment_percent is None:
                    direct_attainment_percent = 80.0  # Default
                
                indirect_attainment_percent = 100.0 - direct_attainment_percent

                # --- DATA DICTIONARY ---
                data = {
                    "subject_code": code,
                    "subject_name": name,
                    "degree_code": degree_code,
                    "semester_id": semester_id,
                    "credits_total": credits_total_val,
                    "student_credits": student_credits_val,
                    "teaching_credits": teaching_credits_val,
                    
                    "workload_breakup_json": workload_json,
                    "L": L_periods,
                    "T": T_periods,
                    "P": P_periods,
                    "S": S_periods,
                    
                    "subject_type": str(row.get("subject_type", "Core") or "Core").strip(),
                    "program_code": str(row.get("program_code", "") or "").strip().upper() or None,
                    "branch_code": str(row.get("branch_code", "") or "").strip().upper() or None,
                    "curriculum_group_code": (
                        str(row.get("curriculum_group_code", "") or "").strip().upper() or None
                    ),
                    "description": str(row.get("description", "") or "").strip() or None,
                    "status": str(row.get("status", "active") or "active").strip(),
                    "direct_source_mode": str(row.get("direct_source_mode", "overall") or "overall").strip(),
                    "internal_marks_max": final_internal_marks,
                    "exam_marks_max": final_exam_marks,
                    "jury_viva_marks_max": final_jury_marks,
                    "min_internal_percent": _get_float(row.get("min_internal_percent")) or 50.0,
                    "min_external_percent": _get_float(row.get("min_external_percent")) or 40.0,
                    "min_overall_percent": _get_float(row.get("min_overall_percent")) or 40.0,
                    "direct_internal_threshold_percent": _get_float(row.get("direct_internal_threshold_percent")) or 50.0,
                    "direct_external_threshold_percent": _get_float(row.get("direct_external_threshold_percent")) or 40.0,
                    "direct_internal_weight_percent": final_internal_weight,
                    "direct_external_weight_percent": final_external_weight,
                    "direct_target_students_percent": direct_attainment_percent,
                    "indirect_target_students_percent": indirect_attainment_percent,
                    "indirect_min_response_rate_percent": _get_float(row.get("indirect_min_response_rate_percent")) or 75.0,
                    "overall_direct_weight_percent": direct_attainment_percent,
                    "overall_indirect_weight_percent": indirect_attainment_percent,
                    "active": to_bool(row.get("active"), default=True),
                    "sort_order": _get_int(row.get("sort_order")) or 100,
                }

                # --- UPSERT LOGIC ---
                final_data = {k: v for k, v in data.items() if v is not None}
                existing_id = _find_existing_subject_id(conn, code, degree_code)
                if existing_id:
                    update_subject_in_conn(conn, existing_id, final_data, actor, validated=False)
                    success_update_count += 1
                else:
                    create_subject_in_conn(conn, final_data, actor, validated=False)
                    success_create_count += 1

            except Exception as e:
                errors.append({
                    "row": row_num,
                    "subject_code": str(row.get("subject_code", "")).strip(),
                    "error": str(e),
                })

        if dry_run:
            trans.rollback()
        else:
            trans.commit()
            
        # Add summary row (only if there are actual errors to show alongside it)
        if errors:
            summary = {
                "row": "SUMMARY",
                "subject_code": "---",
                "error": f"Created: {success_create_count}, Updated: {success_update_count}, Errors: {len(errors)}"
            }
            errors.insert(0, summary)

    except Exception as e:
        try: trans.rollback()
        except Exception: pass
        errors.append({"row": None, "subject_code": "", "error": f"Transaction failed: {e}"})
        success_create_count = 0
        success_update_count = 0

    finally:
        conn.close()

    total_success = success_create_count + success_update_count
    return errors, total_success
