"""
Import functions for bulk subject catalog import
FIXED: Properly handles exam_marks_max defaulting to 0 when not provided,
and uses updated field names for attainment parameters

UPDATED (User Request):
- Reads user-friendly column names from CSV (e.g., "Minimum Internal Passing %")
- Maps friendly names to internal DB column names
- Hard-codes defaults for removed threshold fields
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

    # --- 1. CONFIGURE YOUR CSV HEADER NAMES HERE ---
    # After you run this once, check your console for the debug output
    # and paste the exact header names from your file here.
    
    # --- Core Subject Fields ---
    H_CODE = "subject_code"
    H_NAME = "subject_name"
    H_TYPE = "subject_type"
    H_DEGREE = "degree_code"
    H_PROGRAM = "program_code"
    H_BRANCH = "branch_code"
    H_CURR_GROUP = "curriculum_group_code"
    H_SEM_NUM = "semester_number"
    H_DESC = "description"
    H_STATUS = "status"
    H_ACTIVE = "active"
    H_SORT = "sort_order"
    
    # --- Credits & Workload (L/T/P/S) ---
    H_CREDITS_TOTAL = "credits_total"
    H_CREDITS_STUDENT = "student_credits"
    H_CREDITS_TEACHING = "teaching_credits"
    H_L = "l"
    H_T = "t"
    H_P = "p"
    H_S = "s"
    H_WORKLOAD_JSON = "workload_breakup_json"
    
    # --- Marks (THIS IS THE MOST LIKELY PROBLEM AREA) ---
    H_MAX_INT = "maximum internal marks"
    H_MAX_EXT = "maximum external marks (exam)"
    H_MAX_JURY = "maximum external marks (jury/viva)"
    
    # --- Passing % (THIS IS THE OTHER LIKELY PROBLEM AREA) ---
    H_MIN_INT_PCT = "minimum internal passing %"
    H_MIN_EXT_PCT = "minimum external passing %"
    H_MIN_ALL_PCT = "minimum overall passing %"
    
    # --- Attainment % ---
    H_DIRECT_MODE = "direct_source_mode"
    H_ATT_INT_CONTRIB_PCT = "direct attainment - internal marks contribution %"
    H_ATT_DIRECT_TOTAL_PCT = "direct attainment % in total attainment"
    H_ATT_INDIRECT_RATE_PCT = "minimum indirect attainment through feedback response rate"
    # --- End of Configuration ---


    if df is None or df.empty:
        return [{"row": None, "subject_code": "", "error": "Empty DataFrame"}], 0

    df = df.copy()
    
    # Normalize headers (handles extra spaces, capitalization)
    df.columns = [' '.join(c.strip().lower().split()) for c in df.columns]

    # --- 2. DEBUG: CHECK YOUR CONSOLE/TERMINAL FOR THIS OUTPUT ---
    print("--- DEBUG: CLEANED CSV HEADERS ---")
    print("This is the full list of headers the code is seeing:")
    print(list(df.columns))
    print("-------------------------------------")
    # --- End of Debug ---

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
                raw_code = row.get(H_CODE, "")
                raw_name = row.get(H_NAME, "")
                raw_degree = row.get(H_DEGREE, "")
                raw_sem_num = row.get(H_SEM_NUM)

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
                is_advanced_template = H_WORKLOAD_JSON in df.columns
                raw_workload = row.get(H_WORKLOAD_JSON) if is_advanced_template else None
                
                has_workload_json = (
                    is_advanced_template 
                    and not pd.isna(raw_workload) 
                    and str(raw_workload).strip() 
                    and str(raw_workload).strip().lower() not in ['', 'nan', 'none']
                )
                
                if has_workload_json:
                    workload_json = str(raw_workload).strip()
                    try:
                        workload_components = json.loads(workload_json)
                        if not isinstance(workload_components, list):
                             raise ValueError("JSON must be a list of objects")
                        L_periods = sum(c.get("hours", 0) for c in workload_components if c.get("code", "").upper() == "L")
                        T_periods = sum(c.get("hours", 0) for c in workload_components if c.get("code", "").upper() == "T")
                        P_periods = sum(c.get("hours", 0) for c in workload_components if c.get("code", "").upper() == "P")
                        S_periods = sum(c.get("hours", 0) for c in workload_components if c.get("code", "").upper() == "S")
                    except Exception as json_e:
                        raise ValueError(f"Invalid {H_WORKLOAD_JSON}: {json_e}")
                else:
                    L_periods = helper_safe_float(row.get(H_L), 0.0) if H_L in df.columns else 0.0
                    T_periods = helper_safe_float(row.get(H_T), 0.0) if H_T in df.columns else 0.0
                    P_periods = helper_safe_float(row.get(H_P), 0.0) if H_P in df.columns else 0.0
                    S_periods = helper_safe_float(row.get(H_S), 0.0) if H_S in df.columns else 0.0

                    workload_components = []
                    if L_periods: workload_components.append({"code": "L", "name": "Lectures", "hours": L_periods})
                    if T_periods: workload_components.append({"code": "T", "name": "Tutorials", "hours": T_periods})
                    if P_periods: workload_components.append({"code": "P", "name": "Practical", "hours": P_periods})
                    if S_periods: workload_components.append({"code": "S", "name": "Studio", "hours": S_periods})
                    
                    workload_json = json.dumps(workload_components) if workload_components else None

                # --- CREDITS ---
                credits_total_val = _get_float(row.get(H_CREDITS_TOTAL))
                student_credits_val = _get_float(row.get(H_CREDITS_STUDENT))
                teaching_credits_val = _get_float(row.get(H_CREDITS_TEACHING))

                # --- ATTAINMENT (Read from friendly names) ---
                internal_val = _get_int(row.get(H_MAX_INT))
                exam_val = _get_int(row.get(H_MAX_EXT))
                jury_val = _get_int(row.get(H_MAX_JURY))
                
                final_internal_marks = internal_val if internal_val is not None else 0
                final_exam_marks = exam_val if exam_val is not None else 0
                final_jury_marks = jury_val if jury_val is not None else 0
                
                total_external_marks = final_exam_marks + final_jury_marks
                is_internal_only = final_internal_marks > 0 and total_external_marks == 0
                is_external_only = final_internal_marks == 0 and total_external_marks > 0
                
                # Read internal weight from friendly column name
                internal_weight_csv = _get_float(row.get(H_ATT_INT_CONTRIB_PCT))
                final_internal_weight = internal_weight_csv if internal_weight_csv is not None else 40.0
                final_external_weight = 100.0 - final_internal_weight
                
                if is_internal_only:
                    final_internal_weight = 100.0
                    final_external_weight = 0.0
                elif is_external_only:
                    final_internal_weight = 0.0
                    final_external_weight = 100.0
                
                # Read direct attainment from friendly column name
                direct_attainment_percent = _get_float(row.get(H_ATT_DIRECT_TOTAL_PCT))
                if direct_attainment_percent is None:
                    direct_attainment_percent = 80.0  # Default
                
                indirect_attainment_percent = 100.0 - direct_attainment_percent
                
                
                # --- NEW LOGIC (PER USER REQUEST) ---
                # Get base percentages from CSV (or 0.0 if blank)
                csv_min_int_pct = _get_float(row.get(H_MIN_INT_PCT)) or 0.0
                csv_min_ext_pct = _get_float(row.get(H_MIN_EXT_PCT)) or 0.0
                csv_min_all_pct = _get_float(row.get(H_MIN_ALL_PCT)) or 0.0
                
                # 1. Apply rules for component percentages (if marks are 0, % must be 0)
                final_min_int_pct = csv_min_int_pct if final_internal_marks > 0 else 0.0
                final_min_ext_pct = csv_min_ext_pct if total_external_marks > 0 else 0.0
                
                # 2. Apply new rule for Overall percentage
                # If both components exist, use the CSV value for overall.
                if final_min_int_pct > 0 and final_min_ext_pct > 0:
                    final_min_all_pct = csv_min_all_pct
                # If only Internal component exists, Overall % should match Internal %
                elif final_min_int_pct > 0 and final_min_ext_pct == 0:
                    final_min_all_pct = final_min_int_pct
                # If only External component exists, Overall % should match External %
                elif final_min_int_pct == 0 and final_min_ext_pct > 0:
                    final_min_all_pct = final_min_ext_pct
                # If no components exist, Overall % must be 0
                else:
                    final_min_all_pct = 0.0
                # --- END OF NEW LOGIC ---
                

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
                    
                    "subject_type": str(row.get(H_TYPE, "Core") or "Core").strip(),
                    "program_code": str(row.get(H_PROGRAM, "") or "").strip().upper() or None,
                    "branch_code": str(row.get(H_BRANCH, "") or "").strip().upper() or None,
                    "curriculum_group_code": (
                        str(row.get(H_CURR_GROUP, "") or "").strip().upper() or None
                    ),
                    "description": str(row.get(H_DESC, "") or "").strip() or None,
                    
                    # This is now case-insensitive
                    "status": str(row.get(H_STATUS, "active") or "active").lower().strip(),
                    
                    "direct_source_mode": str(row.get(H_DIRECT_MODE, "overall") or "overall").strip(),
                    
                    "internal_marks_max": final_internal_marks,
                    "exam_marks_max": final_exam_marks,
                    "jury_viva_marks_max": final_jury_marks,
                    
                    # Use the final, rule-adjusted percentages
                    "min_internal_percent": final_min_int_pct,
                    "min_external_percent": final_min_ext_pct,
                    "min_overall_percent": final_min_all_pct,
                    
                    # Hard-code defaults for removed fields
                    "direct_internal_threshold_percent": 50.0,
                    "direct_external_threshold_percent": 40.0,
                    
                    "direct_internal_weight_percent": final_internal_weight,
                    "direct_external_weight_percent": final_external_weight,
                    "direct_target_students_percent": direct_attainment_percent,
                    "indirect_target_students_percent": indirect_attainment_percent,
                    "indirect_min_response_rate_percent": _get_float(row.get(H_ATT_INDIRECT_RATE_PCT)) or 0.0,
                    "overall_direct_weight_percent": direct_attainment_percent,
                    "overall_indirect_weight_percent": indirect_attainment_percent,
                    
                    "active": to_bool(row.get(H_ACTIVE), default=True),
                    "sort_order": _get_int(row.get(H_SORT)) or 100,
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
                    "subject_code": str(row.get(H_CODE, "")).strip(),
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
