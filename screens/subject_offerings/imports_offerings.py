"""
Import functions for bulk subject offerings import
"""

from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
from sqlalchemy.engine import Engine

# Use relative imports for modules in the same package
from .helpers import exec_query, to_bool, safe_int
from .offerings_crud import create_offering_from_catalog
from .constants import validate_offering_uniqueness, SUBJECT_TYPES, STATUS_VALUES

# --- LOCAL HELPERS ---

def _get_str(val: Any) -> Optional[str]:
    """Safely get a stripped string or None."""
    if val is None or pd.isna(val): return None
    s = str(val).strip()
    return s if s else None

def _find_catalog_subject_id(
    engine: Engine, subject_code: str, degree_code: str
) -> Optional[int]:
    """Finds an active subject in the catalog by code and degree."""
    with engine.begin() as conn:
        row = exec_query(conn, """
            SELECT id FROM subjects_catalog
            WHERE subject_code = :sc AND degree_code = :dc AND active = 1
            LIMIT 1
        """, {"sc": subject_code, "dc": degree_code}).fetchone()
    return row[0] if row else None

def _get_catalog_row(engine: Engine, catalog_subject_id: int) -> Optional[Dict[str, Any]]:
    """Fetches the full catalog row for validation."""
    with engine.begin() as conn:
        row = exec_query(conn, """
            SELECT program_code, branch_code FROM subjects_catalog WHERE id = :id
        """, {"id": catalog_subject_id}).fetchone()
    return dict(row._mapping) if row else None

def _validate_uniqueness_with_engine(
    engine: Engine, data: Dict[str, Any]
) -> Tuple[bool, str]:
    """Wraps the uniqueness check in its own transaction."""
    with engine.begin() as conn:
        return validate_offering_uniqueness(conn, data)


def import_offerings_from_df(
    engine: Engine, 
    df: pd.DataFrame, 
    dry_run: bool,
    actor: str
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Import subject offerings from a DataFrame.
    - Matches subjects on (subject_code, degree_code) from catalog.
    - Creates new offerings in subject_offerings table.
    
    NOTE: Due to the design of offerings_crud.py, each creation is a separate
    transaction. This is not a single atomic bulk import.
    """
    if df is None or df.empty:
        return [{"row": None, "subject_code": "", "error": "Empty DataFrame"}], 0

    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    errors: List[Dict[str, Any]] = []
    success_create_count = 0

    for idx, row in df.iterrows():
        row_num = idx + 2  # assuming row 1 is header
        
        subject_code_raw = _get_str(row.get("subject_code"))
        degree_code_raw = _get_str(row.get("degree_code"))
        
        try:
            # --- 1. Get Core Fields ---
            degree_code = str(degree_code_raw or "").strip().upper()
            ay_label = _get_str(row.get("ay_label"))
            year = safe_int(row.get("year"), default=0)
            term = safe_int(row.get("term"), default=0)
            subject_code = str(subject_code_raw or "").strip().upper()
            subject_type = _get_str(row.get("subject_type"))
            
            if not all([degree_code, ay_label, year > 0, term > 0, subject_code, subject_type]):
                raise ValueError(
                    "Missing required fields: degree_code, ay_label, year, term, subject_code, subject_type"
                )

            # --- 2. Validate Enums ---
            if subject_type not in SUBJECT_TYPES:
                raise ValueError(f"Invalid subject_type: '{subject_type}'. Must be one of: {', '.join(SUBJECT_TYPES)}")
            
            status = _get_str(row.get("status", "draft")) or "draft"
            if status not in STATUS_VALUES:
                raise ValueError(f"Invalid status: '{status}'. Must be one of: {', '.join(STATUS_VALUES)}")

            # --- 3. Find Catalog Subject ID ---
            catalog_subject_id = _find_catalog_subject_id(engine, subject_code, degree_code)
            if not catalog_subject_id:
                raise ValueError(
                    f"Active subject not found in catalog for subject_code '{subject_code}' and degree_code '{degree_code}'"
                )

            # --- 4. Get Optional Fields ---
            instructor_email = _get_str(row.get("instructor_email"))
            
            # Division logic
            applies_to_all_divisions = to_bool(row.get("applies_to_all_divisions"), default=True)
            division_code = _get_str(row.get("division_code"))
            
            if not applies_to_all_divisions and not division_code:
                raise ValueError("division_code is required when applies_to_all_divisions is False")
            if applies_to_all_divisions and division_code:
                division_code = None # Enforce consistency
            
            # --- 5. Call CRUD function or Dry Run Check ---
            if not dry_run:
                # This function handles its own transaction and audit logging
                create_offering_from_catalog(
                    engine=engine,
                    catalog_subject_id=catalog_subject_id,
                    ay_label=ay_label,
                    year=year,
                    term=term,
                    actor=actor,
                    division_code=division_code,
                    applies_to_all_divisions=applies_to_all_divisions,
                    instructor_email=instructor_email
                )
            else:
                # In dry run, we must manually validate uniqueness
                catalog_details = _get_catalog_row(engine, catalog_subject_id)
                data_for_validation = {
                    "ay_label": ay_label,
                    "degree_code": degree_code,
                    "program_code": catalog_details.get("program_code"),
                    "branch_code": catalog_details.get("branch_code"),
                    "year": year,
                    "term": term,
                    "division_code": division_code,
                    "subject_code": subject_code,
                }
                ok, msg = _validate_uniqueness_with_engine(engine, data_for_validation)
                if not ok:
                    raise ValueError(msg)

            success_create_count += 1

        except Exception as e:
            errors.append({
                "row": row_num,
                "subject_code": str(subject_code_raw or ""),
                "degree_code": str(degree_code_raw or ""),
                "error": str(e),
            })
            
    # Add summary row
    if errors or success_create_count > 0:
        summary_msg = ""
        if dry_run:
            summary_msg = f"Would create: {success_create_count}, Errors: {len(errors)}"
        else:
            summary_msg = f"Created: {success_create_count}, Errors: {len(errors)}"
            
        summary = {
            "row": "SUMMARY",
            "subject_code": "---",
            "degree_code": "---",
            "error": summary_msg
        }
        errors.insert(0, summary)

    return errors, success_create_count
