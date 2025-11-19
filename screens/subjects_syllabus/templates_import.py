"""
Import functions for bulk syllabus template import
UPDATED to match subjects_syllabus_schema.py structure
"""

from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
import json

# Use relative imports for modules in the same package
from .helpers import exec_query, to_bool, safe_float as helper_safe_float
from .templates_crud import create_syllabus_template
from .constants import validate_subject


# --- TEMPLATE IMPORT COLUMNS ---
TEMPLATE_IMPORT_COLUMNS = [
    "subject_code",
    "version",
    "name",
    "description",
    "effective_from_ay",
    "degree_code",
    "program_code",
    "branch_code",
    "point_sequence",
    "point_title",
    "point_description",
    "point_tags",
    "point_resources",
    "point_hours_weight",
]


# --- LOCAL HELPERS ---
def _get_int(val: Any) -> Optional[int]:
    if val is None or pd.isna(val): return None
    try: return int(val)
    except (ValueError, TypeError): return None


def _get_float(val: Any) -> Optional[float]:
    if val is None or pd.isna(val): return None
    try: return float(val)
    except (ValueError, TypeError): return None


def _get_str(val: Any) -> Optional[str]:
    if val is None or pd.isna(val): return None
    s = str(val).strip()
    return s if s else None


def _find_existing_template(conn, subject_code: str, version: str) -> Optional[int]:
    """Check if a template with this subject_code and version already exists."""
    row = exec_query(conn, """
        SELECT id FROM syllabus_templates
        WHERE subject_code = :sc AND version = :v
        LIMIT 1
    """, {"sc": subject_code, "v": version}).fetchone()
    return row[0] if row else None


def _validate_subject_exists(conn, subject_code: str) -> bool:
    """Check if the subject exists in subjects_catalog."""
    row = exec_query(conn, """
        SELECT 1 FROM subjects_catalog
        WHERE subject_code = :sc AND active = 1
        LIMIT 1
    """, {"sc": subject_code}).fetchone()
    return row is not None


def import_templates_from_df(
    engine, 
    df: pd.DataFrame, 
    dry_run: bool,
    actor: str,
    allow_update: bool = False
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Import syllabus templates from a DataFrame with dry run support.
    
    The CSV format groups multiple points per template:
    - Each unique (subject_code, version) combination represents one template
    - Multiple rows with the same (subject_code, version) but different point_sequence
      represent multiple points within that template
    
    Args:
        engine: Database engine
        df: DataFrame with template data
        dry_run: If True, validate but don't commit changes
        actor: User performing the import
        allow_update: If True, update existing templates; if False, skip duplicates
    
    Returns:
        Tuple of (errors_list, success_count)
    """
    if df is None or df.empty:
        return [{"row": None, "subject_code": "", "error": "Empty DataFrame"}], 0

    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    errors: List[Dict[str, Any]] = []
    success_create_count = 0
    success_update_count = 0

    conn = engine.connect()
    trans = conn.begin()

    try:
        # Group rows by (subject_code, version) to identify unique templates
        template_groups = df.groupby(['subject_code', 'version'], dropna=False)
        
        for (subject_code, version), group_df in template_groups:
            first_row_idx = group_df.index[0] + 2  # +2 for header and 0-indexing
            
            try:
                # --- VALIDATE TEMPLATE-LEVEL DATA ---
                subject_code = _get_str(subject_code)
                version = _get_str(version)
                
                if not subject_code or not version:
                    raise ValueError("subject_code and version are required")
                
                # Get template-level data from first row (should be same across all rows in group)
                first_row = group_df.iloc[0]
                
                name = _get_str(first_row.get("name"))
                description = _get_str(first_row.get("description"))
                effective_from_ay = _get_str(first_row.get("effective_from_ay"))
                degree_code = _get_str(first_row.get("degree_code"))
                program_code = _get_str(first_row.get("program_code"))
                branch_code = _get_str(first_row.get("branch_code"))
                
                if not name:
                    raise ValueError("Template name is required")
                
                # Validate subject exists
                if not _validate_subject_exists(conn, subject_code):
                    raise ValueError(f"Subject '{subject_code}' not found or inactive")
                
                # Check if template already exists
                existing_template_id = _find_existing_template(conn, subject_code, version)
                
                if existing_template_id and not allow_update:
                    raise ValueError(
                        f"Template version '{version}' already exists for {subject_code}. "
                        "Enable 'Allow Updates' to overwrite."
                    )
                
                # --- PROCESS POINTS ---
                points = []
                for idx, row in group_df.iterrows():
                    row_num = idx + 2
                    
                    try:
                        point_seq = _get_int(row.get("point_sequence"))
                        point_title = _get_str(row.get("point_title"))
                        point_desc = _get_str(row.get("point_description"))
                        point_tags = _get_str(row.get("point_tags"))
                        point_resources = _get_str(row.get("point_resources"))
                        point_hours = _get_float(row.get("point_hours_weight")) or 0.0
                        
                        if not point_seq:
                            raise ValueError(f"Row {row_num}: point_sequence is required")
                        
                        if not point_title:
                            raise ValueError(f"Row {row_num}: point_title is required")
                        
                        # Build point dict for new schema
                        points.append({
                            "sequence": point_seq,
                            "point_type": "unit",  # Default type
                            "code": None,
                            "title": point_title,
                            "description": point_desc,
                            "tags": point_tags,
                            "resources": point_resources,
                            "hours_weight": point_hours,
                        })
                        
                    except Exception as point_error:
                        errors.append({
                            "row": row_num,
                            "subject_code": subject_code,
                            "version": version,
                            "error": f"Point error: {str(point_error)}",
                        })
                        raise  # Re-raise to skip this entire template
                
                if not points:
                    raise ValueError("At least one point with a title is required")
                
                # Sort points by sequence
                points.sort(key=lambda p: p["sequence"])
                
                # --- CREATE OR UPDATE TEMPLATE ---
                if existing_template_id:
                    # Update: Delete old template and recreate
                    # (In a production system, you might want more sophisticated merging)
                    if not dry_run:
                        exec_query(conn, """
                            DELETE FROM syllabus_template_points 
                            WHERE template_id = :tid
                        """, {"tid": existing_template_id})
                        
                        exec_query(conn, """
                            DELETE FROM syllabus_templates 
                            WHERE id = :tid
                        """, {"tid": existing_template_id})
                    
                    success_update_count += 1
                else:
                    success_create_count += 1
                
                # Create new template (whether it's a true create or recreate after delete)
                if not dry_run:
                    create_syllabus_template(
                        engine=engine,
                        subject_code=subject_code,
                        version=version,
                        name=name,
                        points=points,
                        actor=actor,
                        description=description,
                        effective_from_ay=effective_from_ay,
                        degree_code=degree_code,
                        program_code=program_code,
                        branch_code=branch_code,
                    )
                
            except Exception as template_error:
                errors.append({
                    "row": first_row_idx,
                    "subject_code": str(subject_code or ""),
                    "version": str(version or ""),
                    "error": str(template_error),
                })
        
        if dry_run:
            trans.rollback()
        else:
            trans.commit()
        
        # Add summary row
        if errors or success_create_count or success_update_count:
            summary = {
                "row": "SUMMARY",
                "subject_code": "---",
                "version": "---",
                "error": f"Created: {success_create_count}, Updated: {success_update_count}, Errors: {len(errors)}"
            }
            errors.insert(0, summary)
    
    except Exception as e:
        try: 
            trans.rollback()
        except Exception: 
            pass
        errors.append({
            "row": None, 
            "subject_code": "", 
            "version": "",
            "error": f"Transaction failed: {e}"
        })
        success_create_count = 0
        success_update_count = 0
    
    finally:
        conn.close()
    
    total_success = success_create_count + success_update_count
    return errors, total_success
