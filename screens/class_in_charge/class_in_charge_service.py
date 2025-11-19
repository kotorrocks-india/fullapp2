# services/class_in_charge_service.py
"""
Service layer for Class-in-Charge management.
Handles all business logic, validations, and database operations.
"""

from __future__ import annotations
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, date, timedelta
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
import json
import logging

try:
    from screens.academic_years.db import compute_terms_with_validation
except ImportError:
    compute_terms_with_validation = None


log = logging.getLogger(__name__)


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def validate_assignment_dates(
    engine: Engine,
    ay_code: str,
    degree_code: str,
    program_code: Optional[str],
    branch_code: Optional[str],
    year: int,
    term: int,
    start_date: date,
    end_date: date,
    min_days: int = 30
) -> Tuple[bool, Optional[str]]:
    """
    Validate assignment dates against specific Term boundaries and minimum duration.
    
    Returns: (is_valid, error_message)
    """
    # Check basic date logic
    if end_date <= start_date:
        return False, "End date must be after start date"
    
    # Check minimum duration
    duration = (end_date - start_date).days
    if duration < min_days:
        return False, f"Assignment must be at least {min_days} days long (currently {duration} days)"
    
    # --- MODIFIED: Validate against specific TERM dates ---
    
    if compute_terms_with_validation is None:
        # Fallback if academic calendar module isn't available
        return True, "Warning: Could not import academic calendar. Skipping term boundary validation."

    # --- Primary Logic: Validate against Term ---
    try:
        with engine.begin() as conn:
            calculated_terms, warnings = compute_terms_with_validation(
                conn,
                ay_code=ay_code,
                degree_code=degree_code,
                program_code=program_code,
                branch_code=branch_code,
                progression_year=year
            )
        
        if not calculated_terms or len(calculated_terms) < term:
            return False, f"Could not find calendar data for {ay_code}, Year {year}, Term {term}."
            
        term_data = calculated_terms[term - 1]
        term_start = date.fromisoformat(term_data['start_date'])
        term_end = date.fromisoformat(term_data['end_date'])
        
        if start_date < term_start or start_date > term_end:
            return False, f"Start date must be within Term {term} boundaries ({term_start} to {term_end})"
        
        if end_date < term_start or end_date > term_end:
            return False, f"End date must be within Term {term} boundaries ({term_start} to {term_end})"
            
    except Exception as e:
        return False, f"Error validating term dates: {e}"
    
    return True, None

def check_overlapping_assignments(
    engine: Engine,
    ay_code: str,
    degree_code: str,
    program_code: str,
    branch_code: str,
    year: int,
    term: int,
    division_code: Optional[str],
    start_date: date,
    end_date: date,
    exclude_id: Optional[int] = None
) -> Tuple[bool, Optional[str]]:
    """
    Check for overlapping assignments in the same scope.
    
    Returns: (has_conflict, error_message)
    """
    with engine.begin() as conn:
        query = """
            SELECT id, faculty_name, start_date, end_date
            FROM class_in_charge_assignments
            WHERE ay_code = :ay
              AND degree_code = :deg
              AND program_code = :prog
              AND branch_code = :br
              AND year = :yr
              AND term = :trm
              AND status = 'active'
              AND (
                  (start_date <= :end AND end_date >= :start)
              )
        """
        
        params = {
            "ay": ay_code,
            "deg": degree_code,
            "prog": program_code,
            "br": branch_code,
            "yr": year,
            "trm": term,
            "start": start_date.isoformat(),
            "end": end_date.isoformat()
        }
        
        # Handle division_code
        if division_code:
            query += " AND division_code = :div"
            params["div"] = division_code
        else:
            query += " AND (division_code IS NULL OR division_code = '')"
        
        # Exclude current assignment if editing
        if exclude_id:
            query += " AND id != :exc_id"
            params["exc_id"] = exclude_id
        
        conflict = conn.execute(sa_text(query), params).fetchone()
        
        if conflict:
            return True, (
                f"Overlapping assignment found: {conflict[1]} "
                f"({conflict[2]} to {conflict[3]})"
            )
    
    return False, None


def check_faculty_availability(
    engine: Engine,
    faculty_id: int,
    ay_code: str,
    exclude_id: Optional[int] = None
) -> Tuple[bool, Optional[str]]:
    """
    Check if faculty already has an active CIC assignment in this AY.
    
    Returns: (is_available, warning_message)
    """
    with engine.begin() as conn:
        query = """
            SELECT id, degree_code, program_code, branch_code, year, term
            FROM class_in_charge_assignments
            WHERE faculty_id = :fid
              AND ay_code = :ay
              AND status = 'active'
        """
        params = {"fid": faculty_id, "ay": ay_code}
        
        if exclude_id:
            query += " AND id != :exc_id"
            params["exc_id"] = exclude_id
        
        existing = conn.execute(sa_text(query), params).fetchone()
        
        if existing:
            return False, (
                f"Faculty already has an active CIC assignment in {ay_code}: "
                f"{existing[1]}/{existing[2]}/{existing[3]} - Year {existing[4]}, Term {existing[5]}"
            )
    
    return True, None


def check_admin_position_conflict(
    engine: Engine,
    faculty_id: int
) -> Tuple[bool, Optional[str]]:
    """
    Check if faculty holds an administrative position.
    
    Returns: (has_position, warning_message)
    """
    with engine.begin() as conn:
        # Check administrative positions
        admin_pos = conn.execute(sa_text("""
            SELECT p.position_title, pa.degree_code, pa.branch_code
            FROM position_assignments pa
            JOIN administrative_positions p ON p.position_code = pa.position_code
            WHERE pa.assignee_type = 'faculty'
              AND pa.is_active = 1
              AND EXISTS (
                  SELECT 1 FROM faculty_profiles fp 
                  WHERE fp.id = :fid AND fp.email = pa.assignee_email
              )
            LIMIT 1
        """), {"fid": faculty_id}).fetchone()
        
        if admin_pos:
            scope = f" for {admin_pos[1]}"
            if admin_pos[2]:
                scope += f"/{admin_pos[2]}"
            
            return True, (
                f"⚠️ Faculty holds administrative position: {admin_pos[0]}{scope}. "
                "This may impact CIC workload."
            )
    
    return False, None


def check_branch_head_conflict(
    engine: Engine,
    faculty_id: int,
    ay_code: str
) -> Tuple[bool, Optional[str]]:
    """
    Check if faculty is Branch Head in the same AY.
    Per specification: "Faculty cannot be Class-in-Charge and Branch Head 
    simultaneously in the same Academic Year."
    
    Returns: (has_conflict, error_message)
    """
    with engine.begin() as conn:
        # Check if branch_head_assignments table exists
        table_exists = conn.execute(sa_text("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='branch_head_assignments'
        """)).fetchone()
        
        if not table_exists:
            return False, None
        
        # Check for active Branch Head assignment in same AY
        bh = conn.execute(sa_text("""
            SELECT branch_code, degree_code
            FROM branch_head_assignments
            WHERE faculty_id = :fid
              AND ay_code = :ay
              AND status = 'active'
            LIMIT 1
        """), {"fid": faculty_id, "ay": ay_code}).fetchone()
        
        if bh:
            return True, (
                f"Faculty is already Branch Head for {bh[1]}/{bh[0]} "
                f"in {ay_code}. Cannot be CIC in the same Academic Year."
            )
    
    return False, None


def get_faculty_warnings(
    engine: Engine,
    faculty_id: int,
    ay_code: str
) -> List[str]:
    """
    Get all warnings for a faculty member.
    
    Returns: List of warning messages
    """
    warnings = []
    
    # Check admin position
    has_admin, admin_msg = check_admin_position_conflict(engine, faculty_id)
    if has_admin and admin_msg:
        warnings.append(admin_msg)
    
    # Check availability
    available, avail_msg = check_faculty_availability(engine, faculty_id, ay_code)
    if not available and avail_msg:
        warnings.append(avail_msg)
    
    # Check Branch Head conflict
    is_bh, bh_msg = check_branch_head_conflict(engine, faculty_id, ay_code)
    if is_bh and bh_msg:
        warnings.append(bh_msg)
    
    return warnings


# ============================================================================
# CRUD OPERATIONS
# ============================================================================

def create_assignment(
    engine: Engine,
    data: Dict[str, Any],
    actor: str,
    skip_validation: bool = False
) -> Tuple[Optional[int], List[str], List[str]]:
    """
    Create a new CIC assignment.
    
    Args:
        engine: Database engine
        data: Assignment data
        actor: Email of user creating assignment
        skip_validation: Skip business rule validation (for superadmin)
    
    Returns:
        (assignment_id, errors, warnings)
    """
    errors = []
    warnings = []
    
    # Extract data
    ay_code = data.get("ay_code")
    degree_code = data.get("degree_code")
    program_code = data.get("program_code")
    branch_code = data.get("branch_code")
    year = data.get("year")
    term = data.get("term")
    division_code = data.get("division_code")
    faculty_id = data.get("faculty_id")
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    status = data.get("status", "active")
    
    # Get faculty details
    with engine.begin() as conn:
        faculty = conn.execute(sa_text("""
            SELECT email, name, status 
            FROM faculty_profiles 
            WHERE id = :fid
        """), {"fid": faculty_id}).fetchone()
        
        if not faculty:
            errors.append("Faculty not found")
            return None, errors, warnings
        
        if faculty[2] != 'active':
            errors.append(f"Faculty is not active (status: {faculty[2]})")
    
    if errors:
        return None, errors, warnings
    
    faculty_email = faculty[0]
    faculty_name = faculty[1]
    
    # Validations
    if not skip_validation:
    # Date validation
        valid, date_err = validate_assignment_dates(
            engine, 
            ay_code=ay_code,
            degree_code=degree_code,
            program_code=program_code,
            branch_code=branch_code,
            year=year,
            term=term,
            start_date=start_date, 
            end_date=end_date
        )
        # --- *** SYNTAX FIX IS HERE *** ---
        if not valid:
        # --- *** END SYNTAX FIX *** ---
            errors.append(date_err)
        
        # Overlap check
        has_overlap, overlap_err = check_overlapping_assignments(
            engine, ay_code, degree_code, program_code, branch_code,
            year, term, division_code, start_date, end_date
        )
        if has_overlap:
            errors.append(overlap_err)
        
        # Branch Head conflict (this is a hard error)
        is_bh, bh_err = check_branch_head_conflict(engine, faculty_id, ay_code)
        if is_bh:
            errors.append(bh_err)
        
        # Faculty availability (warning, not error)
        available, avail_warn = check_faculty_availability(engine, faculty_id, ay_code)
        if not available and avail_warn:
            warnings.append(avail_warn)
        
        # Admin position (warning, not error)
        has_admin, admin_warn = check_admin_position_conflict(engine, faculty_id)
        if has_admin and admin_warn:
            warnings.append(admin_warn)
    
    if errors:
        return None, errors, warnings
    
    # Create assignment
    try:
        with engine.begin() as conn:
            result = conn.execute(sa_text("""
                INSERT INTO class_in_charge_assignments (
                    ay_code, degree_code, program_code, branch_code,
                    year, term, division_code,
                    faculty_id, faculty_email, faculty_name,
                    start_date, end_date, status,
                    approval_status, created_by, created_at
                ) VALUES (
                    :ay, :deg, :prog, :br,
                    :yr, :trm, :div,
                    :fid, :femail, :fname,
                    :start, :end, :status,
                    'pending', :actor, CURRENT_TIMESTAMP
                )
            """), {
                "ay": ay_code,
                "deg": degree_code,
                "prog": program_code,
                "br": branch_code,
                "yr": year,
                "trm": term,
                "div": division_code,
                "fid": faculty_id,
                "femail": faculty_email,
                "fname": faculty_name,
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "status": status,
                "actor": actor
            })
            
            assignment_id = result.lastrowid
            
            # Audit log
            conn.execute(sa_text("""
                INSERT INTO class_in_charge_audit (
                    assignment_id, action, ay_code, degree_code,
                    program_code, branch_code, year, term, division_code,
                    faculty_email, actor_email, source, occurred_at
                ) VALUES (
                    :aid, 'CREATE', :ay, :deg,
                    :prog, :br, :yr, :trm, :div,
                    :femail, :actor, 'ui', CURRENT_TIMESTAMP
                )
            """), {
                "aid": assignment_id,
                "ay": ay_code,
                "deg": degree_code,
                "prog": program_code,
                "br": branch_code,
                "yr": year,
                "trm": term,
                "div": division_code,
                "femail": faculty_email,
                "actor": actor
            })
            
            log.info(f"Created CIC assignment {assignment_id} for {faculty_name}")
            return assignment_id, errors, warnings
            
    except Exception as e:
        log.error(f"Failed to create CIC assignment: {e}")
        errors.append(f"Database error: {str(e)}")
        return None, errors, warnings


def update_assignment(
    engine: Engine,
    assignment_id: int,
    data: Dict[str, Any],
    actor: str,
    skip_validation: bool = False
) -> Tuple[bool, List[str], List[str]]:
    """
    Update an existing CIC assignment.
    
    Returns: (success, errors, warnings)
    """
    errors = []
    warnings = []
    
    # Get current assignment
    with engine.begin() as conn:
        current = conn.execute(sa_text("""
            SELECT * FROM class_in_charge_assignments WHERE id = :id
        """), {"id": assignment_id}).fetchone()
        
        if not current:
            errors.append("Assignment not found")
            return False, errors, warnings
        
        current_dict = dict(current._mapping)
    
    # Track changes
    changes = {}
    for key, new_value in data.items():
        old_value = current_dict.get(key)
        # Handle date objects vs date strings
        if isinstance(old_value, str) and isinstance(new_value, date):
            old_value_date = None
            try:
                old_value_date = date.fromisoformat(old_value)
            except Exception:
                pass # Not a valid date string
            
            if old_value_date != new_value:
                 changes[key] = {"old": old_value, "new": new_value}
        
        elif old_value != new_value:
            changes[key] = {"old": old_value, "new": new_value}
    
    if not changes:
        warnings.append("No changes detected")
        return True, errors, warnings
    
    # Validate if dates are changing
    if not skip_validation and ("start_date" in changes or "end_date" in changes):
        new_start = data.get("start_date", current_dict["start_date"])
        new_end = data.get("end_date", current_dict["end_date"])

        # Ensure we are comparing date objects
        if isinstance(new_start, str):
            new_start = date.fromisoformat(new_start)
        if isinstance(new_end, str):
            new_end = date.fromisoformat(new_end)
        
        # --- THIS IS THE CORRECTED VALIDATION CALL ---
        valid, date_err = validate_assignment_dates(
            engine, 
            ay_code=current_dict["ay_code"],
            degree_code=current_dict["degree_code"],
            program_code=current_dict.get("program_code"),
            branch_code=current_dict.get("branch_code"),
            year=current_dict["year"],
            term=current_dict["term"],
            start_date=new_start, 
            end_date=new_end
        )
        if not valid:
            errors.append(date_err)
        # --- END CORRECTION ---
        
        # Check overlaps
        has_overlap, overlap_err = check_overlapping_assignments(
            engine,
            current_dict["ay_code"],
            current_dict["degree_code"],
            current_dict["program_code"],
            current_dict["branch_code"],
            current_dict["year"],
            current_dict["term"],
            current_dict.get("division_code"),
            new_start,
            new_end,
            exclude_id=assignment_id
        )
        if has_overlap:
            errors.append(overlap_err)
    
    # Check faculty change
    if "faculty_id" in changes and not skip_validation:
        new_fid = data["faculty_id"]
        
        # Get new faculty details
        with engine.begin() as conn:
            faculty = conn.execute(sa_text("""
                SELECT email, name, status 
                FROM faculty_profiles 
                WHERE id = :fid
            """), {"fid": new_fid}).fetchone()
            
            if not faculty:
                errors.append("New faculty not found")
            elif faculty[2] != 'active':
                errors.append(f"New faculty is not active (status: {faculty[2]})")
            else:
                # Check conflicts for new faculty
                is_bh, bh_err = check_branch_head_conflict(
                    engine, new_fid, current_dict["ay_code"]
                )
                if is_bh:
                    errors.append(bh_err)
                
                # Warnings
                faculty_warnings = get_faculty_warnings(
                    engine, new_fid, current_dict["ay_code"]
                )
                warnings.extend(faculty_warnings)
                
                # Update faculty details in data
                data["faculty_email"] = faculty[0]
                data["faculty_name"] = faculty[1]
    
    if errors:
        return False, errors, warnings
    
    # Build update query
    update_fields = []
    params = {"id": assignment_id, "actor": actor}
    
    for key, value in data.items():
        if key in changes:
            update_fields.append(f"{key} = :{key}")
            # Convert date objects back to isoformat strings for DB
            if isinstance(value, date):
                params[key] = value.isoformat()
            else:
                params[key] = value
    
    if not update_fields:
        return True, errors, warnings
    
    update_fields.append("updated_at = CURRENT_TIMESTAMP")
    update_fields.append("updated_by = :actor")
    
    try:
        with engine.begin() as conn:
            conn.execute(sa_text(f"""
                UPDATE class_in_charge_assignments
                SET {', '.join(update_fields)}
                WHERE id = :id
            """), params)
            
            # Audit log
            conn.execute(sa_text("""
                INSERT INTO class_in_charge_audit (
                    assignment_id, action, changed_fields,
                    actor_email, source, occurred_at
                ) VALUES (
                    :aid, 'UPDATE', :changes,
                    :actor, 'ui', CURRENT_TIMESTAMP
                )
            """), {
                "aid": assignment_id,
                "changes": json.dumps(changes, default=str), # Use default=str for date objects
                "actor": actor
            })
            
            log.info(f"Updated CIC assignment {assignment_id}")
            return True, errors, warnings
            
    except Exception as e:
        log.error(f"Failed to update CIC assignment: {e}")
        errors.append(f"Database error: {str(e)}")
        return False, errors, warnings
    
def change_cic(
    engine: Engine,
    assignment_id: int,
    new_faculty_id: int,
    new_faculty_email: str,
    new_faculty_name: str,
    actor: str,
    reason: str
) -> Tuple[bool, List[str]]:
    """
    Change the CIC for an existing assignment.
    
    This function:
    1. Validates the new faculty is not already assigned to this year/term
    2. Updates the assignment with new faculty details
    3. Records the change in audit trail
    
    Returns: (success, errors)
    """
    errors = []
    
    try:
        with engine.begin() as conn:
            # Get current assignment details
            current = conn.execute(sa_text("""
                SELECT * FROM class_in_charge_assignments WHERE id = :id
            """), {"id": assignment_id}).fetchone()
            
            if not current:
                errors.append("Assignment not found")
                return False, errors
            
            current_dict = dict(current._mapping)
            
            # Update the assignment
            conn.execute(sa_text("""
                UPDATE class_in_charge_assignments
                SET faculty_id = :fid,
                    faculty_email = :femail,
                    faculty_name = :fname,
                    updated_at = CURRENT_TIMESTAMP,
                    updated_by = :actor
                WHERE id = :id
            """), {
                "id": assignment_id,
                "fid": new_faculty_id,
                "femail": new_faculty_email,
                "fname": new_faculty_name,
                "actor": actor
            })
            
            # Record in audit trail
            changed_fields = {
                "faculty_id": {"old": current_dict['faculty_id'], "new": new_faculty_id},
                "faculty_email": {"old": current_dict['faculty_email'], "new": new_faculty_email},
                "faculty_name": {"old": current_dict['faculty_name'], "new": new_faculty_name}
            }
            
            conn.execute(sa_text("""
                INSERT INTO class_in_charge_audit (
                    assignment_id, action, reason,
                    changed_fields, actor_email, source, occurred_at,
                    ay_code, degree_code, program_code, branch_code,
                    year, term, division_code, faculty_email
                ) VALUES (
                    :aid, 'CHANGE_CIC', :reason,
                    :changes, :actor, 'ui', CURRENT_TIMESTAMP,
                    :ay, :deg, :prog, :br,
                    :yr, :trm, :div, :femail
                )
            """), {
                "aid": assignment_id,
                "reason": reason,
                "changes": json.dumps(changed_fields),
                "actor": actor,
                "ay": current_dict['ay_code'],
                "deg": current_dict['degree_code'],
                "prog": current_dict['program_code'],
                "br": current_dict['branch_code'],
                "yr": current_dict['year'],
                "trm": current_dict['term'],
                "div": current_dict.get('division_code'),
                "femail": new_faculty_email
            })
            
            log.info(f"Changed CIC for assignment {assignment_id}: {current_dict['faculty_name']} → {new_faculty_name}")
            return True, errors
            
    except Exception as e:
        log.error(f"Failed to change CIC: {e}")
        errors.append(f"Database error: {str(e)}")
        return False, errors


def change_status(
    engine: Engine,
    assignment_id: int,
    new_status: str,
    actor: str,
    reason: Optional[str] = None
) -> Tuple[bool, List[str]]:
    """
    Change assignment status (active, inactive, suspended).
    
    Returns: (success, errors)
    """
    errors = []
    
    if new_status not in ['active', 'inactive', 'suspended']:
        errors.append(f"Invalid status: {new_status}")
        return False, errors
    
    try:
        with engine.begin() as conn:
            # Get current status
            current = conn.execute(sa_text("""
                SELECT status FROM class_in_charge_assignments WHERE id = :id
            """), {"id": assignment_id}).fetchone()
            
            if not current:
                errors.append("Assignment not found")
                return False, errors
            
            if current[0] == new_status:
                errors.append(f"Assignment already has status: {new_status}")
                return False, errors
            
            # Update status
            conn.execute(sa_text("""
                UPDATE class_in_charge_assignments
                SET status = :status,
                    updated_at = CURRENT_TIMESTAMP,
                    updated_by = :actor
                WHERE id = :id
            """), {"id": assignment_id, "status": new_status, "actor": actor})
            
            # Audit log
            action = f"STATUS_CHANGE_{new_status.upper()}"
            conn.execute(sa_text("""
                INSERT INTO class_in_charge_audit (
                    assignment_id, action, reason,
                    changed_fields, actor_email, source, occurred_at
                ) VALUES (
                    :aid, :action, :reason,
                    :changes, :actor, 'ui', CURRENT_TIMESTAMP
                )
            """), {
                "aid": assignment_id,
                "action": action,
                "reason": reason,
                "changes": json.dumps({"status": {"old": current[0], "new": new_status}}),
                "actor": actor
            })
            
            log.info(f"Changed CIC assignment {assignment_id} status to {new_status}")
            return True, errors
            
    except Exception as e:
        log.error(f"Failed to change status: {e}")
        errors.append(f"Database error: {str(e)}")
        return False, errors


def delete_assignment(
    engine: Engine,
    assignment_id: int,
    actor: str,
    reason: str
) -> Tuple[bool, List[str]]:
    """
    Hard delete a CIC assignment.
    
    Returns: (success, errors)
    """
    errors = []
    
    try:
        with engine.begin() as conn:
            # Get assignment details for audit
            assignment = conn.execute(sa_text("""
                SELECT * FROM class_in_charge_assignments WHERE id = :id
            """), {"id": assignment_id}).fetchone()
            
            if not assignment:
                errors.append("Assignment not found")
                return False, errors
            
            assignment_dict = dict(assignment._mapping)
            
            # Audit log before deletion
            conn.execute(sa_text("""
                INSERT INTO class_in_charge_audit (
                    assignment_id, action, reason,
                    ay_code, degree_code, program_code, branch_code,
                    year, term, division_code, faculty_email,
                    actor_email, source, occurred_at
                ) VALUES (
                    :aid, 'DELETE', :reason,
                    :ay, :deg, :prog, :br,
                    :yr, :trm, :div, :femail,
                    :actor, 'ui', CURRENT_TIMESTAMP
                )
            """), {
                "aid": assignment_id,
                "reason": reason,
                "ay": assignment_dict["ay_code"],
                "deg": assignment_dict["degree_code"],
                "prog": assignment_dict["program_code"],
                "br": assignment_dict["branch_code"],
                "yr": assignment_dict["year"],
                "trm": assignment_dict["term"],
                "div": assignment_dict.get("division_code"),
                "femail": assignment_dict["faculty_email"],
                "actor": actor
            })
            
            # Delete assignment
            conn.execute(sa_text("""
                DELETE FROM class_in_charge_assignments WHERE id = :id
            """), {"id": assignment_id})
            
            log.info(f"Deleted CIC assignment {assignment_id}")
            return True, errors
            
    except Exception as e:
        log.error(f"Failed to delete assignment: {e}")
        errors.append(f"Database error: {str(e)}")
        return False, errors


# ============================================================================
# QUERY HELPERS
# ============================================================================

def get_assignment_by_id(engine: Engine, assignment_id: int) -> Optional[Dict]:
    """Get full assignment details from the base table."""
    with engine.begin() as conn:
        # Query the base table directly to avoid UNION errors with views
        result = conn.execute(sa_text("""
            SELECT * FROM class_in_charge_assignments 
            WHERE id = :id
            LIMIT 1
        """), {"id": assignment_id}).fetchone()
        
        if result:
            return dict(result._mapping)
    return None

def list_assignments(
    engine: Engine,
    filters: Optional[Dict[str, Any]] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Dict]:
    """List assignments with optional filters."""
    query = "SELECT * FROM v_cic_assignment_history WHERE 1=1"
    params = {}
    
    if filters:
        if filters.get("ay_code"):
            query += " AND ay_code = :ay"
            params["ay"] = filters["ay_code"]
        
        if filters.get("degree_code"):
            query += " AND degree_code = :deg"
            params["deg"] = filters["degree_code"]
        
        if filters.get("program_code"):
            query += " AND program_code = :prog"
            params["prog"] = filters["program_code"]
        
        if filters.get("branch_code"):
            query += " AND branch_code = :br"
            params["br"] = filters["branch_code"]
        
        if filters.get("year"):
            query += " AND year = :yr"
            params["yr"] = filters["year"]
        
        if filters.get("term"):
            query += " AND term = :trm"
            params["trm"] = filters["term"]
        
        if filters.get("faculty_id"):
            query += " AND faculty_id = :fid"
            params["fid"] = filters["faculty_id"]
        
        if filters.get("status"):
            if isinstance(filters["status"], list):
                placeholders = ",".join([f":st{i}" for i in range(len(filters["status"]))])
                query += f" AND status IN ({placeholders})"
                for i, st in enumerate(filters["status"]):
                    params[f"st{i}"] = st
            else:
                query += " AND status = :status"
                params["status"] = filters["status"]
        
        if filters.get("expiring_soon"):
            query += " AND DATE(end_date) <= DATE('now', '+30 days')"
    
    query += " ORDER BY ay_code DESC, created_at DESC"
    query += f" LIMIT {limit} OFFSET {offset}"
    
    with engine.begin() as conn:
        results = conn.execute(sa_text(query), params).fetchall()
        return [dict(r._mapping) for r in results]


def get_expiring_assignments(engine: Engine, days: int = 30) -> List[Dict]:
    """Get assignments expiring within specified days."""
    with engine.begin() as conn:
        results = conn.execute(sa_text("""
            SELECT * FROM v_cic_current_assignments
            WHERE days_until_expiry <= :days
              AND days_until_expiry >= 0
            ORDER BY days_until_expiry ASC
        """), {"days": days}).fetchall()
        
        return [dict(r._mapping) for r in results]


def get_expired_assignments(engine: Engine) -> List[Dict]:
    """Get assignments that have expired but are still active."""
    with engine.begin() as conn:
        results = conn.execute(sa_text("""
            SELECT * FROM class_in_charge_assignments
            WHERE status = 'active'
              AND DATE(end_date) < DATE('now')
            ORDER BY end_date ASC
        """), {"days": days}).fetchall()
        
        return [dict(r._mapping) for r in results]


# Export all functions
__all__ = [
    'validate_assignment_dates',
    'check_overlapping_assignments',
    'check_faculty_availability',
    'check_admin_position_conflict',
    'check_branch_head_conflict',
    'get_faculty_warnings',
    'create_assignment',
    'update_assignment',
    'change_cic',
    'change_status',
    'delete_assignment',
    'get_assignment_by_id',
    'list_assignments',
    'get_expiring_assignments',
    'get_expired_assignments'
]
