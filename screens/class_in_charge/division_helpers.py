# screens/class_in_charge/division_helpers.py
"""
Division Detection Helpers for Class-in-Charge Module

Integrates with student schema's division_master and student_enrollments tables
to provide accurate division detection for CIC assignments.
"""

from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from typing import List, Dict, Optional


def get_divisions_for_scope(
    engine: Engine,
    degree_code: str,
    program_code: Optional[str],
    branch_code: Optional[str],
    ay_code: str,
    year: int
) -> List[str]:
    """
    Get list of active divisions for a specific academic scope.
    
    Detection strategy:
    1. First tries division_master table (authoritative source)
    2. Falls back to student_enrollments if division_master not available
    3. Returns empty list if no divisions exist
    
    Args:
        engine: Database engine
        degree_code: Degree code
        program_code: Program code (optional)
        branch_code: Branch code (optional)
        ay_code: Academic year code
        year: Current year number
    
    Returns:
        List of division codes (e.g., ['A', 'B', 'C'])
    """
    
    # Helper to execute queries
    def _fetch_all(query: str, params: dict) -> List[Dict]:
        with engine.begin() as conn:
            result = conn.execute(sa_text(query), params)
            return [dict(row._mapping) for row in result.fetchall()]
    
    # Strategy 1: Try division_master table (most reliable)
    try:
        # First, determine the batch for this scope
        batch_query = """
            SELECT DISTINCT se.batch
            FROM student_enrollments se
            WHERE se.degree_code = :deg
              AND (:prog IS NULL OR se.program_code = :prog)
              AND (:br IS NULL OR se.branch_code = :br)
              AND se.current_year = :yr
              AND se.enrollment_status = 'active'
            LIMIT 1
        """
        
        batch_result = _fetch_all(batch_query, {
            'deg': degree_code,
            'prog': program_code,
            'br': branch_code,
            'yr': year
        })
        
        if batch_result and batch_result[0].get('batch'):
            batch = batch_result[0]['batch']
            
            # Query division_master for this scope
            division_query = """
                SELECT division_code, division_name, capacity, active
                FROM division_master
                WHERE degree_code = :deg
                  AND (batch = :batch OR batch IS NULL)
                  AND current_year = :yr
                  AND active = 1
                ORDER BY division_code
            """
            
            divisions = _fetch_all(division_query, {
                'deg': degree_code,
                'batch': batch,
                'yr': year
            })
            
            if divisions:
                return [d['division_code'] for d in divisions if d.get('division_code')]
    
    except Exception as e:
        # division_master table might not exist or have different structure
        print(f"Warning: Could not query division_master: {e}")
    
    # Strategy 2: Fall back to student_enrollments
    try:
        enrollment_query = """
            SELECT DISTINCT se.division_code
            FROM student_enrollments se
            WHERE se.degree_code = :deg
              AND (:prog IS NULL OR se.program_code = :prog)
              AND (:br IS NULL OR se.branch_code = :br)
              AND se.current_year = :yr
              AND se.division_code IS NOT NULL
              AND se.division_code != ''
              AND se.enrollment_status = 'active'
            ORDER BY se.division_code
        """
        
        divisions = _fetch_all(enrollment_query, {
            'deg': degree_code,
            'prog': program_code,
            'br': branch_code,
            'yr': year
        })
        
        return [d['division_code'] for d in divisions if d.get('division_code')]
    
    except Exception as e:
        print(f"Warning: Could not query student_enrollments: {e}")
        return []


def get_division_details(
    engine: Engine,
    degree_code: str,
    batch: str,
    year: int
) -> List[Dict]:
    """
    Get detailed information about divisions including capacity.
    
    Returns list of dicts with:
    - division_code
    - division_name
    - capacity
    - current_student_count (from enrollments)
    - available_capacity
    """
    
    def _fetch_all(query: str, params: dict) -> List[Dict]:
        with engine.begin() as conn:
            result = conn.execute(sa_text(query), params)
            return [dict(row._mapping) for row in result.fetchall()]
    
    try:
        query = """
            SELECT 
                dm.division_code,
                dm.division_name,
                dm.capacity,
                COUNT(se.id) as current_student_count,
                CASE 
                    WHEN dm.capacity IS NULL THEN NULL
                    ELSE MAX(0, dm.capacity - COUNT(se.id))
                END as available_capacity
            FROM division_master dm
            LEFT JOIN student_enrollments se ON 
                se.degree_code = dm.degree_code
                AND se.batch = dm.batch
                AND se.current_year = dm.current_year
                AND se.division_code = dm.division_code
                AND se.enrollment_status = 'active'
            WHERE dm.degree_code = :deg
              AND dm.batch = :batch
              AND dm.current_year = :yr
              AND dm.active = 1
            GROUP BY dm.division_code, dm.division_name, dm.capacity
            ORDER BY dm.division_code
        """
        
        return _fetch_all(query, {
            'deg': degree_code,
            'batch': batch,
            'yr': year
        })
    
    except Exception:
        return []


def has_divisions(
    engine: Engine,
    degree_code: str,
    program_code: Optional[str],
    branch_code: Optional[str],
    ay_code: str,
    year: int
) -> bool:
    """
    Check if a scope has divisions defined.
    
    Returns:
        True if divisions exist, False otherwise
    """
    divisions = get_divisions_for_scope(
        engine, degree_code, program_code, branch_code, ay_code, year
    )
    return len(divisions) > 0


def validate_division_code(
    engine: Engine,
    degree_code: str,
    batch: str,
    year: int,
    division_code: str
) -> tuple[bool, Optional[str]]:
    """
    Validate that a division code exists and is active.
    
    Returns:
        (is_valid, error_message)
    """
    
    def _fetch_one(query: str, params: dict) -> Optional[Dict]:
        with engine.begin() as conn:
            result = conn.execute(sa_text(query), params)
            row = result.fetchone()
            return dict(row._mapping) if row else None
    
    try:
        query = """
            SELECT division_code, division_name, active, capacity
            FROM division_master
            WHERE degree_code = :deg
              AND batch = :batch
              AND current_year = :yr
              AND division_code = :div
        """
        
        division = _fetch_one(query, {
            'deg': degree_code,
            'batch': batch,
            'yr': year,
            'div': division_code
        })
        
        if not division:
            return False, f"Division '{division_code}' not found for {degree_code} batch {batch} year {year}"
        
        if not division.get('active'):
            return False, f"Division '{division_code}' is inactive"
        
        return True, None
    
    except Exception as e:
        # If division_master doesn't exist, assume any division code is valid
        return True, None


def get_student_count_by_division(
    engine: Engine,
    degree_code: str,
    program_code: Optional[str],
    branch_code: Optional[str],
    batch: str,
    year: int
) -> Dict[str, int]:
    """
    Get count of students in each division for a scope.
    
    Returns:
        Dict mapping division_code to student count
        e.g., {'A': 45, 'B': 42, 'C': 38}
    """
    
    def _fetch_all(query: str, params: dict) -> List[Dict]:
        with engine.begin() as conn:
            result = conn.execute(sa_text(query), params)
            return [dict(row._mapping) for row in result.fetchall()]
    
    try:
        query = """
            SELECT 
                se.division_code,
                COUNT(*) as student_count
            FROM student_enrollments se
            WHERE se.degree_code = :deg
              AND (:prog IS NULL OR se.program_code = :prog)
              AND (:br IS NULL OR se.branch_code = :br)
              AND se.batch = :batch
              AND se.current_year = :yr
              AND se.division_code IS NOT NULL
              AND se.division_code != ''
              AND se.enrollment_status = 'active'
            GROUP BY se.division_code
            ORDER BY se.division_code
        """
        
        results = _fetch_all(query, {
            'deg': degree_code,
            'prog': program_code,
            'br': branch_code,
            'batch': batch,
            'yr': year
        })
        
        return {r['division_code']: r['student_count'] for r in results}
    
    except Exception:
        return {}


# Export all functions
__all__ = [
    'get_divisions_for_scope',
    'get_division_details',
    'has_divisions',
    'validate_division_code',
    'get_student_count_by_division'
]
