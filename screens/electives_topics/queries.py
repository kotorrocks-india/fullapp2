# screens/electives_topics/queries.py
"""
Query functions for electives topics module.
FIXED: Topics can be created before offerings exist (matching intended workflow)
"""

from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine


def _exec(conn, sql: str, params: dict = None):
    """Execute SQL with parameters."""
    return conn.execute(sa_text(sql), params or {})


def _fetch_one(engine: Engine, sql: str, params: dict = None) -> Optional[Dict]:
    """Fetch single row."""
    with engine.begin() as conn:
        result = _exec(conn, sql, params).fetchone()
        return dict(result._mapping) if result else None


def _fetch_all(engine: Engine, sql: str, params: dict = None) -> List[Dict]:
    """Fetch all rows."""
    with engine.begin() as conn:
        results = _exec(conn, sql, params).fetchall()
        return [dict(r._mapping) for r in results]


# ===========================================================================
# TIMING VALIDATION (NEW)
# ===========================================================================


def get_term_start_date(engine: Engine, ay_label: str, year: int, term: int) -> Optional[datetime]:
    """
    Get the start date for a specific term.
    Assumes term starts at regular intervals from AY start date.
    """
    ay = _fetch_one(engine, """
        SELECT start_date, end_date FROM academic_years WHERE ay_code = :ay
    """, {"ay": ay_label})
    
    if not ay or not ay.get("start_date"):
        return None
    
    try:
        ay_start = datetime.fromisoformat(ay["start_date"])
        
        # Estimate term start based on year and term
        # Assuming: Year 1 Term 1 = AY start, each term is ~4 months
        months_offset = ((year - 1) * 12) + ((term - 1) * 4)
        term_start = ay_start + timedelta(days=months_offset * 30)  # Rough estimate
        
        return term_start
    except (ValueError, TypeError):
        return None


def validate_topic_creation_timing(
    engine: Engine,
    ay_label: str,
    year: int,
    term: int,
    lead_days: int = 21
) -> Tuple[bool, str]:
    """
    Validate that topics are being created within the allowed window.
    Topics should be created at least 'lead_days' before term starts.
    
    Returns: (is_valid, message)
    """
    term_start = get_term_start_date(engine, ay_label, year, term)
    
    if not term_start:
        # If we can't determine term start, allow creation but warn
        return True, "Warning: Could not validate timing - term start date unknown"
    
    now = datetime.now()
    days_until_start = (term_start - now).days
    
    if days_until_start < 0:
        return False, f"Term has already started ({abs(days_until_start)} days ago)"
    
    if days_until_start < lead_days:
        return False, f"Too late! Topics must be created at least {lead_days} days before term starts. Only {days_until_start} days remaining."
    
    return True, f"OK - {days_until_start} days until term starts"


def validate_selection_window_open(
    engine: Engine,
    ay_label: str,
    year: int,
    term: int,
    lead_days: int = 21,
    deadline_days: int = 7
) -> Tuple[bool, str]:
    """
    Validate that student selections are within the allowed window.
    Window: From (term_start - lead_days) to (term_start - deadline_days)
    
    Returns: (is_valid, message)
    """
    term_start = get_term_start_date(engine, ay_label, year, term)
    
    if not term_start:
        return True, "Warning: Could not validate selection window"
    
    now = datetime.now()
    days_until_start = (term_start - now).days
    
    # Check if before window opens
    if days_until_start > lead_days:
        return False, f"Selection window not open yet. Opens in {days_until_start - lead_days} days."
    
    # Check if after window closes
    if days_until_start < deadline_days:
        return False, f"Selection window closed {deadline_days - days_until_start} days ago"
    
    days_remaining = days_until_start - deadline_days
    return True, f"Selection window open - {days_remaining} days remaining"


# ===========================================================================
# CAPACITY VALIDATION (NEW)
# ===========================================================================


def validate_total_capacity(
    engine: Engine,
    subject_code: str,
    ay_label: str,
    year: int,
    term: int,
    degree_code: str,
    program_code: Optional[str] = None,
    branch_code: Optional[str] = None,
    division_code: Optional[str] = None
) -> Tuple[bool, str, Dict]:
    """
    Validate that total capacity across all topics is sufficient for all students.
    
    Returns: (is_valid, message, stats_dict)
    """
    # Get total capacity from topics
    topics = _fetch_all(engine, """
        SELECT id, topic_code_ay, capacity
        FROM elective_topics
        WHERE subject_code = :subj
          AND ay_label = :ay
          AND year = :yr
          AND term = :trm
          AND degree_code = :deg
          AND status = 'published'
    """, {
        "subj": subject_code,
        "ay": ay_label,
        "yr": year,
        "trm": term,
        "deg": degree_code
    })
    
    if not topics:
        return False, "No published topics found", {}
    
    total_capacity = sum(t['capacity'] for t in topics if t['capacity'] > 0)
    unlimited_topics = sum(1 for t in topics if t['capacity'] == 0)
    
    # Count eligible students
    query = """
        SELECT COUNT(DISTINCT se.student_id) as student_count
        FROM student_enrollment se
        WHERE se.degree_code = :deg
          AND se.current_year = :yr
          AND se.status = 'active'
    """
    params = {"deg": degree_code, "yr": year}
    
    if program_code:
        query += " AND se.program_code = :prog"
        params["prog"] = program_code
    
    if branch_code:
        query += " AND se.branch_code = :br"
        params["br"] = branch_code
    
    if division_code:
        query += " AND se.division_code = :div"
        params["div"] = division_code
    
    result = _fetch_one(engine, query, params)
    student_count = result['student_count'] if result else 0
    
    stats = {
        "total_capacity": total_capacity,
        "unlimited_topics": unlimited_topics,
        "student_count": student_count,
        "topics_count": len(topics),
        "capacity_per_student": round(total_capacity / student_count, 2) if student_count > 0 else 0
    }
    
    # Validation
    if unlimited_topics > 0:
        return True, f"OK - {unlimited_topics} topic(s) with unlimited capacity", stats
    
    if total_capacity == 0:
        return False, "No capacity available - all topics have capacity = 0", stats
    
    if student_count == 0:
        return True, "OK - No students enrolled yet", stats
    
    if total_capacity < student_count:
        shortage = student_count - total_capacity
        return False, f"Insufficient capacity! {shortage} students cannot be accommodated. Need to increase capacity or add topics.", stats
    
    buffer = total_capacity - student_count
    return True, f"OK - Capacity sufficient with {buffer} seats buffer", stats


# ===========================================================================
# ELECTIVE SUBJECTS (catalog only - offerings optional)
# ===========================================================================


def fetch_elective_subjects(
    engine: Engine,
    degree_code: str,
    ay_label: str,
    year: int,
    term: int,
    program_code: Optional[str] = None,
    branch_code: Optional[str] = None,
    require_offering: bool = False  # NEW: Make offering optional
) -> List[Dict]:
    """
    Fetch elective/CP subjects for a given context.
    
    UPDATED: By default, fetches from subjects_catalog only.
    Set require_offering=True to also require published offering with syllabus.
    
    FIXED: JOINs with semesters table to filter by year/term
    instead of using non-existent semester_number column.
    """
    
    if not require_offering:
        # NEW BEHAVIOR: Fetch from catalog only
        # FIXED: JOIN with semesters table and filter by year_index, term_index
        return _fetch_all(
            engine,
            """
            SELECT DISTINCT 
                sc.subject_code, 
                sc.subject_name,
                sc.subject_type,
                sc.credits_total,
                sc.degree_code,
                sc.program_code,
                sc.branch_code,
                s.semester_number,
                s.year_index,
                s.term_index,
                s.label as semester_label
            FROM subjects_catalog sc
            JOIN semesters s 
                ON sc.semester_id = s.id
                AND s.degree_code = sc.degree_code
            WHERE sc.degree_code = :deg
              AND LOWER(sc.subject_type) IN ('elective', 'college project')
              AND sc.active = 1
              AND s.year_index = :year
              AND s.term_index = :term
              AND (:prog IS NULL OR sc.program_code = :prog OR sc.program_code IS NULL)
              AND (:br   IS NULL OR sc.branch_code  = :br   OR sc.branch_code  IS NULL)
            ORDER BY sc.subject_code
            """,
            {
                "deg": degree_code,
                "year": year,
                "term": term,
                "prog": program_code,
                "br": branch_code,
            },
        )
    
    # OLD BEHAVIOR: Require offering + syllabus (for compatibility)
    # FIXED: JOIN with semesters table and use case-insensitive matching
    return _fetch_all(
        engine,
        """
        SELECT DISTINCT 
            sc.subject_code, 
            sc.subject_name,
            sc.subject_type,
            sc.credits_total,
            so.id AS offering_id,
            so.instructor_email,
            so.status AS offering_status,
            so.syllabus_template_id,
            s.semester_number,
            s.year_index,
            s.term_index,
            s.label as semester_label
        FROM subjects_catalog sc
        JOIN semesters s 
            ON sc.semester_id = s.id
            AND s.degree_code = sc.degree_code
        JOIN subject_offerings so 
            ON so.subject_code = sc.subject_code
            AND so.degree_code = sc.degree_code
            AND (so.program_code = sc.program_code 
                 OR (so.program_code IS NULL AND sc.program_code IS NULL))
            AND (so.branch_code = sc.branch_code 
                 OR (so.branch_code IS NULL AND sc.branch_code IS NULL))
        WHERE sc.degree_code = :deg
          AND LOWER(sc.subject_type) IN ('elective', 'college project')
          AND s.year_index = :year
          AND s.term_index = :term
          AND so.ay_label = :ay
          AND so.year     = :yr
          AND so.term     = :trm
          AND so.status   = 'published'
          AND so.syllabus_template_id IS NOT NULL
          AND sc.active   = 1
          AND (:prog IS NULL OR sc.program_code = :prog OR sc.program_code IS NULL)
          AND (:br   IS NULL OR sc.branch_code  = :br   OR sc.branch_code  IS NULL)
        ORDER BY sc.subject_code
        """,
        {
            "deg": degree_code,
            "ay": ay_label,
            "yr": year,
            "trm": term,
            "year": year,
            "term": term,
            "prog": program_code,
            "br": branch_code,
        },
    )


def get_subject_details(
    engine: Engine,
    subject_code: str,
    degree_code: str,
    ay_label: str | None = None,
    year: int | None = None,
    term: int | None = None,
) -> Optional[Dict]:
    """
    Get subject details from catalog and optionally from offering.
    
    UPDATED: Returns catalog data even if offering doesn't exist.
    """
    if ay_label and year and term:
        # Try to get offering-specific details
        offering = _fetch_one(
            engine,
            """
            SELECT 
                sc.*,
                so.id AS offering_id,
                so.instructor_email,
                so.syllabus_template_id,
                so.status AS offering_status,
                so.ay_label,
                so.year,
                so.term
            FROM subjects_catalog sc
            LEFT JOIN subject_offerings so 
                ON so.subject_code = sc.subject_code
                AND so.degree_code = sc.degree_code
                AND so.ay_label    = :ay
                AND so.year        = :yr
                AND so.term        = :trm
            WHERE sc.subject_code = :subj
              AND sc.degree_code = :deg
            """,
            {
                "subj": subject_code,
                "deg": degree_code,
                "ay": ay_label,
                "yr": year,
                "trm": term,
            },
        )
        return offering
    else:
        # Catalog-only
        return _fetch_one(
            engine,
            """
            SELECT * FROM subjects_catalog
            WHERE subject_code = :subj
              AND degree_code  = :deg
            """,
            {"subj": subject_code, "deg": degree_code},
        )


def validate_subject_is_elective(
    engine: Engine,
    subject_code: str,
    degree_code: str,
) -> bool:
    """Validate that a subject is an elective or college project."""
    result = _fetch_one(
        engine,
        """
        SELECT subject_type 
        FROM subjects_catalog
        WHERE subject_code = :subj
          AND degree_code  = :deg
          AND LOWER(subject_type) IN ('elective', 'college project')
          AND active = 1
        """,
        {"subj": subject_code, "deg": degree_code},
    )
    return result is not None


def validate_offering_exists(
    engine: Engine,
    subject_code: str,
    degree_code: str,
    ay_label: str,
    year: int,
    term: int,
) -> Tuple[bool, Optional[Dict]]:
    """
    Check if a published offering exists for the subject.
    
    Returns: (exists, offering_dict or None)
    """
    result = _fetch_one(
        engine,
        """
        SELECT id, syllabus_template_id, status, instructor_email
        FROM subject_offerings
        WHERE subject_code = :subj
          AND degree_code  = :deg
          AND ay_label     = :ay
          AND year         = :yr
          AND term         = :trm
          AND status       = 'published'
        """,
        {
            "subj": subject_code,
            "deg": degree_code,
            "ay": ay_label,
            "yr": year,
            "trm": term,
        },
    )
    return (result is not None, result)


# ===========================================================================
# CREATE TOPIC WITH VALIDATION (FIXED)
# ===========================================================================


def create_topic_with_validation(
    engine: Engine, 
    data: Dict, 
    actor: str,
    enforce_timing: bool = True,
    enforce_capacity: bool = False,
    require_offering: bool = False  # NEW: Make offering check optional
) -> int:
    """
    Create topic with validation.
    
    FIXED: By default, only validates:
      1. Subject exists in subjects_catalog
      2. Subject is Elective or College Project
      3. (Optional) Timing: Created within lead time window
      4. (Optional) Capacity: Total capacity is sufficient
      5. (Optional) Offering exists with syllabus
    
    Set require_offering=True for old behavior (requires published offering + syllabus)
    """

    subject_code = data["subject_code"]
    degree_code = data["deg"]
    ay_label = data["ay"]
    year = data["yr"]
    term = data["trm"]

    # 1. Validate subject exists and is of the right type
    if not validate_subject_is_elective(engine, subject_code, degree_code):
        raise ValueError(
            f"Subject {subject_code} is not an Elective or College Project "
            "in the Subjects Catalog."
        )

    # 2. Get subject details from catalog
    subject = get_subject_details(
        engine,
        subject_code=subject_code,
        degree_code=degree_code,
        ay_label=ay_label,
        year=year,
        term=term,
    )
    
    if not subject:
        raise ValueError(f"Subject {subject_code} not found in catalog")
    
    # Use catalog name as canonical subject_name
    data["subj_name"] = subject["subject_name"]

    # 3. (OPTIONAL) Validate timing if enforced
    if enforce_timing:
        lead_days = data.get("elective_selection_lead_days", 21)
        timing_ok, timing_msg = validate_topic_creation_timing(
            engine, ay_label, year, term, lead_days
        )
        if not timing_ok:
            raise ValueError(f"Timing validation failed: {timing_msg}")

    # 4. (OPTIONAL) Validate offering exists if required
    if require_offering:
        offering_exists, offering = validate_offering_exists(
            engine, subject_code, degree_code, ay_label, year, term
        )
        
        if not offering_exists:
            raise ValueError(
                f"No published offering found for {subject_code} in "
                f"{ay_label}, Year {year}, Term {term}. "
                "Create an offering first or set require_offering=False."
            )
        
        if not offering.get("syllabus_template_id"):
            raise ValueError(
                "This subject does not yet have a syllabus. "
                "Please create the main syllabus in the Subjects Syllabus module "
                "before adding elective topics, or set require_offering=False."
            )

    # 5. Create topic
    with engine.begin() as conn:
        topic_code_ay = f"{subject_code}-{data['topic_no']}"

        result = _exec(
            conn,
            """
            INSERT INTO elective_topics (
                subject_code, subject_name, degree_code, program_code, branch_code,
                ay_label, year, term, division_code,
                topic_no, topic_code_ay, topic_name,
                owner_faculty_id, owner_faculty_email, capacity, description,
                prerequisites, learning_outcomes,
                status, last_updated_by
            ) VALUES (
                :subj, :subj_name, :deg, :prog, :br,
                :ay, :yr, :trm, :div,
                :topic_no, :topic_code, :topic_name,
                :owner_id, :owner_email, :cap, :desc,
                :prereq, :lo,
                :status, :actor
            )
            """,
            {
                "subj": subject_code,
                "subj_name": data["subj_name"],
                "deg": degree_code,
                "prog": data.get("prog"),
                "br": data.get("br"),
                "ay": ay_label,
                "yr": year,
                "trm": term,
                "div": data.get("div"),
                "topic_no": data["topic_no"],
                "topic_code": topic_code_ay,
                "topic_name": data["topic_name"],
                "owner_id": data.get("owner_id"),
                "owner_email": data.get("owner_email"),
                "cap": data.get("cap", 0),
                "desc": data.get("desc"),
                "prereq": data.get("prereq"),
                "lo": data.get("lo"),
                "status": data.get("status", "draft"),
                "actor": actor,
            },
        )

        topic_id = result.lastrowid
        
        # 6. (OPTIONAL) Validate total capacity after creation if enforced
        if enforce_capacity and topic_id:
            cap_ok, cap_msg, cap_stats = validate_total_capacity(
                engine,
                subject_code,
                ay_label,
                year,
                term,
                degree_code,
                data.get("prog"),
                data.get("br"),
                data.get("div")
            )
            if not cap_ok:
                # Warning only - don't block topic creation
                import logging
                logging.warning(f"Capacity warning after creating topic: {cap_msg}")
        
        return topic_id
