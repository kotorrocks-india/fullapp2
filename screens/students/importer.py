# app/screens/students/importer.py
# -------------------------------------------------------------------
# FULLY INTEGRATED VERSION with:
# - Email validation (Year 1: personal OK, Year 2+: .edu required)
# - Roll number generation (Year 1: auto-generated from batch)
# - Dynamic custom fields in template/import
# - Batch creation with automatic AY linking
# - Batch deletion (safe)
# - Batch AY link viewer
# - Full student data export
# - Student mover with 30-day cooldown
# - Credential export
# - Division support
# -------------------------------------------------------------------

from __future__ import annotations

from typing import List, Tuple, Dict, Any, Optional, Set
from dataclasses import dataclass, field
import pandas as pd
import streamlit as st
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine, Connection
from collections import defaultdict
import logging
import traceback
from datetime import datetime, timedelta
import re

# Import common helpers
from screens.faculty.utils import _safe_int_convert, _handle_error
from screens.faculty.db import _active_degrees

# Import from student db.py file
from screens.students.db import (
    _ensure_student_username_and_initial_creds,
    _get_student_credentials_to_export,
    _get_existing_enrollment_data,
    _db_get_batches_for_degree,
    _db_get_students_for_mover,
    _db_move_students
)

# Settings helpers
try:
    from screens.students.page import _get_setting, _init_settings_table
except ImportError:
    def _get_setting(conn: Connection, key: str, default: Any = None) -> Any:
        try:
            row = conn.execute(
                sa_text("SELECT value FROM app_settings WHERE key = :key"),
                {"key": key},
            ).fetchone()
            if row:
                return row[0]
        except Exception:
            return default
        return default

    def _init_settings_table(conn: Connection) -> None:
        try:
            conn.execute(sa_text("""
                CREATE TABLE IF NOT EXISTS app_settings (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                );
            """))
        except Exception:
            pass


log = logging.getLogger(__name__)


# ------------------------------------------------------------------
# EMAIL VALIDATION HELPERS - INTEGRATED
# ------------------------------------------------------------------

def _validate_email_for_year(
    conn: Connection,
    email: str,
    current_year: int
) -> Tuple[bool, Optional[str]]:
    """
    Validate email based on student's year and policy settings.
    
    Year 1: Personal email is OK
    Year 2+: .edu email required (if policy enabled)
    
    Returns: (is_valid, error_message)
    """
    if not email or "@" not in email:
        return False, "Invalid email format"
    
    # Get email policy settings
    edu_enabled = _get_setting(conn, "email_edu_enabled", "True") == "True"
    edu_domain = _get_setting(conn, "email_edu_domain", "college.edu")
    
    # Year 1: Personal email is OK
    if current_year == 1:
        return True, None
    
    # Year 2+: .edu email required (if policy enabled)
    if current_year >= 2 and edu_enabled:
        # Check if email ends with allowed .edu domains
        allowed_domains = [d.strip().lower() for d in edu_domain.split(",")]
        email_lower = email.lower()
        
        is_edu = any(
            email_lower.endswith(f"@{domain}") or 
            email_lower.endswith(f".{domain}")
            for domain in allowed_domains
        )
        
        if not is_edu:
            return False, f"Year {current_year} students require .edu email (allowed: {', '.join(allowed_domains)})"
    
    return True, None


# ------------------------------------------------------------------
# ROLL NUMBER GENERATION HELPERS - INTEGRATED
# ------------------------------------------------------------------

def _generate_roll_number_for_first_year(
    conn: Connection,
    student_id: str,
    degree_code: str,
    batch: str,
    name: str = None
) -> Optional[str]:
    """
    Generate roll number for 1st year students based on batch.
    
    Logic:
    - Batch "2024" → Roll format: 2024XXXX (4 digits incremental)
    - Uses batch as prefix, adds sequential number
    """
    # Check roll derivation mode
    derivation_mode = _get_setting(conn, "roll_derivation_mode", "hybrid")
    
    if derivation_mode == "manual":
        # Manual mode: no auto-generation
        return None
    
    # Extract year from batch (e.g., "2024", "2024-A" → "2024")
    year_match = re.search(r'(\d{4})', batch)
    if not year_match:
        log.warning(f"Could not extract year from batch '{batch}'")
        return None
    
    batch_year = year_match.group(1)
    
    # Get next available number for this batch
    try:
        max_roll = conn.execute(sa_text("""
            SELECT MAX(CAST(SUBSTR(e.roll_number, -4) AS INTEGER))
            FROM student_enrollments e
            WHERE e.degree_code = :degree
              AND e.batch = :batch
              AND e.roll_number LIKE :pattern
              AND LENGTH(e.roll_number) = 8
        """), {
            "degree": degree_code,
            "batch": batch,
            "pattern": f"{batch_year}%"
        }).scalar()
        
        next_num = (max_roll + 1) if max_roll else 1
        roll_number = f"{batch_year}{next_num:04d}"
        
        log.info(f"Generated roll number: {roll_number} for batch {batch}")
        return roll_number
        
    except Exception as e:
        log.error(f"Failed to generate roll number: {e}")
        return None


def _validate_roll_number(
    conn: Connection,
    roll_number: str,
    degree_code: str,
    student_id: str
) -> Tuple[bool, Optional[str]]:
    """
    Validate roll number format and uniqueness.
    
    Returns: (is_valid, error_message)
    """
    if not roll_number or not roll_number.strip():
        return False, "Roll number is required"
    
    roll_number = roll_number.strip()
    
    # Check if year extraction is enabled
    year_from_first4 = _get_setting(conn, "roll_year_from_first4", "True") == "True"
    
    if year_from_first4 and len(roll_number) >= 4:
        try:
            extracted_year = int(roll_number[:4])
            # Validate it's a reasonable year (between 2000 and 2100)
            if not (2000 <= extracted_year <= 2100):
                return False, f"Invalid year in roll number: {extracted_year}"
        except ValueError:
            # If configured but doesn't start with year, warn but allow
            log.warning(f"Roll number {roll_number} doesn't start with 4-digit year")
    
    # Check uniqueness (excluding the current student_id)
    existing = conn.execute(sa_text("""
        SELECT COUNT(*) FROM student_enrollments e
        JOIN student_profiles p ON p.id = e.student_profile_id
        WHERE e.roll_number = :roll
          AND p.student_id != :sid
    """), {"roll": roll_number, "sid": student_id}).scalar()
    
    if existing and existing > 0:
        return False, f"Roll number '{roll_number}' already assigned to another student"
    
    return True, None


# ------------------------------------------------------------------
# Degree Duration & Year Management
# ------------------------------------------------------------------

def _get_degree_duration(conn: Connection, degree_code: str) -> Optional[int]:
    """Fetches degree duration (years) from degree_semester_struct."""
    try:
        result = conn.execute(sa_text("""
            SELECT years FROM degree_semester_struct WHERE degree_code = :code
        """), {"code": degree_code}).fetchone()
        
        if result and result[0]:
            return int(result[0])
        
        return None
    except Exception as e:
        log.warning(f"Could not fetch degree duration: {e}")
        return None


def _ensure_degree_years_scaffold(conn: Connection, degree_code: str) -> bool:
    """Ensure degree_year_scaffold rows exist for degree_code, 1..duration."""
    duration = _get_degree_duration(conn, degree_code)
    if not duration or duration < 1:
        return False

    try:
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS degree_year_scaffold (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                degree_code TEXT NOT NULL,
                year_number INTEGER NOT NULL,
                year_name TEXT,
                sort_order INTEGER DEFAULT 0,
                active INTEGER DEFAULT 1,
                UNIQUE(degree_code, year_number)
            )
        """))

        for year_num in range(1, int(duration) + 1):
            exists = conn.execute(sa_text("""
                SELECT 1
                  FROM degree_year_scaffold
                 WHERE degree_code = :code
                   AND year_number = :year
            """), {"code": degree_code, "year": year_num}).fetchone()

            if not exists:
                conn.execute(sa_text("""
                    INSERT INTO degree_year_scaffold
                        (degree_code, year_number, year_name, sort_order, active)
                    VALUES (:code, :year, :name, :sort, 1)
                """), {
                    "code": degree_code,
                    "year": year_num,
                    "name": f"Year {year_num}",
                    "sort": year_num,
                })

        return True
    except Exception as e:
        log.error(f"Failed to ensure degree_year_scaffold for {degree_code}: {e}")
        return False


def _get_valid_years_for_degree(conn: Connection, degree_code: str) -> List[int]:
    """Collect valid years for a degree from duration, scaffold, and enrollments."""
    valid_years: set[int] = set()

    # 1. From degree duration
    duration = _get_degree_duration(conn, degree_code)
    if duration and duration > 0:
        valid_years.update(range(1, int(duration) + 1))

    # 2. From degree_year_scaffold
    try:
        scaffold_years = conn.execute(sa_text("""
            SELECT DISTINCT year_number
              FROM degree_year_scaffold
             WHERE degree_code = :code
             ORDER BY year_number
        """), {"code": degree_code}).fetchall()
        valid_years.update(int(r[0]) for r in scaffold_years if r[0] is not None)
    except Exception:
        pass

    # 3. From existing enrollments
    try:
        enrollment_years = conn.execute(sa_text("""
            SELECT DISTINCT current_year
              FROM student_enrollments
             WHERE degree_code = :code
               AND current_year IS NOT NULL
             ORDER BY current_year
        """), {"code": degree_code}).fetchall()
        valid_years.update(int(r[0]) for r in enrollment_years if r[0] is not None)
    except Exception:
        pass

    return sorted(valid_years)


# ------------------------------------------------------------------
# Batch Creation with AY Linking
# ------------------------------------------------------------------

def _link_batch_to_academic_years(
    conn: Connection,
    degree_code: str,
    batch_code: str,
    batch_id: int,
    intake_year: int,
    duration: int
) -> Tuple[bool, str]:
    """Automatically links a batch to Academic Years based on intake year."""
    try:
        valid_years = _get_valid_years_for_degree(conn, degree_code)
        if not valid_years:
            return False, "No valid years defined for degree"
        
        links_created = []
        warnings = []
        
        for year_num in range(1, duration + 1):
            ay_start_year = intake_year + (year_num - 1)
            ay_end_year_suffix = (ay_start_year + 1) % 100
            ay_code = f"{ay_start_year}-{ay_end_year_suffix:02d}"
            
            ay_exists = conn.execute(sa_text("""
                SELECT 1 FROM academic_years WHERE ay_code = :ay
            """), {"ay": ay_code}).fetchone()
            
            if not ay_exists:
                warnings.append(f"AY {ay_code} doesn't exist (Year {year_num})")
                ay_code_to_insert = None
            else:
                ay_code_to_insert = ay_code
            
            conn.execute(sa_text("""
                INSERT INTO batch_year_scaffold (batch_id, year_number, ay_code, active)
                VALUES (:bid, :year, :ay, 1)
                ON CONFLICT(batch_id, year_number) DO UPDATE SET
                    ay_code = excluded.ay_code,
                    active = 1
            """), {"bid": batch_id, "year": year_num, "ay": ay_code_to_insert})
            
            if ay_code_to_insert:
                links_created.append(f"Year {year_num} → {ay_code}")
        
        success_msg = f"✅ Linked {len(links_created)} years to AYs"
        if warnings:
            success_msg += f"\n⚠️ Warnings: {'; '.join(warnings)}"
        
        return True, success_msg
        
    except Exception as e:
        log.error(f"Failed to link batch to AYs: {e}")
        return False, f"❌ Failed to link batch to AYs: {str(e)}"


def _get_batch_ay_links(conn: Connection, batch_id: int) -> List[Dict[str, Any]]:
    """Get all AY links for a batch."""
    rows = conn.execute(sa_text("""
        SELECT year_number, ay_code
        FROM batch_year_scaffold
        WHERE batch_id = :bid
        ORDER BY year_number
    """), {"bid": batch_id}).fetchall()
    
    return [{"year": r[0], "ay_code": r[1]} for r in rows]


def _create_batch_with_years(
    conn: Connection,
    degree_code: str,
    batch_code: str,
    batch_name: str,
    start_date: str,
) -> Tuple[bool, str]:
    """Create (or reuse) a batch in degree_batches and scaffold its years."""
    try:
        scaffold_ok = _ensure_degree_years_scaffold(conn, degree_code)
        if not scaffold_ok:
            return False, f"❌ Degree {degree_code} has no valid duration defined."

        valid_years = _get_valid_years_for_degree(conn, degree_code)
        if not valid_years:
            return False, f"❌ Degree {degree_code} has no defined years. Set degree duration first."

        degree_duration = _get_degree_duration(conn, degree_code)
        if not degree_duration or degree_duration < 1:
            return False, f"❌ Degree {degree_code} has invalid duration."

        try:
            year_match = re.search(r'(\d{4})', batch_code)
            if year_match:
                intake_year = int(year_match.group(1))
            else:
                intake_year = int(start_date.split('-')[0])
        except Exception:
            return False, f"❌ Could not determine intake year from batch code '{batch_code}'"

        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS degree_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                degree_code TEXT NOT NULL,
                batch_code TEXT NOT NULL,
                batch_name TEXT,
                start_date TEXT,
                end_date TEXT,
                active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(degree_code, batch_code)
            )
        """))

        row = conn.execute(sa_text("""
            SELECT id
              FROM degree_batches
             WHERE degree_code = :degree
               AND batch_code  = :batch
        """), {"degree": degree_code, "batch": batch_code}).fetchone()

        if row:
            batch_id = int(row[0])
            created_new = False
        else:
            conn.execute(sa_text("""
                INSERT INTO degree_batches (degree_code, batch_code, batch_name, start_date)
                VALUES (:degree, :batch, :name, :start)
            """), {
                "degree": degree_code,
                "batch": batch_code,
                "name": batch_name or batch_code,
                "start": start_date,
            })
            row = conn.execute(sa_text("""
                SELECT id
                  FROM degree_batches
                 WHERE degree_code = :degree
                   AND batch_code  = :batch
            """), {"degree": degree_code, "batch": batch_code}).fetchone()
            if not row:
                raise RuntimeError("Could not fetch batch_id after inserting degree_batches row.")
            batch_id = int(row[0])
            created_new = True

        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS batch_year_scaffold (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL,
                year_number INTEGER NOT NULL,
                ay_code TEXT COLLATE NOCASE,
                active INTEGER DEFAULT 1,
                FOREIGN KEY (batch_id) REFERENCES degree_batches(id) ON DELETE CASCADE,
                FOREIGN KEY (ay_code) REFERENCES academic_years(ay_code) ON DELETE SET NULL,
                UNIQUE(batch_id, year_number)
            )
        """))

        link_success, link_message = _link_batch_to_academic_years(
            conn,
            degree_code,
            batch_code,
            batch_id,
            intake_year,
            degree_duration
        )

        if created_new:
            msg = f"✅ Created batch '{batch_code}' for {degree_code}\n{link_message}"
        else:
            msg = f"✅ Batch '{batch_code}' already existed; scaffolding updated\n{link_message}"

        log.info(msg)
        return True, msg

    except Exception as e:
        error_msg = f"❌ Failed to create batch: {str(e)}"
        log.error(f"Batch creation failed: {traceback.format_exc()}")
        return False, error_msg


def _delete_batch(conn: Connection, degree_code: str, batch_code: str) -> Tuple[bool, str]:
    """Delete a batch and all associated data."""
    try:
        has_students = conn.execute(sa_text("""
            SELECT COUNT(*) FROM student_enrollments
            WHERE degree_code = :d AND batch = :b
        """), {"d": degree_code, "b": batch_code}).scalar()
        
        if has_students and has_students > 0:
            return False, f"❌ Cannot delete batch '{batch_code}': {has_students} student(s) still enrolled. Move or remove students first."
        
        batch_row = conn.execute(sa_text("""
            SELECT id FROM degree_batches
            WHERE degree_code = :d AND batch_code = :b
        """), {"d": degree_code, "b": batch_code}).fetchone()
        
        if not batch_row:
            return False, f"❌ Batch '{batch_code}' not found."
        
        batch_id = batch_row[0]
        
        conn.execute(sa_text("""
            DELETE FROM batch_year_scaffold WHERE batch_id = :bid
        """), {"bid": batch_id})
        
        conn.execute(sa_text("""
            DELETE FROM degree_batches
            WHERE degree_code = :d AND batch_code = :b
        """), {"d": degree_code, "b": batch_code})
        
        return True, f"✅ Deleted batch '{batch_code}' successfully."
        
    except Exception as e:
        log.error(f"Failed to delete batch: {e}")
        return False, f"❌ Failed to delete batch: {str(e)}"


def _render_batch_ay_links(engine: Engine, degree_code: str, batch_code: str):
    """Display the AY links for a batch in a nice table."""
    try:
        with engine.connect() as conn:
            batch_row = conn.execute(sa_text("""
                SELECT id FROM degree_batches
                WHERE degree_code = :d AND batch_code = :b
            """), {"d": degree_code, "b": batch_code}).fetchone()
            
            if not batch_row:
                st.warning(f"Batch '{batch_code}' not found")
                return
            
            batch_id = batch_row[0]
            links = _get_batch_ay_links(conn, batch_id)
            
            if not links:
                st.info("No AY links found for this batch")
                return
            
            st.markdown("#### 📅 Academic Year Mapping")
            
            data = []
            for link in links:
                ay_code = link['ay_code']
                year = link['year']
                
                if ay_code:
                    ay_exists = conn.execute(sa_text("""
                        SELECT status FROM academic_years WHERE ay_code = :ay
                    """), {"ay": ay_code}).fetchone()
                    
                    if ay_exists:
                        status = ay_exists[0]
                        status_icon = {"open": "🟢", "closed": "🔴", "planned": "🟡"}.get(status, "⚪")
                        ay_display = f"{status_icon} {ay_code} ({status})"
                    else:
                        ay_display = f"⚠️ {ay_code} (not found)"
                else:
                    ay_display = "❌ Not linked"
                
                data.append({
                    "Year of Study": f"Year {year}",
                    "Academic Year": ay_display
                })
            
            st.dataframe(data, use_container_width=True, hide_index=True)
            
    except Exception as e:
        st.error(f"Failed to display AY links: {e}")


# ------------------------------------------------------------------
# Helpers for Stateful Import
# ------------------------------------------------------------------

@dataclass
class EnrollmentCheckResult:
    unmatched_batches: Set[str] = field(default_factory=set)
    existing_batches: List[str] = field(default_factory=list)
    unmatched_years: Set[str] = field(default_factory=set)
    existing_years: List[int] = field(default_factory=list)
    ignored_rows: int = 0
    invalid_years: Set[int] = field(default_factory=set)


def _pre_check_student_enrollments(df: pd.DataFrame, engine: Engine, degree_code: str) -> Tuple[EnrollmentCheckResult, pd.DataFrame]:
    """Pre-check CSV data for batch/year mismatches."""
    degree_code_clean = degree_code.strip()

    for col in ['degree_code', 'batch', 'current_year']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace('nan', '')

    df_filtered = df[df['degree_code'].str.lower() == degree_code_clean.lower()].copy()
    ignored_rows = len(df) - len(df_filtered)

    if df_filtered.empty:
        raise ValueError(f"No rows found in the CSV for the selected degree '{degree_code_clean}'.")

    with engine.connect() as conn:
        valid_years = _get_valid_years_for_degree(conn, degree_code_clean)
        degree_duration = _get_degree_duration(conn, degree_code_clean)
    
    if not valid_years and degree_duration:
        with engine.begin() as conn:
            _ensure_degree_years_scaffold(conn, degree_code_clean)
            valid_years = _get_valid_years_for_degree(conn, degree_code_clean)

    csv_batches = set(df_filtered['batch'].dropna().unique()) - {''}
    csv_years_raw = set(df_filtered['current_year'].dropna().unique()) - {''}
    
    csv_years = set()
    invalid_years = set()
    
    for year_str in csv_years_raw:
        try:
            year_int = int(year_str)
            if valid_years and year_int not in valid_years:
                invalid_years.add(year_int)
            else:
                csv_years.add(year_int)
        except ValueError:
            invalid_years.add(year_str)

    with engine.connect() as conn:
        existing_data = _get_existing_enrollment_data(engine, degree_code_clean)
        db_batches = existing_data['batches']
    
    db_years = sorted(list(valid_years))

    result = EnrollmentCheckResult(
        unmatched_batches=csv_batches - set(db_batches),
        existing_batches=sorted(db_batches),
        unmatched_years=csv_years - set(db_years),
        existing_years=db_years,
        ignored_rows=ignored_rows,
        invalid_years=invalid_years
    )

    return result, df_filtered


def _build_translation_map(
    mappings: Dict[str, Dict[str, str]]
) -> Dict[str, Dict[str, str]]:
    """Build translation map from user selections."""
    translation_map = {}
    for aff_type, type_mappings in mappings.items():
        translation_map[aff_type] = {}
        for code, action in type_mappings.items():
            if action == "[USE_NEW]":
                translation_map[aff_type][code] = code
            else:
                translation_map[aff_type][code] = action

    return translation_map


# ------------------------------------------------------------------
# Main Import Logic - WITH EMAIL & ROLL VALIDATION INTEGRATED
# ------------------------------------------------------------------

def _show_no_degrees_help(engine: Engine, context: str = "student operations"):
    """Show help message if no degrees exist."""
    with engine.begin() as conn:
        degrees = _active_degrees(conn)
        if degrees:
            return True

    st.warning(f"⚠️ No degrees found. Set up degrees before {context}.")
    st.markdown("""
### 🚀 Getting Started

1. **Create Degrees** with defined duration (e.g., BTech = 5 years)
2. **Import Students** with batch and year values
3. **View and manage** students

""")

    with st.expander("➕ Quick Create Your First Degree", expanded=True):
        col1, col2, col3 = st.columns(3)

        with col1:
            degree_code = st.text_input("Degree Code*", placeholder="e.g., BTech")
        with col2:
            degree_name = st.text_input("Degree Name", placeholder="e.g., Bachelor of Technology")
        with col3:
            duration = st.number_input("Years/Duration*", min_value=1, max_value=10, value=4)

        if st.button("✨ Create Degree", type="primary"):
            if not degree_code or not degree_code.strip():
                st.error("❌ Degree code is required")
            else:
                try:
                    with engine.begin() as conn:
                        existing = conn.execute(sa_text(
                            "SELECT 1 FROM degrees WHERE LOWER(code) = LOWER(:code)"
                        ), {"code": degree_code.strip()}).fetchone()

                        if existing:
                            st.error(f"❌ Degree '{degree_code}' already exists")
                        else:
                            conn.execute(sa_text("""
                                INSERT INTO degrees (code, title, active, sort_order, created_at, updated_at)
                                VALUES (:code, :name, 1, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                            """), {
                                "code": degree_code.strip(),
                                "name": degree_name.strip() or degree_code.strip()
                            })
                            
                            conn.execute(sa_text("""
                                INSERT INTO degree_semester_struct (degree_code, years, terms_per_year, active, updated_at)
                                VALUES (:code, :years, 2, 1, CURRENT_TIMESTAMP)
                            """), {
                                "code": degree_code.strip(),
                                "years": int(duration)
                            })
                            
                            _ensure_degree_years_scaffold(conn, degree_code.strip())
                            
                            st.success(f"✅ Created degree: **{degree_code}** ({duration} years)")
                            st.cache_data.clear()
                            st.balloons()
                            st.rerun()
                except Exception as e:
                    st.error(f"❌ Failed: {e}")
                    log.error(f"Degree creation failed: {e}")

    return False


def _import_students_with_validation(
    engine: Engine,
    df: pd.DataFrame,
    dry_run: bool,
    mappings: Optional[Dict[str, Dict[str, str]]] = None,
    conn_for_transaction: Optional[Connection] = None
) -> Tuple[List[Dict[str, Any]], int, List[Dict[str, Any]]]:
    """
    Import students with strict validation.
    NOW INCLUDES: Email validation & Roll number generation.
    """
    df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]
    errors: List[Dict[str, Any]] = []
    skipped_rows: List[Dict[str, Any]] = []
    success_count = 0
    translation_map = _build_translation_map(mappings) if mappings else {}

    # Metadata fetch
    with engine.begin() as meta_conn:
        all_degrees = _active_degrees(meta_conn)
        
        active_custom_fields_rows = meta_conn.execute(sa_text(
            "SELECT code FROM student_custom_profile_fields WHERE active = 1"
        )).fetchall()
        active_custom_fields = {row[0] for row in active_custom_fields_rows}

    # Transaction logic
    if conn_for_transaction:
        conn = conn_for_transaction
        trans = conn.begin_nested()
        should_close = False
    else:
        conn = engine.connect()
        trans = conn.begin()
        should_close = True

    try:
        for idx, row in df.iterrows():
            row_num = idx + 2
            try:
                # 1. Validate Profile Data
                email = str(row.get('email', '')).strip().lower()
                student_id = str(row.get('student_id', '')).strip()
                name = str(row.get('name', '')).strip()

                if not name or not email or not student_id:
                    errors.append({'row': row_num, 'email': email, 'error': "Missing required fields: name, email, student_id"})
                    continue

                # 1.5 Process Enrollment Data First (needed for validation)
                degree_code = str(row.get('degree_code', '')).strip()
                if not degree_code:
                    errors.append({'row': row_num, 'email': email, 'error': "Missing degree_code"})
                    continue
                
                if degree_code not in all_degrees:
                    errors.append({'row': row_num, 'email': email, 'error': f"Degree '{degree_code}' not found"})
                    continue
                
                batch = str(row.get('batch', '')).strip()
                current_year = str(row.get('current_year', '')).strip()
                
                if not batch or not current_year:
                    errors.append({'row': row_num, 'email': email, 'error': "Missing batch or year"})
                    continue
                
                try:
                    year_int = int(current_year)
                    valid_years = _get_valid_years_for_degree(conn, degree_code)
                    if valid_years and year_int not in valid_years:
                        errors.append({
                            'row': row_num, 
                            'email': email, 
                            'error': f"Year {year_int} is outside degree duration (valid: {valid_years})"
                        })
                        continue
                except ValueError:
                    errors.append({'row': row_num, 'email': email, 'error': f"Invalid year value: {current_year}"})
                    continue
                
                mapped_batch = translation_map.get('batch', {}).get(batch, batch)
                mapped_year = translation_map.get('year', {}).get(current_year, current_year)
                
                if mapped_batch == "[IGNORE]" or mapped_year == "[IGNORE]":
                    skipped_rows.append({"row": row_num, "email": email, "reason": "Ignored by mapping"})
                    continue

                # **NEW: EMAIL VALIDATION BASED ON YEAR**
                email_valid, email_error = _validate_email_for_year(conn, email, year_int)
                if not email_valid:
                    errors.append({
                        'row': row_num, 
                        'email': email, 
                        'error': f"Email validation failed: {email_error}"
                    })
                    continue

                # **NEW: ROLL NUMBER GENERATION/VALIDATION**
                provided_roll = str(row.get('roll_number', '')).strip() or None
                
                if provided_roll:
                    # Validate provided roll number
                    roll_valid, roll_error = _validate_roll_number(conn, provided_roll, degree_code, student_id)
                    if not roll_valid:
                        errors.append({
                            'row': row_num, 
                            'email': email, 
                            'error': f"Roll number validation failed: {roll_error}"
                        })
                        continue
                    roll_number = provided_roll
                else:
                    # Generate roll number for 1st year students
                    if year_int == 1:
                        generated_roll = _generate_roll_number_for_first_year(
                            conn, student_id, degree_code, mapped_batch, name
                        )
                        if generated_roll:
                            roll_number = generated_roll
                        else:
                            # Fallback to student_id
                            roll_number = student_id
                            log.warning(f"Roll generation failed for {student_id}, using student_id")
                    else:
                        # Year 2+: use student_id as roll number if not provided
                        roll_number = student_id

                # 2. Upsert Profile
                profile_id = conn.execute(sa_text(
                    "SELECT id FROM student_profiles WHERE student_id = :sid"
                ), {"sid": student_id}).fetchone()

                if profile_id:
                    profile_id = profile_id[0]
                    conn.execute(sa_text("""
                        UPDATE student_profiles
                        SET name = :name, email = :email, phone = :phone, status = :status, updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                    """), {
                        "name": name, "email": email, "phone": row.get('phone'),
                        "status": row.get('status', 'Good'), "id": profile_id
                    })
                else:
                    res = conn.execute(sa_text("""
                        INSERT INTO student_profiles (name, email, student_id, phone, status)
                        VALUES (:name, :email, :sid, :phone, :status)
                    """), {
                        "name": name, "email": email, "sid": student_id,
                        "phone": row.get('phone'), "status": row.get('status', 'Good')
                    })
                    profile_id = res.lastrowid

                # 3. Ensure Credentials
                _ensure_student_username_and_initial_creds(conn, profile_id, name, student_id)

                # 4. Process Enrollment (continued)
                batch_exists = conn.execute(sa_text(
                    "SELECT 1 FROM degree_batches WHERE degree_code = :d AND batch_code = :b"
                ), {"d": degree_code, "b": mapped_batch}).fetchone()
                
                if not batch_exists:
                    legacy_batch = conn.execute(sa_text(
                        "SELECT 1 FROM student_enrollments WHERE degree_code = :d AND batch = :b LIMIT 1"
                    ), {"d": degree_code, "b": mapped_batch}).fetchone()
                    if not legacy_batch:
                        errors.append({'row': row_num, 'email': email, 'error': f"Batch '{mapped_batch}' does not exist. Create it first."})
                        continue
                
                program_code = str(row.get('program_code', '')).strip() or None
                branch_code = str(row.get('branch_code', '')).strip() or None
                
                # Handle division_code from CSV
                division_code = str(row.get('division_code', '')).strip() or None
                
                # Validate division exists if provided
                if division_code:
                    div_enabled = _get_setting(conn, "div_enabled", "True") == "True"
                    
                    if div_enabled:
                        div_exists = conn.execute(sa_text("""
                            SELECT 1 FROM division_master
                            WHERE degree_code = :degree
                              AND batch = :batch
                              AND current_year = :year
                              AND division_code = :div
                              AND active = 1
                        """), {
                            "degree": degree_code,
                            "batch": mapped_batch,
                            "year": mapped_year,
                            "div": division_code
                        }).fetchone()
                        
                        if not div_exists:
                            import_optional = _get_setting(conn, "div_import_optional", "True") == "True"
                            
                            if not import_optional:
                                errors.append({
                                    'row': row_num, 
                                    'email': email, 
                                    'error': f"Division '{division_code}' not found for {degree_code}/{mapped_batch}/Year {mapped_year}"
                                })
                                continue
                            else:
                                division_code = None

                # Upsert Enrollment WITH ROLL NUMBER
                enrollment_id = conn.execute(sa_text("""
                    SELECT id FROM student_enrollments
                    WHERE student_profile_id = :pid AND degree_code = :degree
                """), {
                    "pid": profile_id, "degree": degree_code
                }).fetchone()

                if enrollment_id:
                    conn.execute(sa_text("""
                        UPDATE student_enrollments
                        SET batch = :batch, 
                            program_code = :prog, 
                            branch_code = :branch, 
                            division_code = :div,
                            roll_number = :roll,
                            current_year = :year,
                            enrollment_status = :status, 
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                    """), {
                        "batch": mapped_batch, 
                        "prog": program_code, 
                        "branch": branch_code,
                        "div": division_code,
                        "roll": roll_number,
                        "year": mapped_year, 
                        "status": row.get('enrollment_status', 'active'),
                        "id": enrollment_id[0]
                    })
                else:
                    conn.execute(sa_text("""
                        INSERT INTO student_enrollments (
                            student_profile_id, 
                            degree_code, 
                            program_code, 
                            branch_code,
                            division_code,
                            roll_number,
                            batch, 
                            current_year, 
                            enrollment_status, 
                            is_primary
                        ) VALUES (
                            :pid, 
                            :degree, 
                            :prog, 
                            :branch, 
                            :div,
                            :roll,
                            :batch, 
                            :year, 
                            :status, 
                            1
                        )
                    """), {
                        "pid": profile_id, 
                        "degree": degree_code, 
                        "prog": program_code,
                        "branch": branch_code, 
                        "div": division_code,
                        "roll": roll_number,
                        "batch": mapped_batch, 
                        "year": mapped_year,
                        "status": row.get('enrollment_status', 'active')
                    })

                # 5. Process Custom Fields
                for field_code in active_custom_fields:
                    if field_code in row:
                        value = row.get(field_code)
                        if pd.isna(value):
                            value = None
                        
                        conn.execute(sa_text("""
                            INSERT INTO student_custom_profile_data (student_profile_id, field_code, value, updated_at)
                            VALUES (:pid, :code, :val, CURRENT_TIMESTAMP)
                            ON CONFLICT(student_profile_id, field_code) DO UPDATE SET
                                value = excluded.value,
                                updated_at = CURRENT_TIMESTAMP
                        """), {
                            "pid": profile_id,
                            "code": field_code,
                            "val": str(value) if value is not None else None
                        })

                success_count += 1

            except Exception as e:
                errors.append({'row': row_num, 'email': str(row.get('email', '')).strip().lower(), 'error': str(e)})

        if dry_run:
            trans.rollback()
        else:
            trans.commit()

    except Exception:
        if trans:
            trans.rollback()
        raise

    finally:
        if should_close:
            conn.close()

    return errors, success_count, skipped_rows


# ------------------------------------------------------------------
# STATEFUL IMPORT UI
# ------------------------------------------------------------------

def _reset_student_import_state():
    """Reset session state for student import."""
    st.session_state.student_import_step = 'initial'
    st.session_state.student_import_mappings = {}
    st.session_state.student_import_validation_data = None
    st.session_state.student_import_df = None
    log.debug("Reset student import state")


def _add_student_import_export_section(engine: Engine):
    """
    Main UI for student import/export with batch creation.
    """
    st.divider()
    st.subheader("📥📤 Student Import/Export")

    if not _show_no_degrees_help(engine, "student import"):
        return

    # State Initialization
    if 'student_import_step' not in st.session_state:
        _reset_student_import_state()

    # Select Degree
    with engine.begin() as conn:
        degrees = _active_degrees(conn)
        if not degrees:
            st.error("❌ No degrees available")
            return

    if st.session_state.get('student_import_degree'):
        selected_degree = st.session_state.student_import_degree
        with engine.connect() as conn:
            degree_duration = _get_degree_duration(conn, selected_degree)
            valid_years = _get_valid_years_for_degree(conn, selected_degree)
        
        st.info(f"📋 **Degree:** `{selected_degree}` | **Duration:** {degree_duration} years | **Valid Years:** {valid_years}")
    else:
        selected_degree_raw = st.selectbox("Select Degree", options=degrees, key="degree_selector_student")

        if st.button("Confirm Degree Selection", type="primary"):
            st.session_state.student_import_degree = selected_degree_raw.strip()
            st.rerun()

        st.warning("⚠️ Confirm degree selection to proceed")
        return

    # Export Section - Dynamic Template
    with st.expander("📥 Download Template"):
        base_columns = "name,email,student_id,date_of_joining,phone,status,degree_code,program_code,branch_code,batch,current_year,division_code,roll_number,enrollment_status"
        try:
            with engine.connect() as conn:
                active_fields = conn.execute(sa_text(
                    "SELECT code FROM student_custom_profile_fields WHERE active = 1 ORDER BY sort_order, code"
                )).fetchall()
                custom_columns = [f[0] for f in active_fields]
            
            all_columns_header = base_columns + "," + ",".join(custom_columns)
            st.info(f"📝 Template includes: roll_number (optional for Year 1, auto-generated), division_code (optional)")
            st.caption(f"Active custom fields ({len(custom_columns)}): {', '.join(custom_columns) if custom_columns else 'None'}")
        except Exception as e:
            all_columns_header = base_columns
            st.warning(f"Could not fetch custom fields for template: {e}")

        st.download_button(
            label="Download CSV Template",
            data=all_columns_header,
            file_name="student_template.csv",
            mime="text/csv"
        )
        
        # Show email & roll policies
        with engine.connect() as conn:
            edu_enabled = _get_setting(conn, "email_edu_enabled", "True") == "True"
            edu_domain = _get_setting(conn, "email_edu_domain", "college.edu")
            roll_mode = _get_setting(conn, "roll_derivation_mode", "hybrid")
        
        st.markdown("#### 📧 Email Policy")
        if edu_enabled:
            st.success(f"✅ Year 1: Personal email OK | Year 2+: .edu email required ({edu_domain})")
        else:
            st.info("ℹ️ Email validation disabled")
        
        st.markdown("#### 🔢 Roll Number Policy")
        if roll_mode == "auto":
            st.success("✅ Auto-generated for Year 1 (format: YYYYXXXX based on batch)")
        elif roll_mode == "hybrid":
            st.success("✅ Auto-generated for Year 1 if not provided (format: YYYYXXXX based on batch)")
        else:
            st.info("ℹ️ Manual entry required")

    # Batch Creation/Management Section
    with st.expander("➕ Create / Manage Batches"):
        st.markdown(f"### Manage Batches for {selected_degree}")
        
        with engine.connect() as conn:
            degree_duration = _get_degree_duration(conn, selected_degree)
            valid_years = _get_valid_years_for_degree(conn, selected_degree)
            existing_batches = _db_get_batches_for_degree(conn, selected_degree)
        
        if not degree_duration:
            st.error(f"❌ Degree '{selected_degree}' has no duration set.")
        elif not valid_years:
            st.error(f"❌ No valid years for degree.")
        else:
            # VIEW EXISTING BATCHES
            if existing_batches:
                st.markdown("#### 📋 Existing Batches")
                view_batch = st.selectbox(
                    "View AY Links for Batch:",
                    options=[""] + [b['code'] for b in existing_batches],
                    key="view_batch_links"
                )
                
                if view_batch:
                    _render_batch_ay_links(engine, selected_degree, view_batch)
                
                st.divider()
            
            # CREATE BATCH
            st.markdown("#### ➕ Create New Batch")
            st.success(f"✅ Degree duration: **{degree_duration} years**")
            st.info("💡 Batch will be automatically linked to Academic Years based on intake year")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                batch_code = st.text_input(
                    "Batch Code*", 
                    placeholder="e.g., 2021, 2022-A",
                    help="Must contain a 4-digit year (e.g., 2021)",
                    key="create_batch_code"
                )
            with col2:
                batch_name = st.text_input("Batch Name", placeholder="e.g., 2021-2025 Batch", key="create_batch_name")
            with col3:
                start_date = st.date_input("Start Date*", datetime.now(), key="create_batch_date")
            
            if batch_code:
                # Preview the AY mapping
                try:
                    year_match = re.search(r'(\d{4})', batch_code)
                    if year_match:
                        intake_year = int(year_match.group(1))
                        st.caption("**Preview of AY Mapping:**")
                        preview_data = []
                        for year_num in range(1, min(degree_duration + 1, 6)):
                            ay_start = intake_year + (year_num - 1)
                            ay_end = (ay_start + 1) % 100
                            preview_data.append(f"Year {year_num} → {ay_start}-{ay_end:02d}")
                        st.code("\n".join(preview_data))
                except Exception:
                    st.warning("⚠️ Could not parse year from batch code for preview")
            
            if st.button("Create Batch & Link to AYs", type="primary", key="create_batch_btn"):
                if not batch_code or not batch_code.strip() or not start_date:
                    st.error("❌ Batch Code and Start Date are required")
                else:
                    with engine.begin() as conn:
                        success, message = _create_batch_with_years(
                            conn, 
                            selected_degree, 
                            batch_code.strip(),
                            batch_name.strip(),
                            str(start_date)
                        )
                    
                    if success:
                        st.success(message)
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(message)
            
            # DELETE BATCH
            if existing_batches:
                st.divider()
                st.markdown("#### 🗑️ Delete Existing Batch")
                st.warning("⚠️ You can only delete batches that have no students enrolled.")
                
                batch_to_delete = st.selectbox(
                    "Select Batch to Delete",
                    options=[""] + [b['code'] for b in existing_batches],
                    key="delete_batch_selector"
                )
                
                if batch_to_delete:
                    with engine.connect() as conn:
                        student_count = conn.execute(sa_text("""
                            SELECT COUNT(*) FROM student_enrollments
                            WHERE degree_code = :d AND batch = :b
                        """), {"d": selected_degree, "b": batch_to_delete}).scalar() or 0
                    
                    if student_count > 0:
                        st.error(f"❌ Cannot delete: Batch '{batch_to_delete}' has **{student_count} student(s)** enrolled.")
                        st.info("Move or remove students first using the Student Mover tool.")
                    else:
                        st.success(f"✅ Batch '{batch_to_delete}' has **no students** - safe to delete.")
                        
                        confirm_delete = st.checkbox(
                            f"I confirm I want to delete batch '{batch_to_delete}'",
                            key="confirm_delete_batch"
                        )
                        
                        if confirm_delete and st.button("🗑️ Delete Batch", type="secondary", key="delete_batch_btn"):
                            with engine.begin() as conn:
                                success, message = _delete_batch(conn, selected_degree, batch_to_delete)
                            
                            if success:
                                st.success(message)
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error(message)

    # File Uploader
    st.markdown("### 📤 Import Student Data")
    up = st.file_uploader("Upload CSV", type="csv", key="student_uploader")

    if st.session_state.student_import_step != 'initial':
        if st.button("🔄 Cancel", key="cancel_import"):
            _reset_student_import_state()
            st.rerun()

    st.divider()

    # State: Initial (Validation)
    if st.session_state.student_import_step == 'initial':
        st.markdown("#### Step 1: Validate File")

        if not up:
            st.warning("Upload CSV to begin")
            if 'student_import_df' in st.session_state:
                _reset_student_import_state()
            return

        if st.button("🔍 Validate File", type="primary"):
            try:
                up.seek(0)
                df = pd.read_csv(up)

                with st.spinner("Validating..."):
                    try:
                        validation_data, filtered_df = _pre_check_student_enrollments(df, engine, selected_degree)
                        
                        # Check for invalid years
                        if validation_data.invalid_years:
                            with engine.connect() as conn:
                                degree_duration = _get_degree_duration(conn, selected_degree)
                            
                            st.error(f"""
### ❌ Invalid Years Detected
Your CSV contains years outside the degree duration.
**Degree Duration:** {degree_duration} years (valid: 1-{degree_duration})
**Invalid Years in CSV:** {sorted(validation_data.invalid_years)}
**Fix:**
1. Update your CSV to use years 1-{degree_duration} only
2. Remove rows with invalid year values
3. Upload corrected CSV
                            """)
                            return

                        st.session_state.student_import_validation_data = validation_data
                        st.session_state.student_import_df = filtered_df
                        st.session_state.student_import_mappings = {"batch": {}, "year": {}}

                        if validation_data.ignored_rows > 0:
                            st.info(f"✅ {len(filtered_df)} rows for {selected_degree}, {validation_data.ignored_rows} ignored")

                        if validation_data.unmatched_batches:
                            st.session_state.student_import_step = 'map_batches'
                        elif validation_data.unmatched_years:
                            st.session_state.student_import_step = 'map_years'
                        else:
                            st.session_state.student_import_step = 'ready_to_import'

                        st.rerun()

                    except ValueError as ve:
                        st.error(f"""
### ❌ Validation Failed
{str(ve)}
**Fix:**
1. Ensure `degree_code` column has values matching: **{selected_degree}**
2. All rows must have this degree code
3. Upload corrected CSV
                        """)
                        return
                    except Exception as inner_e:
                        st.error(f"❌ Error: {str(inner_e)}")
                        log.error(f"Validation error: {traceback.format_exc()}")
                        return
            except pd.errors.ParserError:
                st.error("❌ CSV Format Error - Invalid CSV file")
                return
            except Exception as e:
                st.error(f"❌ Unexpected Error: {str(e)}")
                log.error(f"Unexpected error: {traceback.format_exc()}")
                return

    # State: Map Batches
    elif st.session_state.student_import_step == 'map_batches':
        st.markdown("#### Step 2: Map Batches")
        st.warning("⚠️ New batches found. Map them to existing batches or ignore.")
        st.info("To add new batches, use the 'Create New Batch' expander above first, then re-validate your file.")

        data: EnrollmentCheckResult = st.session_state.student_import_validation_data
        
        options = ["[Select]"] + data.existing_batches + ["[-- Ignore --]"]

        valid = True
        col1, col2 = st.columns([1, 2])
        col1.markdown("**CSV Batch**")
        col2.markdown("**Action**")

        for code in sorted(list(data.unmatched_batches)):
            col1.write(f"`{code}`")
            stored = st.session_state.student_import_mappings['batch'].get(code, "[Select]")
            if stored == "[IGNORE]":
                stored = "[-- Ignore --]"
            try:
                default_idx = options.index(stored)
            except:
                default_idx = 0
            choice = col2.selectbox(f"batch_{code}", options, key=f"map_batch_{code}", index=default_idx, label_visibility="collapsed")
            if choice == "[-- Ignore --]":
                st.session_state.student_import_mappings['batch'][code] = "[IGNORE]"
            else:
                st.session_state.student_import_mappings['batch'][code] = choice
            if choice == "[Select]":
                valid = False

        if st.button("➡️ Next", type="primary"):
            if not valid:
                st.error("❌ Select action for each batch")
            else:
                data = st.session_state.student_import_validation_data
                if data.unmatched_years:
                    st.session_state.student_import_step = 'map_years'
                else:
                    st.session_state.student_import_step = 'ready_to_import'
                st.rerun()

    # State: Map Years
    elif st.session_state.student_import_step == 'map_years':
        st.markdown("#### Step 3: Map Years")
        st.warning("⚠️ New years found. Map them to existing years or ignore.")
        data: EnrollmentCheckResult = st.session_state.student_import_validation_data
        options = ["[Select]"] + [str(y) for y in data.existing_years] + ["[-- Use New --]", "[-- Ignore --]"]
        valid = True
        col1, col2 = st.columns([1, 2])
        col1.markdown("**CSV Year**")
        col2.markdown("**Action**")
        for code in sorted(list(data.unmatched_years)):
            col1.write(f"`{code}`")
            stored = st.session_state.student_import_mappings['year'].get(code, "[Select]")
            if stored == "[USE_NEW]":
                stored = "[-- Use New --]"
            elif stored == "[IGNORE]":
                stored = "[-- Ignore --]"
            try:
                default_idx = options.index(stored)
            except:
                default_idx = 0
            choice = col2.selectbox(f"year_{code}", options, key=f"map_year_{code}", index=default_idx, label_visibility="collapsed")
            if choice == "[-- Use New --]":
                st.session_state.student_import_mappings['year'][code] = "[USE_NEW]"
            elif choice == "[-- Ignore --]":
                st.session_state.student_import_mappings['year'][code] = "[IGNORE]"
            else:
                st.session_state.student_import_mappings['year'][code] = choice
            if choice == "[Select]":
                valid = False
        if st.button("➡️ Next", type="primary"):
            if not valid:
                st.error("❌ Select action for each year")
            else:
                st.session_state.student_import_step = 'ready_to_import'
                st.rerun()

    # State: Ready to Import
    elif st.session_state.student_import_step == 'ready_to_import':
        st.markdown("#### Step 4: Review & Import")
        st.success("✅ Ready to import with email & roll number validation")
        
        with st.expander("ℹ️ What happens during import"):
            st.markdown("""
**Email Validation:**
- Year 1 students: Personal email accepted
- Year 2+ students: .edu email required (based on settings)

**Roll Number Generation:**
- Year 1 students: Auto-generated if not provided (format: YYYYXXXX)
- Year 2+ students: Uses student_id if not provided

**Division Assignment:**
- Optional field, validated if provided
- Must exist in Division Editor for degree/batch/year
            """)
        
        if 'student_import_df' not in st.session_state or st.session_state.student_import_df is None:
            st.error("❌ Session data lost")
            return
        with st.expander("🔍 Mappings"):
            st.json(st.session_state.student_import_mappings)
        df_to_import = st.session_state.student_import_df
        mappings = st.session_state.student_import_mappings
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🧪 Dry Run", key="dry_run"):
                with engine.begin() as conn:
                    trans = conn.begin_nested()
                    try:
                        with st.spinner("Dry run with validation..."):
                            errors, success, skipped = _import_students_with_validation(
                                engine, df_to_import, dry_run=True, mappings=mappings, conn_for_transaction=conn
                            )
                        if errors:
                            st.warning(f"⚠️ {success} OK, {len(errors)} errors")
                            st.dataframe(pd.DataFrame(errors)[['row', 'email', 'error']], use_container_width=True)
                        else:
                            st.success(f"✅ Dry run successful. All {success} records validated.")
                        if skipped:
                            st.info(f"ℹ️ {len(skipped)} skipped")
                    finally:
                        trans.rollback()
        with col2:
            if st.button("🚀 Import", key="execute", type="primary"):
                try:
                    with st.spinner("Importing with validation..."):
                        errors, success, skipped = _import_students_with_validation(
                            engine, df_to_import, dry_run=False, mappings=mappings
                        )
                    if errors:
                        st.error(f"❌ {len(errors)} errors in {len(df_to_import)} rows")
                        st.dataframe(pd.DataFrame(errors)[['row', 'email', 'error']], use_container_width=True)
                        st.download_button(
                            "Download Errors",
                            pd.DataFrame(errors).to_csv(index=False),
                            "errors.csv",
                            "text/csv"
                        )
                    else:
                        st.success(f"✅ Imported {success} students with validated emails & roll numbers")
                        st.balloons()
                    if skipped:
                        st.info(f"ℹ️ {len(skipped)} skipped")
                    st.cache_data.clear()
                    _reset_student_import_state()
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Import failed: {str(e)}")
                    log.error(f"Import failed: {traceback.format_exc()}")


# ------------------------------------------------------------------
# UI SECTION 2: STUDENT MOVER
# ------------------------------------------------------------------

def _add_student_mover_section(engine: Engine):
    """Student mover with 30-day cooldown."""
    st.divider()
    st.subheader("🚚 Student Mover")

    if not _show_no_degrees_help(engine, "student moving"):
        return

    st.info("Move students between batches/degrees. A 30-day cooldown applies to all moves.")

    with engine.begin() as conn:
        _init_settings_table(conn)
        all_degrees = _active_degrees(conn)
        next_batch_only = _get_setting(conn, "mover_next_only", "True") == "True"

        if not all_degrees:
            st.warning("No degrees found")
            return

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**1. Select Source**")
        from_degree = st.selectbox("From Degree", all_degrees, key="move_from_degree")

        with engine.begin() as conn:
            from_batches_data = _db_get_batches_for_degree(conn, from_degree)
            from_batches = [b['code'] for b in from_batches_data]

        if not from_batches:
            st.warning(f"No batches found for {from_degree}. Create batches in the Import tab.")
            return

        from_batch = st.selectbox("From Batch", from_batches, key="move_from_batch")

        if st.button("Get Students"):
            with engine.begin() as conn:
                df_students = _db_get_students_for_mover(conn, from_degree, from_batch)

            st.session_state.students_to_move_df = df_students

        if "students_to_move_df" not in st.session_state:
            st.write("Click 'Get Students'")
            return

        st.markdown("**2. Select Students**")
        df_students = st.session_state.students_to_move_df
        
        # Add cooldown column
        df_students['On Cooldown'] = False
        if 'Last Moved On' in df_students.columns:
            thirty_days_ago = datetime.now() - timedelta(days=30)
            last_moved_dt = pd.to_datetime(df_students['Last Moved On'], errors='coerce')
            df_students['On Cooldown'] = (last_moved_dt > thirty_days_ago)
        
        edited_df = st.data_editor(
            df_students, 
            key="mover_editor", 
            use_container_width=True,
            column_order=["Move", "Student ID", "Name", "On Cooldown", "Last Moved On"],
            column_config={
                "Profile ID": None,
                "Enrollment ID": None,
                "Last Moved On": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"),
                "On Cooldown": st.column_config.CheckboxColumn(disabled=True)
            },
            disabled=["Student ID", "Name", "Email", "Current Year", "Last Moved On", "On Cooldown"]
        )
        students_to_move_df = edited_df[edited_df["Move"] == True]

    with col2:
        st.markdown("**3. Select Destination**")
        to_degree = st.selectbox("To Degree", all_degrees, key="move_to_degree")

        with engine.begin() as conn:
            to_batches_data = _db_get_batches_for_degree(conn, to_degree)
            to_batches_all = [b['code'] for b in to_batches_data]

        # Filter to_batches if policy is enabled
        to_batches_options = to_batches_all
        if next_batch_only and from_degree == to_degree and from_batch in to_batches_all:
            st.info("ℹ️ Policy active: Only the next sequential batch is shown.")
            try:
                current_index = to_batches_all.index(from_batch)
                if current_index + 1 < len(to_batches_all):
                    to_batches_options = [to_batches_all[current_index + 1]]
                else:
                    to_batches_options = []
            except ValueError:
                pass

        option = st.radio(
            "Batch", 
            ["Existing", "New"], 
            horizontal=True,
            disabled=next_batch_only
        )

        to_batch = None
        if option == "Existing":
            if not to_batches_options:
                st.error(f"No batches for {to_degree}" + (" (or no 'next' batch)" if next_batch_only else ""))
                return
            to_batch = st.selectbox("To Batch", to_batches_options, key="move_to_batch")
        else:
            to_batch = st.text_input("New Batch", key="move_new_batch")
            st.warning("Creating new batches this way is not recommended. Use the Import tab.")

        to_year = st.number_input("Year", min_value=1, max_value=10, value=1)

    st.divider()

    if students_to_move_df.empty:
        st.warning("Select students to move")
        return

    if not to_batch:
        st.warning("Select/enter destination batch")
        return

    st.warning(f"Move {len(students_to_move_df)} students to {to_degree} Batch {to_batch} Year {to_year}")

    if st.button("🚀 Execute", type="primary"):
        on_cooldown_df = students_to_move_df[students_to_move_df["On Cooldown"] == True]
        valid_to_move_df = students_to_move_df[students_to_move_df["On Cooldown"] == False]
        
        enrollment_ids = valid_to_move_df["Enrollment ID"].tolist()
        
        moved = 0
        if not enrollment_ids:
            st.error("❌ No valid students to move (all selected are on cooldown).")
            return

        try:
            with engine.begin() as conn:
                moved = _db_move_students(conn, enrollment_ids, to_degree, to_batch, to_year)

            success_msg = f"✅ Moved {moved} students."
            warning_msg = ""
            
            if not on_cooldown_df.empty:
                warning_msg = f" ⚠️ {len(on_cooldown_df)} students were not moved as they are on a 30-day cooldown."
            
            st.success(success_msg + warning_msg)
            st.cache_data.clear()

            if "students_to_move_df" in st.session_state:
                del st.session_state.students_to_move_df

            st.rerun()

        except Exception as e:
            st.error(f"❌ Failed: {str(e)}")
            log.error(f"Move failed: {traceback.format_exc()}")


# ------------------------------------------------------------------
# UI SECTION 3: CREDENTIAL EXPORT
# ------------------------------------------------------------------

def _add_student_credential_export_section(engine: Engine):
    """Export student credentials."""
    st.divider()
    st.subheader("🔑 Export Credentials")

    with engine.begin() as conn:
        degrees = _active_degrees(conn)

    st.info("Export usernames and initial passwords for students who have not logged in.")

    if st.button("Generate & Download", disabled=(not degrees)):
        try:
            with st.spinner("Generating..."):
                df_creds = _get_student_credentials_to_export(engine)

            if df_creds.empty:
                st.warning("No new credentials to export")
                return

            csv = df_creds.to_csv(index=False)

            st.download_button(
                "Download Credentials",
                data=csv,
                file_name="student_credentials.csv",
                mime="text/csv"
            )

        except Exception as e:
            st.error(f"❌ Failed: {str(e)}")
            log.error(f"Export failed: {traceback.format_exc()}")


# ------------------------------------------------------------------
# UI SECTION 4: STUDENT DATA EXPORTER
# ------------------------------------------------------------------

@st.cache_data
def _get_student_data_to_export(_engine: Engine) -> pd.DataFrame:
    """
    Fetches all student profile, enrollment, and custom field data
    and merges it into a single wide DataFrame for export.
    """
    with _engine.connect() as conn:
        # 1. Fetch base profile and enrollment data (WITH roll_number)
        base_sql = """
            SELECT
                p.id as student_profile_id,
                p.name,
                p.email,
                p.student_id,
                p.phone,
                p.status,
                e.degree_code,
                e.program_code,
                e.branch_code,
                e.batch,
                e.current_year,
                e.division_code,
                e.roll_number,
                e.enrollment_status
            FROM student_profiles p
            LEFT JOIN student_enrollments e ON p.id = e.student_profile_id AND e.is_primary = 1
            ORDER BY p.student_id
        """
        base_df = pd.read_sql_query(base_sql, conn)
        
        if base_df.empty:
            return pd.DataFrame()

        # 2. Fetch all custom data in long format
        custom_sql = """
            SELECT
                student_profile_id,
                field_code,
                value
            FROM student_custom_profile_data
        """
        custom_df = pd.read_sql_query(custom_sql, conn)
        
        if custom_df.empty:
            return base_df

        # 3. Pivot custom data from long to wide
        try:
            pivoted_df = custom_df.pivot(
                index='student_profile_id',
                columns='field_code',
                values='value'
            ).reset_index()
        except Exception as e:
            log.error(f"Failed to pivot custom data: {e}")
            return base_df

        # 4. Merge base data with pivoted custom data
        final_df = pd.merge(
            base_df,
            pivoted_df,
            on='student_profile_id',
            how='left'
        )
        
        # 5. Get all custom field codes to ensure columns exist even if no data
        all_custom_fields = conn.execute(sa_text(
            "SELECT code FROM student_custom_profile_fields ORDER BY code"
        )).fetchall()
        all_custom_codes = [f[0] for f in all_custom_fields]
        
        for code in all_custom_codes:
            if code not in final_df.columns:
                final_df[code] = None
                
        # Reorder columns: base, then custom
        final_columns = base_df.columns.tolist() + all_custom_codes
        final_columns_ordered = []
        for col in final_columns:
            if col not in final_columns_ordered:
                final_columns_ordered.append(col)

        return final_df[final_columns_ordered]


def _add_student_data_export_section(engine: Engine):
    """UI for the full student data exporter."""
    st.divider()
    st.subheader("📊 Export Full Student Data")
    st.info("Download a single CSV file containing all student profile, enrollment, and custom field data.")

    if st.button("Generate & Download Student Data"):
        try:
            with st.spinner("Generating full student export..."):
                df_export = _get_student_data_to_export(engine)

            if df_export.empty:
                st.warning("No student data to export.")
                return

            csv = df_export.to_csv(index=False)

            st.download_button(
                "Download Data",
                data=csv,
                file_name="student_full_export.csv",
                mime="text/csv"
            )

        except Exception as e:
            st.error(f"❌ Failed to export data: {str(e)}")
            log.error(f"Full export failed: {traceback.format_exc()}")
