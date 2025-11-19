# schemas/students_schema.py
"""
Student Management Schema - WITH DIVISION MANAGEMENT
- Added division_master table for division definitions
- Added division_assignments table for tracking division changes
- All existing tables unchanged
"""
from __future__ import annotations
from sqlalchemy.engine import Engine
from sqlalchemy import text as sa_text
from core.schema_registry import register


@register("students")
def install_schema(engine: Engine) -> None:
    """
    Installs all student-related tables with proper columns.
    """
    with engine.begin() as conn:
        # 1. student_profiles
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS student_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_id TEXT UNIQUE NOT NULL,
                name TEXT,
                email TEXT,
                username TEXT UNIQUE,
                phone TEXT,
                status TEXT DEFAULT 'active',
                dob TEXT,
                gender TEXT,
                address TEXT,
                guardian_name TEXT,
                guardian_phone TEXT,
                guardian_email TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                active INTEGER DEFAULT 1
            )
        """))
        
        # Add status column if it doesn't exist (migration)
        try:
            conn.execute(sa_text(
                "ALTER TABLE student_profiles ADD COLUMN status TEXT DEFAULT 'active'"
            ))
        except Exception:
            pass  # Column already exists
        
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_student_profiles_student_id ON student_profiles(student_id)"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_student_profiles_email ON student_profiles(email)"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_student_profiles_username ON student_profiles(username)"))

        # 2. student_enrollments
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS student_enrollments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_profile_id INTEGER NOT NULL,
                degree_code TEXT NOT NULL,
                program_code TEXT,
                branch_code TEXT,
                batch TEXT,
                current_year INTEGER,
                division_code TEXT,
                roll_number TEXT,
                admission_date TEXT,
                graduation_date TEXT,
                enrollment_status TEXT DEFAULT 'active',
                is_primary INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (student_profile_id) REFERENCES student_profiles(id) ON DELETE CASCADE
            )
        """))
        
        # Add enrollment_status column if it doesn't exist (migration)
        try:
            conn.execute(sa_text(
                "ALTER TABLE student_enrollments ADD COLUMN enrollment_status TEXT DEFAULT 'active'"
            ))
        except Exception:
            pass  # Column already exists
        
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_student_enrollments_profile ON student_enrollments(student_profile_id)"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_student_enrollments_degree ON student_enrollments(degree_code)"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_student_enrollments_batch ON student_enrollments(batch)"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_student_enrollments_division ON student_enrollments(division_code)"))

        # 3. student_initial_credentials
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS student_initial_credentials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_profile_id INTEGER NOT NULL UNIQUE,
                username TEXT NOT NULL,
                plaintext TEXT NOT NULL,
                consumed INTEGER DEFAULT 0,
                consumed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (student_profile_id) REFERENCES student_profiles(id) ON DELETE CASCADE
            )
        """))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_student_initial_credentials_profile ON student_initial_credentials(student_profile_id)"))

        # 4. student_custom_profile_fields
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS student_custom_profile_fields (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                label TEXT NOT NULL,
                dtype TEXT NOT NULL,
                required INTEGER DEFAULT 0,
                active INTEGER DEFAULT 1,
                sort_order INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # 5. student_custom_profile_data
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS student_custom_profile_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_profile_id INTEGER NOT NULL,
                field_code TEXT NOT NULL,
                value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (student_profile_id) REFERENCES student_profiles(id) ON DELETE CASCADE,
                FOREIGN KEY (field_code) REFERENCES student_custom_profile_fields(code) ON DELETE CASCADE,
                UNIQUE(student_profile_id, field_code)
            )
        """))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_student_custom_data_profile ON student_custom_profile_data(student_profile_id)"))

        # 6. degree_batches
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
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_degree_batches_degree ON degree_batches(degree_code)"))

        # 7. app_settings
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # 8. degree_year_scaffold
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

        # 9. batch_year_scaffold
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

        # 10. student_mover_audit
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS student_mover_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                moved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                moved_by TEXT,
                student_profile_id INTEGER NOT NULL,
                enrollment_id INTEGER NOT NULL,
                from_degree_code TEXT,
                from_batch TEXT,
                from_year INTEGER,
                from_program_code TEXT,
                from_branch_code TEXT,
                from_division_code TEXT,
                to_degree_code TEXT,
                to_batch TEXT,
                to_year INTEGER,
                reason TEXT
            )
        """))

        # ============================================================
        # NEW: DIVISION MANAGEMENT TABLES
        # ============================================================

        # 11. division_master - Master list of all divisions
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS division_master (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                degree_code TEXT NOT NULL,
                batch TEXT,
                current_year INTEGER,
                division_code TEXT NOT NULL,
                division_name TEXT NOT NULL,
                capacity INTEGER,
                active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(degree_code, batch, current_year, division_code)
            )
        """))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_division_master_degree ON division_master(degree_code)"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_division_master_batch ON division_master(batch)"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_division_master_year ON division_master(current_year)"))

        # 12. division_assignment_audit - Track all division changes with reason
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS division_assignment_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_profile_id INTEGER NOT NULL,
                enrollment_id INTEGER NOT NULL,
                from_division_code TEXT,
                to_division_code TEXT,
                reason TEXT,
                assigned_by TEXT,
                assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (student_profile_id) REFERENCES student_profiles(id) ON DELETE CASCADE,
                FOREIGN KEY (enrollment_id) REFERENCES student_enrollments(id) ON DELETE CASCADE
            )
        """))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_division_audit_student ON division_assignment_audit(student_profile_id)"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_division_audit_enrollment ON division_assignment_audit(enrollment_id)"))

        # 13. student_status_audit - Track all student status changes with reason
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS student_status_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_profile_id INTEGER NOT NULL,
                from_status TEXT,
                to_status TEXT,
                reason TEXT,
                changed_by TEXT,
                changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (student_profile_id) REFERENCES student_profiles(id) ON DELETE CASCADE
            )
        """))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_status_audit_student ON student_status_audit(student_profile_id)"))

    print("✅ Student schema installed successfully with division management and status audit tables")
