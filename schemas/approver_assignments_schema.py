from __future__ import annotations
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

try:
    from core.schema_registry import register
except ImportError:
    def register(func): return func


@register
def ensure_approver_assignments_schema(engine: Engine):
    """
    Ensures the dynamic approval assignment tables exist
    ('approver_assignments' and 'approval_rules_config').
    """
    with engine.begin() as conn:

        # --- 1. Create 'approver_assignments' table ---
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS approver_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            object_type TEXT NOT NULL,
            action TEXT NOT NULL,
            approver_email TEXT NOT NULL,
            approver_name TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            degree_code TEXT,
            program_code TEXT,
            branch_code TEXT,
            assigned_by TEXT NOT NULL,
            assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            deactivated_by TEXT,
            deactivated_at DATETIME,
            notes TEXT,
            UNIQUE(object_type, action, approver_email, degree_code, program_code, branch_code)
        )
        """))

        conn.execute(sa_text(
            "CREATE INDEX IF NOT EXISTS idx_approver_lookup "
            "ON approver_assignments(object_type, action, is_active)"
        ))
        conn.execute(sa_text(
            "CREATE INDEX IF NOT EXISTS idx_approver_email "
            "ON approver_assignments(approver_email, is_active)"
        ))

        # --- 2. Create 'approval_rules_config' table ---
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS approval_rules_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            object_type TEXT NOT NULL,
            action TEXT NOT NULL,
            require_user_assignment INTEGER DEFAULT 1,
            fallback_to_roles INTEGER DEFAULT 1,
            requires_reason INTEGER DEFAULT 1,
            min_approvers INTEGER DEFAULT 1,
            approval_rule TEXT DEFAULT 'either_one',
            auto_approve_after_hours INTEGER,
            notify_approvers INTEGER DEFAULT 1,
            notification_method TEXT DEFAULT 'email',
            linked_page_permission TEXT,
            created_by TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_by TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(object_type, action)
        )
        """))

        # --- 3. Add new column if it's missing (for migration) ---
        try:
            cols = {
                row[1]
                for row in conn.execute(
                    sa_text("PRAGMA table_info(approval_rules_config)")
                ).fetchall()
            }
            if "linked_page_permission" not in cols:
                conn.execute(
                    sa_text(
                        "ALTER TABLE approval_rules_config "
                        "ADD COLUMN linked_page_permission TEXT"
                    )
                )
        except Exception:
            # Table might not exist yet; CREATE TABLE above handles first-time setup
            pass

        # --- 4. Insert default rules (safe to re-run) ---
        # Use dicts + named parameters so SQLAlchemy 2 is happy
        default_rules = [
            {
                "object_type": "degree",
                "action": "delete",
                "require_user_assignment": 1,
                "fallback_to_roles": 1,
                "requires_reason": 1,
                "min_approvers": 1,
                "linked_page_permission": "Degrees",
            },
            {
                "object_type": "program",
                "action": "delete",
                "require_user_assignment": 1,
                "fallback_to_roles": 1,
                "requires_reason": 1,
                "min_approvers": 1,
                "linked_page_permission": "Programs / Branches",
            },
            {
                "object_type": "branch",
                "action": "delete",
                "require_user_assignment": 1,
                "fallback_to_roles": 1,
                "requires_reason": 1,
                "min_approvers": 1,
                "linked_page_permission": "Programs / Branches",
            },
            {
                "object_type": "faculty",
                "action": "delete",
                "require_user_assignment": 1,
                "fallback_to_roles": 1,
                "requires_reason": 1,
                "min_approvers": 1,
                "linked_page_permission": "Faculty",
            },
            {
                "object_type": "semester",
                "action": "delete",
                "require_user_assignment": 1,
                "fallback_to_roles": 1,
                "requires_reason": 1,
                "min_approvers": 1,
                "linked_page_permission": "Semesters",
            },
            {
                "object_type": "affiliation",
                "action": "edit_in_use",
                "require_user_assignment": 1,
                "fallback_to_roles": 1,
                "requires_reason": 1,
                "min_approvers": 1,
                "linked_page_permission": "Faculty",
            },
            {
                "object_type": "semesters",
                "action": "binding_change",
                "require_user_assignment": 1,
                "fallback_to_roles": 1,
                "requires_reason": 0,
                "min_approvers": 1,
                "linked_page_permission": "Semesters",
            },
            {
                "object_type": "semesters",
                "action": "edit_structure",
                "require_user_assignment": 1,
                "fallback_to_roles": 1,
                "requires_reason": 1,
                "min_approvers": 1,
                "linked_page_permission": "Semesters",
            },
        ]

        insert_sql = sa_text("""
            INSERT OR IGNORE INTO approval_rules_config 
                (object_type, action, require_user_assignment, fallback_to_roles,
                 requires_reason, min_approvers, linked_page_permission)
            VALUES (
                :object_type,
                :action,
                :require_user_assignment,
                :fallback_to_roles,
                :requires_reason,
                :min_approvers,
                :linked_page_permission
            )
        """)

        for rule in default_rules:
            conn.execute(insert_sql, rule)
