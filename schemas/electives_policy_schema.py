from __future__ import annotations

import sqlite3
from sqlalchemy.engine import Engine

# --- 1. Import the registry decorator ---
try:
    from core.schema_registry import register
except ImportError:
    # Fallback if the registry isn't available for some reason
    def register(func): return func


SCHEMA_VERSION = 1


# --- 2. Register the installer ---
@register
def install(engine: Engine) -> None:
    """
    Create the electives_policy table and related indexes.

    This is intentionally degree-aware but NOT tied to a specific
    discipline – engineering vs architecture differences are expressed
    via the policy flags, not via separate tables.
    """
    
    # Manage the raw connection safely
    conn = None
    try:
        conn = engine.raw_connection()
        cur = conn.cursor()

        # Main table
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS electives_policy (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                -- Scope
                degree_code TEXT NOT NULL,
                program_code TEXT,
                branch_code TEXT,
                -- 'degree', 'program', or 'branch' – mostly for UI clarity
                scope_level TEXT NOT NULL DEFAULT 'degree',

                -- Behaviour flags
                -- How electives exist in this degree:
                --   'topics_only'  : base subject + many topics (B.Arch style studios)
                --   'subject_only' : students pick one subject, no sub-topics
                --   'both'         : allow both patterns if needed
                elective_mode TEXT NOT NULL DEFAULT 'topics_only',

                -- How students get allocated:
                --   'upload_only'   : office/faculty upload final allocations
                --   'rank_and_auto' : students give choices, engine allocates
                allocation_mode TEXT NOT NULL DEFAULT 'upload_only',

                -- For 'rank_and_auto' – how many preferences per slot (0 = disabled)
                max_choices_per_slot INTEGER NOT NULL DEFAULT 0,

                -- Topic capacity behaviour:
                --   'manual'      : admin enters capacity per topic
                --   'equal_split' : total seats ÷ number of topics
                --   'unlimited'   : no hard cap at topic level
                default_topic_capacity_strategy TEXT NOT NULL DEFAULT 'manual',

                -- Can a topic mix batches? (e.g. vertical studios)
                cross_batch_allowed INTEGER NOT NULL DEFAULT 0,

                -- Can a topic mix branches/programs inside the degree?
                cross_branch_allowed INTEGER NOT NULL DEFAULT 0,

                -- For tight timetables (engineering):
                -- whether to run timetable clash checks during allocation
                uses_timetable_clash_check INTEGER NOT NULL DEFAULT 0,

                -- Allow multiple historical rows; only one active per scope
                is_active INTEGER NOT NULL DEFAULT 1,

                notes TEXT,

                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        # One active policy row per (degree, program, branch) combination
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_electives_policy_scope
            ON electives_policy (
                degree_code,
                COALESCE(program_code, ''),
                COALESCE(branch_code, ''),
                is_active
            );
            """
        )

        conn.commit()

    finally:
        if conn:
            conn.close()
