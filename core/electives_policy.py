from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any, Optional, Tuple


@dataclass
class ElectivesPolicy:
    """
    In-memory representation of one electives policy row.
    """
    id: Optional[int]
    degree_code: str
    program_code: Optional[str]
    branch_code: Optional[str]
    scope_level: str

    elective_mode: str
    allocation_mode: str
    max_choices_per_slot: int
    default_topic_capacity_strategy: str

    cross_batch_allowed: bool
    cross_branch_allowed: bool
    uses_timetable_clash_check: bool

    is_active: bool
    notes: Optional[str]


# ---------- row <-> object helpers ----------

def _row_to_policy(row: sqlite3.Row | Tuple[Any, ...] | None) -> Optional[ElectivesPolicy]:
    """
    Robustly convert a database row to a policy object using column names.
    """
    if row is None:
        return None

    # We wrap this in a try/except block to catch missing columns
    try:
        # Use dictionary-style access. This is safe even if the query 
        # returns extra columns (like 'specificity_rank').
        return ElectivesPolicy(
            id=row["id"],
            degree_code=row["degree_code"],
            program_code=row["program_code"],
            branch_code=row["branch_code"],
            scope_level=row["scope_level"],
            elective_mode=row["elective_mode"],
            allocation_mode=row["allocation_mode"],
            max_choices_per_slot=int(row["max_choices_per_slot"] or 0),
            default_topic_capacity_strategy=row["default_topic_capacity_strategy"],
            cross_batch_allowed=bool(row["cross_batch_allowed"]),
            cross_branch_allowed=bool(row["cross_branch_allowed"]),
            uses_timetable_clash_check=bool(row["uses_timetable_clash_check"]),
            is_active=bool(row["is_active"]),
            notes=row["notes"],
        )
    except Exception as e:
        # If a column is missing, we print it to the server console 
        # and return None so the app doesn't crash entirely.
        print(f"Error converting policy row: {e}")
        return None


# ---------- core API ----------

def fetch_effective_policy(
    conn: sqlite3.Connection,
    *,
    degree_code: str,
    program_code: Optional[str] = None,
    branch_code: Optional[str] = None,
) -> Optional[ElectivesPolicy]:
    """
    Return the most specific active policy for a given context.
    """
    # conn.row_factory = sqlite3.Row  <- This line is not reliable
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row  # <-- FIXED: Set row_factory on the cursor

    # We explicitly select columns to ensure we get exactly what we expect
    cur.execute(
        """
        SELECT
            id, degree_code, program_code, branch_code, scope_level,
            elective_mode, allocation_mode, max_choices_per_slot,
            default_topic_capacity_strategy, cross_batch_allowed,
            cross_branch_allowed, uses_timetable_clash_check,
            is_active, notes,
            
            -- Calculated rank for sorting (not used in the object)
            CASE
                WHEN branch_code IS NOT NULL THEN 3
                WHEN program_code IS NOT NULL THEN 2
                ELSE 1
            END AS specificity_rank
        FROM electives_policy
        WHERE
            degree_code = ?
            AND is_active = 1
            AND (program_code IS NULL OR program_code = ?)
            AND (branch_code IS NULL OR branch_code = ?)
        ORDER BY
            specificity_rank DESC,
            id DESC
        LIMIT 1;
        """,
        (degree_code, program_code, branch_code),
    )

    row = cur.fetchone()
    return _row_to_policy(row)

def get_policy_for_scope(
    conn: sqlite3.Connection,
    *,
    degree_code: str,
    program_code: Optional[str],
    branch_code: Optional[str],
) -> Optional[ElectivesPolicy]:
    """
    Get policy for an exact scope (no fallback).
    """
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            id, degree_code, program_code, branch_code, scope_level,
            elective_mode, allocation_mode, max_choices_per_slot,
            default_topic_capacity_strategy, cross_batch_allowed,
            cross_branch_allowed, uses_timetable_clash_check,
            is_active, notes
        FROM electives_policy
        WHERE
            degree_code = ?
            AND COALESCE(program_code, '') = COALESCE(?, '')
            AND COALESCE(branch_code, '') = COALESCE(?, '')
            AND is_active = 1
        ORDER BY id DESC
        LIMIT 1;
        """,
        (degree_code, program_code, branch_code),
    )

    row = cur.fetchone()
    return _row_to_policy(row)


def upsert_policy(
    conn: sqlite3.Connection,
    policy: ElectivesPolicy,
) -> ElectivesPolicy:
    """
    Insert or update a policy row.
    """
    # conn.row_factory = sqlite3.Row  <- This line is not reliable
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row  # <-- FIXED: Set row_factory on the cursor

    # If we have an ID, update directly
    if policy.id is not None:
        cur.execute(
            """
            UPDATE electives_policy
            SET
                scope_level = ?,
                elective_mode = ?,
                allocation_mode = ?,
                max_choices_per_slot = ?,
                default_topic_capacity_strategy = ?,
                cross_batch_allowed = ?,
                cross_branch_allowed = ?,
                uses_timetable_clash_check = ?,
                is_active = ?,
                notes = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?;
            """,
            (
                policy.scope_level,
                policy.elective_mode,
                policy.allocation_mode,
                policy.max_choices_per_slot,
                policy.default_topic_capacity_strategy,
                int(policy.cross_batch_allowed),
                int(policy.cross_branch_allowed),
                int(policy.uses_timetable_clash_check),
                int(policy.is_active),
                policy.notes,
                policy.id,
            ),
        )
        conn.commit()
        return policy

    # No ID provided. Check if an active policy exists for this EXACT scope
    # to prevent duplicates.
    cur.execute(
        """
        SELECT id
        FROM electives_policy
        WHERE
            degree_code = ?
            AND COALESCE(program_code, '') = COALESCE(?, '')
            AND COALESCE(branch_code, '') = COALESCE(?, '')
            AND is_active = 1
        ORDER BY id DESC
        LIMIT 1;
        """,
        (policy.degree_code, policy.program_code, policy.branch_code),
    )
    existing = cur.fetchone()

    if existing is not None:
        # Update the existing active row
        # This line will no longer crash because 'existing' is a Row object
        existing_id = existing["id"]
        policy.id = existing_id

        cur.execute(
            """
            UPDATE electives_policy
            SET
                scope_level = ?,
                elective_mode = ?,
                allocation_mode = ?,
                max_choices_per_slot = ?,
                default_topic_capacity_strategy = ?,
                cross_batch_allowed = ?,
                cross_branch_allowed = ?,
                uses_timetable_clash_check = ?,
                is_active = ?,
                notes = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?;
            """,
            (
                policy.scope_level,
                policy.elective_mode,
                policy.allocation_mode,
                policy.max_choices_per_slot,
                policy.default_topic_capacity_strategy,
                int(policy.cross_batch_allowed),
                int(policy.cross_branch_allowed),
                int(policy.uses_timetable_clash_check),
                int(policy.is_active),
                policy.notes,
                policy.id,
            ),
        )
        conn.commit()
        return policy

    # No existing row found: Insert new
    cur.execute(
        """
        INSERT INTO electives_policy (
            degree_code,
            program_code,
            branch_code,
            scope_level,
            elective_mode,
            allocation_mode,
            max_choices_per_slot,
            default_topic_capacity_strategy,
            cross_batch_allowed,
            cross_branch_allowed,
            uses_timetable_clash_check,
            is_active,
            notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            policy.degree_code,
            policy.program_code,
            policy.branch_code,
            policy.scope_level,
            policy.elective_mode,
            policy.allocation_mode,
            policy.max_choices_per_slot,
            policy.default_topic_capacity_strategy,
            int(policy.cross_batch_allowed),
            int(policy.cross_branch_allowed),
            int(policy.uses_timetable_clash_check),
            int(policy.is_active),
            policy.notes,
        ),
    )
    policy.id = cur.lastrowid
    conn.commit()
    return policy

def deactivate_policy_for_scope(
    conn: sqlite3.Connection,
    *,
    degree_code: str,
    program_code: Optional[str],
    branch_code: Optional[str],
) -> None:
    """
    Soft-delete: mark all policies for this exact scope as inactive.
    """
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE electives_policy
        SET is_active = 0,
            updated_at = CURRENT_TIMESTAMP
        WHERE
            degree_code = ?
            AND COALESCE(program_code, '') = COALESCE(?, '')
            AND COALESCE(branch_code, '') = COALESCE(?, '');
        """,
        (degree_code, program_code, branch_code),
    )
    conn.commit()
