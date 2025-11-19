# screens/students/page.py
# -------------------------------------------------------------------
# MODIFIED VERSION
# - Wrapped global render() in __main__ check to fix Double Rendering
#   while ensuring the page still loads.
# -------------------------------------------------------------------
from __future__ import annotations

import traceback
from typing import Optional, Any, List, Dict
import pandas as pd

import streamlit as st
import sqlalchemy
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine, Connection


# ────────────────────────────────────────────────────────────────────────────────
# Settings helpers
# ────────────────────────────────────────────────────────────────────────────────

def _get_setting(conn: Connection, key: str, default: Any = None) -> Any:
    """Gets a setting value from the database."""
    try:
        row = conn.execute(
            sa_text("SELECT value FROM app_settings WHERE key = :key"),
            {"key": key}
        ).fetchone()
        if row:
            return row[0]
    except Exception:
        pass
    return default


def _set_setting(conn: Connection, key: str, value: Any):
    """Saves a setting value to the database."""
    conn.execute(sa_text("""
        INSERT INTO app_settings (key, value)
        VALUES (:key, :value)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """), {"key": key, "value": str(value)})


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


# ────────────────────────────────────────────────────────────────────────────────
# Small helpers
# ────────────────────────────────────────────────────────────────────────────────

def _k(s: str) -> str:
    """Per-page key namespace to avoid collisions if rendered twice."""
    return f"students__{s}"


def _ensure_engine(engine: Optional[Engine]) -> Engine:
    if engine is not None:
        return engine
    from core.settings import load_settings
    from core.db import get_engine
    settings = load_settings()
    return get_engine(settings.db.url)


def _table_exists(conn, name: str) -> bool:
    try:
        row = conn.execute(
            sa_text("SELECT 1 FROM sqlite_master WHERE type='table' AND name=:n"),
            {"n": name},
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def _students_tables_exist(engine: Engine) -> bool:
    try:
        with engine.connect() as conn:
            return _table_exists(conn, "student_profiles")
    except Exception:
        return False


def _students_tables_snapshot(engine: Engine) -> None:
    with st.expander("Database snapshot (students tables)", expanded=False):
        try:
            with engine.connect() as conn:
                names = (
                    "student_profiles",
                    "student_enrollments",
                    "student_initial_credentials",
                    "student_custom_profile_fields",
                    "student_custom_profile_data",
                    "degrees",
                    "programs",
                    "branches",
                    "degree_batches",
                    "app_settings",
                    "division_master",
                    "division_assignment_audit"
                )
                info = {n: _table_exists(conn, n) for n in names}
                st.write(info)
                if info.get("student_profiles"):
                    total = conn.execute(
                        sa_text("SELECT COUNT(*) FROM student_profiles")
                    ).scalar() or 0
                    st.caption(f"student_profiles count: {total}")
        except Exception:
            st.warning("Could not probe students tables.")
            st.code(traceback.format_exc())


# ────────────────────────────────────────────────────────────────────────────────
# Optional: Bulk Operations import (defensive)
# ────────────────────────────────────────────────────────────────────────────────
_bulk_err = None
_render_bulk_ops = None
try:
    from screens.students.bulk_ops import render as _render_bulk_ops
except Exception as _e:
    _bulk_err = _e


# ────────────────────────────────────────────────────────────────────────────────
# Division Management Helpers
# ────────────────────────────────────────────────────────────────────────────────

def _get_divisions_for_scope(conn: Connection, degree_code: str, batch: str = None, year: int = None) -> List[Dict[str, Any]]:
    """Get divisions for a given scope."""
    query = """
        SELECT id, division_code, division_name, capacity, active
        FROM division_master
        WHERE degree_code = :degree
    """
    params = {"degree": degree_code}
    
    if batch:
        query += " AND batch = :batch"
        params["batch"] = batch
    
    if year:
        query += " AND current_year = :year"
        params["year"] = year
    
    query += " ORDER BY division_code"
    
    rows = conn.execute(sa_text(query), params).fetchall()
    return [{"id": r[0], "code": r[1], "name": r[2], "capacity": r[3], "active": r[4]} for r in rows]


def _get_division_student_count(conn: Connection, degree_code: str, batch: str, year: int, division_code: str) -> int:
    """Get count of students in a division."""
    count = conn.execute(sa_text("""
        SELECT COUNT(*) FROM student_enrollments
        WHERE degree_code = :degree
          AND batch = :batch
          AND current_year = :year
          AND division_code = :div
          AND is_primary = 1
    """), {
        "degree": degree_code,
        "batch": batch,
        "year": year,
        "div": division_code
    }).scalar()
    return count or 0


def _create_division(conn: Connection, degree_code: str, batch: str, year: int, 
                    division_code: str, division_name: str, capacity: int = None) -> bool:
    """Create a new division."""
    try:
        conn.execute(sa_text("""
            INSERT INTO division_master (degree_code, batch, current_year, division_code, division_name, capacity, active)
            VALUES (:degree, :batch, :year, :code, :name, :capacity, 1)
        """), {
            "degree": degree_code,
            "batch": batch,
            "year": year,
            "code": division_code,
            "name": division_name,
            "capacity": capacity
        })
        return True
    except Exception as e:
        st.error(f"Failed to create division: {e}")
        return False


def _update_division(conn: Connection, division_id: int, division_name: str = None, 
                    capacity: int = None, active: bool = None) -> bool:
    """Update division details."""
    try:
        updates = []
        params = {"id": division_id}
        
        if division_name is not None:
            updates.append("division_name = :name")
            params["name"] = division_name
        
        if capacity is not None:
            updates.append("capacity = :capacity")
            params["capacity"] = capacity
        
        if active is not None:
            updates.append("active = :active")
            params["active"] = 1 if active else 0
        
        if updates:
            query = f"UPDATE division_master SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP WHERE id = :id"
            conn.execute(sa_text(query), params)
        
        return True
    except Exception as e:
        st.error(f"Failed to update division: {e}")
        return False


def _delete_division(conn: Connection, division_id: int, division_code: str) -> tuple[bool, str]:
    """Delete a division if no students are assigned."""
    # Check if any students are in this division
    count = conn.execute(sa_text("""
        SELECT COUNT(*) FROM student_enrollments
        WHERE division_code = :div AND is_primary = 1
    """), {"div": division_code}).scalar()
    
    if count and count > 0:
        return False, f"Cannot delete: {count} student(s) still assigned to this division"
    
    try:
        conn.execute(sa_text("DELETE FROM division_master WHERE id = :id"), {"id": division_id})
        return True, "Division deleted successfully"
    except Exception as e:
        return False, f"Failed to delete: {e}"


# ────────────────────────────────────────────────────────────────────────────────
# Settings Tab Helpers
# ────────────────────────────────────────────────────────────────────────────────

def _render_custom_fields_settings(engine: Engine):
    """Manage custom profile fields for students."""
    st.markdown("### 📝 Custom Profile Fields")
    st.caption("Define additional fields to capture student information beyond standard profile data.")
    
    try:
        with engine.connect() as conn:
            fields = conn.execute(sa_text("""
                SELECT id, code, label, dtype, required, active, sort_order
                FROM student_custom_profile_fields
                ORDER BY sort_order, code
            """)).fetchall()
            
            if fields:
                st.markdown("#### Existing Custom Fields")
                for field in fields:
                    with st.expander(f"**{field[2]}** (`{field[1]}`) - {'Active' if field[5] else 'Inactive'}"):
                        col1, col2, col3 = st.columns([2, 1, 1])
                        col1.text_input("Label", value=field[2], key=f"field_label_{field[0]}", disabled=True)
                        col2.text_input("Type", value=field[3], key=f"field_type_{field[0]}", disabled=True)
                        col3.checkbox("Required", value=bool(field[4]), key=f"field_req_{field[0]}", disabled=True)
                        
                        if st.button("🗑️ Delete Field", key=f"del_field_{field[0]}"):
                            with engine.begin() as conn_b:
                                conn_b.execute(sa_text(
                                    "DELETE FROM student_custom_profile_data WHERE field_code = :code"
                                ), {"code": field[1]})
                                conn_b.execute(sa_text(
                                    "DELETE FROM student_custom_profile_fields WHERE code = :code"
                                ), {"code": field[1]})
                            st.success(f"Deleted field: {field[2]}")
                            st.rerun()
            else:
                st.info("No custom fields defined yet.")
        
        with st.expander("➕ Add New Custom Field", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                new_code = st.text_input("Field Code*", placeholder="e.g., blood_group, is_hostel_resident", key=_k("new_field_code"))
                new_label = st.text_input("Field Label*", placeholder="e.g., Blood Group, Hostel Resident?", key=_k("new_field_label"))
            with col2:
                new_dtype = st.selectbox("Data Type*", ["text", "number", "date", "choice", "boolean"], key=_k("new_field_dtype"))
                new_required = st.checkbox("Required Field", key=_k("new_field_required"))
                new_active = st.checkbox("Active", value=True, key=_k("new_field_active"))

            if st.button("Add Custom Field", type="primary", key=_k("add_field_btn")):
                if not new_code or not new_label:
                    st.error("Field code and label are required")
                else:
                    try:
                        with engine.begin() as conn_b:
                            conn_b.execute(sa_text("""
                                INSERT INTO student_custom_profile_fields (code, label, dtype, required, active, sort_order)
                                VALUES (:code, :label, :dtype, :req, :active, 100)
                            """), {
                                "code": new_code.strip().lower().replace(" ", "_"),
                                "label": new_label.strip(),
                                "dtype": new_dtype,
                                "req": 1 if new_required else 0,
                                "active": 1 if new_active else 0
                            })
                        st.success(f"✅ Added custom field: {new_label}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to add field: {e} (Is the code unique?)")
    
    except Exception as e:
        st.error(f"Failed to load custom fields: {e}")


def _render_roll_number_policy(engine: Engine):
    """Configure roll number derivation and validation policies."""
    st.markdown("### 🔢 Roll Number Policy")
    st.caption("Define how roll numbers are generated, validated, and scoped.")
    
    with engine.connect() as conn:
        derivation_mode = st.radio(
            "Roll Number Generation",
            ["hybrid", "manual", "auto"],
            index=["hybrid", "manual", "auto"].index(_get_setting(conn, "roll_derivation_mode", "hybrid")),
            help="Hybrid: Auto-generate with manual override. Manual: Always enter manually. Auto: Fully automated.",
            key=_k("roll_derivation_mode")
        )
        
        year_from_first4 = st.checkbox(
            "Extract year from first 4 digits",
            value=_get_setting(conn, "roll_year_from_first4", "True") == "True",
            help="e.g., '2021' from roll number '20211234'",
            key=_k("year_from_first4")
        )
        
        per_degree_regex = st.checkbox(
            "Allow per-degree regex patterns",
            value=_get_setting(conn, "roll_per_degree_regex", "True") == "True",
            help="Enable custom validation patterns for each degree",
            key=_k("per_degree_regex")
        )

        st.divider()
    
        if st.button("💾 Save Roll Number Policy", type="primary", key=_k("save_roll_policy")):
            with engine.begin() as conn_b:
                _set_setting(conn_b, "roll_derivation_mode", derivation_mode)
                _set_setting(conn_b, "roll_year_from_first4", year_from_first4)
                _set_setting(conn_b, "roll_per_degree_regex", per_degree_regex)
            st.success("✅ Roll number policy saved")
            st.rerun()


def _render_email_lifecycle_policy(engine: Engine):
    """Configure email lifecycle requirements (.edu and personal email)."""
    st.markdown("### 📧 Email Lifecycle Policy")
    st.caption("Manage .edu email requirements and post-graduation personal email transitions.")
    
    with engine.connect() as conn:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### .edu Email Requirement")
            edu_email_enabled = st.checkbox(
                "Require .edu email",
                value=_get_setting(conn, "email_edu_enabled", "True") == "True",
                help="Students must provide an institutional email",
                key=_k("edu_email_enabled")
            )
            
            edu_enforcement_months = st.number_input(
                "Enforcement period (months)",
                min_value=1,
                max_value=24,
                value=int(_get_setting(conn, "email_edu_months", 6)),
                help="Grace period after joining to provide .edu email",
                key=_k("edu_enforcement_months")
            )
            
            edu_domain = st.text_input(
                "Allowed domain(s)",
                value=_get_setting(conn, "email_edu_domain", "college.edu"),
                placeholder="e.g., college.edu",
                help="Comma-separated list of allowed domains",
                key=_k("edu_domain")
            )
        
        with col2:
            st.markdown("#### Post-Graduation Personal Email")
            personal_email_enabled = st.checkbox(
                "Require personal email after graduation",
                value=_get_setting(conn, "email_personal_enabled", "True") == "True",
                help="Students must provide personal email before graduation",
                key=_k("personal_email_enabled")
            )
            
            personal_enforcement_months = st.number_input(
                "Enforcement period (months after graduation)",
                min_value=1,
                max_value=24,
                value=int(_get_setting(conn, "email_personal_months", 6)),
                help="Time to provide personal email after graduation",
                key=_k("personal_enforcement_months")
            )
    
    st.divider()
    
    if st.button("💾 Save Email Policy", type="primary", key=_k("save_email_policy")):
        with engine.begin() as conn:
            _set_setting(conn, "email_edu_enabled", edu_email_enabled)
            _set_setting(conn, "email_edu_months", edu_enforcement_months)
            _set_setting(conn, "email_edu_domain", edu_domain)
            _set_setting(conn, "email_personal_enabled", personal_email_enabled)
            _set_setting(conn, "email_personal_months", personal_enforcement_months)
        st.success("✅ Email lifecycle policy saved")
        st.rerun()


def _render_student_status_settings(engine: Engine):
    """Configure available student statuses and their effects."""
    st.markdown("### 🎓 Student Status Configuration")
    st.caption("Define available student statuses and their behavioral effects.")
    
    default_statuses = {
        "Good": {
            "effects": {"include_in_current_ay": True},
            "badge": None,
            "note": "Active student in good standing"
        },
        "Hold": {
            "effects": {"include_in_current_ay": False},
            "badge": None,
            "note": "Hidden from current AY calculations"
        },
        "Left": {
            "effects": {"include_in_current_ay": False, "future_allocations": False},
            "badge": "Left",
            "note": "Student has left the institution"
        },
        "Transferred": {
            "effects": {"include_in_current_ay": False, "future_allocations": False},
            "badge": "Transferred",
            "note": "Transferred to another institution"
        },
        "Graduated": {
            "effects": {"include_in_current_ay": False, "eligible_for_transcript": True},
            "badge": "Graduated",
            "note": "Completed the program"
        },
        "Deceased": {
            "effects": {"include_in_current_ay": False, "record_frozen": True, "restricted_access": True},
            "badge": "Deceased",
            "note": "Record is frozen and access is restricted"
        },
        "YearDrop": {
            "effects": {"include_in_current_ay": True},
            "badge": "Year Drop",
            "note": "Student has dropped a year but remains enrolled"
        }
    }
    
    for status_name, config in default_statuses.items():
        with st.expander(f"**{status_name}** {('🏷️ ' + config['badge']) if config['badge'] else ''}"):
            st.caption(config['note'])
            effects = config['effects']
            cols = st.columns(3)
            for i, (effect, value) in enumerate(effects.items()):
                with cols[i % 3]:
                    icon = "✅" if value else "❌"
                    st.markdown(f"{icon} `{effect}`")
    
    st.divider()
    st.info("💡 Status definitions are configured in the YAML policy. Editing UI coming soon.")


def _render_division_editor(engine: Engine):
    """NEW: Division Editor - Create and manage divisions."""
    st.markdown("### 🏫 Division Editor")
    st.caption("Create and manage divisions/sections for organizing students.")
    
    # Get degrees
    with engine.connect() as conn:
        degrees = conn.execute(sa_text(
            "SELECT code FROM degrees WHERE active = 1 ORDER BY sort_order, code"
        )).fetchall()
        degree_list = [d[0] for d in degrees]
    
    if not degree_list:
        st.warning("No active degrees found. Create degrees first.")
        return
    
    # Selection
    col1, col2, col3 = st.columns(3)
    with col1:
        selected_degree = st.selectbox("Degree", degree_list, key=_k("div_degree"))
    
    # Get batches for selected degree
    with engine.connect() as conn:
        batches = conn.execute(sa_text("""
            SELECT DISTINCT batch FROM student_enrollments 
            WHERE degree_code = :degree AND batch IS NOT NULL
            ORDER BY batch DESC
        """), {"degree": selected_degree}).fetchall()
        batch_list = [b[0] for b in batches]
    
    with col2:
        if not batch_list:
            st.warning("No batches found for this degree")
            return
        selected_batch = st.selectbox("Batch", batch_list, key=_k("div_batch"))
    
    # Get years for selected degree/batch
    with engine.connect() as conn:
        years = conn.execute(sa_text("""
            SELECT DISTINCT current_year FROM student_enrollments 
            WHERE degree_code = :degree AND batch = :batch AND current_year IS NOT NULL
            ORDER BY current_year
        """), {"degree": selected_degree, "batch": selected_batch}).fetchall()
        year_list = [y[0] for y in years]
    
    with col3:
        if not year_list:
            st.warning("No years found for this batch")
            return
        selected_year = st.selectbox("Year", year_list, key=_k("div_year"))
    
    st.divider()
    
    # Display existing divisions
    with engine.connect() as conn:
        divisions = _get_divisions_for_scope(conn, selected_degree, selected_batch, selected_year)
    
    if divisions:
        st.markdown("#### 📋 Existing Divisions")
        
        for div in divisions:
            with st.expander(f"**{div['code']}** - {div['name']} {'✅' if div['active'] else '❌'}"):
                # Get student count
                with engine.connect() as conn:
                    count = _get_division_student_count(conn, selected_degree, selected_batch, selected_year, div['code'])
                
                col1, col2, col3 = st.columns([2, 1, 1])
                
                with col1:
                    st.metric("Students Assigned", count)
                    if div['capacity']:
                        utilization = (count / div['capacity']) * 100
                        st.progress(min(utilization / 100, 1.0))
                        st.caption(f"Capacity: {count}/{div['capacity']} ({utilization:.1f}%)")
                
                with col2:
                    new_name = st.text_input("Name", value=div['name'], key=f"div_name_{div['id']}")
                    new_capacity = st.number_input("Capacity", value=div['capacity'] or 0, min_value=0, key=f"div_cap_{div['id']}")
                
                with col3:
                    new_active = st.checkbox("Active", value=bool(div['active']), key=f"div_active_{div['id']}")
                    
                    if st.button("💾 Update", key=f"update_div_{div['id']}"):
                        with engine.begin() as conn:
                            if _update_division(conn, div['id'], new_name, new_capacity if new_capacity > 0 else None, new_active):
                                st.success(f"Updated {div['code']}")
                                st.rerun()
                
                # Delete button
                if count == 0:
                    if st.button(f"🗑️ Delete {div['code']}", key=f"del_div_{div['id']}", type="secondary"):
                        with engine.begin() as conn:
                            success, msg = _delete_division(conn, div['id'], div['code'])
                        if success:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)
                else:
                    st.caption(f"⚠️ Cannot delete: {count} students assigned")
    
    else:
        st.info("No divisions defined for this scope yet.")
    
    st.divider()
    
    # Create new division
    with st.expander("➕ Create New Division", expanded=False):
        st.markdown(f"**Creating for:** {selected_degree} / {selected_batch} / Year {selected_year}")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            new_div_code = st.text_input("Division Code*", placeholder="e.g., A, B, C", key=_k("new_div_code"))
        with col2:
            new_div_name = st.text_input("Division Name*", placeholder="e.g., Division A", key=_k("new_div_name"))
        with col3:
            new_div_capacity = st.number_input("Capacity (optional)", min_value=0, value=60, key=_k("new_div_capacity"))
        
        if st.button("Create Division", type="primary", key=_k("create_div_btn")):
            if not new_div_code or not new_div_name:
                st.error("Division code and name are required")
            else:
                with engine.begin() as conn:
                    if _create_division(
                        conn, 
                        selected_degree, 
                        selected_batch, 
                        selected_year,
                        new_div_code.strip().upper(),
                        new_div_name.strip(),
                        new_div_capacity if new_div_capacity > 0 else None
                    ):
                        st.success(f"✅ Created division: {new_div_code}")
                        st.rerun()


def _render_division_settings(engine: Engine):
    """Configure division/section management rules."""
    st.markdown("### 🏫 Division/Section Settings")
    st.caption("Configure how students are organized into divisions or sections.")
    
    with engine.connect() as conn:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Division Configuration")
            divisions_enabled = st.checkbox(
                "Enable divisions per term",
                value=_get_setting(conn, "div_enabled", "True") == "True",
                key=_k("divisions_enabled")
            )
            
            free_form_names = st.checkbox(
                "Allow free-form division names",
                value=_get_setting(conn, "div_free_form", "True") == "True",
                help="If unchecked, use predefined list",
                key=_k("free_form_names")
            )
            
            unique_scope = st.selectbox(
                "Uniqueness scope",
                ["degree_year_term", "degree_year", "degree", "global"],
                index=["degree_year_term", "degree_year", "degree", "global"].index(_get_setting(conn, "div_unique_scope", "degree_year_term")),
                help="Where division names must be unique",
                key=_k("unique_scope")
            )
        
        with col2:
            st.markdown("#### Import & Copy Settings")
            
            import_optional = st.checkbox(
                "Division column optional in imports",
                value=_get_setting(conn, "div_import_optional", "True") == "True",
                key=_k("import_optional")
            )
            
            copy_from_previous = st.checkbox(
                "Enable copy from previous term",
                value=_get_setting(conn, "div_copy_prev", "True") == "True",
                help="Allow copying division assignments from prior term",
                key=_k("copy_from_previous")
            )
            
            block_publish_unassigned = st.checkbox(
                "Block publish when students unassigned",
                value=_get_setting(conn, "div_block_publish", "True") == "True",
                help="Prevent publishing marks/attendance if students lack divisions",
                key=_k("block_publish")
            )
        
        with st.expander("🔢 Division Capacity (Optional)"):
            capacity_mode = st.radio(
                "Capacity tracking",
                ["off", "soft_limit", "hard_limit"],
                index=["off", "soft_limit", "hard_limit"].index(_get_setting(conn, "div_capacity_mode", "off")),
                help="Soft: warn on breach. Hard: block on breach.",
                key=_k("capacity_mode")
            )
            
            if capacity_mode != "off":
                default_capacity = st.number_input(
                    "Default division capacity",
                    min_value=1,
                    value=int(_get_setting(conn, "div_default_capacity", 60)),
                    key=_k("default_capacity")
                )
    
    if st.button("💾 Save Division Settings", type="primary", key=_k("save_division_settings")):
        with engine.begin() as conn:
            _set_setting(conn, "div_enabled", divisions_enabled)
            _set_setting(conn, "div_free_form", free_form_names)
            _set_setting(conn, "div_unique_scope", unique_scope)
            _set_setting(conn, "div_import_optional", import_optional)
            _set_setting(conn, "div_copy_prev", copy_from_previous)
            _set_setting(conn, "div_block_publish", block_publish_unassigned)
            _set_setting(conn, "div_capacity_mode", capacity_mode)
            if capacity_mode != "off":
                _set_setting(conn, "div_default_capacity", default_capacity)
        st.success("✅ Division settings saved")
        st.rerun()


def _render_publish_guardrails(engine: Engine):
    """Configure publish guardrails and validation checks."""
    st.markdown("### 🛡️ Publish Guardrails")
    st.caption("Define checks that must pass before publishing marks or attendance.")
    
    with engine.connect() as conn:
        guard_unassigned = st.checkbox("Block publish if program/branch/division unassigned", value=_get_setting(conn, "guard_unassigned", "True") == "True", key=_k("guard_unassigned"))
        guard_duplicates = st.checkbox("Block publish if duplicates unresolved", value=_get_setting(conn, "guard_duplicates", "True") == "True", key=_k("guard_duplicates"))
        guard_invalid = st.checkbox("Block publish if invalid roll or email", value=_get_setting(conn, "guard_invalid", "True") == "True", key=_k("guard_invalid"))
        guard_batch_mismatch = st.checkbox("Block publish if batch mismatch detected", value=_get_setting(conn, "guard_batch_mismatch", "True") == "True", key=_k("guard_batch_mismatch"))
        guard_capacity = st.checkbox("Block publish on hard capacity breach", value=_get_setting(conn, "guard_capacity", "False") == "True", key=_k("guard_capacity"))
    
    st.divider()
    
    if st.button("💾 Save Guardrails", type="primary", key=_k("save_guardrails")):
        with engine.begin() as conn:
            _set_setting(conn, "guard_unassigned", guard_unassigned)
            _set_setting(conn, "guard_duplicates", guard_duplicates)
            _set_setting(conn, "guard_invalid", guard_invalid)
            _set_setting(conn, "guard_batch_mismatch", guard_batch_mismatch)
            _set_setting(conn, "guard_capacity", guard_capacity)
        st.success("✅ Publish guardrails saved")
        st.rerun()


def _render_mover_settings(engine: Engine):
    """Configure student mover policies."""
    st.markdown("### 🚚 Student Mover Settings")
    st.caption("Control how students can be moved between batches, degrees, and divisions.")
    
    with engine.connect() as conn:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Within-Term Division Moves")
            within_term_enabled = st.checkbox(
                "Enable within-term division moves",
                value=_get_setting(conn, "mover_within_term", "True") == "True",
                key=_k("mover_within_term")
            )
            
            require_reason_within = st.checkbox(
                "Require reason for move",
                value=_get_setting(conn, "mover_within_reason", "True") == "True",
                key=_k("mover_within_reason")
            )
        
        with col2:
            st.markdown("#### Cross-Batch Moves")
            cross_batch_enabled = st.checkbox(
                "Enable cross-batch moves",
                value=_get_setting(conn, "mover_cross_batch", "True") == "True",
                key=_k("mover_cross_batch")
            )
            
            next_batch_only = st.checkbox(
                "Restrict to next batch only",
                value=_get_setting(conn, "mover_next_only", "True") == "True",
                help="Students can only move to the immediately following batch",
                key=_k("mover_next_only")
            )
            
            require_reason_cross = st.checkbox(
                "Require reason for move",
                value=_get_setting(conn, "mover_cross_reason", "True") == "True",
                key=_k("mover_cross_reason")
            )
    
    st.divider()
    
    if st.button("💾 Save Mover Settings", type="primary", key=_k("save_mover_settings")):
        with engine.begin() as conn:
            _set_setting(conn, "mover_within_term", within_term_enabled)
            _set_setting(conn, "mover_within_reason", require_reason_within)
            _set_setting(conn, "mover_cross_batch", cross_batch_enabled)
            _set_setting(conn, "mover_next_only", next_batch_only)
            _set_setting(conn, "mover_cross_reason", require_reason_cross)
        st.success("✅ Student mover settings saved")
        st.rerun()


def _render_settings_tab(engine: Engine):
    """Main settings tab with all configuration sections."""
    st.subheader("⚙️ Student Settings")
    
    # Settings categories - REMOVED "🔐 Access" tab
    settings_sections = st.tabs([
        "📝 Custom Fields",
        "🔢 Roll Numbers",
        "📧 Email Policy",
        "🎓 Student Status",
        "🏫 Division Editor",
        "⚙️ Division Settings",
        "🛡️ Guardrails",
        "🚚 Movers"
    ])
    
    with settings_sections[0]:
        _render_custom_fields_settings(engine)
    
    with settings_sections[1]:
        _render_roll_number_policy(engine)
    
    with settings_sections[2]:
        _render_email_lifecycle_policy(engine)
    
    with settings_sections[3]:
        _render_student_status_settings(engine)
    
    with settings_sections[4]:
        _render_division_editor(engine)
    
    with settings_sections[5]:
        _render_division_settings(engine)
    
    with settings_sections[6]:
        _render_publish_guardrails(engine)
    
    with settings_sections[7]:
        _render_mover_settings(engine)


# ────────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────────

# screens/students/page.py

def render(engine: Optional[Engine] = None, **kwargs) -> None:
    engine = _ensure_engine(engine)

    st.title("👨‍🎓 Students")
    st.caption(f"Module file: `{__file__}`")

    if not _students_tables_exist(engine):
        # This is a basic check. If the main 'student_profiles' table is missing,
        # we assume it's a fresh install and show a generic setup message.
        st.warning("⚠️ Student tables not found.")
        st.info(
            """
            ### 🚀 Welcome to the Students Module!
            
            To get started, the application's database schema needs to be initialized.
            
            **Please contact your system administrator** to run the 
            initial database setup.
            
            Once set up, you will need to:
            1. Create **Degrees** (in 🎓 Degrees)
            2. Create **Academic Years** (in 🗓️ Academic Years)
            3. Return here to import and manage students.
            """
        )
        return

    # --- START FIX: Robust check for dependent tables ---
    degree_count = 0
    ay_count = 0
    
    try:
        with engine.connect() as conn:
            # Check for degrees
            try:
                degree_count = conn.execute(sa_text(
                    "SELECT COUNT(*) FROM degrees WHERE active = 1"
                )).scalar()
            except sqlalchemy.exc.OperationalError as e:
                if "no such table" in str(e):
                    degree_count = 0  # Expected on fresh install
                else:
                    raise  # Re-raise unexpected errors

            # Check for academic years
            try:
                ay_count = conn.execute(sa_text(
                    "SELECT COUNT(*) FROM academic_years WHERE status = 'open'"
                )).scalar()
            except sqlalchemy.exc.OperationalError as e:
                if "no such table" in str(e):
                    ay_count = 0  # Expected on fresh install
                else:
                    raise # Re-raise unexpected errors

    except Exception as e:
        st.error(f"An unexpected error occurred while checking prerequisites: {e}")
        st.code(traceback.format_exc())
        return
    # --- END FIX ---


    if not degree_count or degree_count == 0:
        st.warning("⚠️ No active degrees found")
        st.info("""
        ### 🚀 Getting Started with Students
        
        Before you can add students, you need to set up the degree structure:
        
        1. **Create Degrees** (e.g., B.Tech, M.Tech)
           - Go to **🎓 Degrees** page
           - Define degree duration (number of years)
           - Activate the degree
        
        2. **Create Programs & Branches** (optional but recommended)
           - Go to **📚 Programs / Branches** page
           - Define programs (e.g., Engineering, Science)
           - Define branches under each program (e.g., Computer Science, Electronics)
        
        3. **Return here** to import students
        """)
        return

    # --- *** FIX: Removed duplicate _students_tables_snapshot() and ay_count check *** ---
    # This snapshot now only runs ONCE when all prerequisite checks have passed.
    _students_tables_snapshot(engine)

    tab_list, tab_bulk, tab_settings = st.tabs(
        ["Student List", "Bulk Operations", "Settings"]
    )

    with tab_list:
        try:
            st.subheader("All Students")
            with engine.connect() as conn:
                if not _table_exists(conn, "student_profiles"):
                    st.info("`student_profiles` not found. Use the schema installer.")
                else:
                    rows = conn.execute(
                        sa_text(
                            """
                            SELECT id,
                                   COALESCE(name, email, '') AS display_name,
                                   email,
                                   student_id,
                                   COALESCE(updated_at, '1970-01-01') AS uat
                            FROM student_profiles
                            ORDER BY uat DESC, id DESC
                            LIMIT 50
                            """
                        )
                    ).fetchall()

                    if not rows:
                        st.info("No student records yet. Use **Bulk Operations** to import.")
                    else:
                        data = [
                            {
                                "id": r[0],
                                "name": r[1],
                                "email": r[2],
                                "student_id": r[3],
                            }
                            for r in rows
                        ]
                        st.dataframe(data, use_container_width=True)
        except Exception:
            st.error("Student List failed.")
            st.code(traceback.format_exc())

    with tab_bulk:
        if _bulk_err:
            st.error("Bulk Operations import failed.")
            st.code(
                "Traceback (most recent call last):\n"
                + "".join(
                    traceback.format_exception_only(type(_bulk_err), _bulk_err)
                )
            )
        else:
            try:
                if _render_bulk_ops:
                    _render_bulk_ops(engine)
                else:
                    st.info("Bulk operations UI not available in this build.")
            except Exception:
                st.error("Bulk Operations failed.")
                st.code(traceback.format_exc())

    with tab_settings:
        try:
            _render_settings_tab(engine)
        except Exception:
            st.error("Settings tab failed.")
            st.code(traceback.format_exc())


# Wrap the call to prevent side-effects when imported by other modules
if __name__ == "__main__":
    render()
