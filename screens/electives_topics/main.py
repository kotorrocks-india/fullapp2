# screens/electives_topics/main.py
"""
Electives & College Projects Management (Slide 18) - Complete Implementation
UPDATED: Integrated with electives_policy module and enhanced syllabus display
"""

import traceback
import json
import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

# Core imports
try:
    from core.settings import load_settings
    from core.db import get_engine
    from core.policy import require_page, can_edit_page, user_roles
    from core.forms import tagline, success
except Exception as e:
    st.error(f"Core imports failed: {e}")
    st.stop()

# Electives policy (optional but recommended)
try:
    from core import electives_policy as core_electives_policy
except Exception:
    core_electives_policy = None

# Schema
try:
    from screens.electives_topics.schema import (
        install_electives_schema,
        refresh_capacity_tracking,
    )
except Exception as e:
    st.error(f"Schema import failed: {e}")
    st.stop()

# Query helpers
try:
    from screens.electives_topics import queries as et_queries
except Exception as e:
    st.error(f"Queries import failed: {e}")
    st.stop()

# Import modules (allocation / import-export are optional)
try:
    from screens.electives_topics import allocation_engine
    from screens.electives_topics import import_export
except Exception:
    allocation_engine = None
    import_export = None

PAGE_TITLE = "Electives & College Projects"

# ===========================================================================
# DATABASE HELPERS
# ===========================================================================


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


def _load_effective_policy(
    engine: Engine,
    degree_code: str,
    program_code: Optional[str],
    branch_code: Optional[str],
) -> Optional[Dict]:
    """
    Read-only lookup into core.electives_policy.
    Returns a dict or None if policy module/table is not available.
    """
    if core_electives_policy is None:
        return None

    try:
        raw_conn = engine.raw_connection()
        try:
            policy_obj = core_electives_policy.fetch_effective_policy(
                conn=raw_conn,
                degree_code=degree_code,
                program_code=program_code,
                branch_code=branch_code,
            )
            
            if policy_obj is None:
                return None
            
            # Convert ElectivesPolicy dataclass to dict
            return {
                'id': policy_obj.id,
                'degree_code': policy_obj.degree_code,
                'program_code': policy_obj.program_code,
                'branch_code': policy_obj.branch_code,
                'scope_level': policy_obj.scope_level,
                'elective_mode': policy_obj.elective_mode,
                'allocation_mode': policy_obj.allocation_mode,
                'max_choices_per_slot': policy_obj.max_choices_per_slot,
                'default_topic_capacity_strategy': policy_obj.default_topic_capacity_strategy,
                'cross_batch_allowed': policy_obj.cross_batch_allowed,
                'cross_branch_allowed': policy_obj.cross_branch_allowed,
                'uses_timetable_clash_check': policy_obj.uses_timetable_clash_check,
                'is_active': policy_obj.is_active,
                'notes': policy_obj.notes,
            }
        finally:
            raw_conn.close()
    except Exception as e:
        # Soft-fail: electives module should still work without policy
        st.warning(f"Elective policy lookup failed: {e}")
        return None


# ===========================================================================
# FETCH FUNCTIONS
# ===========================================================================

def _suggest_batches_for_window(
    engine: Engine,
    degree_code: str,
    ay_label: str,
    year: int,
) -> List[str]:
    """Suggest likely batch codes for a selection window."""
    try:
        rows = _fetch_all(
            engine,
            """
            SELECT DISTINCT batch
            FROM batch_year_scaffold
            WHERE degree_code = :deg
              AND year_num    = :yr
            ORDER BY batch
            """,
            {"deg": degree_code, "yr": year},
        )
        batches = [r["batch"] for r in rows if r.get("batch")]
        if batches:
            return batches
    except Exception:
        pass

    # Fallback: infer from AY label
    if ay_label:
        m = re.search(r"(20\d{2})", ay_label)
        if m:
            return [m.group(1)]

    return []


@st.cache_data(ttl=300)
def fetch_degrees(_engine: Engine) -> List[Dict]:
    """Fetch active degrees."""
    return _fetch_all(
        _engine,
        """
        SELECT code, title 
        FROM degrees 
        WHERE active = 1 
        ORDER BY sort_order, code
        """,
    )


@st.cache_data(ttl=300)
def fetch_programs(_engine: Engine, degree_code: str) -> List[Dict]:
    """Fetch programs for degree."""
    return _fetch_all(
        _engine,
        """
        SELECT program_code, program_name 
        FROM programs 
        WHERE degree_code = :d 
          AND active = 1
        ORDER BY sort_order, program_code
        """,
        {"d": degree_code},
    )


@st.cache_data(ttl=300)
def fetch_branches(
    _engine: Engine, degree_code: str, program_code: Optional[str]
) -> List[Dict]:
    """Fetch branches."""
    if program_code:
        return _fetch_all(
            _engine,
            """
            SELECT b.branch_code, b.branch_name 
            FROM branches b
            JOIN programs p ON p.id = b.program_id
            WHERE p.degree_code = :d 
              AND p.program_code = :p 
              AND b.active = 1
            ORDER BY b.sort_order
            """,
            {"d": degree_code, "p": program_code},
        )
    else:
        return _fetch_all(
            _engine,
            """
            SELECT branch_code, branch_name 
            FROM branches 
            WHERE degree_code = :d 
              AND active = 1
            ORDER BY sort_order
            """,
            {"d": degree_code},
        )


@st.cache_data(ttl=300)
def fetch_academic_years(_engine: Engine) -> List[str]:
    """Fetch academic years that are usable for electives."""
    rows = _fetch_all(
        _engine,
        """
        SELECT ay_code, status
        FROM academic_years 
        ORDER BY start_date DESC
        """,
    )

    if not rows:
        return []

    # Prefer open/active years
    open_like = [
        r["ay_code"]
        for r in rows
        if str(r.get("status", "")).lower() in ("open", "active")
    ]
    if open_like:
        return open_like

    # Fall back to planned years
    planned = [
        r["ay_code"]
        for r in rows
        if str(r.get("status", "")).lower() == "planned"
    ]
    return planned


def fetch_topics_for_subject(
    engine: Engine,
    subject_code: str,
    ay_label: str,
    year: int,
    term: int,
    degree_code: str,
    division_code: Optional[str] = None,
) -> List[Dict]:
    """Fetch topics for a subject."""
    return _fetch_all(
        engine,
        """
        SELECT * FROM v_elective_topics_summary
        WHERE subject_code = :subj
          AND ay_label     = :ay
          AND year         = :yr
          AND term         = :trm
          AND degree_code  = :deg
          AND (division_code = :div OR (division_code IS NULL AND :div IS NULL))
        ORDER BY topic_no
        """,
        {
            "subj": subject_code,
            "ay": ay_label,
            "yr": year,
            "trm": term,
            "deg": degree_code,
            "div": division_code,
        },
    )


def get_syllabus_details(engine: Engine, subject_code: str, degree_code: str, 
                         ay_label: str = None, year: int = None, term: int = None) -> Optional[Dict]:
    """
    Get syllabus details for subject from subjects_catalog.
    
    UPDATED: No longer requires subject_offerings to exist.
    Topics are created before offerings, so we read directly from catalog.
    """
    
    return _fetch_one(
        engine,
        """
        SELECT 
            sc.*,
            sc.semester_id as semester_number,
            s.year_index,
            s.term_index,
            s.label as semester_label
        FROM subjects_catalog sc
        LEFT JOIN semesters s 
            ON sc.semester_id = s.id
        WHERE sc.subject_code = :subj
          AND sc.degree_code = :deg
          AND sc.active = 1
        LIMIT 1
        """,
        {
            "subj": subject_code,
            "deg": degree_code
        }
    )

@st.cache_data(ttl=300)
def fetch_faculty_list(_engine: Engine) -> List[Dict]:
    """Fetch active faculty for dropdowns."""
    return _fetch_all(
        _engine,
        """
        SELECT id, name, email 
        FROM faculty_profiles 
        WHERE status = 'active' 
        ORDER BY name
        """,
    )

# ===========================================================================
# CRUD OPERATIONS
# ===========================================================================


def update_topic(engine: Engine, topic_id: int, data: Dict, actor: str) -> bool:
    """Update existing topic."""
    with engine.begin() as conn:
        _exec(
            conn,
            """
            UPDATE elective_topics
            SET topic_name          = :name,
                capacity            = :cap,
                owner_faculty_id    = :owner_id,
                owner_faculty_email = :owner_email,
                description         = :desc,
                prerequisites       = :prereq,
                learning_outcomes   = :lo,
                status              = :status,
                updated_at          = :now,
                last_updated_by     = :actor
            WHERE id = :id
            """,
            {
                "id": topic_id,
                **data,
                "now": datetime.now(),
                "actor": actor,
            },
        )
        return True


def publish_topic(engine: Engine, topic_id: int, actor: str) -> bool:
    """Publish a topic."""
    with engine.begin() as conn:
        _exec(
            conn,
            """
            UPDATE elective_topics
            SET status          = 'published',
                updated_at      = :now,
                last_updated_by = :actor
            WHERE id = :id
            """,
            {"id": topic_id, "now": datetime.now(), "actor": actor},
        )
        return True


def unpublish_topic(engine: Engine, topic_id: int, actor: str) -> bool:
    """Unpublish a topic (back to draft)."""
    result = _fetch_one(
        engine,
        """
        SELECT COUNT(*) AS cnt 
        FROM elective_student_selections s
        JOIN elective_topics t ON t.topic_code_ay = s.topic_code_ay
        WHERE t.id = :id
          AND s.status IN ('confirmed', 'waitlisted')
        """,
        {"id": topic_id},
    )

    if result and result["cnt"] > 0:
        raise ValueError("Cannot unpublish topic with confirmed/waitlisted students")

    with engine.begin() as conn:
        _exec(
            conn,
            """
            UPDATE elective_topics
            SET status          = 'draft',
                updated_at      = :now,
                last_updated_by = :actor
            WHERE id = :id
            """,
            {"id": topic_id, "now": datetime.now(), "actor": actor},
        )
        return True


def delete_topic(engine: Engine, topic_id: int) -> bool:
    """Delete topic (only if no students assigned)."""
    with engine.begin() as conn:
        _exec(conn, "DELETE FROM elective_topics WHERE id = :id", {"id": topic_id})
        return True


# ===========================================================================
# SELECTION WINDOW OPERATIONS
# ===========================================================================


def get_selection_window(
    engine: Engine,
    subject_code: str,
    degree_code: str,
    ay_label: str,
    year: int,
    term: int,
) -> Optional[Dict]:
    """Get selection window for subject."""
    return _fetch_one(
        engine,
        """
        SELECT * FROM elective_selection_windows
        WHERE subject_code = :subj
          AND degree_code  = :deg
          AND ay_label     = :ay
          AND year         = :yr
          AND term         = :trm
        """,
        {
            "subj": subject_code,
            "deg": degree_code,
            "ay": ay_label,
            "yr": year,
            "trm": term,
        },
    )


def save_selection_window(engine: Engine, data: Dict) -> bool:
    """Save or update selection window."""
    with engine.begin() as conn:
        _exec(
            conn,
            """
            INSERT INTO elective_selection_windows (
                subject_code, degree_code, program_code, branch_code,
                ay_label, year, term, batch, division_code,
                start_datetime, end_datetime, timezone,
                auto_confirm_enabled, auto_confirm_order, min_satisfaction_percent,
                is_active, created_by
            ) VALUES (
                :subj, :deg, :prog, :br,
                :ay, :yr, :trm, :batch, :div,
                :start, :end, :tz,
                :auto, :order, :min_sat,
                :active, :actor
            )
            ON CONFLICT(subject_code, degree_code, program_code, branch_code,
                        ay_label, year, term, batch, division_code)
            DO UPDATE SET
                start_datetime           = excluded.start_datetime,
                end_datetime             = excluded.end_datetime,
                timezone                 = excluded.timezone,
                auto_confirm_enabled     = excluded.auto_confirm_enabled,
                auto_confirm_order       = excluded.auto_confirm_order,
                min_satisfaction_percent = excluded.min_satisfaction_percent,
                is_active                = excluded.is_active,
                updated_at               = CURRENT_TIMESTAMP,
                last_updated_by          = excluded.created_by
            """,
            data,
        )
        return True


# ===========================================================================
# DISPLAY HELPERS
# ===========================================================================

def display_syllabus_info(syllabus: Dict):
    """Display comprehensive syllabus information in an expandable section."""
    with st.expander("📋 Subject Syllabus & Assessment Details (Inherited by All Topics)", expanded=False):
        
        # Semester & Credits
        st.markdown("### 📚 Semester & Credits")
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Semester", syllabus.get('semester_number', 'N/A'))
            ltps = f"{syllabus.get('L', 0)}-{syllabus.get('T', 0)}-{syllabus.get('P', 0)}-{syllabus.get('S', 0)}"
            st.caption(f"**Workload (L/T/P/S):** {ltps}")
        
        with col2:
            total_credits = syllabus.get('credits_total', 0)
            st.metric("Total Credits", total_credits)
            st.caption("All elective topics inherit this credit value")
        
        # Workload Breakup
        if syllabus.get('workload_breakup_json'):
            try:
                workload = json.loads(syllabus['workload_breakup_json'])
                if workload and isinstance(workload, list):
                    st.markdown("#### ⏱️ Workload Breakup")
                    workload_df = pd.DataFrame(workload)
                    st.dataframe(workload_df, use_container_width=True, hide_index=True)
            except:
                pass
        
        st.divider()
        
        # Assessment (Max Marks)
        st.markdown("### 📊 Assessment (Max Marks)")
        col1, col2, col3, col4 = st.columns(4)
        
        internal_max = syllabus.get('internal_marks_max', 0)
        external_max = syllabus.get('exam_marks_max', 0)
        jury_max = syllabus.get('jury_viva_marks_max', 0)
        total_max = internal_max + external_max + jury_max
        
        with col1:
            st.metric("Maximum Internal Marks", internal_max)
        
        with col2:
            st.metric("Maximum External Marks (Exam)", external_max)
        
        with col3:
            st.metric("Maximum External Marks (Jury/Viva)", jury_max)
        
        with col4:
            st.metric("**TOTAL MARKS**", total_max, help="Sum of all assessment components")
        
        st.divider()
        
        # Passing Threshold
        st.markdown("### ✅ Passing Threshold")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            min_internal = syllabus.get('min_internal_percent', 50.0)
            st.metric("Minimum Internal Passing %", f"{min_internal}%")
            st.caption(f"Must score ≥ {min_internal}% of {internal_max} marks")
        
        with col2:
            min_external = syllabus.get('min_external_percent', 40.0)
            st.metric("Minimum External Passing %", f"{min_external}%")
            st.caption(f"Must score ≥ {min_external}% of {external_max + jury_max} marks")
        
        with col3:
            min_overall = syllabus.get('min_overall_percent', 40.0)
            st.metric("Minimum Overall Passing %", f"{min_overall}%")
            st.caption(f"Must score ≥ {min_overall}% of {total_max} marks")
        
        st.divider()
        
        # Attainment Requirements
        st.markdown("### 🎯 Attainment Requirements (optional)")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Direct Attainment")
            direct_source = syllabus.get('direct_source_mode', 'overall')
            st.write(f"**Source:** {direct_source.replace('_', ' ').title()}")
            
            if direct_source == 'overall':
                int_weight = syllabus.get('direct_internal_weight_percent', 40.0)
                ext_weight = syllabus.get('direct_external_weight_percent', 60.0)
                st.write(f"• Internal Weight: {int_weight}%")
                st.write(f"• External Weight: {ext_weight}%")
                st.caption(f"Combined: {int_weight}% × Internal + {ext_weight}% × External")
            else:
                int_thresh = syllabus.get('direct_internal_threshold_percent', 50.0)
                ext_thresh = syllabus.get('direct_external_threshold_percent', 40.0)
                st.write(f"• Internal Threshold: {int_thresh}%")
                st.write(f"• External Threshold: {ext_thresh}%")
            
            target_students = syllabus.get('direct_target_students_percent', 50.0)
            st.write(f"• Target: {target_students}% of students should attain")
        
        with col2:
            st.markdown("#### Overall Attainment")
            direct_w = syllabus.get('overall_direct_weight_percent', 80.0)
            indirect_w = syllabus.get('overall_indirect_weight_percent', 20.0)
            
            st.write(f"**Direct Weight:** {direct_w}%")
            st.write(f"**Indirect Weight:** {indirect_w}%")
            st.caption(f"Final = {direct_w}% × Direct + {indirect_w}% × Indirect")
            
            st.markdown("#### Indirect Attainment")
            indirect_target = syllabus.get('indirect_target_students_percent', 50.0)
            min_response = syllabus.get('indirect_min_response_rate_percent', 75.0)
            st.write(f"• Target: {indirect_target}% of students")
            st.write(f"• Min Response Rate: {min_response}%")
        
        st.info("ℹ️ These assessment and attainment settings are automatically inherited by all elective topics created under this subject.")


def display_policy_info(policy: Dict):
    """Display electives policy information."""
    with st.expander("⚙️ Effective Elective Policy (read-only)", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Selection & Allocation")
            mode_labels = {
                'topics_only': 'Topics only (B.Arch-style)',
                'subject_only': 'Subject only (no sub-topics)',
                'both': 'Both patterns allowed'
            }
            st.write(f"**Elective Mode:** {mode_labels.get(policy.get('elective_mode', 'topics_only'), policy.get('elective_mode'))}")
            
            alloc_labels = {
                'upload_only': 'Upload only (faculty assigns)',
                'rank_and_auto': 'Rank + auto allocation'
            }
            st.write(f"**Allocation Mode:** {alloc_labels.get(policy.get('allocation_mode', 'upload_only'), policy.get('allocation_mode'))}")
            
            max_choices = policy.get('max_choices_per_slot', 0)
            if max_choices > 0:
                st.write(f"**Max ranked choices per student:** {max_choices}")
            
            strategy_labels = {
                'manual': 'Manual per-topic capacity',
                'equal_split': 'Equal split (total ÷ topics)',
                'unlimited': 'Unlimited (no cap)'
            }
            st.write(f"**Capacity Strategy:** {strategy_labels.get(policy.get('default_topic_capacity_strategy', 'manual'), policy.get('default_topic_capacity_strategy'))}")
        
        with col2:
            st.markdown("#### Flexibility & Constraints")
            if policy.get('cross_batch_allowed'):
                st.success("✅ **Vertical studios allowed** (cross-batch)")
            else:
                st.info("🔒 Cross-batch mixing: **Not allowed**")
            
            if policy.get('cross_branch_allowed'):
                st.success("✅ **Cross-branch topics allowed**")
            else:
                st.info("🔒 Cross-branch mixing: **Not allowed**")
            
            if policy.get('uses_timetable_clash_check'):
                st.info("⏰ **Timetable clash checks:** Enabled")
            else:
                st.caption("⏰ Timetable clash checks: Disabled")
        
        if policy.get('notes'):
            st.markdown("#### Notes")
            st.caption(policy['notes'])
        
        st.caption(f"Policy scope: **{policy.get('scope_level', 'degree')}** level")


# ===========================================================================
# MAIN RENDER FUNCTION
# ===========================================================================


def render():
    """Main render function."""

    # Initialize
    settings = load_settings()
    engine = get_engine(settings.db.url)

    # Install schema if needed
    try:
        install_electives_schema(engine)
    except Exception as e:
        st.warning(f"Schema initialization: {e}")

    # Auth check
    require_page(PAGE_TITLE)
    actor = st.session_state.get("user_email", "system")
    current_user_roles = user_roles()
    CAN_EDIT = can_edit_page(PAGE_TITLE, current_user_roles)

    # Header
    st.title(PAGE_TITLE)
    st.caption("Manage elective topics, student selections, and allocation")

    # Context selection
    try:
        st.subheader("🔍 Context Selection")

        degrees = fetch_degrees(engine)
        if not degrees:
            st.error("No degrees found. Please create degrees first.")
            st.info("Navigate to the 'Degrees' management page to create them.")
            st.stop()

        # ROW 1: Degree + Academic Year
        col_deg, col_ay = st.columns([2, 1])

        with col_deg:
            degree_code = st.selectbox(
                "Degree*",
                options=[d["code"] for d in degrees],
                format_func=lambda x: next(
                    d["title"] for d in degrees if d["code"] == x
                ),
            )

        with col_ay:
            academic_years = fetch_academic_years(engine)
            if not academic_years:
                st.error("No usable academic years (open/active/planned) found.")
                st.stop()
            ay_label = st.selectbox("Academic Year*", options=academic_years)

        # ROW 2: Program / Branch / Year / Term
        col_prog, col_branch, col_year, col_term = st.columns([2, 2, 1, 1])

        with col_prog:
            programs = fetch_programs(engine, degree_code)
            program_code = None
            if programs:
                program_code = st.selectbox(
                    "Program",
                    options=[None] + [p["program_code"] for p in programs],
                    format_func=lambda x: "All"
                    if x is None
                    else next(
                        p["program_name"]
                        for p in programs
                        if p["program_code"] == x
                    ),
                )

        with col_branch:
            branches = fetch_branches(engine, degree_code, program_code)
            branch_code = None
            if branches:
                branch_code = st.selectbox(
                    "Branch",
                    options=[None] + [b["branch_code"] for b in branches],
                    format_func=lambda x: "All"
                    if x is None
                    else next(
                        b["branch_name"]
                        for b in branches
                        if b["branch_code"] == x
                    ),
                )

        with col_year:
            year = st.number_input("Year*", min_value=1, max_value=5, value=3)

        with col_term:
            term = st.number_input("Term*", min_value=1, max_value=2, value=1)

        # Get elective subjects
        elective_subjects = et_queries.fetch_elective_subjects(
            engine,
            degree_code,
            ay_label,
            year,
            term,
            program_code,
            branch_code,
        )

        if not elective_subjects:
            st.info("No elective/college project subjects found for this context.")
            st.caption(
                "Check that the subject is in **Subjects Catalog** with type "
                "'Elective' or 'College Project', has a **published** offering "
                f"for {ay_label}, Year {year}, Term {term}, and that a main "
                "syllabus has been created in the Subjects Syllabus module."
            )
            st.stop()

        subject_code = st.selectbox(
            "Select Elective/CP Subject*",
            options=[s["subject_code"] for s in elective_subjects],
            format_func=lambda x: f"{x} - {next(s['subject_name'] for s in elective_subjects if s['subject_code'] == x)}",
        )

        subject = next(s for s in elective_subjects if s["subject_code"] == subject_code)
        
        # Load effective electives policy for this scope (if available)
        policy = _load_effective_policy(
            engine=engine,
            degree_code=degree_code,
            program_code=program_code,
            branch_code=branch_code,
        )
        
        # Display policy info if available
        if policy:
            display_policy_info(policy)
        
        # Get syllabus details
        syllabus = get_syllabus_details(
            engine, subject_code, degree_code, ay_label, year, term
        )
        
        # Display syllabus info if available
        if syllabus:
            display_syllabus_info(syllabus)

    except OperationalError as e:
        st.error(f"Database Error: {e}")
        st.info(
            "It looks like some core tables are missing. "
            "Please navigate to the setup pages first."
        )
        st.stop()
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        st.exception(e)
        st.stop()

    st.markdown("---")

    # Tabs
    tabs = st.tabs(
        [
            "📋 Topics",
            "👥 Student Selections",
            "📊 Capacity View",
            "🎯 Final Assignments",
            "⚙️ Settings",
            "📥 Import/Export",
            "📜 Audit",
        ]
    )

    # Topics for this subject/context
    topics = fetch_topics_for_subject(
        engine, subject_code, ay_label, year, term, degree_code
    )
    
    # Fetch faculty list for dropdowns
    faculty_list = fetch_faculty_list(engine)

    # TAB 1: TOPICS
    with tabs[0]:
        render_topics_tab(
            engine,
            subject,
            syllabus,
            topics,
            faculty_list,
            degree_code,
            program_code,
            branch_code,
            ay_label,
            year,
            term,
            actor,
            CAN_EDIT,
            current_user_roles,
            policy,
        )

    # TAB 2: STUDENT SELECTIONS
    with tabs[1]:
        render_selections_tab(
            engine,
            subject_code,
            ay_label,
            year,
            term,
            degree_code,
            program_code,
            branch_code,
            policy,
        )

    # TAB 3: CAPACITY VIEW
    with tabs[2]:
        render_capacity_tab(topics)

    # TAB 4: FINAL ASSIGNMENTS
    with tabs[3]:
        render_assignments_tab(engine, subject_code, ay_label, year, term, degree_code)

    # TAB 5: SETTINGS
    with tabs[4]:
        render_settings_tab(
            engine,
            subject_code,
            degree_code,
            program_code,
            branch_code,
            ay_label,
            year,
            term,
            actor,
            CAN_EDIT,
            policy,
        )

    # TAB 6: IMPORT/EXPORT
    with tabs[5]:
        render_import_export_tab(
            engine,
            subject,
            subject_code,
            degree_code,
            program_code,
            branch_code,
            ay_label,
            year,
            term,
            actor,
            CAN_EDIT,
        )

    # TAB 7: AUDIT
    with tabs[6]:
        render_audit_tab(engine)


# ===========================================================================
# TAB RENDERING FUNCTIONS
# ===========================================================================


def render_topics_tab(
    engine,
    subject,
    syllabus,
    topics,
    faculty_list,
    degree_code,
    program_code,
    branch_code,
    ay_label,
    year,
    term,
    actor,
    CAN_EDIT,
    current_user_roles,
    policy,
):
    """Render topics management tab."""

    st.subheader(f"📋 Topics for {subject['subject_name']}")
    
    # --- Faculty Dropdown Helper ---
    # Create a mapping of {faculty_id: "Name (email)"} for the selectbox
    # Add a "None" option
    faculty_options = {f["id"]: f for f in faculty_list}
    faculty_display_options = {
        f_id: f"{f['name']} ({f['email']})" for f_id, f in faculty_options.items()
    }
    faculty_display_list = [None] + list(faculty_options.keys())
    
    def format_faculty(f_id):
        if f_id is None:
            return "--- Select Faculty (Optional) ---"
        return faculty_display_options.get(f_id, "Unknown Faculty")
    # --- End Helper ---
    
    # Show syllabus inheritance notice
    if syllabus:
        internal_max = syllabus.get('internal_marks_max', 0)
        external_max = syllabus.get('exam_marks_max', 0)
        jury_max = syllabus.get('jury_viva_marks_max', 0)
        total_max = internal_max + external_max + jury_max
        
        st.info(
            f"ℹ️ All topics inherit: **{syllabus.get('credits_total', 'N/A')} credits**, "
            f"**{total_max} total marks** ({internal_max} internal + {external_max} external + {jury_max} jury/viva), "
            f"**Pass: {syllabus.get('min_overall_percent', 40)}%** overall, "
            f"{syllabus.get('min_internal_percent', 50)}% internal, "
            f"{syllabus.get('min_external_percent', 40)}% external"
        )

    if topics:
        # Check for duplicate topic names
        topic_names = [t['topic_name'] for t in topics]
        duplicates = [name for name in topic_names if topic_names.count(name) > 1]
        if duplicates:
            st.warning(f"⚠️ Duplicate topic names found: {', '.join(set(duplicates))}. "
                      "Each topic should have a unique name!")
        
        for topic in topics:
            with st.expander(
                f"#{topic['topic_no']} - {topic['topic_name']} "
                f"[{topic['status'].upper()}]",
                expanded=False,
            ):
                col1, col2, col3 = st.columns(3)

                with col1:
                    st.write(f"**Code:** {topic['topic_code_ay']}")
                    st.write(
                        f"**Capacity:** {topic['max_capacity']} "
                        f"({topic['remaining_capacity']} remaining)"
                    )

                with col2:
                    st.write(f"**Confirmed:** {topic['confirmed_count']}")
                    st.write(f"**Waitlisted:** {topic['waitlisted_count']}")

                with col3:
                    st.write(f"**Status:** {topic['status']}")
                    if topic.get("owner_faculty_email"):
                        faculty_name = "Unknown"
                        if topic.get("owner_faculty_id") in faculty_options:
                             faculty_name = faculty_options[topic["owner_faculty_id"]]["name"]
                        st.write(f"**Faculty:** {faculty_name}")

                if topic.get("description"):
                    st.markdown("**Description:**")
                    st.write(topic["description"])
                
                # Show inherited syllabus settings in compact form
                if syllabus:
                    with st.expander("📊 Assessment & Credits (Inherited)", expanded=False):
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.caption("**Credits & Workload**")
                            st.write(f"• Total Credits: {syllabus.get('credits_total', 'N/A')}")
                            ltps = f"{syllabus.get('L', 0)}-{syllabus.get('T', 0)}-{syllabus.get('P', 0)}-{syllabus.get('S', 0)}"
                            st.write(f"• L-T-P-S: {ltps}")
                        
                        with col2:
                            st.caption("**Marks Distribution**")
                            st.write(f"• Internal: {syllabus.get('internal_marks_max', 0)}")
                            st.write(f"• External (Exam): {syllabus.get('exam_marks_max', 0)}")
                            if syllabus.get('jury_viva_marks_max', 0) > 0:
                                st.write(f"• Jury/Viva: {syllabus['jury_viva_marks_max']}")
                            total = (syllabus.get('internal_marks_max', 0) + 
                                    syllabus.get('exam_marks_max', 0) +
                                    syllabus.get('jury_viva_marks_max', 0))
                            st.write(f"• **Total: {total}**")
                        
                        with col3:
                            st.caption("**Pass Criteria**")
                            st.write(f"• Overall: {syllabus.get('min_overall_percent', 40)}%")
                            st.write(f"• Internal: {syllabus.get('min_internal_percent', 50)}%")
                            st.write(f"• External: {syllabus.get('min_external_percent', 40)}%")

                # Actions
                if CAN_EDIT:
                    col1, col2, col3, col4, col5 = st.columns(5)

                    with col1:
                        if (
                            topic["status"] == "draft"
                            and st.button("✅ Publish", key=f"pub_{topic['id']}")
                        ):
                            try:
                                publish_topic(engine, topic["id"], actor)
                                st.success("Published!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {e}")

                    with col2:
                        if (
                            topic["status"] == "published"
                            and st.button("📝 Unpublish", key=f"unpub_{topic['id']}")
                        ):
                            try:
                                unpublish_topic(engine, topic["id"], actor)
                                st.success("Unpublished!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {e}")

                    with col3:
                        if st.button("✏️ Edit", key=f"edit_{topic['id']}"):
                            st.session_state[f"editing_{topic['id']}"] = True
                            st.rerun()

                    with col4:
                        if st.button("🗑️ Delete", key=f"del_{topic['id']}"):
                            st.session_state[f"confirm_delete_{topic['id']}"] = True
                            st.rerun()

                    with col5:
                        if st.button(
                            "🔄 Refresh Capacity", key=f"cap_{topic['id']}"
                        ):
                            try:
                                refresh_capacity_tracking(
                                    engine,
                                    topic["topic_code_ay"],
                                    topic["ay_label"],
                                )
                                st.success("Capacity refreshed!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {e}")

                # Delete confirmation
                if st.session_state.get(f"confirm_delete_{topic['id']}", False):
                    st.warning(f"⚠️ Are you sure you want to delete topic #{topic['topic_no']} - {topic['topic_name']}?")
                    col_yes, col_no = st.columns(2)
                    with col_yes:
                        if st.button("✅ Yes, Delete", key=f"yes_del_{topic['id']}", type="primary"):
                            try:
                                delete_topic(engine, topic["id"])
                                st.success("Deleted!")
                                del st.session_state[f"confirm_delete_{topic['id']}"]
                                st.cache_data.clear()
                                st.rerun()
                            except Exception as e:
                                st.error(f"Cannot delete: {e}")
                    with col_no:
                        if st.button("❌ Cancel", key=f"no_del_{topic['id']}"):
                            del st.session_state[f"confirm_delete_{topic['id']}"]
                            st.rerun()

                # Edit form
                if st.session_state.get(f'editing_{topic["id"]}', False):
                    with st.form(f"edit_form_{topic['id']}"):
                        st.markdown("### Edit Topic")

                        name = st.text_input("Topic Name*", value=topic["topic_name"])
                        
                        # --- Find default index for faculty dropdown ---
                        default_faculty_id = topic.get("owner_faculty_id")
                        default_index = 0
                        if default_faculty_id in faculty_display_list:
                            default_index = faculty_display_list.index(default_faculty_id)
                        # ---
                        
                        owner_id = st.selectbox(
                            "Faculty Owner",
                            options=faculty_display_list,
                            format_func=format_faculty,
                            index=default_index
                        )
                        
                        capacity = st.number_input(
                            "Capacity", value=topic["max_capacity"], min_value=0
                        )
                        desc = st.text_area(
                            "Description", value=topic.get("description", "")
                        )

                        col1, col2 = st.columns(2)
                        with col1:
                            if st.form_submit_button("💾 Save"):
                                # Check for duplicate names (excluding current topic)
                                existing_names = [t['topic_name'] for t in topics if t['id'] != topic['id']]
                                if name in existing_names:
                                    st.error(f"❌ Topic name '{name}' already exists! Each topic must have a unique name.")
                                elif not name.strip():
                                    st.error("❌ Topic name cannot be empty!")
                                else:
                                    try:
                                        # Get email from selected ID
                                        owner_email = None
                                        if owner_id in faculty_options:
                                            owner_email = faculty_options[owner_id]["email"]
                                        
                                        update_topic(
                                            engine,
                                            topic["id"],
                                            {
                                                "name": name,
                                                "cap": capacity,
                                                "owner_id": owner_id,
                                                "owner_email": owner_email,
                                                "desc": desc,
                                                "prereq": topic.get("prerequisites"),
                                                "lo": topic.get("learning_outcomes"),
                                                "status": topic["status"],
                                            },
                                            actor,
                                        )
                                        st.success("Updated!")
                                        del st.session_state[f'editing_{topic["id"]}']
                                        st.cache_data.clear()
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Error: {e}")

                        with col2:
                            if st.form_submit_button("❌ Cancel"):
                                del st.session_state[f'editing_{topic["id"]}']
                                st.rerun()
    else:
        st.info("No topics created yet for this subject.")

    # Create new topic
    if CAN_EDIT:
        st.markdown("---")
        st.subheader("➕ Create New Topic")

        with st.form("create_topic_form"):
            col1, col2 = st.columns(2)

            with col1:
                next_topic_no = len(topics) + 1
                topic_no = st.number_input("Topic No.", min_value=1, value=next_topic_no)
                topic_name = st.text_input("Topic Name*")

            with col2:
                capacity = st.number_input(
                    "Capacity (0 = unlimited)", min_value=0, value=30
                )
                owner_id = st.selectbox(
                    "Faculty Owner",
                    options=faculty_display_list,
                    format_func=format_faculty,
                    index=0
                )

            description = st.text_area("Description")

            submitted = st.form_submit_button("Create Topic", type="primary")

            if submitted:
                if not topic_name:
                    st.error("Topic name is required")
                else:
                    # Check for duplicate topic names
                    existing_names = [t['topic_name'] for t in topics]
                    if topic_name in existing_names:
                        st.error(f"❌ Topic name '{topic_name}' already exists! Each topic must have a unique name.")
                    else:
                        try:
                            # Get email from selected ID
                            owner_email = None
                            if owner_id in faculty_options:
                                owner_email = faculty_options[owner_id]["email"]
                            
                            topic_data = {
                                "subject_code": subject["subject_code"],
                                "subj_name": subject["subject_name"],
                                "deg": degree_code,
                                "prog": program_code,
                                "br": branch_code,
                                "ay": ay_label,
                                "yr": year,
                                "trm": term,
                                "div": None,
                                "topic_no": topic_no,
                                "topic_name": topic_name,
                                "owner_id": owner_id,
                                "owner_email": owner_email,
                                "cap": capacity,
                                "desc": description,
                                "prereq": None,
                                "lo": None,
                                "status": "draft",
                            }

                            can_override = any(role in current_user_roles for role in ['superadmin', 'academic_admin'])

                            topic_id = et_queries.create_topic_with_validation(
                                engine, 
                                topic_data, 
                                actor,
                                enforce_timing=not can_override,
                                require_offering=False
                            )

                            st.success(f"✅ Topic created! ID: {topic_id}")
                            st.cache_data.clear()
                            st.rerun()

                        except Exception as e:
                            st.error(f"Error: {e}")


def render_selections_tab(
    engine,
    subject_code,
    ay_label,
    year,
    term,
    degree_code,
    program_code,
    branch_code,
    policy,
):
    """Render student selections tab."""

    st.subheader("👥 Student Selections")

    # Get selection window
    window = get_selection_window(
        engine, subject_code, degree_code, ay_label, year, term
    )

    if not window:
        st.info("No selection window configured. Configure in Settings tab.")
        return

    # Check window status
    now = datetime.now()
    start = datetime.fromisoformat(window["start_datetime"])
    end = datetime.fromisoformat(window["end_datetime"])

    if now < start:
        st.info(f"Selection window opens on {start.strftime('%Y-%m-%d %H:%M')}")
    elif now > end:
        st.warning(f"Selection window closed on {end.strftime('%Y-%m-%d %H:%M')}")
    else:
        st.success(f"Selection window is OPEN until {end.strftime('%Y-%m-%d %H:%M')}")

    # Get selections
    selections = _fetch_all(
        engine,
        """
        SELECT * FROM v_student_selections_detail
        WHERE subject_code = :subj
        AND ay_label = :ay
        ORDER BY status, student_roll_no
    """,
        {"subj": subject_code, "ay": ay_label},
    )

    if selections:
        df = pd.DataFrame(selections)

        # Filter by status
        status_filter = st.multiselect(
            "Filter by Status",
            options=["draft", "confirmed", "waitlisted", "withdrawn"],
            default=["draft", "confirmed", "waitlisted"],
        )

        filtered_df = df[df["status"].isin(status_filter)]

        st.dataframe(
            filtered_df[
                [
                    "student_roll_no",
                    "student_name",
                    "topic_name",
                    "rank_choice",
                    "status",
                    "confirmed_at",
                ]
            ],
            use_container_width=True,
        )

        # Run allocation
        if window["auto_confirm_enabled"] and allocation_engine:
            st.markdown("---")
            st.subheader("Allocation Engine")
            st.caption("Run allocation to assign students to topics based on preferences.")
            
            # Derive strategy & satisfaction target from policy (if any)
            strategy = None
            min_satisfaction = 50.0
            
            if policy:
                # Determine strategy from policy
                alloc_mode = policy.get("allocation_mode")
                if alloc_mode == "rank_and_auto":
                    strategy = ["student_select_ranked"]
                elif alloc_mode == "upload_only":
                    strategy = ["manual_assign"]
                else:
                    strategy = ["student_select_ranked"]  # Default
                
                # Get satisfaction target (not directly in policy, use default)
                min_satisfaction = 50.0
            
            if st.button("▶️ Run Allocation Now", type="primary"):
                try:
                    with st.spinner("Running allocation..."):
                        result = allocation_engine.trigger_allocation(
                            engine=engine,
                            subject_code=subject_code,
                            ay_label=ay_label,
                            year=year,
                            term=term,
                            degree_code=degree_code,
                            strategy=strategy,
                            min_satisfaction=min_satisfaction,
                        )

                    st.success("✅ Allocation Complete!")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Students Assigned", result["students_assigned"])
                    with col2:
                        st.metric("Students Waitlisted", result["students_waitlisted"])
                    with col3:
                        st.metric(
                            "Top Choice Satisfaction",
                            f"{result['top_choice_satisfaction']:.1f}%",
                        )
                    st.rerun()

                except Exception as e:
                    st.error(f"Allocation failed: {e}")
                    st.exception(e)
    else:
        st.info("No student selections yet")


def render_capacity_tab(topics):
    """Render capacity overview tab."""

    st.subheader("📊 Capacity Overview")

    if topics:
        col1, col2, col3, col4 = st.columns(4)

        total_topics = len(topics)
        total_capacity = sum(
            t["max_capacity"] for t in topics if t["max_capacity"] > 0
        )
        total_confirmed = sum(t["confirmed_count"] for t in topics)
        total_waitlisted = sum(t["waitlisted_count"] for t in topics)

        with col1:
            st.metric("Total Topics", total_topics)
        with col2:
            st.metric(
                "Total Capacity", total_capacity if total_capacity > 0 else "Unlimited"
            )
        with col3:
            st.metric("Confirmed", total_confirmed)
        with col4:
            st.metric("Waitlisted", total_waitlisted)

        # Chart
        st.markdown("---")
        capacity_df = pd.DataFrame(topics)

        chart_data = capacity_df[
            [
                "topic_name",
                "max_capacity",
                "confirmed_count",
                "remaining_capacity",
            ]
        ].copy()
        chart_data = chart_data[chart_data["max_capacity"] > 0]

        if not chart_data.empty:
            st.bar_chart(
                chart_data.set_index("topic_name")[
                    ["confirmed_count", "remaining_capacity"]
                ],
                use_container_width=True,
            )
        else:
            st.info("All topics have unlimited capacity")
    else:
        st.info("No topics available")


def render_assignments_tab(engine, subject_code, ay_label, year, term, degree_code):
    """Render final assignments tab."""

    st.subheader("🎯 Final Assignments")

    selections = _fetch_all(
        engine,
        """
        SELECT * FROM v_student_selections_detail
        WHERE subject_code = :subj
        AND ay_label = :ay
        AND status IN ('confirmed', 'waitlisted')
        ORDER BY topic_name, student_roll_no
    """,
        {
            "subj": subject_code,
            "ay": ay_label,
        },
    )

    if selections:
        df = pd.DataFrame(selections)
        display_cols = [
            "student_roll_no",
            "student_name",
            "topic_name",
            "status",
            "rank_choice",
            "confirmed_at",
        ]
        st.dataframe(df[display_cols], use_container_width=True)

        # Export
        csv = df.to_csv(index=False)
        st.download_button(
            "📥 Download CSV",
            csv,
            file_name=f"elective_assignments_{subject_code}_{ay_label}.csv",
            mime="text/csv",
        )
    else:
        st.info("No confirmed assignments yet")


def render_settings_tab(
    engine,
    subject_code,
    degree_code,
    program_code,
    branch_code,
    ay_label,
    year,
    term,
    actor,
    CAN_EDIT,
    policy,
):
    """Render settings tab."""

    st.subheader("⚙️ Selection Window Settings")

    if not CAN_EDIT:
        st.warning("You don't have permission to edit settings")
        return

    window = get_selection_window(
        engine, subject_code, degree_code, ay_label, year, term
    )

    # Suggest batches for this degree / AY / year
    suggested_batches = _suggest_batches_for_window(
        engine,
        degree_code=degree_code,
        ay_label=ay_label,
        year=year,
    )

    existing_batch = None
    if window and "batch" in window:
        existing_batch = window["batch"]

    # Batch selection UI
    batch_value = None
    
    if suggested_batches:
        base_label_all = "(All batches for this year)"
        base_label_custom = "Custom…"
        options = [base_label_all] + suggested_batches + [base_label_custom]

        if existing_batch and existing_batch in suggested_batches:
            default_index = 1 + suggested_batches.index(existing_batch)
        elif existing_batch and existing_batch not in suggested_batches:
            default_index = len(options) - 1
        else:
            default_index = 0

        batch_choice = st.selectbox(
            "Batch for this window",
            options=options,
            index=default_index,
            help="If left as 'All batches', the window is visible to all batches in this degree/year.",
        )

        if batch_choice == base_label_all:
            batch_value = None
        elif batch_choice == base_label_custom:
            batch_value = st.text_input(
                "Custom batch (e.g. 2021)",
                value=existing_batch or "",
            ) or None
        else:
            batch_value = batch_choice
    else:
        batch_value = st.text_input(
            "Batch (optional – e.g. 2021; leave blank for all batches)",
            value=existing_batch or "",
        ) or None

    # Form for window configuration
    with st.form("window_config_form"):
        col1, col2 = st.columns(2)

        with col1:
            start_date = st.date_input(
                "Start Date",
                value=datetime.fromisoformat(window["start_datetime"]).date()
                if window
                else datetime.now().date(),
            )
            start_time = st.time_input(
                "Start Time",
                value=datetime.fromisoformat(window["start_datetime"]).time()
                if window
                else datetime.now().time(),
            )

        with col2:
            end_date = st.date_input(
                "End Date",
                value=datetime.fromisoformat(window["end_datetime"]).date()
                if window
                else (datetime.now() + timedelta(days=14)).date(),
            )
            end_time = st.time_input(
                "End Time",
                value=datetime.fromisoformat(window["end_datetime"]).time()
                if window
                else datetime.now().time(),
            )

        st.markdown("---")

        # Use policy defaults if available
        default_auto = True
        default_min_sat = 50
        
        if policy:
            alloc_mode = policy.get("allocation_mode")
            default_auto = (alloc_mode == "rank_and_auto")
        
        auto_confirm = st.checkbox(
            "Enable Auto-Confirmation",
            value=window["auto_confirm_enabled"] if window else default_auto,
        )

        if auto_confirm:
            min_satisfaction = st.slider(
                "Minimum Top Choice Satisfaction %",
                min_value=0,
                max_value=100,
                value=int(window["min_satisfaction_percent"])
                if window
                else default_min_sat,
            )
        else:
            min_satisfaction = window["min_satisfaction_percent"] if window else None

        is_active = st.checkbox(
            "Window Active", value=window["is_active"] if window else True
        )

        if st.form_submit_button("💾 Save Settings", type="primary"):
            try:
                start_datetime = datetime.combine(start_date, start_time)
                end_datetime = datetime.combine(end_date, end_time)

                if start_datetime >= end_datetime:
                    st.error("End datetime must be after start datetime")
                else:
                    # Build strategy from policy
                    strategy_list = ["student_select_ranked"]
                    if policy:
                        alloc_mode = policy.get("allocation_mode")
                        if alloc_mode == "upload_only":
                            strategy_list = ["manual_assign"]
                    
                    save_selection_window(
                        engine,
                        {
                            "subj": subject_code,
                            "deg": degree_code,
                            "prog": program_code,
                            "br": branch_code,
                            "ay": ay_label,
                            "yr": year,
                            "trm": term,
                            "batch": batch_value,
                            "div": None,
                            "start": start_datetime.isoformat(),
                            "end": end_datetime.isoformat(),
                            "tz": "Asia/Kolkata",
                            "auto": 1 if auto_confirm else 0,
                            "order": json.dumps(strategy_list),
                            "min_sat": min_satisfaction,
                            "active": 1 if is_active else 0,
                            "actor": actor,
                        },
                    )

                    st.success("✅ Settings saved!")
                    st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")


def render_import_export_tab(
    engine: Engine,
    subject: Dict[str, Any],
    subject_code: str,
    degree_code: str,
    program_code: Optional[str],
    branch_code: Optional[str],
    ay_label: str,
    year: int,
    term: int,
    actor: str,
    CAN_EDIT: bool,
):
    """Render import/export tab."""

    st.subheader("📥 Import/Export")

    if import_export is None:
        st.warning("Import/Export module not available.")
        return

    # Topics Import / Export
    st.markdown("### 📚 Topics Import / Export")

    col_tpl, col_up = st.columns(2)

    with col_tpl:
        st.caption("Download a CSV template for elective topics.")
        if st.button("📄 Download Topics Template"):
            df_tpl = import_export.get_topics_import_template()
            csv = df_tpl.to_csv(index=False)
            st.download_button(
                "Download Topics Template CSV",
                csv,
                file_name="topics_import_template.csv",
                mime="text/csv",
            )

    with col_up:
        st.caption("Upload a CSV to create/update elective topics.")
        uploaded_topics = st.file_uploader(
            "Upload Topics CSV", type=["csv"], key="topics_csv_uploader"
        )

        if uploaded_topics is not None:
            try:
                df_topics = pd.read_csv(uploaded_topics)

                is_valid, errors = import_export.validate_topics_csv(df_topics)
                if not is_valid:
                    st.error("Validation failed:")
                    for err in errors:
                        st.error(f"• {err}")
                else:
                    st.success("Validation passed. Preview below:")
                    st.dataframe(df_topics, use_container_width=True, hide_index=True)

                    if CAN_EDIT and st.button("✅ Import Topics"):
                        ok, bad, errs = import_export.import_topics_from_csv(
                            engine=engine,
                            df=df_topics,
                            subject_code=subject_code,
                            degree_code=degree_code,
                            program_code=program_code,
                            branch_code=branch_code,
                            ay_label=ay_label,
                            year=year,
                            term=term,
                            actor=actor,
                            overwrite_existing=False,
                        )
                        st.success(
                            f"Imported {ok} topics, {bad} errors."
                        )
                        if errs:
                            with st.expander("Show topic import errors"):
                                for e in errs:
                                    st.error(f"• {e}")
            except Exception as e:
                st.error(f"Error reading topics CSV: {e}")

    # Export topics / assignments
    st.markdown("### 📤 Export Topics & Assignments")

    col_et, col_ea, col_roster = st.columns(3)

    with col_et:
        if st.button("📥 Export Topics"):
            df = import_export.export_topics_to_csv(
                engine, subject_code, ay_label, year, term, degree_code
            )
            if df.empty:
                st.info("No topics found for this context.")
            else:
                csv = df.to_csv(index=False)
                st.download_button(
                    "Download Topics CSV",
                    csv,
                    file_name=f"topics_{subject_code}_{ay_label}.csv",
                    mime="text/csv",
                )

    with col_ea:
        if st.button("📥 Export Confirmed Assignments"):
            df = import_export.export_selections_to_csv(
                engine,
                subject_code,
                ay_label,
                year,
                term,
                degree_code,
                status_filter="confirmed",
            )
            if df.empty:
                st.info("No confirmed assignments yet. Run allocation first.")
            else:
                csv = df.to_csv(index=False)
                st.download_button(
                    "Download Assignments CSV",
                    csv,
                    file_name=f"assignments_{subject_code}_{ay_label}.csv",
                    mime="text/csv",
                )

    with col_roster:
        if st.button("📥 Export Per-Topic Rosters"):
            df = import_export.export_topic_rosters_to_csv(
                engine,
                subject_code,
                ay_label,
                year,
                term,
                degree_code,
            )
            if df.empty:
                st.info("No confirmed students yet for any topic.")
            else:
                csv = df.to_csv(index=False)
                st.download_button(
                    "Download Topic Rosters CSV",
                    csv,
                    file_name=f"topic_rosters_{subject_code}_{ay_label}.csv",
                    mime="text/csv",
                )

    # Bulk Student Selections + Allocation
    st.markdown("### 👥 Bulk Student Selections & Allocation")

    if allocation_engine is None:
        st.info(
            "Allocation engine module not available. "
            "Bulk allocation cannot be triggered from here."
        )
        return

    col_tpl2, col_up2 = st.columns(2)

    with col_tpl2:
        st.caption("Download a CSV template for student elective choices.")
        if st.button("📄 Download Selections Template"):
            df_tpl_sel = import_export.get_selections_import_template()
            csv = df_tpl_sel.to_csv(index=False)
            st.download_button(
                "Download Selections Template CSV",
                csv,
                file_name="selections_import_template.csv",
                mime="text/csv",
            )

    with col_up2:
        st.caption(
            "Upload a CSV with student_roll_no, topic_code_ay, rank_choice, status."
        )
        uploaded_sel = st.file_uploader(
            "Upload Student Selections CSV",
            type=["csv"],
            key="selections_csv_uploader",
        )

    if uploaded_sel is not None:
        try:
            df_sel = pd.read_csv(uploaded_sel)

            is_valid, errors = import_export.validate_selections_csv(
                df_sel, engine, subject_code, ay_label
            )

            if not is_valid:
                st.error("Validation failed:")
                for err in errors:
                    st.error(f"• {err}")
                return

            st.success("Selections CSV validated. Preview below:")
            st.dataframe(df_sel, use_container_width=True, hide_index=True)

            if CAN_EDIT and st.button("✅ Import Selections & Run Allocation"):
                ok, bad, errs = import_export.import_selections_from_csv(
                    engine=engine,
                    df=df_sel,
                    subject_code=subject_code,
                    degree_code=degree_code,
                    ay_label=ay_label,
                    year=year,
                    term=term,
                    actor=actor,
                )

                st.success(f"Imported {ok} selections, {bad} errors.")
                if errs:
                    with st.expander("Show selection import errors"):
                        for e in errs:
                            st.error(f"• {e}")

                try:
                    result = allocation_engine.trigger_allocation(
                        engine=engine,
                        subject_code=subject_code,
                        ay_label=ay_label,
                        year=year,
                        term=term,
                        degree_code=degree_code,
                        strategy=["student_select_ranked"],
                        min_satisfaction=50.0,
                    )

                    st.success("Allocation completed.")
                    with st.expander("Allocation Summary", expanded=True):
                        st.write(
                            {
                                "run_id": result.get("run_id"),
                                "students_assigned": result.get("students_assigned"),
                                "students_waitlisted": result.get("students_waitlisted"),
                                "top_choice_satisfaction": result.get(
                                    "top_choice_satisfaction"
                                ),
                                "message": result.get("message"),
                            }
                        )

                    df_final = import_export.export_selections_to_csv(
                        engine,
                        subject_code,
                        ay_label,
                        year,
                        term,
                        degree_code,
                        status_filter="confirmed",
                    )
                    if not df_final.empty:
                        csv_final = df_final.to_csv(index=False)
                        st.download_button(
                            "⬇️ Download Final Confirmed Assignments CSV",
                            csv_final,
                            file_name=f"assignments_{subject_code}_{ay_label}.csv",
                            mime="text/csv",
                        )
                    else:
                        st.info(
                            "No confirmed assignments found after allocation."
                        )

                except Exception as e:
                    st.error(f"Error during allocation: {e}")

        except Exception as e:
            st.error(f"Error reading selections CSV: {e}")


def render_audit_tab(engine):
    """Render audit trail tab."""

    st.subheader("📜 Audit Trail")

    audit_type = st.radio("Audit Type", ["Topics", "Selections"], horizontal=True)

    if audit_type == "Topics":
        logs = _fetch_all(
            engine,
            """
            SELECT * FROM elective_topics_audit
            ORDER BY occurred_at DESC LIMIT 100
        """,
        )
    else:
        logs = _fetch_all(
            engine,
            """
            SELECT * FROM elective_selections_audit
            ORDER BY occurred_at DESC LIMIT 100
        """,
        )

    if logs:
        df = pd.DataFrame(logs)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No audit logs found")


# ===========================================================================
# ENTRY POINT
# ===========================================================================

if __name__ == "__main__":
    render()
else:
    render()
