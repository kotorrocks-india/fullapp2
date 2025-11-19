# screens/electives_topics/student_selection.py
"""
Student-facing elective selection interface.
Handles student preference submission with ranked choices.
UPDATED: Integrated with electives_policy for max choices and selection mode
"""

from __future__ import annotations
import streamlit as st
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

import logging
logger = logging.getLogger(__name__)

# Electives policy (optional)
try:
    from core import electives_policy as core_electives_policy
except Exception:
    core_electives_policy = None


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


# ===========================================================================
# STUDENT AUTHENTICATION & CONTEXT
# ===========================================================================

def get_student_from_session() -> Optional[Dict]:
    """Get current student from session."""
    # This should integrate with your actual auth system
    if 'student_roll_no' in st.session_state:
        return {
            'student_roll_no': st.session_state['student_roll_no'],
            'name': st.session_state.get('student_name', 'Student'),
            'email': st.session_state.get('student_email'),
            'degree_code': st.session_state.get('degree_code'),
            'program_code': st.session_state.get('program_code'),
            'branch_code': st.session_state.get('branch_code'),
            'current_year': st.session_state.get('current_year'),
            'division_code': st.session_state.get('division_code'),
            'batch': st.session_state.get('batch')
        }
    return None


def authenticate_student(engine: Engine, student_roll_no: str, password: str = None) -> Optional[Dict]:
    """
    Authenticate student and load their details.
    Simplified version - integrate with your actual auth.
    """
    student = _fetch_one(engine, """
        SELECT 
            sp.id,
            sp.student_id AS student_roll_no,
            sp.name,
            sp.email,
            se.degree_code,
            se.program_code,
            se.branch_code,
            se.current_year,
            se.batch,
            se.division_code
        FROM student_profiles sp
        JOIN student_enrollments se ON se.student_profile_id = sp.id
        WHERE sp.student_id = :roll
        AND sp.status = 'active'
        AND se.enrollment_status = 'active'
        AND se.is_primary = 1
    """, {"roll": student_roll_no})
    
    return student


# ===========================================================================
# POLICY HELPERS
# ===========================================================================

def _get_policy_for_window(engine: Engine, window: Dict) -> Optional[Dict]:
    """
    Load effective electives policy for a specific selection window.
    Uses window scope: degree / program / branch / AY / year / term / subject.
    """
    if core_electives_policy is None:
        return None
    
    try:
        raw_conn = engine.raw_connection()
        try:
            policy_obj = core_electives_policy.fetch_effective_policy(
                raw_conn,
                degree_code=window.get("degree_code"),
                program_code=window.get("program_code"),
                branch_code=window.get("branch_code"),
            )
            
            if policy_obj is None:
                return None
            
            # Convert to dict
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
        logger.warning("Elective policy lookup failed for window %s: %s", window.get("id"), e)
        return None


# ===========================================================================
# SELECTION WINDOW QUERIES
# ===========================================================================

def get_active_selection_windows(
    engine: Engine,
    degree_code: str,
    year: int,
    program_code: str = None,
    branch_code: str = None,
    batch: str = None,
) -> List[Dict]:
    """Get active selection windows for student's context."""
    return _fetch_all(engine, """
        SELECT 
            w.*,
            so.subject_name,
            so.subject_type,
            so.credits_total
        FROM elective_selection_windows w
        JOIN subject_offerings so 
            ON so.subject_code = w.subject_code
            AND so.ay_label = w.ay_label
            AND so.year = w.year
            AND so.term = w.term
        WHERE w.degree_code = :deg
          AND w.year        = :yr
          AND w.is_active   = 1
          AND w.manually_closed = 0
          AND datetime('now') BETWEEN datetime(w.start_datetime) AND datetime(w.end_datetime)
          AND so.subject_type IN ('Elective', 'College Project')
          AND so.status = 'published'
          AND (:prog  IS NULL OR w.program_code = :prog  OR w.program_code  IS NULL)
          AND (:br    IS NULL OR w.branch_code  = :br    OR w.branch_code   IS NULL)
          AND (:batch IS NULL OR w.batch        = :batch OR w.batch IS NULL)
        ORDER BY w.start_datetime
    """, {
        "deg": degree_code,
        "yr": year,
        "prog": program_code,
        "br": branch_code,
        "batch": batch,
    })


def get_window_time_remaining(window: Dict) -> str:
    """Get human-readable time remaining in window."""
    try:
        end_time = datetime.fromisoformat(window['end_datetime'])
        now = datetime.now()
        
        if now >= end_time:
            return "Closed"
        
        delta = end_time - now
        hours = delta.total_seconds() / 3600
        
        if hours < 1:
            minutes = int(delta.total_seconds() / 60)
            return f"{minutes} minutes remaining"
        elif hours < 24:
            return f"{int(hours)} hours remaining"
        else:
            days = int(hours / 24)
            return f"{days} days remaining"
    except:
        return "Unknown"


# ===========================================================================
# TOPIC QUERIES
# ===========================================================================

def fetch_available_topics(engine: Engine, subject_code: str, degree_code: str,
                           ay_label: str, year: int, term: int,
                           division_code: str = None) -> List[Dict]:
    """Fetch available topics with real-time capacity."""
    return _fetch_all(engine, """
        SELECT 
            t.*,
            COALESCE(c.confirmed_count, 0) AS confirmed_count,
            COALESCE(c.waitlisted_count, 0) AS waitlisted_count,
            COALESCE(c.remaining_capacity, t.capacity) AS remaining_capacity,
            CASE 
                WHEN t.capacity = 0 THEN 0
                WHEN COALESCE(c.confirmed_count, 0) >= t.capacity THEN 1
                ELSE 0
            END AS is_full
        FROM elective_topics t
        LEFT JOIN elective_capacity_tracking c 
            ON c.topic_code_ay = t.topic_code_ay 
            AND c.ay_label = t.ay_label
        WHERE t.subject_code = :subj
        AND t.degree_code = :deg
        AND t.ay_label = :ay
        AND t.year = :yr
        AND t.term = :trm
        AND t.status = 'published'
        AND (:div IS NULL OR t.division_code = :div OR t.division_code IS NULL)
        ORDER BY t.topic_no
    """, {
        "subj": subject_code,
        "deg": degree_code,
        "ay": ay_label,
        "yr": year,
        "trm": term,
        "div": division_code
    })


# ===========================================================================
# STUDENT PREFERENCE QUERIES
# ===========================================================================

def get_student_preferences(engine: Engine, student_roll_no: str, 
                            subject_code: str, ay_label: str) -> List[Dict]:
    """Get student's current preferences for a subject."""
    return _fetch_all(engine, """
        SELECT * FROM elective_student_selections
        WHERE student_roll_no = :roll
        AND subject_code = :subj
        AND ay_label = :ay
        ORDER BY rank_choice
    """, {
        "roll": student_roll_no,
        "subj": subject_code,
        "ay": ay_label
    })


def has_confirmed_selection(engine: Engine, student_roll_no: str,
                           subject_code: str, ay_label: str) -> bool:
    """Check if student has confirmed selection."""
    result = _fetch_one(engine, """
        SELECT COUNT(*) AS cnt FROM elective_student_selections
        WHERE student_roll_no = :roll
        AND subject_code = :subj
        AND ay_label = :ay
        AND status IN ('confirmed', 'waitlisted')
    """, {
        "roll": student_roll_no,
        "subj": subject_code,
        "ay": ay_label
    })
    return result and result['cnt'] > 0


# ===========================================================================
# PREFERENCE SUBMISSION
# ===========================================================================

def save_student_preferences(
    engine: Engine, 
    student: Dict, 
    subject_code: str, 
    ay_label: str,
    year: int, 
    term: int,
    preferences: Dict[int, str],
    selection_strategy: str = "student_select_ranked"
) -> bool:
    """
    Save student's ranked preferences.
    
    Args:
        preferences: Dict mapping rank (1, 2, 3) to topic_code_ay
        selection_strategy: Strategy mode from policy
    
    Returns:
        True if successful
    """
    try:
        with engine.begin() as conn:
            # Get subject details
            subject = _fetch_one(engine, """
                SELECT subject_name FROM subject_offerings
                WHERE subject_code = :subj AND ay_label = :ay
            """, {"subj": subject_code, "ay": ay_label})
            
            # Clear existing draft preferences for this subject
            _exec(conn, """
                DELETE FROM elective_student_selections
                WHERE student_roll_no = :roll
                AND subject_code = :subj
                AND ay_label = :ay
                AND status = 'draft'
            """, {
                "roll": student['student_roll_no'],
                "subj": subject_code,
                "ay": ay_label
            })
            
            # Insert new preferences
            for rank, topic_code_ay in preferences.items():
                # Get topic details
                topic = _fetch_one(engine, """
                    SELECT topic_name FROM elective_topics
                    WHERE topic_code_ay = :code AND ay_label = :ay
                """, {"code": topic_code_ay, "ay": ay_label})
                
                if not topic:
                    logger.warning(f"Topic {topic_code_ay} not found")
                    continue
                
                # Insert preference
                _exec(conn, """
                    INSERT INTO elective_student_selections (
                        student_id, student_roll_no, student_name, student_email,
                        degree_code, program_code, branch_code,
                        ay_label, year, term, division_code, batch,
                        subject_code, topic_code_ay, topic_name,
                        rank_choice, selection_strategy, status,
                        selected_at
                    ) VALUES (
                        :sid, :roll, :name, :email,
                        :deg, :prog, :br,
                        :ay, :yr, :trm, :div, :batch,
                        :subj, :topic_code, :topic_name,
                        :rank, :strategy, 'draft',
                        :now
                    )
                """, {
                    "sid": student.get('id'),
                    "roll": student['student_roll_no'],
                    "name": student['name'],
                    "email": student.get('email'),
                    "deg": student['degree_code'],
                    "prog": student.get('program_code'),
                    "br": student.get('branch_code'),
                    "ay": ay_label,
                    "yr": year,
                    "trm": term,
                    "div": student.get('division_code'),
                    "batch": student.get('batch'),
                    "subj": subject_code,
                    "topic_code": topic_code_ay,
                    "topic_name": topic['topic_name'],
                    "rank": rank,
                    "strategy": selection_strategy,
                    "now": datetime.now()
                })
                
                # Save to history
                _exec(conn, """
                    INSERT INTO elective_preference_history (
                        student_roll_no, subject_code, degree_code,
                        ay_label, year, term,
                        rank, topic_code_ay, topic_name,
                        submitted_at, source
                    ) VALUES (
                        :roll, :subj, :deg,
                        :ay, :yr, :trm,
                        :rank, :topic, :topic_name,
                        :now, 'student_portal'
                    )
                """, {
                    "roll": student['student_roll_no'],
                    "subj": subject_code,
                    "deg": student['degree_code'],
                    "ay": ay_label,
                    "yr": year,
                    "trm": term,
                    "rank": rank,
                    "topic": topic_code_ay,
                    "topic_name": topic['topic_name'],
                    "now": datetime.now()
                })
            
            logger.info(f"Saved {len(preferences)} preferences for {student['student_roll_no']}")
            return True
            
    except Exception as e:
        logger.error(f"Error saving preferences: {e}", exc_info=True)
        return False


# ===========================================================================
# UI COMPONENTS
# ===========================================================================

def render_topic_card(topic: Dict, rank: int = None, compact: bool = False):
    """Render a topic card with details."""
    
    # Capacity badge
    if topic['is_full']:
        capacity_badge = f"🔴 Full ({topic['waitlisted_count']} waitlisted)"
        capacity_color = "red"
    elif topic['capacity'] == 0:
        capacity_badge = "🟢 Unlimited"
        capacity_color = "green"
    elif topic['remaining_capacity'] < 5:
        capacity_badge = f"🟡 {topic['remaining_capacity']}/{topic['capacity']} remaining"
        capacity_color = "orange"
    else:
        capacity_badge = f"🟢 {topic['remaining_capacity']}/{topic['capacity']} available"
        capacity_color = "green"
    
    if compact:
        st.markdown(f"""
        **{rank}. {topic['topic_name']}** `{topic['topic_code_ay']}`  
        {capacity_badge}
        """)
    else:
        with st.container():
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.markdown(f"### {topic['topic_name']}")
                st.caption(f"Code: {topic['topic_code_ay']}")
                
                if topic.get('description'):
                    st.write(topic['description'])
                
                if topic.get('owner_faculty_email'):
                    st.caption(f"👤 Faculty: {topic['owner_faculty_email']}")
            
            with col2:
                st.metric("Capacity", capacity_badge)
                if topic['confirmed_count'] > 0:
                    st.caption(f"✅ {topic['confirmed_count']} confirmed")
                if topic['waitlisted_count'] > 0:
                    st.caption(f"⏳ {topic['waitlisted_count']} waitlisted")


def render_confirmation_details(engine: Engine, student_roll_no: str,
                                subject_code: str, ay_label: str):
    """Show confirmation details for student."""
    selection = _fetch_one(engine, """
        SELECT * FROM v_student_selections_detail
        WHERE student_roll_no = :roll
        AND subject_code = :subj
        AND ay_label = :ay
        AND status IN ('confirmed', 'waitlisted')
    """, {
        "roll": student_roll_no,
        "subj": subject_code,
        "ay": ay_label
    })
    
    if not selection:
        st.warning("No confirmation found")
        return
    
    if selection['status'] == 'confirmed':
        st.success(f"✅ Confirmed: {selection['topic_name']}")
    else:
        st.info(f"⏳ Waitlisted: {selection['topic_name']}")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write(f"**Topic Code:** {selection['topic_code_ay']}")
        st.write(f"**Your Choice:** Rank #{selection['rank_choice']}")
        if selection.get('confirmed_at'):
            st.write(f"**Confirmed:** {selection['confirmed_at']}")
    
    with col2:
        if selection.get('owner_faculty_email'):
            st.write(f"**Faculty:** {selection['owner_faculty_email']}")
        st.write(f"**Status:** {selection['status'].title()}")


# ===========================================================================
# MAIN SELECTION INTERFACE
# ===========================================================================

def render_student_selection_interface(engine: Engine):
    """Main student selection interface."""
    
    st.title("🎯 Elective Selection Portal")
    
    # Check authentication
    student = get_student_from_session()
    
    if not student:
        st.error("🔒 Please log in to access elective selection")
        
        # Simple login form (integrate with your actual auth)
        with st.form("student_login"):
            st.subheader("Student Login")
            roll_no = st.text_input("Roll Number")
            password = st.text_input("Password", type="password")
            
            if st.form_submit_button("Login"):
                auth_student = authenticate_student(engine, roll_no, password)
                if auth_student:
                    # Set session state
                    for key, value in auth_student.items():
                        st.session_state[key] = value
                    st.success("Login successful!")
                    st.rerun()
                else:
                    st.error("Invalid credentials")
        
        return
    
    # Display student info
    with st.sidebar:
        st.markdown("### 👤 Student Profile")
        st.write(f"**Name:** {student['name']}")
        st.write(f"**Roll No:** {student['student_roll_no']}")
        st.write(f"**Degree:** {student['degree_code']}")
        st.write(f"**Year:** {student['current_year']}")
        if student.get('branch_code'):
            st.write(f"**Branch:** {student['branch_code']}")
        
        if st.button("Logout"):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
    
    # Get active windows
    windows = get_active_selection_windows(
        engine,
        student['degree_code'],
        student['current_year'],
        student.get('program_code'),
        student.get('branch_code'),
        student.get('batch'),
    )
    
    if not windows:
        st.info("📅 No elective selection windows are currently open.")
        st.markdown("Please check back later or contact your class coordinator.")
        return
    
    # Display each active window
    for window in windows:
        render_subject_selection_card(engine, student, window)


def render_subject_selection_card(engine: Engine, student: Dict, window: Dict):
    """Render selection card for one subject."""
    
    with st.expander(
        f"📚 {window['subject_name']} ({window['subject_code']})", 
        expanded=True
    ):
        # Window info
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Subject Type", window['subject_type'])
        with col2:
            st.metric("Credits", window['credits_total'])
        with col3:
            time_remaining = get_window_time_remaining(window)
            st.metric("Time Remaining", time_remaining)
        
        st.caption(f"Selection Period: {window['start_datetime']} to {window['end_datetime']}")
        
        # Check if already confirmed
        if has_confirmed_selection(engine, student['student_roll_no'], 
                                   window['subject_code'], window['ay_label']):
            render_confirmation_details(
                engine, 
                student['student_roll_no'],
                window['subject_code'],
                window['ay_label']
            )
            return
        
        # Get existing preferences (draft)
        existing_prefs = get_student_preferences(
            engine,
            student['student_roll_no'],
            window['subject_code'],
            window['ay_label']
        )
        
        # Get available topics
        topics = fetch_available_topics(
            engine,
            window['subject_code'],
            window['degree_code'],
            window['ay_label'],
            window['year'],
            window['term'],
            student.get('division_code')
        )
        
        if not topics:
            st.warning("No topics available for selection")
            return
        
        st.markdown("---")
        st.markdown("### 📝 Submit Your Preferences")
        
        # Default max ranks
        max_rank = 3
        selection_strategy = "student_select_ranked"
        
        # Apply policy, if available
        policy = _get_policy_for_window(engine, window)
        if policy:
            if policy.get("max_choices_per_slot"):
                try:
                    max_rank = max(1, int(policy["max_choices_per_slot"]))
                except Exception:
                    pass
            
            mode = policy.get("allocation_mode")
            if mode == "upload_only":
                # Manual assignment - still let student indicate preferences but inform them
                st.info("⚠️ **Manual Assignment Mode**: Your preferences will be reviewed by faculty/admin who will make the final assignments.")
                selection_strategy = "manual_assign"
            elif mode == "rank_and_auto":
                selection_strategy = "student_select_ranked"
        
        st.info(f"Select your preferences in order. You can select up to **{max_rank}** topic(s). The allocation system will try to assign you to your highest ranked choice that has capacity.")
        
        # Preference form
        with st.form(key=f"pref_form_{window['subject_code']}"):
            preferences = {}
            existing_by_rank = {p['rank_choice']: p for p in existing_prefs if p['status'] == 'draft'}
            
            # Create topic options
            topic_options = {}
            for t in topics:
                capacity_str = (
                    f"(FULL - {t['waitlisted_count']} waitlisted)" 
                    if t['is_full'] 
                    else f"({t['remaining_capacity']}/{t['capacity']} slots)"
                    if t['capacity'] > 0
                    else "(Unlimited)"
                )
                label = f"{t['topic_code_ay']} - {t['topic_name']} {capacity_str}"
                topic_options[t['topic_code_ay']] = label
            
            # Rank selection
            for rank in range(1, max_rank + 1):
                st.markdown(f"#### {'🥇' if rank==1 else '🥈' if rank==2 else '🥉'} Choice #{rank}")
                
                # Pre-select if exists
                default_idx = 0
                if rank in existing_by_rank:
                    try:
                        existing_code = existing_by_rank[rank]['topic_code_ay']
                        default_idx = list(topic_options.keys()).index(existing_code) + 1
                    except:
                        pass
                
                selected = st.selectbox(
                    f"Select your {rank}{'st' if rank==1 else 'nd' if rank==2 else 'rd'} choice",
                    options=["-- None --"] + list(topic_options.values()),
                    index=default_idx,
                    key=f"pref_{rank}_{window['subject_code']}"
                )
                
                if selected != "-- None --":
                    # Extract topic_code_ay
                    topic_code = selected.split(" - ")[0]
                    preferences[rank] = topic_code
            
            st.markdown("---")
            
            # Show selected topics
            if preferences:
                st.markdown("#### Your Selections:")
                for rank, topic_code in preferences.items():
                    topic = next((t for t in topics if t['topic_code_ay'] == topic_code), None)
                    if topic:
                        render_topic_card(topic, rank, compact=True)
                
                st.markdown("---")
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                submitted = st.form_submit_button(
                    "✅ Submit Preferences" if not existing_prefs else "🔄 Update Preferences",
                    type="primary",
                    use_container_width=True
                )
            
            with col2:
                if existing_prefs:
                    clear = st.form_submit_button("🗑️ Clear", use_container_width=True)
                else:
                    clear = False
            
            if clear:
                # Clear preferences
                with engine.begin() as conn:
                    _exec(conn, """
                        DELETE FROM elective_student_selections
                        WHERE student_roll_no = :roll
                        AND subject_code = :subj
                        AND ay_label = :ay
                        AND status = 'draft'
                    """, {
                        "roll": student['student_roll_no'],
                        "subj": window['subject_code'],
                        "ay": window['ay_label']
                    })
                st.success("Preferences cleared!")
                st.rerun()
            
            if submitted:
                if not preferences:
                    st.error("Please select at least one preference")
                else:
                    # Validate no duplicates
                    if len(preferences.values()) != len(set(preferences.values())):
                        st.error("You cannot select the same topic multiple times!")
                    else:
                        # Save preferences
                        success = save_student_preferences(
                            engine=engine,
                            student=student,
                            subject_code=window['subject_code'],
                            ay_label=window['ay_label'],
                            year=window['year'],
                            term=window['term'],
                            preferences=preferences,
                            selection_strategy=selection_strategy,
                        )
                        
                        if success:
                            st.success("✅ Preferences submitted successfully!")
                            st.balloons()
                            
                            # Show next steps
                            if selection_strategy == "manual_assign":
                                st.info("""
                                **What happens next?**
                                
                                1. Your preferences have been saved
                                2. Faculty/admin will review all student preferences
                                3. Final assignments will be made manually
                                4. You'll be notified when your assignment is confirmed
                                """)
                            else:
                                st.info("""
                                **What happens next?**
                                
                                1. Your preferences have been saved
                                2. The allocation system will run automatically
                                3. You'll be assigned to the highest-ranked topic with available capacity
                                4. You'll receive a notification when your selection is confirmed
                                """)
                            
                            st.rerun()
                        else:
                            st.error("Failed to save preferences. Please try again.")


# ===========================================================================
# ENTRY POINT
# ===========================================================================

def render(engine: Engine = None):
    """Main entry point for student selection interface."""
    if engine is None:
        from core.db import get_engine
        from core.settings import load_settings
        settings = load_settings()
        engine = get_engine(settings.db.url)
    
    render_student_selection_interface(engine)


if __name__ == "__main__":
    render()