# screens/students/status_viewer.py
"""
Student Status Viewer and Editor
- View all students with their current status
- Edit individual student status with reason
- Filter by status, degree, batch, year
- Track status change history
"""

from __future__ import annotations
from typing import List, Dict, Any, Optional
import pandas as pd
import streamlit as st
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from datetime import datetime
import logging

log = logging.getLogger(__name__)


def _ensure_status_audit_table(engine: Engine):
    """Create student_status_audit table if it doesn't exist."""
    with engine.begin() as conn:
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
        conn.execute(sa_text(
            "CREATE INDEX IF NOT EXISTS idx_status_audit_student ON student_status_audit(student_profile_id)"
        ))


def _get_student_statuses() -> List[str]:
    """Get list of valid student statuses."""
    return ["Good", "Hold", "Left", "Transferred", "Graduated", "Deceased", "YearDrop"]


def _get_students_for_status_viewer(
    engine: Engine,
    degree_code: str = None,
    batch: str = None,
    year: int = None,
    status_filter: str = None
) -> pd.DataFrame:
    """Get students with their current status and enrollment details."""
    
    query = """
        SELECT 
            p.id AS profile_id,
            p.student_id,
            p.name,
            p.email,
            p.status,
            e.degree_code,
            e.batch,
            e.current_year,
            e.program_code,
            e.branch_code,
            e.division_code,
            e.enrollment_status,
            p.updated_at
        FROM student_profiles p
        LEFT JOIN student_enrollments e ON p.id = e.student_profile_id AND e.is_primary = 1
        WHERE 1=1
    """
    
    params = {}
    
    if degree_code:
        query += " AND e.degree_code = :degree"
        params["degree"] = degree_code
    
    if batch:
        query += " AND e.batch = :batch"
        params["batch"] = batch
    
    if year:
        query += " AND e.current_year = :year"
        params["year"] = year
    
    if status_filter and status_filter != "All":
        query += " AND p.status = :status"
        params["status"] = status_filter
    
    query += " ORDER BY p.student_id"
    
    with engine.connect() as conn:
        rows = conn.execute(sa_text(query), params).fetchall()
    
    if not rows:
        return pd.DataFrame()
    
    df = pd.DataFrame(rows, columns=[
        "Profile ID", "Student ID", "Name", "Email", "Status",
        "Degree", "Batch", "Year", "Program", "Branch", "Division",
        "Enrollment Status", "Last Updated"
    ])
    
    return df


def _update_student_status(
    engine: Engine,
    profile_id: int,
    new_status: str,
    reason: str = None
) -> tuple[bool, str]:
    """Update student status and log the change."""
    try:
        with engine.begin() as conn:
            # Get current status
            current = conn.execute(sa_text(
                "SELECT status FROM student_profiles WHERE id = :id"
            ), {"id": profile_id}).fetchone()
            
            if not current:
                return False, "Student not found"
            
            old_status = current[0]
            
            if old_status == new_status:
                return False, "Status unchanged"
            
            # Update status
            conn.execute(sa_text("""
                UPDATE student_profiles 
                SET status = :status, updated_at = CURRENT_TIMESTAMP
                WHERE id = :id
            """), {"status": new_status, "id": profile_id})
            
            # Log change
            conn.execute(sa_text("""
                INSERT INTO student_status_audit (
                    student_profile_id, from_status, to_status, reason, changed_by
                ) VALUES (:pid, :from, :to, :reason, :by)
            """), {
                "pid": profile_id,
                "from": old_status,
                "to": new_status,
                "reason": reason,
                "by": None  # TODO: Add user tracking
            })
            
        return True, f"Status updated: {old_status} → {new_status}"
    
    except Exception as e:
        log.error(f"Failed to update status: {e}")
        return False, f"Update failed: {str(e)}"


def _get_status_history(engine: Engine, profile_id: int) -> pd.DataFrame:
    """Get status change history for a student."""
    with engine.connect() as conn:
        rows = conn.execute(sa_text("""
            SELECT 
                from_status,
                to_status,
                reason,
                changed_by,
                changed_at
            FROM student_status_audit
            WHERE student_profile_id = :pid
            ORDER BY changed_at DESC
        """), {"pid": profile_id}).fetchall()
    
    if not rows:
        return pd.DataFrame()
    
    df = pd.DataFrame(rows, columns=[
        "From Status", "To Status", "Reason", "Changed By", "Changed At"
    ])
    
    return df


def _render_status_statistics(engine: Engine):
    """Display status distribution statistics."""
    with engine.connect() as conn:
        rows = conn.execute(sa_text("""
            SELECT 
                COALESCE(p.status, 'NULL') AS status,
                COUNT(*) AS count
            FROM student_profiles p
            GROUP BY p.status
            ORDER BY count DESC
        """)).fetchall()
    
    if not rows:
        st.info("No students found")
        return
    
    st.markdown("### 📊 Status Distribution")
    
    cols = st.columns(len(rows))
    
    status_icons = {
        "Good": "✅",
        "Hold": "⏸️",
        "Left": "🚪",
        "Transferred": "➡️",
        "Graduated": "🎓",
        "Deceased": "🕊️",
        "YearDrop": "⬇️"
    }
    
    for i, (status, count) in enumerate(rows):
        with cols[i]:
            icon = status_icons.get(status, "❓")
            st.metric(f"{icon} {status}", count)


def render_status_viewer(engine: Engine):
    """Main UI for student status viewer and editor."""
    
    st.subheader("👥 Student Status Viewer")
    
    # Ensure audit table exists
    _ensure_status_audit_table(engine)
    
    # Show statistics
    _render_status_statistics(engine)
    
    st.divider()
    
    # Filters
    st.markdown("### 🔍 Filter Students")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Get filter options
    with engine.connect() as conn:
        degrees = conn.execute(sa_text(
            "SELECT DISTINCT degree_code FROM student_enrollments ORDER BY degree_code"
        )).fetchall()
        degree_list = ["All"] + [d[0] for d in degrees if d[0]]
    
    with col1:
        selected_degree = st.selectbox("Degree", degree_list, key="status_degree")
    
    # Get batches for selected degree
    if selected_degree != "All":
        with engine.connect() as conn:
            batches = conn.execute(sa_text("""
                SELECT DISTINCT batch FROM student_enrollments 
                WHERE degree_code = :degree AND batch IS NOT NULL
                ORDER BY batch DESC
            """), {"degree": selected_degree}).fetchall()
            batch_list = ["All"] + [b[0] for b in batches]
    else:
        batch_list = ["All"]
    
    with col2:
        selected_batch = st.selectbox("Batch", batch_list, key="status_batch")
    
    # Get years
    if selected_degree != "All" and selected_batch != "All":
        with engine.connect() as conn:
            years = conn.execute(sa_text("""
                SELECT DISTINCT current_year FROM student_enrollments 
                WHERE degree_code = :degree AND batch = :batch AND current_year IS NOT NULL
                ORDER BY current_year
            """), {"degree": selected_degree, "batch": selected_batch}).fetchall()
            year_list = ["All"] + [y[0] for y in years]
    else:
        year_list = ["All"]
    
    with col3:
        selected_year = st.selectbox("Year", year_list, key="status_year")
    
    with col4:
        status_filter = st.selectbox(
            "Status", 
            ["All"] + _get_student_statuses(),
            key="status_filter"
        )
    
    # Apply filters
    if st.button("🔍 Search", type="primary"):
        df = _get_students_for_status_viewer(
            engine,
            degree_code=selected_degree if selected_degree != "All" else None,
            batch=selected_batch if selected_batch != "All" else None,
            year=selected_year if selected_year != "All" else None,
            status_filter=status_filter if status_filter != "All" else None
        )
        
        st.session_state.status_viewer_df = df
    
    st.divider()
    
    # Display results
    if "status_viewer_df" not in st.session_state:
        st.info("Click 'Search' to view students")
        return
    
    df = st.session_state.status_viewer_df
    
    if df.empty:
        st.warning("No students found with selected filters")
        return
    
    st.markdown(f"### 📋 Students ({len(df)} found)")
    
    # Display table with status badges
    display_df = df.copy()
    display_df = display_df[[
        "Student ID", "Name", "Email", "Status", 
        "Degree", "Batch", "Year", "Division"
    ]]
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    st.divider()
    
    # Edit student status
    st.markdown("### ✏️ Edit Student Status")
    
    col1, col2 = st.columns(2)
    
    with col1:
        student_ids = df["Student ID"].tolist()
        selected_student_id = st.selectbox(
            "Select Student",
            student_ids,
            key="edit_student_id"
        )
        
        if selected_student_id:
            student_row = df[df["Student ID"] == selected_student_id].iloc[0]
            current_status = student_row["Status"]
            profile_id = student_row["Profile ID"]
            
            st.info(f"**Current Status:** {current_status}")
            
            new_status = st.selectbox(
                "New Status",
                _get_student_statuses(),
                index=_get_student_statuses().index(current_status) if current_status in _get_student_statuses() else 0,
                key="new_status"
            )
    
    with col2:
        if selected_student_id:
            st.markdown("**Status Definitions:**")
            
            status_help = {
                "Good": "Active student in good standing",
                "Hold": "Hidden from current AY calculations",
                "Left": "Student has left the institution",
                "Transferred": "Transferred to another institution",
                "Graduated": "Completed the program",
                "Deceased": "Record is frozen",
                "YearDrop": "Student has dropped a year but remains enrolled"
            }
            
            st.caption(status_help.get(new_status, ""))
            
            reason = st.text_area(
                "Reason for Change*",
                placeholder="e.g., Failed internal exams, Completed degree requirements, Student request",
                key="status_change_reason"
            )
    
    if selected_student_id:
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("💾 Update Status", type="primary", key="update_status_btn"):
                if not reason.strip():
                    st.error("❌ Reason is required")
                else:
                    success, message = _update_student_status(
                        engine,
                        profile_id,
                        new_status,
                        reason.strip()
                    )
                    
                    if success:
                        st.success(message)
                        st.cache_data.clear()
                        
                        # Refresh data
                        df = _get_students_for_status_viewer(
                            engine,
                            degree_code=selected_degree if selected_degree != "All" else None,
                            batch=selected_batch if selected_batch != "All" else None,
                            year=selected_year if selected_year != "All" else None,
                            status_filter=status_filter if status_filter != "All" else None
                        )
                        st.session_state.status_viewer_df = df
                        st.rerun()
                    else:
                        st.error(message)
        
        with col2:
            if st.button("📜 View History", key="view_history_btn"):
                history_df = _get_status_history(engine, profile_id)
                
                if not history_df.empty:
                    st.markdown(f"#### Status History: {student_row['Name']}")
                    st.dataframe(history_df, use_container_width=True, hide_index=True)
                else:
                    st.info("No status change history")


def render(engine: Engine):
    """Main entry point for status viewer module."""
    st.title("👥 Student Status Management")
    render_status_viewer(engine)
