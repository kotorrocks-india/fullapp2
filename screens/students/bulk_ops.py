# app/screens/students/bulk_ops.py
# -------------------------------------------------------------------
# MODIFIED VERSION
# - Added "Division Mover" tab for moving students between divisions
# - Added "Status Viewer" tab for viewing/editing student status
# - Added division assignment audit logging
# -------------------------------------------------------------------

import streamlit as st
from sqlalchemy.engine import Engine
from sqlalchemy import text as sa_text

from screens.students.importer import (
    _add_student_import_export_section,
    _add_student_mover_section,
    _add_student_credential_export_section,
    _add_student_data_export_section
)

# NOTE: The 'status_viewer' import was moved into the render()
# function to prevent circular import errors.


def _add_division_mover_section(engine: Engine):
    """NEW: Move students between divisions with reason tracking."""
    st.divider()
    st.subheader("🏫 Division Mover")
    st.info("Move students between divisions within the same degree/batch/year. All moves are audited with reason.")
    
    # Get degrees
    with engine.connect() as conn:
        degrees = conn.execute(sa_text(
            "SELECT code FROM degrees WHERE active = 1 ORDER BY sort_order, code"
        )).fetchall()
        degree_list = [d[0] for d in degrees]
    
    if not degree_list:
        st.warning("No active degrees found.")
        return
    
    # Selection
    col1, col2, col3 = st.columns(3)
    
    with col1:
        selected_degree = st.selectbox("Degree", degree_list, key="divmover_degree")
    
    # Get batches
    with engine.connect() as conn:
        batches = conn.execute(sa_text("""
            SELECT DISTINCT batch FROM student_enrollments 
            WHERE degree_code = :degree AND batch IS NOT NULL
            ORDER BY batch DESC
        """), {"degree": selected_degree}).fetchall()
        batch_list = [b[0] for b in batches]
    
    with col2:
        if not batch_list:
            st.warning("No batches found")
            return
        selected_batch = st.selectbox("Batch", batch_list, key="divmover_batch")
    
    # Get years
    with engine.connect() as conn:
        years = conn.execute(sa_text("""
            SELECT DISTINCT current_year FROM student_enrollments 
            WHERE degree_code = :degree AND batch = :batch AND current_year IS NOT NULL
            ORDER BY current_year
        """), {"degree": selected_degree, "batch": selected_batch}).fetchall()
        year_list = [y[0] for y in years]
    
    with col3:
        if not year_list:
            st.warning("No years found")
            return
        selected_year = st.selectbox("Year", year_list, key="divmover_year")
    
    st.divider()
    
    # Get divisions for this scope
    with engine.connect() as conn:
        divisions = conn.execute(sa_text("""
            SELECT division_code, division_name, capacity 
            FROM division_master
            WHERE degree_code = :degree 
              AND batch = :batch 
              AND current_year = :year
              AND active = 1
            ORDER BY division_code
        """), {
            "degree": selected_degree,
            "batch": selected_batch,
            "year": selected_year
        }).fetchall()
        
        division_list = [{"code": d[0], "name": d[1], "capacity": d[2]} for d in divisions]
    
    if not division_list:
        st.warning("No divisions defined for this scope. Create divisions in Settings > Division Editor first.")
        return
    
    # Display division summary
    st.markdown("#### 📊 Division Summary")
    summary_data = []
    
    for div in division_list:
        with engine.connect() as conn:
            count = conn.execute(sa_text("""
                SELECT COUNT(*) FROM student_enrollments
                WHERE degree_code = :degree
                  AND batch = :batch
                  AND current_year = :year
                  AND division_code = :div
                  AND is_primary = 1
            """), {
                "degree": selected_degree,
                "batch": selected_batch,
                "year": selected_year,
                "div": div["code"]
            }).scalar() or 0
        
        summary_data.append({
            "Division": f"{div['code']} - {div['name']}",
            "Students": count,
            "Capacity": div['capacity'] or "N/A",
            "Utilization": f"{(count/div['capacity']*100):.1f}%" if div['capacity'] else "N/A"
        })
    
    st.dataframe(summary_data, use_container_width=True, hide_index=True)
    
    st.divider()
    
    # Select source division
    st.markdown("#### 1️⃣ Select Source Division")
    from_division = st.selectbox(
        "From Division",
        [d["code"] for d in division_list],
        key="divmover_from"
    )
    
    # Get students in source division
    if st.button("Get Students", key="divmover_get_students"):
        with engine.connect() as conn:
            students = conn.execute(sa_text("""
                SELECT 
                    p.id AS profile_id,
                    p.student_id,
                    p.name,
                    p.email,
                    e.id AS enrollment_id,
                    e.division_code
                FROM student_enrollments e
                JOIN student_profiles p ON p.id = e.student_profile_id
                WHERE e.degree_code = :degree
                  AND e.batch = :batch
                  AND e.current_year = :year
                  AND e.division_code = :div
                  AND e.is_primary = 1
                ORDER BY p.student_id
            """), {
                "degree": selected_degree,
                "batch": selected_batch,
                "year": selected_year,
                "div": from_division
            }).fetchall()
        
        if not students:
            st.warning(f"No students found in division {from_division}")
            return
        
        import pandas as pd
        df = pd.DataFrame(students, columns=[
            "Profile ID", "Student ID", "Name", "Email", "Enrollment ID", "Current Division"
        ])
        df["Move"] = False
        
        st.session_state.divmover_students = df
    
    # Display students
    if "divmover_students" not in st.session_state:
        st.info("Click 'Get Students' to load students from source division")
        return
    
    st.markdown("#### 2️⃣ Select Students to Move")
    
    edited_df = st.data_editor(
        st.session_state.divmover_students,
        key="divmover_editor",
        use_container_width=True,
        column_config={
            "Profile ID": None,
            "Enrollment ID": None
        },
        disabled=["Student ID", "Name", "Email", "Current Division"]
    )
    
    selected_students = edited_df[edited_df["Move"] == True]
    
    if selected_students.empty:
        st.warning("Select students to move")
        return
    
    st.markdown("#### 3️⃣ Select Destination Division")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Filter out source division
        to_division_list = [d["code"] for d in division_list if d["code"] != from_division]
        
        if not to_division_list:
            st.error("No other divisions available as destination")
            return
        
        to_division = st.selectbox(
            "To Division",
            to_division_list,
            key="divmover_to"
        )
    
    with col2:
        # Check mover settings
        with engine.connect() as conn:
            require_reason = conn.execute(sa_text(
                "SELECT value FROM app_settings WHERE key = 'mover_within_reason'"
            )).fetchone()
            
            require_reason = (require_reason and require_reason[0] == "True") if require_reason else True
        
        move_reason = st.text_area(
            "Reason for move" + (" *" if require_reason else " (optional)"),
            placeholder="e.g., Student request, Better class fit, Capacity balancing",
            key="divmover_reason"
        )
    
    st.divider()
    
    # Execute move
    st.warning(f"⚠️ Move {len(selected_students)} student(s) from {from_division} to {to_division}")
    
    if st.button("🚀 Execute Move", type="primary", key="divmover_execute"):
        if require_reason and not move_reason.strip():
            st.error("❌ Reason is required for division moves")
            return
        
        try:
            with engine.begin() as conn:
                enrollment_ids = selected_students["Enrollment ID"].tolist()
                
                for idx, row in selected_students.iterrows():
                    enrollment_id = row["Enrollment ID"]
                    profile_id = row["Profile ID"]
                    
                    # Update division
                    conn.execute(sa_text("""
                        UPDATE student_enrollments
                        SET division_code = :to_div,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = :eid
                    """), {
                        "to_div": to_division,
                        "eid": enrollment_id
                    })
                    
                    # Log in audit table
                    conn.execute(sa_text("""
                        INSERT INTO division_assignment_audit (
                            student_profile_id,
                            enrollment_id,
                            from_division_code,
                            to_division_code,
                            reason,
                            assigned_by
                        ) VALUES (
                            :pid,
                            :eid,
                            :from_div,
                            :to_div,
                            :reason,
                            NULL
                        )
                    """), {
                        "pid": profile_id,
                        "eid": enrollment_id,
                        "from_div": from_division,
                        "to_div": to_division,
                        "reason": move_reason.strip() or None
                    })
            
            st.success(f"✅ Successfully moved {len(selected_students)} student(s) to division {to_division}")
            st.cache_data.clear()
            
            # Clear session state
            if "divmover_students" in st.session_state:
                del st.session_state.divmover_students
            
            st.rerun()
            
        except Exception as e:
            st.error(f"❌ Move failed: {str(e)}")


def render(engine: Engine):
    """
    Renders six-tab UI for student bulk operations.
    """
    
    # Check if degrees exist
    with engine.begin() as conn:
        degree_check = conn.execute(sa_text(
            "SELECT COUNT(*) FROM degrees WHERE active = 1"
        )).scalar()
        has_degrees = degree_check and degree_check > 0

    if not has_degrees:
        st.warning("⚠️ No degrees found. Set up degrees first.")
        st.info("""
### 🚀 Getting Started

1. Create Degrees (with duration)
2. Import Students
3. Manage Students

Go to Degrees page to get started.
        """)
        return

    # Create tabs
    st.markdown("## 📥 Student Bulk Operations")
    
    # CORRECTED: Unpacked 6 tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📥 Import Students",
        "🚚 Student Mover", 
        "🏫 Division Mover",
        "👥 Status Viewer",
        "🔑 Export Credentials",
        "📊 Export Full Data"
    ])

    # Tab 1: Import
    with tab1:
        _add_student_import_export_section(engine)

    # Tab 2: Mover
    with tab2:
        _add_student_mover_section(engine)

    # Tab 3: Division Mover (NEW)
    with tab3:
        _add_division_mover_section(engine)

    # Tab 4: Status Viewer
    with tab4:
        # CORRECTED: Indentation fixed and import moved here
        try:
            from screens.students.status_viewer import render_status_viewer
            render_status_viewer(engine)
        except ImportError:
            st.error("Status viewer not available")

    # Tab 5: Credentials
    # CORRECTED: Now tab5
    with tab5:
        _add_student_credential_export_section(engine)

    # Tab 6: Full Exporter
    # CORRECTED: Now tab6
    with tab6:
        _add_student_data_export_section(engine)
