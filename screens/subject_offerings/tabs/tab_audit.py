"""
Audit Trail Tab - View audit logs for offerings
Fixed to match the schema column names
"""

import streamlit as st
import pandas as pd
from ..helpers import exec_query, rows_to_dicts


def render(engine, actor: str, CAN_EDIT: bool):
    """Render the Audit Trail tab."""
    st.subheader("📜 Audit Trail")
    st.caption("View all changes to subject offerings")
    
    with engine.begin() as conn:
        # Fixed: Use 'occurred_at' instead of 'created_at'
        logs = exec_query(conn, """
            SELECT 
                offering_id, subject_code, degree_code, ay_label,
                action, operation, note, reason, actor, actor_role,
                source, occurred_at
            FROM subject_offerings_audit
            ORDER BY id DESC 
            LIMIT 200
        """).fetchall()
    
    if logs:
        df = pd.DataFrame(rows_to_dicts(logs))
        
        st.markdown(f"### Recent Activity (Last {len(df)} records)")
        
        # Filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Filter by action
            actions = df['action'].unique().tolist()
            selected_actions = st.multiselect(
                "Filter by Action",
                options=actions,
                default=actions,
                key="audit_action_filter"
            )
        
        with col2:
            # Filter by actor
            actors = df['actor'].unique().tolist()
            selected_actors = st.multiselect(
                "Filter by Actor",
                options=actors,
                default=actors,
                key="audit_actor_filter"
            )
        
        with col3:
            # Filter by degree
            if 'degree_code' in df.columns:
                degrees = df['degree_code'].unique().tolist()
                selected_degrees = st.multiselect(
                    "Filter by Degree",
                    options=degrees,
                    default=degrees,
                    key="audit_degree_filter"
                )
            else:
                selected_degrees = None
        
        # Apply filters
        filtered_df = df.copy()
        
        if selected_actions:
            filtered_df = filtered_df[filtered_df['action'].isin(selected_actions)]
        
        if selected_actors:
            filtered_df = filtered_df[filtered_df['actor'].isin(selected_actors)]
        
        if selected_degrees:
            filtered_df = filtered_df[filtered_df['degree_code'].isin(selected_degrees)]
        
        st.markdown(f"**Showing {len(filtered_df)} of {len(df)} records**")
        
        # Display columns configuration
        display_cols = [
            'occurred_at', 'action', 'operation', 'subject_code', 
            'degree_code', 'ay_label', 'actor', 'note', 'reason'
        ]
        display_cols = [c for c in display_cols if c in filtered_df.columns]
        
        # Format datetime for display
        if 'occurred_at' in filtered_df.columns:
            filtered_df['occurred_at'] = pd.to_datetime(filtered_df['occurred_at']).dt.strftime('%Y-%m-%d %H:%M')
        
        st.dataframe(
            filtered_df[display_cols], 
            use_container_width=True,
            height=500
        )
        
        # Download options
        st.markdown("---")
        st.subheader("📥 Export Audit Logs")
        
        col1, col2 = st.columns(2)
        
        with col1:
            csv = filtered_df.to_csv(index=False)
            st.download_button(
                "Download as CSV",
                csv,
                file_name="offerings_audit_log.csv",
                mime="text/csv",
                key="audit_csv_download"
            )
        
        with col2:
            # Optional: Add Excel export
            import io
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                filtered_df.to_excel(writer, index=False, sheet_name='Audit Log')
            
            st.download_button(
                "Download as Excel",
                buffer.getvalue(),
                file_name="offerings_audit_log.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="audit_excel_download"
            )
        
        # Action breakdown
        st.markdown("---")
        st.subheader("📊 Audit Statistics")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Actions", len(filtered_df))
        
        with col2:
            st.metric("Unique Actors", filtered_df['actor'].nunique())
        
        with col3:
            st.metric("Unique Offerings", filtered_df['offering_id'].nunique())
        
        # Action breakdown chart
        if len(filtered_df) > 0:
            st.markdown("#### Actions Breakdown")
            action_counts = filtered_df['action'].value_counts()
            st.bar_chart(action_counts)
        
        # Recent activity timeline
        if 'occurred_at' in filtered_df.columns and len(filtered_df) > 0:
            st.markdown("#### Activity Over Time")
            timeline_df = filtered_df.copy()
            timeline_df['occurred_at'] = pd.to_datetime(timeline_df['occurred_at'])
            timeline_df['date'] = timeline_df['occurred_at'].dt.date
            daily_counts = timeline_df.groupby('date').size().reset_index(name='count')
            st.line_chart(daily_counts.set_index('date')['count'])
        
    else:
        st.info("No audit logs found. Activity will appear here once offerings are created or modified.")
        
        st.markdown("---")
        st.markdown("""
        ### What gets logged?
        
        The audit trail captures:
        - ✅ **Create** - New offerings created
        - ✅ **Update** - Field changes
        - ✅ **Delete** - Offering deletions
        - ✅ **Publish/Archive** - Status changes
        - ✅ **Freeze/Unfreeze** - Lock controls
        - ✅ **Override Enable/Disable** - Catalog inheritance changes
        - ✅ **Copy Forward** - AY-to-AY replication
        - ✅ **Bulk Update** - Mass operations
        - ✅ **Import/Export** - Data transfers
        
        All actions include:
        - Actor (who performed it)
        - Timestamp (when)
        - Reason (why)
        - Changed fields (what)
        - Source (UI/API/Import)
        """)
