# screens/subject_cos_rubrics/course_outcomes_audit_tab.py
"""
Renders the Audit Trail tab for Course Outcomes.
Queries the 'subject_cos_audit' table.
"""

import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

def fetch_audit_logs(engine: Engine, offering_id: int) -> List[Dict]:
    """Fetch audit logs for a specific offering."""
    try:
        with engine.begin() as conn:
            result = conn.execute(sa_text("""
                SELECT 
                    occurred_at_utc, 
                    actor_id, 
                    operation, 
                    action,
                    co_code, 
                    reason, 
                    changed_fields,
                    note,
                    source
                FROM subject_cos_audit
                WHERE offering_id = :offering_id
                ORDER BY occurred_at_utc DESC
                LIMIT 200
            """), {"offering_id": offering_id})
            
            return [dict(row._mapping) for row in result]
    except Exception as e:
        logger.error(f"Error fetching CO audit logs: {e}", exc_info=True)
        st.error(f"Failed to fetch audit logs: {e}")
        return []

def render_co_audit_tab(engine: Engine, offering_id: Optional[int], offering_info: Optional[Dict]):
    """Main render function for CO Audit Trail tab."""
    
    st.markdown("""
    This tab shows a log of all changes (create, update, delete) made to the
    Course Outcomes for the selected subject offering.
    """)
    
    if not offering_id:
        st.info("Please select a subject offering from the filters above to see its audit trail.")
        return

    st.markdown(f"#### 📜 Audit Trail for: `{offering_info['subject_code']} - {offering_info['subject_name']}`")
    
    with st.spinner("Loading audit logs..."):
        logs = fetch_audit_logs(engine, offering_id)
        
        if not logs:
            st.info("No audit history found for this offering.")
            return
            
        df = pd.DataFrame(logs)
        
        # Reformat columns for display
        df['Time'] = pd.to_datetime(df['occurred_at_utc']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df.rename(columns={
            'actor_id': 'Actor',
            'operation': 'Operation',
            'action': 'Action',
            'co_code': 'CO Code',
            'reason': 'Reason',
            'changed_fields': 'Changes',
            'note': 'Note',
            'source': 'Source'
        }, inplace=True)
        
        st.dataframe(df[[
            'Time', 'Actor', 'Action', 'CO Code', 
            'Reason', 'Changes', 'Note', 'Source'
        ]], use_container_width=True)
