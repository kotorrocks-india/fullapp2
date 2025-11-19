# screens/subject_cos_rubrics/course_outcomes_tab.py
"""
Course Outcomes Tab

Manages Course Outcomes (COs) for published subject offerings.
Features:
- View existing COs for a subject offering
- Add/Edit/Delete COs
- Map COs to POs, PSOs, and PEOs
- Manage CO weights and Bloom levels
- Export/Import COs
"""

import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from typing import List, Dict, Optional, Tuple
import json
import logging

logger = logging.getLogger(__name__)


# ===========================================================================
# DATA FETCHING FUNCTIONS
# ===========================================================================

def fetch_published_offerings_for_cos(engine: Engine, degree_code: str, ay_label: str) -> List[Dict]:
    """Fetch published offerings for CO management."""
    with engine.begin() as conn:
        result = conn.execute(sa_text("""
            SELECT 
                so.id,
                so.subject_code,
                sc.subject_name,
                so.year,
                so.term,
                so.program_code,
                so.branch_code,
                so.division_code
            FROM subject_offerings so
            LEFT JOIN subjects_catalog sc ON 
                so.subject_code = sc.subject_code 
                AND so.degree_code = sc.degree_code
            WHERE so.status = 'published'
            AND so.degree_code = :degree_code
            AND so.ay_label = :ay_label
            ORDER BY so.year, so.term, so.subject_code
        """), {"degree_code": degree_code, "ay_label": ay_label})
        return [dict(row._mapping) for row in result]


def fetch_cos_for_offering(engine: Engine, offering_id: int) -> List[Dict]:
    """Fetch all COs for a specific offering."""
    with engine.begin() as conn:
        # Uses the correct schema from comprehensive_subjects_schema.py
        result = conn.execute(sa_text("""
            SELECT 
                id, co_code, title, description, bloom_level,
                sequence, weight_in_direct, status, knowledge_type,
                created_at, updated_at
            FROM subject_cos
            WHERE offering_id = :offering_id
            ORDER BY sequence, co_code
        """), {"offering_id": offering_id})
        
        cos = [dict(row._mapping) for row in result]
        
        # Fetch correlations for each CO
        for co in cos:
            # PO correlations
            po_result = conn.execute(sa_text("""
                SELECT po_code, correlation_value
                FROM co_po_correlations
                WHERE co_id = :co_id
            """), {"co_id": co['id']})
            co['po_correlations'] = {row._mapping['po_code']: row._mapping['correlation_value'] 
                                    for row in po_result}
            
            # PSO correlations
            pso_result = conn.execute(sa_text("""
                SELECT pso_code, correlation_value
                FROM co_pso_correlations
                WHERE co_id = :co_id
            """), {"co_id": co['id']})
            co['pso_correlations'] = {row._mapping['pso_code']: row._mapping['correlation_value'] 
                                     for row in pso_result}
            
            # PEO correlations
            peo_result = conn.execute(sa_text("""
                SELECT peo_code, correlation_value
                FROM co_peo_correlations
                WHERE co_id = :co_id
            """), {"co_id": co['id']})
            co['peo_correlations'] = {row._mapping['peo_code']: row._mapping['correlation_value'] 
                                     for row in peo_result}
        
        return cos


def fetch_pos_for_degree(engine: Engine, degree_code: str, program_code: Optional[str] = None) -> List[Dict]:
    """Fetch POs for a degree/program."""
    with engine.begin() as conn:
        query = """
            SELECT oi.code, oi.description
            FROM outcomes_items oi
            JOIN outcomes_sets os ON oi.set_id = os.id
            WHERE os.degree_code = :degree_code
            AND os.set_type = 'pos'
            AND os.status = 'published'
            AND os.is_current = 1
        """
        params = {"degree_code": degree_code}
        
        if program_code:
            query += " AND (os.program_code = :program_code OR os.program_code IS NULL)"
            params["program_code"] = program_code
        else:
            query += " AND os.program_code IS NULL"
        
        query += " ORDER BY oi.sort_order, oi.code"
        
        result = conn.execute(sa_text(query), params)
        return [dict(row._mapping) for row in result]


def fetch_psos_for_program(engine: Engine, degree_code: str, program_code: Optional[str], 
                          branch_code: Optional[str] = None) -> List[Dict]:
    """Fetch PSOs for a program/branch."""
    
    with engine.begin() as conn:
        query = """
            SELECT oi.code, oi.description
            FROM outcomes_items oi
            JOIN outcomes_sets os ON oi.set_id = os.id
            WHERE os.degree_code = :degree_code
            AND os.set_type = 'psos'
            AND os.status = 'published'
            AND os.is_current = 1
        """
        params = {"degree_code": degree_code}
        
        if program_code:
            query += " AND os.program_code = :program_code"
            params["program_code"] = program_code
        else:
             query += " AND os.program_code IS NULL"

        if branch_code:
            query += " AND (os.branch_code = :branch_code OR os.branch_code IS NULL)"
            params["branch_code"] = branch_code
        else:
            query += " AND os.branch_code IS NULL"
        
        query += " ORDER BY oi.sort_order, oi.code"
        
        result = conn.execute(sa_text(query), params)
        return [dict(row._mapping) for row in result]


def fetch_peos_for_degree(engine: Engine, degree_code: str, program_code: Optional[str] = None) -> List[Dict]:
    """Fetch PEOs for a degree/program."""
    with engine.begin() as conn:
        query = """
            SELECT oi.code, oi.description
            FROM outcomes_items oi
            JOIN outcomes_sets os ON oi.set_id = os.id
            WHERE os.degree_code = :degree_code
            AND os.set_type = 'peos'
            AND os.status = 'published'
            AND os.is_current = 1
        """
        params = {"degree_code": degree_code}
        
        if program_code:
            query += " AND (os.program_code = :program_code OR os.program_code IS NULL)"
            params["program_code"] = program_code
        else:
            query += " AND os.program_code IS NULL"

        query += " AND os.branch_code IS NULL" # PEOs are not usually at branch level
        
        query += " ORDER BY oi.sort_order, oi.code"
        
        result = conn.execute(sa_text(query), params)
        return [dict(row._mapping) for row in result]


# ===========================================================================
# DATA MODIFICATION FUNCTIONS
# ===========================================================================

def save_co(engine: Engine, offering_id: int, co_data: Dict, co_id: Optional[int] = None) -> bool:
    """Save a CO (create or update)."""
    try:
        with engine.begin() as conn:
            if co_id:
                # Update existing CO
                conn.execute(sa_text("""
                    UPDATE subject_cos
                    SET co_code = :co_code,
                        title = :title,
                        description = :description,
                        bloom_level = :bloom_level,
                        sequence = :sequence,
                        weight_in_direct = :weight_in_direct,
                        status = :status,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :co_id
                """), {
                    "co_id": co_id,
                    "co_code": co_data['co_code'],
                    "title": co_data['title'],
                    "description": co_data['description'],
                    "bloom_level": co_data['bloom_level'],
                    "sequence": co_data['sequence'],
                    "weight_in_direct": co_data['weight_in_direct'],
                    "status": co_data['status']
                })
            else:
                # Insert new CO
                result = conn.execute(sa_text("""
                    INSERT INTO subject_cos (
                        offering_id, co_code, title, description, bloom_level,
                        sequence, weight_in_direct, status,
                        created_at, updated_at
                    ) VALUES (
                        :offering_id, :co_code, :title, :description, :bloom_level,
                        :sequence, :weight_in_direct, :status,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                """), {
                    "offering_id": offering_id,
                    "co_code": co_data['co_code'],
                    "title": co_data['title'],
                    "description": co_data['description'],
                    "bloom_level": co_data['bloom_level'],
                    "sequence": co_data['sequence'],
                    "weight_in_direct": co_data['weight_in_direct'],
                    "status": co_data['status']
                })
                co_id = result.lastrowid
            
            # Save correlations
            if co_id:
                # Delete existing correlations
                conn.execute(sa_text("DELETE FROM co_po_correlations WHERE co_id = :co_id"), {"co_id": co_id})
                conn.execute(sa_text("DELETE FROM co_pso_correlations WHERE co_id = :co_id"), {"co_id": co_id})
                conn.execute(sa_text("DELETE FROM co_peo_correlations WHERE co_id = :co_id"), {"co_id": co_id})
                
                # Insert PO correlations
                for po_code, value in co_data.get('po_correlations', {}).items():
                    if value and value > 0:
                        conn.execute(sa_text("""
                            INSERT INTO co_po_correlations (co_id, po_code, correlation_value)
                            VALUES (:co_id, :po_code, :value)
                        """), {"co_id": co_id, "po_code": po_code, "value": value})
                
                # Insert PSO correlations
                for pso_code, value in co_data.get('pso_correlations', {}).items():
                    if value and value > 0:
                        conn.execute(sa_text("""
                            INSERT INTO co_pso_correlations (co_id, pso_code, correlation_value)
                            VALUES (:co_id, :pso_code, :value)
                        """), {"co_id": co_id, "pso_code": pso_code, "value": value})
                
                # Insert PEO correlations
                for peo_code, value in co_data.get('peo_correlations', {}).items():
                    if value and value > 0:
                        conn.execute(sa_text("""
                            INSERT INTO co_peo_correlations (co_id, peo_code, correlation_value)
                            VALUES (:co_id, :peo_code, :value)
                        """), {"co_id": co_id, "peo_code": peo_code, "value": value})
        
        return True
    except Exception as e:
        logger.error(f"Error saving CO: {e}", exc_info=True)
        return False


def delete_co(engine: Engine, co_id: int) -> bool:
    """Delete a CO."""
    try:
        with engine.begin() as conn:
            # Delete correlations first (should cascade, but being explicit)
            conn.execute(sa_text("DELETE FROM co_po_correlations WHERE co_id = :co_id"), {"co_id": co_id})
            conn.execute(sa_text("DELETE FROM co_pso_correlations WHERE co_id = :co_id"), {"co_id": co_id})
            conn.execute(sa_text("DELETE FROM co_peo_correlations WHERE co_id = :co_id"), {"co_id": co_id})
            
            # Delete CO
            conn.execute(sa_text("DELETE FROM subject_cos WHERE id = :co_id"), {"co_id": co_id})
        
        return True
    except Exception as e:
        logger.error(f"Error deleting CO: {e}", exc_info=True)
        return False


# ===========================================================================
# UI RENDERING FUNCTIONS
# ===========================================================================

def render_co_form(engine: Engine, offering_id: int, offering_info: Dict, 
                  pos: List[Dict], psos: List[Dict], peos: List[Dict],
                  co_data: Optional[Dict] = None):
    """Render form to add/edit a CO."""
    
    st.markdown("### ✏️ " + ("Edit Course Outcome" if co_data else "Add Course Outcome"))
    
    with st.form(key=f"co_form_{co_data['id'] if co_data else 'new'}"):
        col1, col2 = st.columns(2)
        
        with col1:
            co_code = st.text_input(
                "CO Code*",
                value=co_data['co_code'] if co_data else "",
                placeholder="e.g., CO1"
            )
            
            bloom_level = st.selectbox(
                "Bloom Level*",
                options=["Remember", "Understand", "Apply", "Analyze", "Evaluate", "Create"],
                index=["Remember", "Understand", "Apply", "Analyze", "Evaluate", "Create"].index(
                    co_data['bloom_level']) if co_data and co_data['bloom_level'] else 0
            )
        
        with col2:
            sequence = st.number_input(
                "Sequence*",
                min_value=1,
                max_value=100,
                value=int(co_data['sequence']) if co_data and co_data.get('sequence') else 1
            )
            
            weight_in_direct = st.number_input(
                "Weight in Direct Attainment*",
                min_value=0.0,
                max_value=1.0,
                step=0.01,
                value=float(co_data['weight_in_direct']) if co_data and co_data.get('weight_in_direct') else 0.0,
                help="Decimal between 0 and 1. All CO weights should sum to 1.0"
            )
        
        title = st.text_input(
            "CO Title*",
            value=co_data['title'] if co_data else "",
            placeholder="e.g., Analyze the complexity of data structures"
        )

        co_description = st.text_area(
            "CO Description*",
            value=co_data['description'] if co_data else "",
            height=100,
            placeholder="Describe what students should be able to do after completing this course"
        )
        
        status = st.selectbox(
            "Status",
            options=["draft", "published"],
            index=["draft", "published"].index(co_data['status']) if co_data and co_data.get('status') else 0
        )
        
        # Correlations section
        st.markdown("#### 🔗 Correlations")
        st.info("Correlation values: 1 = Low, 2 = Medium, 3 = High, 0 = None")
        
        # PO Correlations
        if pos:
            st.markdown("**Program Outcomes (POs)**")
            po_cols = st.columns(min(len(pos), 5))
            po_correlations = {}
            for idx, po in enumerate(pos):
                with po_cols[idx % len(po_cols)]:
                    current_value = co_data['po_correlations'].get(po['code'], 0) if co_data else 0
                    po_correlations[po['code']] = st.selectbox(
                        po['code'],
                        options=[0, 1, 2, 3],
                        index=[0, 1, 2, 3].index(current_value),
                        key=f"po_{po['code']}_{co_data['id'] if co_data else 'new'}",
                        help=po['description'][:100]
                    )
        else:
            st.warning("No POs found for this degree/program. Please publish them on the Outcomes page.")
            po_correlations = {}
        
        # PSO Correlations
        if psos:
            st.markdown("**Program Specific Outcomes (PSOs)**")
            pso_cols = st.columns(min(len(psos), 5))
            pso_correlations = {}
            for idx, pso in enumerate(psos):
                with pso_cols[idx % len(pso_cols)]:
                    current_value = co_data['pso_correlations'].get(pso['code'], 0) if co_data else 0
                    pso_correlations[pso['code']] = st.selectbox(
                        pso['code'],
                        options=[0, 1, 2, 3],
                        index=[0, 1, 2, 3].index(current_value),
                        key=f"pso_{pso['code']}_{co_data['id'] if co_data else 'new'}",
                        help=pso['description'][:100]
                    )
        else:
            st.warning("No PSOs found for this degree/program. Please publish them on the Outcomes page.")
            pso_correlations = {}
        
        # PEO Correlations
        if peos:
            st.markdown("**Program Educational Objectives (PEOs)**")
            peo_cols = st.columns(min(len(peos), 5))
            peo_correlations = {}
            for idx, peo in enumerate(peos):
                with peo_cols[idx % len(peo_cols)]:
                    current_value = co_data['peo_correlations'].get(peo['code'], 0) if co_data else 0
                    peo_correlations[peo['code']] = st.selectbox(
                        peo['code'],
                        options=[0, 1, 2, 3],
                        index=[0, 1, 2, 3].index(current_value),
                        key=f"peo_{peo['code']}_{co_data['id'] if co_data else 'new'}",
                        help=peo['description'][:100]
                    )
        else:
            st.warning("No PEOs found for this degree/program. Please publish them on the Outcomes page.")
            peo_correlations = {}
        
        # Submit button
        submitted = st.form_submit_button("💾 Save CO", use_container_width=True)
        
        if submitted:
            # Validation
            if not co_code or not title or not co_description:
                st.error("CO Code, Title, and Description are required")
                return
            
            new_co_data = {
                'co_code': co_code,
                'title': title,
                'description': co_description,
                'bloom_level': bloom_level,
                'sequence': sequence,
                'weight_in_direct': weight_in_direct,
                'status': status,
                'po_correlations': po_correlations,
                'pso_correlations': pso_correlations,
                'peo_correlations': peo_correlations
            }
            
            # Save CO
            co_id = co_data['id'] if co_data else None
            success = save_co(engine, offering_id, new_co_data, co_id)
            
            if success:
                st.success("✅ CO saved successfully!")
                st.session_state.editing_co = None # Clear editing state
                st.session_state.show_co_form = False # Close form
                st.rerun()
            else:
                st.error("❌ Failed to save CO")


def render_co_list(engine: Engine, offering_id: int, cos: List[Dict]):
    """Render list of COs with actions."""
    
    if not cos:
        st.info("No Course Outcomes defined for this offering yet.")
        return
    
    # Calculate total weight
    total_weight = sum(float(co.get('weight_in_direct', 0)) for co in cos)
    
    st.markdown(f"### 📚 Course Outcomes ({len(cos)} total)")
    
    # Weight validation
    if abs(total_weight - 1.0) > 0.01 and total_weight > 0:
        st.warning(f"⚠️ Total weight is {total_weight:.2f}. It should sum to 1.0 for proper attainment calculation.")
    elif total_weight == 0:
         st.info("Total CO weight is 0.0. Remember to assign weights for attainment calculation.")
    else:
        st.success(f"✅ Total weight: {total_weight:.2f}")
    
    # Display COs
    for co in cos:
        with st.expander(f"**{co['co_code']}**: {co.get('title', 'No Title')}", expanded=False):
            col1, col2, col3 = st.columns([2, 2, 1])
            
            with col1:
                st.markdown(f"**Description:** {co.get('description', 'No Description')}")
                st.markdown(f"**Bloom Level:** {co['bloom_level']}")
                st.markdown(f"**Sequence:** {co.get('sequence', 'N/A')}")
            
            with col2:
                st.markdown(f"**Weight:** {co.get('weight_in_direct', 'N/A')}")
                st.markdown(f"**Status:** {co.get('status', 'N/A')}")
            
            with col3:
                if st.button("✏️ Edit", key=f"edit_co_{co['id']}", use_container_width=True):
                    st.session_state.editing_co = co
                    st.session_state.show_co_form = False # Ensure add form is closed
                    st.rerun()
                
                if st.button("🗑️ Delete", key=f"delete_co_{co['id']}", use_container_width=True):
                    if delete_co(engine, co['id']):
                        st.success("✅ CO deleted")
                        st.session_state.editing_co = None # Clear editing state
                        st.rerun()
                    else:
                        st.error("❌ Failed to delete CO")
            
            # Show correlations
            if co.get('po_correlations'):
                st.markdown("**PO Correlations:**")
                po_text = ", ".join([f"{k}: {v}" for k, v in co['po_correlations'].items() if v > 0])
                st.markdown(po_text if po_text else "None")
            
            if co.get('pso_correlations'):
                st.markdown("**PSO Correlations:**")
                pso_text = ", ".join([f"{k}: {v}" for k, v in co['pso_correlations'].items() if v > 0])
                st.markdown(pso_text if po_text else "None")
            
            if co.get('peo_correlations'):
                st.markdown("**PEO Correlations:**")
                peo_text = ", ".join([f"{k}: {v}" for k, v in co['peo_correlations'].items() if v > 0])
                st.markdown(peo_text if po_text else "None")


def render_course_outcomes_tab(engine: Engine):
    """Main render function for Course Outcomes tab."""
    
    st.markdown("""
    Manage Course Outcomes (COs) for published subject offerings. Define what students
    should achieve upon completing a subject, and map COs to Program Outcomes (POs),
    Program Specific Outcomes (PSOs), and Program Educational Objectives (PEOs).
    """)
    
    # Offering selection
    st.markdown("### 🎯 Select Subject Offering")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Degree selection
        degrees = []
        with engine.begin() as conn:
            result = conn.execute(sa_text("""
                SELECT code, title FROM degrees WHERE active = 1 ORDER BY sort_order
            """))
            degrees = [dict(row._mapping) for row in result]
        
        if not degrees:
            st.warning("No degrees found")
            return
        
        degree_options = {f"{d['code']} - {d['title']}": d['code'] for d in degrees}
        selected_degree = st.selectbox("Degree", options=list(degree_options.keys()), key="co_degree")
        degree_code = degree_options[selected_degree]
    
    with col2:
        # Academic Year selection
        academic_years = []
        with engine.begin() as conn:
            result = conn.execute(sa_text("""
                SELECT ay_code FROM academic_years ORDER BY ay_code DESC
            """))
            academic_years = [dict(row._mapping) for row in result]
        
        if not academic_years:
            st.warning("No academic years found")
            return
        
        ay_options = {ay['ay_code']: ay['ay_code'] for ay in academic_years}
        selected_ay = st.selectbox("Academic Year", options=list(ay_options.keys()), key="co_ay")
        ay_code = selected_ay
    
    # Fetch offerings
    offerings = fetch_published_offerings_for_cos(engine, degree_code, ay_code)
    
    if not offerings:
        st.warning("No published offerings found for the selected degree and academic year.")
        return
    
    # Offering selection
    offering_options = {
        f"{o['subject_code']} - Y{o['year']}T{o['term']}" + 
        (f" - {o['program_code']}" if o['program_code'] else "") +
        (f"/{o['branch_code']}" if o['branch_code'] else "") +
        (f"/Div {o['division_code']}" if o['division_code'] else ""): o['id']
        for o in offerings
    }
    
    selected_offering_key = st.selectbox(
        "Subject Offering",
        options=list(offering_options.keys()),
        key="co_offering"
    )
    
    if not selected_offering_key:
        st.info("Please select a subject offering to manage COs.")
        return

    offering_id = offering_options[selected_offering_key]
    
    # Get full offering info
    offering_info = next(o for o in offerings if o['id'] == offering_id)
    
    # Fetch COs for this offering
    cos = fetch_cos_for_offering(engine, offering_id)
    
    # Fetch POs, PSOs, PEOs for correlations
    pos = fetch_pos_for_degree(engine, degree_code, offering_info.get('program_code'))
    psos = fetch_psos_for_program(engine, degree_code, offering_info.get('program_code'), 
                                   offering_info.get('branch_code'))
    peos = fetch_peos_for_degree(engine, degree_code, offering_info.get('program_code'))
    
    # Show current COs
    render_co_list(engine, offering_id, cos)
    
    st.markdown("---")
    
    # Add/Edit CO form
    editing_co = st.session_state.get('editing_co')
    
    if editing_co:
        render_co_form(engine, offering_id, offering_info, pos, psos, peos, editing_co)
        if st.button("❌ Cancel Edit"):
            del st.session_state.editing_co
            st.rerun()
    elif st.session_state.get('show_co_form'):
        render_co_form(engine, offering_id, offering_info, pos, psos, peos)
        if st.button("❌ Cancel"):
            st.session_state.show_co_form = False
            st.rerun()
    else:
        if st.button("➕ Add New CO", type="primary", use_container_width=True):
            st.session_state.show_co_form = True
            st.session_state.editing_co = None # Ensure edit mode is off
            st.rerun()
