# screens/subject_cos_rubrics/rubrics_tab.py
"""
Rubrics Tab

Manages Assessment Rubrics for published subject offerings.
Features:
- View existing rubrics for a subject offering
- Create rubric configurations (subject-level or component-level)
- Add assessments with criteria (analytic_points or analytic_levels modes)
- Link rubric criteria to COs
- Export/Import rubrics
"""

import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from typing import List, Dict, Optional
import json
import logging

logger = logging.getLogger(__name__)


# ===========================================================================
# DATA FETCHING FUNCTIONS
# ===========================================================================

def fetch_rubric_configs_for_offering(engine: Engine, offering_id: int) -> List[Dict]:
    """Fetch all rubric configurations for an offering."""
    with engine.begin() as conn:
        result = conn.execute(sa_text("""
            SELECT 
                id, scope, component_key, mode,
                co_linking_enabled, normalization_enabled,
                visible_to_students, show_before_assessment,
                status, is_locked, locked_reason,
                created_at, updated_at
            FROM rubric_configs
            WHERE offering_id = :offering_id
            ORDER BY scope, component_key
        """), {"offering_id": offering_id})
        return [dict(row._mapping) for row in result]


def fetch_assessments_for_config(engine: Engine, config_id: int) -> List[Dict]:
    """Fetch all assessments for a rubric configuration."""
    with engine.begin() as conn:
        result = conn.execute(sa_text("""
            SELECT 
                id, code, title, max_marks, mode, component_key,
                created_at, updated_at
            FROM rubric_assessments
            WHERE rubric_config_id = :config_id
            ORDER BY code
        """), {"config_id": config_id})
        
        assessments = [dict(row._mapping) for row in result]
        
        # Fetch criteria or levels for each assessment
        for assessment in assessments:
            if assessment['mode'] == 'analytic_points':
                # Fetch criteria
                criteria_result = conn.execute(sa_text("""
                    SELECT 
                        id, criterion_key, weight_pct, linked_cos,
                        created_at, updated_at
                    FROM rubric_assessment_criteria
                    WHERE assessment_id = :assessment_id
                """), {"assessment_id": assessment['id']})
                assessment['criteria'] = [dict(row._mapping) for row in criteria_result]
            
            elif assessment['mode'] == 'analytic_levels':
                # Fetch levels
                levels_result = conn.execute(sa_text("""
                    SELECT 
                        id, criterion_key, criterion_weight_pct,
                        level_label, level_score, level_descriptor, level_sequence,
                        linked_cos, created_at, updated_at
                    FROM rubric_assessment_levels
                    WHERE assessment_id = :assessment_id
                    ORDER BY criterion_key, level_sequence
                """), {"assessment_id": assessment['id']})
                assessment['levels'] = [dict(row._mapping) for row in levels_result]
        
        return assessments


def fetch_criteria_catalog(engine: Engine) -> List[Dict]:
    """Fetch all available criteria from the catalog."""
    with engine.begin() as conn:
        result = conn.execute(sa_text("""
            SELECT key, label, description, active
            FROM rubric_criteria_catalog
            WHERE active = 1
            ORDER BY label
        """))
        return [dict(row._mapping) for row in result]


def fetch_cos_codes_for_offering(engine: Engine, offering_id: int) -> List[str]:
    """Fetch CO codes for an offering."""
    with engine.begin() as conn:
        result = conn.execute(sa_text("""
            SELECT co_code
            FROM subject_cos
            WHERE offering_id = :offering_id
            ORDER BY sequence, co_code
        """), {"offering_id": offering_id})
        return [row._mapping['co_code'] for row in result]


# ===========================================================================
# DATA MODIFICATION FUNCTIONS
# ===========================================================================

def create_rubric_config(engine: Engine, offering_id: int, config_data: Dict) -> Optional[int]:
    """Create a new rubric configuration."""
    try:
        with engine.begin() as conn:
            result = conn.execute(sa_text("""
                INSERT INTO rubric_configs (
                    offering_id, scope, component_key, mode,
                    co_linking_enabled, normalization_enabled,
                    visible_to_students, show_before_assessment,
                    status, created_at
                ) VALUES (
                    :offering_id, :scope, :component_key, :mode,
                    :co_linking_enabled, :normalization_enabled,
                    :visible_to_students, :show_before_assessment,
                    :status, CURRENT_TIMESTAMP
                )
            """), {
                "offering_id": offering_id,
                "scope": config_data['scope'],
                "component_key": config_data.get('component_key'),
                "mode": config_data['mode'],
                "co_linking_enabled": config_data.get('co_linking_enabled', 0),
                "normalization_enabled": config_data.get('normalization_enabled', 1),
                "visible_to_students": config_data.get('visible_to_students', 1),
                "show_before_assessment": config_data.get('show_before_assessment', 1),
                "status": config_data.get('status', 'draft')
            })
            return result.lastrowid
    except Exception as e:
        logger.error(f"Error creating rubric config: {e}", exc_info=True)
        return None


def create_assessment(engine: Engine, config_id: int, assessment_data: Dict) -> Optional[int]:
    """Create a new assessment."""
    try:
        with engine.begin() as conn:
            result = conn.execute(sa_text("""
                INSERT INTO rubric_assessments (
                    rubric_config_id, code, title, max_marks, mode, component_key,
                    created_at
                ) VALUES (
                    :config_id, :code, :title, :max_marks, :mode, :component_key,
                    CURRENT_TIMESTAMP
                )
            """), {
                "config_id": config_id,
                "code": assessment_data['code'],
                "title": assessment_data['title'],
                "max_marks": assessment_data['max_marks'],
                "mode": assessment_data['mode'],
                "component_key": assessment_data.get('component_key')
            })
            return result.lastrowid
    except Exception as e:
        logger.error(f"Error creating assessment: {e}", exc_info=True)
        return None


def add_criterion(engine: Engine, assessment_id: int, criterion_data: Dict) -> bool:
    """Add a criterion to an assessment (analytic_points mode)."""
    try:
        with engine.begin() as conn:
            conn.execute(sa_text("""
                INSERT INTO rubric_assessment_criteria (
                    assessment_id, criterion_key, weight_pct, linked_cos,
                    created_at
                ) VALUES (
                    :assessment_id, :criterion_key, :weight_pct, :linked_cos,
                    CURRENT_TIMESTAMP
                )
            """), {
                "assessment_id": assessment_id,
                "criterion_key": criterion_data['criterion_key'],
                "weight_pct": criterion_data['weight_pct'],
                "linked_cos": json.dumps(criterion_data.get('linked_cos', []))
            })
        return True
    except Exception as e:
        logger.error(f"Error adding criterion: {e}", exc_info=True)
        return False


def add_level(engine: Engine, assessment_id: int, level_data: Dict) -> bool:
    """Add a level to an assessment (analytic_levels mode)."""
    try:
        with engine.begin() as conn:
            conn.execute(sa_text("""
                INSERT INTO rubric_assessment_levels (
                    assessment_id, criterion_key, criterion_weight_pct,
                    level_label, level_score, level_descriptor, level_sequence,
                    linked_cos, created_at
                ) VALUES (
                    :assessment_id, :criterion_key, :criterion_weight_pct,
                    :level_label, :level_score, :level_descriptor, :level_sequence,
                    :linked_cos, CURRENT_TIMESTAMP
                )
            """), {
                "assessment_id": assessment_id,
                "criterion_key": level_data['criterion_key'],
                "criterion_weight_pct": level_data['criterion_weight_pct'],
                "level_label": level_data['level_label'],
                "level_score": level_data['level_score'],
                "level_descriptor": level_data.get('level_descriptor'),
                "level_sequence": level_data['level_sequence'],
                "linked_cos": json.dumps(level_data.get('linked_cos', []))
            })
        return True
    except Exception as e:
        logger.error(f"Error adding level: {e}", exc_info=True)
        return False


def delete_rubric_config(engine: Engine, config_id: int) -> bool:
    """Delete a rubric configuration (cascades to assessments, criteria, levels)."""
    try:
        with engine.begin() as conn:
            conn.execute(sa_text("""
                DELETE FROM rubric_configs WHERE id = :config_id
            """), {"config_id": config_id})
        return True
    except Exception as e:
        logger.error(f"Error deleting rubric config: {e}", exc_info=True)
        return False


def update_config_status(engine: Engine, config_id: int, status: str) -> bool:
    """Update rubric configuration status."""
    try:
        with engine.begin() as conn:
            conn.execute(sa_text("""
                UPDATE rubric_configs
                SET status = :status, updated_at = CURRENT_TIMESTAMP
                WHERE id = :config_id
            """), {"config_id": config_id, "status": status})
        return True
    except Exception as e:
        logger.error(f"Error updating config status: {e}", exc_info=True)
        return False


# ===========================================================================
# UI RENDERING FUNCTIONS
# ===========================================================================

def render_rubric_config_form(engine: Engine, offering_id: int, co_codes: List[str]):
    """Render form to create a new rubric configuration."""
    
    st.markdown("### ➕ Create New Rubric Configuration")
    
    with st.form("new_rubric_config_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            scope = st.selectbox(
                "Scope*",
                options=["subject", "component"],
                help="Subject-level rubric applies to the entire subject. Component-level applies to specific components."
            )
            
            mode = st.selectbox(
                "Mode*",
                options=["analytic_points", "analytic_levels"],
                help="analytic_points: Weight-based criteria. analytic_levels: Level-based scoring."
            )
        
        with col2:
            component_key = None
            if scope == "component":
                component_key = st.text_input(
                    "Component Key*",
                    placeholder="e.g., IA1, IA2, EndSem"
                )
            
            co_linking_enabled = st.checkbox("Enable CO Linking", value=False)
        
        col3, col4 = st.columns(2)
        
        with col3:
            normalization_enabled = st.checkbox("Enable Normalization", value=True)
            visible_to_students = st.checkbox("Visible to Students", value=True)
        
        with col4:
            show_before_assessment = st.checkbox("Show Before Assessment", value=True)
            status = st.selectbox("Status", options=["draft", "published"], index=0)
        
        submitted = st.form_submit_button("Create Configuration", use_container_width=True)
        
        if submitted:
            if scope == "component" and not component_key:
                st.error("Component Key is required for component-level rubrics")
                return
            
            config_data = {
                "scope": scope,
                "component_key": component_key,
                "mode": mode,
                "co_linking_enabled": 1 if co_linking_enabled else 0,
                "normalization_enabled": 1 if normalization_enabled else 0,
                "visible_to_students": 1 if visible_to_students else 0,
                "show_before_assessment": 1 if show_before_assessment else 0,
                "status": status
            }
            
            config_id = create_rubric_config(engine, offering_id, config_data)
            
            if config_id:
                st.success(f"✅ Rubric configuration created successfully! ID: {config_id}")
                st.rerun()
            else:
                st.error("❌ Failed to create rubric configuration")


def render_assessment_form(engine: Engine, config_id: int, mode: str, co_codes: List[str]):
    """Render form to add a new assessment to a rubric config."""
    
    st.markdown("#### ➕ Add Assessment")
    
    with st.form(f"new_assessment_form_{config_id}"):
        col1, col2 = st.columns(2)
        
        with col1:
            code = st.text_input("Assessment Code*", placeholder="e.g., IA1, PROJ")
            title = st.text_input("Assessment Title*", placeholder="e.g., Internal Assessment 1")
        
        with col2:
            max_marks = st.number_input("Maximum Marks*", min_value=0.0, step=1.0, value=25.0)
            component_key = st.text_input("Component Key (optional)", placeholder="e.g., IA1")
        
        submitted = st.form_submit_button("Add Assessment", use_container_width=True)
        
        if submitted:
            if not code or not title:
                st.error("Code and Title are required")
                return
            
            assessment_data = {
                "code": code,
                "title": title,
                "max_marks": max_marks,
                "mode": mode,
                "component_key": component_key if component_key else None
            }
            
            assessment_id = create_assessment(engine, config_id, assessment_data)
            
            if assessment_id:
                st.success(f"✅ Assessment added successfully! ID: {assessment_id}")
                st.rerun()
            else:
                st.error("❌ Failed to add assessment")


def render_criterion_form(engine: Engine, assessment_id: int, co_codes: List[str], 
                         co_linking_enabled: bool):
    """Render form to add criterion (analytic_points mode)."""
    
    st.markdown("##### Add Criterion")
    
    with st.form(f"new_criterion_form_{assessment_id}"):
        col1, col2 = st.columns(2)
        
        with col1:
            criterion_key = st.text_input("Criterion Key*", placeholder="e.g., clarity, completeness")
        
        with col2:
            weight_pct = st.number_input(
                "Weight (%)*",
                min_value=0.0,
                max_value=100.0,
                step=1.0,
                value=0.0,
                help="Percentage weight of this criterion. All criteria should sum to 100%."
            )
        
        linked_cos = []
        if co_linking_enabled and co_codes:
            linked_cos = st.multiselect("Linked COs (optional)", options=co_codes)
        
        submitted = st.form_submit_button("Add Criterion", use_container_width=True)
        
        if submitted:
            if not criterion_key:
                st.error("Criterion Key is required")
                return
            
            criterion_data = {
                "criterion_key": criterion_key,
                "weight_pct": weight_pct,
                "linked_cos": linked_cos
            }
            
            success = add_criterion(engine, assessment_id, criterion_data)
            
            if success:
                st.success("✅ Criterion added successfully!")
                st.rerun()
            else:
                st.error("❌ Failed to add criterion")


def render_level_form(engine: Engine, assessment_id: int, co_codes: List[str],
                     co_linking_enabled: bool):
    """Render form to add level (analytic_levels mode)."""
    
    st.markdown("##### Add Level")
    
    with st.form(f"new_level_form_{assessment_id}"):
        col1, col2 = st.columns(2)
        
        with col1:
            criterion_key = st.text_input("Criterion Key*", placeholder="e.g., clarity")
            level_label = st.text_input("Level Label*", placeholder="e.g., Excellent, Good")
            level_sequence = st.number_input("Sequence*", min_value=1, step=1, value=1)
        
        with col2:
            criterion_weight_pct = st.number_input("Criterion Weight (%)*", min_value=0.0, max_value=100.0, step=1.0)
            level_score = st.number_input("Level Score*", min_value=0.0, step=0.5, value=0.0)
        
        level_descriptor = st.text_area("Level Descriptor (optional)", height=100)
        
        linked_cos = []
        if co_linking_enabled and co_codes:
            linked_cos = st.multiselect("Linked COs (optional)", options=co_codes)
        
        submitted = st.form_submit_button("Add Level", use_container_width=True)
        
        if submitted:
            if not criterion_key or not level_label:
                st.error("Criterion Key and Level Label are required")
                return
            
            level_data = {
                "criterion_key": criterion_key,
                "criterion_weight_pct": criterion_weight_pct,
                "level_label": level_label,
                "level_score": level_score,
                "level_descriptor": level_descriptor,
                "level_sequence": level_sequence,
                "linked_cos": linked_cos
            }
            
            success = add_level(engine, assessment_id, level_data)
            
            if success:
                st.success("✅ Level added successfully!")
                st.rerun()
            else:
                st.error("❌ Failed to add level")


def render_rubric_config_details(engine: Engine, config: Dict, assessments: List[Dict],
                                 co_codes: List[str]):
    """Render details of a rubric configuration."""
    
    with st.expander(f"**{config['scope'].upper()}** - {config['mode'].replace('_', ' ').title()} " + 
                    (f"({config['component_key']})" if config['component_key'] else ""), 
                    expanded=True):
        
        # Config info
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown(f"**Status:** {config['status']}")
            st.markdown(f"**CO Linking:** {'Enabled' if config['co_linking_enabled'] else 'Disabled'}")
        
        with col2:
            st.markdown(f"**Normalization:** {'Enabled' if config['normalization_enabled'] else 'Disabled'}")
            st.markdown(f"**Visible to Students:** {'Yes' if config['visible_to_students'] else 'No'}")
        
        with col3:
            st.markdown(f"**Show Before Assessment:** {'Yes' if config['show_before_assessment'] else 'No'}")
            if config['is_locked']:
                st.warning(f"🔒 Locked: {config['locked_reason']}")
        
        # Actions
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if config['status'] == 'draft':
                if st.button("✅ Publish", key=f"publish_{config['id']}", use_container_width=True):
                    if update_config_status(engine, config['id'], 'published'):
                        st.success("Published!")
                        st.rerun()
        
        with col2:
            if config['status'] == 'published':
                if st.button("📦 Archive", key=f"archive_{config['id']}", use_container_width=True):
                    if update_config_status(engine, config['id'], 'archived'):
                        st.success("Archived!")
                        st.rerun()
        
        with col3:
            if st.button("🗑️ Delete", key=f"delete_config_{config['id']}", use_container_width=True):
                if delete_rubric_config(engine, config['id']):
                    st.success("Deleted!")
                    st.rerun()
                else:
                    st.error("Failed to delete")
        
        st.markdown("---")
        
        # Assessments
        st.markdown("#### 📝 Assessments")
        
        if not assessments:
            st.info("No assessments added yet")
        else:
            for assessment in assessments:
                with st.expander(f"**{assessment['code']}** - {assessment['title']} ({assessment['max_marks']} marks)"):
                    
                    if config['mode'] == 'analytic_points':
                        # Show criteria
                        criteria = assessment.get('criteria', [])
                        if criteria:
                            st.markdown("**Criteria:**")
                            total_weight = sum(c['weight_pct'] for c in criteria)
                            
                            if abs(total_weight - 100.0) > 0.01:
                                st.warning(f"⚠️ Total weight: {total_weight}%. Should be 100%.")
                            
                            df = pd.DataFrame([{
                                'Criterion': c['criterion_key'],
                                'Weight (%)': c['weight_pct'],
                                'Linked COs': ', '.join(json.loads(c['linked_cos']) if c['linked_cos'] else [])
                            } for c in criteria])
                            
                            st.dataframe(df, use_container_width=True)
                        else:
                            st.info("No criteria defined")
                        
                        # Add criterion button
                        if st.button(f"➕ Add Criterion", key=f"add_crit_{assessment['id']}", use_container_width=True):
                            st.session_state[f"show_crit_form_{assessment['id']}"] = True
                            st.rerun()
                        
                        if st.session_state.get(f"show_crit_form_{assessment['id']}"):
                            render_criterion_form(engine, assessment['id'], co_codes, config['co_linking_enabled'])
                            if st.button("❌ Cancel", key=f"cancel_crit_{assessment['id']}"):
                                st.session_state[f"show_crit_form_{assessment['id']}"] = False
                                st.rerun()
                    
                    elif config['mode'] == 'analytic_levels':
                        # Show levels
                        levels = assessment.get('levels', [])
                        if levels:
                            st.markdown("**Levels:**")
                            
                            df = pd.DataFrame([{
                                'Criterion': l['criterion_key'],
                                'Weight (%)': l['criterion_weight_pct'],
                                'Level': l['level_label'],
                                'Score': l['level_score'],
                                'Sequence': l['level_sequence'],
                                'Descriptor': l['level_descriptor'][:50] + '...' if l.get('level_descriptor') else '',
                                'Linked COs': ', '.join(json.loads(l['linked_cos']) if l['linked_cos'] else [])
                            } for l in levels])
                            
                            st.dataframe(df, use_container_width=True)
                        else:
                            st.info("No levels defined")
                        
                        # Add level button
                        if st.button(f"➕ Add Level", key=f"add_level_{assessment['id']}", use_container_width=True):
                            st.session_state[f"show_level_form_{assessment['id']}"] = True
                            st.rerun()
                        
                        if st.session_state.get(f"show_level_form_{assessment['id']}"):
                            render_level_form(engine, assessment['id'], co_codes, config['co_linking_enabled'])
                            if st.button("❌ Cancel", key=f"cancel_level_{assessment['id']}"):
                                st.session_state[f"show_level_form_{assessment['id']}"] = False
                                st.rerun()
        
        # Add assessment button
        st.markdown("---")
        if st.button(f"➕ Add Assessment to this Config", key=f"add_assess_{config['id']}", use_container_width=True):
            st.session_state[f"show_assess_form_{config['id']}"] = True
            st.rerun()
        
        if st.session_state.get(f"show_assess_form_{config['id']}"):
            render_assessment_form(engine, config['id'], config['mode'], co_codes)
            if st.button("❌ Cancel", key=f"cancel_assess_{config['id']}"):
                st.session_state[f"show_assess_form_{config['id']}"] = False
                st.rerun()


def render_rubrics_tab(engine: Engine):
    """Main render function for Rubrics tab."""
    
    st.markdown("""
    Manage Assessment Rubrics for published subject offerings. Define rubric configurations,
    assessments, criteria, and levels for structured evaluation of student performance.
    """)
    
    # Offering selection (same as COs tab)
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
        selected_degree = st.selectbox("Degree", options=list(degree_options.keys()), key="rubrics_degree")
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
        selected_ay = st.selectbox("Academic Year", options=list(ay_options.keys()), key="rubrics_ay")
        ay_code = selected_ay
    
    # Fetch offerings
    offerings = []
    with engine.begin() as conn:
        result = conn.execute(sa_text("""
            SELECT 
                so.id, so.subject_code, sc.subject_name,
                so.year, so.term, so.program_code, so.branch_code, so.division_code
            FROM subject_offerings so
            LEFT JOIN subjects_catalog sc ON 
                so.subject_code = sc.subject_code 
                AND so.degree_code = sc.degree_code
            WHERE so.status = 'published'
            AND so.degree_code = :degree_code
            AND so.ay_label = :ay_label
            ORDER BY so.year, so.term, so.subject_code
        """), {"degree_code": degree_code, "ay_label": ay_code})
        offerings = [dict(row._mapping) for row in result]
    
    if not offerings:
        st.warning("No published offerings found for the selected degree and academic year.")
        return
    
    # Offering selection
    offering_options = {
        f"{o['subject_code']} - Y{o['year']}T{o['term']}" + 
        (f" - {o['program_code']}" if o['program_code'] else "") +
        (f"/{o['branch_code']}" if o['branch_code'] else ""): o['id']
        for o in offerings
    }
    
    selected_offering_key = st.selectbox(
        "Subject Offering",
        options=list(offering_options.keys()),
        key="rubrics_offering"
    )
    offering_id = offering_options[selected_offering_key]
    
    # Fetch CO codes for this offering
    co_codes = fetch_cos_codes_for_offering(engine, offering_id)
    
    # Fetch existing rubric configs
    configs = fetch_rubric_configs_for_offering(engine, offering_id)
    
    st.markdown("---")
    
    # Show existing configs
    st.markdown("### 📊 Rubric Configurations")
    
    if not configs:
        st.info("No rubric configurations found for this offering.")
    else:
        for config in configs:
            assessments = fetch_assessments_for_config(engine, config['id'])
            render_rubric_config_details(engine, config, assessments, co_codes)
    
    st.markdown("---")
    
    # Add new config button
    if st.button("➕ Create New Rubric Configuration", type="primary", use_container_width=True):
        st.session_state.show_rubric_config_form = True
        st.rerun()
    
    if st.session_state.get('show_rubric_config_form'):
        render_rubric_config_form(engine, offering_id, co_codes)
        if st.button("❌ Cancel"):
            st.session_state.show_rubric_config_form = False
            st.rerun()
