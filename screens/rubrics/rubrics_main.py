# screens/rubrics/rubrics_main.py
"""Complete Rubrics Management UI (Slide 21)"""

import streamlit as st
import pandas as pd
import json
from datetime import datetime
from typing import Optional, List, Dict
import traceback

from core.settings import load_settings
from core.db import get_engine
from core.policy import require_page, can_edit_page, user_roles
from core.forms import tagline, success, error_box

# Import service
try:
    from screens.rubrics.rubrics_service import RubricsService
    from screens.subject_cos.models import RubricConfig, RubricAssessment, AuditEntry
except:
    from rubrics_service import RubricsService
    from sc_models import RubricConfig, RubricAssessment, AuditEntry

PAGE_TITLE = "📋 Rubrics Management"


# ===========================================================================
# HELPER FUNCTIONS
# ===========================================================================

@st.cache_data(ttl=300)
def fetch_offerings(_engine):
    """Fetch published offerings."""
    with _engine.begin() as conn:
        result = conn.execute("""
        SELECT 
            so.id, so.subject_code, so.subject_name, so.subject_type,
            so.degree_code, so.ay_label, so.year, so.term,
            so.program_code, so.branch_code, so.status
        FROM subject_offerings so
        WHERE so.status = 'published'
        ORDER BY so.ay_label DESC, so.year, so.term, so.subject_code
        """).fetchall()
        return [dict(row._mapping) for row in result]


def get_offering_label(offering: Dict) -> str:
    """Generate offering label."""
    parts = [
        offering['subject_code'],
        offering['subject_name'],
        f"({offering['ay_label']}, Y{offering['year']}, T{offering['term']})"
    ]
    if offering.get('program_code'):
        parts.insert(2, offering['program_code'])
    if offering.get('branch_code'):
        parts.insert(3, offering['branch_code'])
    return " - ".join(parts)


# ===========================================================================
# TAB 1: RUBRIC CONFIGURATION
# ===========================================================================

def render_rubric_config_tab(engine, service: RubricsService, actor: str, can_edit: bool):
    """Tab for creating and managing rubric configurations."""
    st.subheader("📝 Rubric Configuration")

    # Filter: Select offering
    offerings = fetch_offerings(engine)
    if not offerings:
        st.info("No published offerings found. Please publish offerings first.")
        return

    offering_id = st.selectbox(
        "Select Offering*",
        options=[o['id'] for o in offerings],
        format_func=lambda x: get_offering_label(
            next(o for o in offerings if o['id'] == x)
        ),
        key="rubric_offering"
    )

    if not offering_id:
        return

    offering = next(o for o in offerings if o['id'] == offering_id)

    st.markdown("---")

    # Show existing rubrics for this offering
    rubrics = service.list_rubrics_for_offering(offering_id)

    if rubrics:
        st.markdown("#### Existing Rubrics")
        for rubric in rubrics:
            with st.expander(
                f"📋 {rubric['scope'].upper()} Rubric "
                f"(v{rubric['version']}) - {rubric['status'].upper()}"
            ):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.write(f"**Mode:** {rubric['mode']}")
                    st.write(f"**CO Linking:** {'Yes' if rubric['co_linking_enabled'] else 'No'}")
                    st.write(f"**Visible to Students:** {'Yes' if rubric['visible_to_students'] else 'No'}")
                
                with col2:
                    st.write(f"**Normalization:** {'Yes' if rubric['normalization_enabled'] else 'No'}")
                    st.write(f"**Show Before Assessment:** {'Yes' if rubric['show_before_assessment'] else 'No'}")
                    st.write(f"**Version:** {rubric['version']}")
                
                with col3:
                    st.write(f"**Locked:** {'Yes' if rubric['is_locked'] else 'No'}")
                    if rubric['locked_reason']:
                        st.write(f"**Lock Reason:** {rubric['locked_reason']}")
                    st.write(f"**Status:** {rubric['status']}")

                # Actions
                action_cols = st.columns(5)
                
                with action_cols[0]:
                    if st.button("View Details", key=f"view_{rubric['id']}"):
                        st.session_state[f'viewing_rubric_{rubric["id"]}'] = True
                        st.rerun()
                
                if can_edit:
                    with action_cols[1]:
                        if rubric['status'] == 'draft' and not rubric['is_locked']:
                            if st.button("Publish", key=f"publish_{rubric['id']}"):
                                try:
                                    audit = AuditEntry(
                                        actor_id=actor, actor_role='admin',
                                        operation='publish_rubric'
                                    )
                                    service.publish_rubric(rubric['id'], audit)
                                    success("Rubric published successfully!")
                                    st.cache_data.clear()
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                    
                    with action_cols[2]:
                        if rubric['is_locked']:
                            if st.button("Unlock", key=f"unlock_{rubric['id']}"):
                                st.session_state[f'unlocking_{rubric["id"]}'] = True
                        else:
                            if st.button("Lock", key=f"lock_{rubric['id']}"):
                                st.session_state[f'locking_{rubric["id"]}'] = True
                    
                    with action_cols[3]:
                        if st.button("New Version", key=f"version_{rubric['id']}"):
                            try:
                                audit = AuditEntry(
                                    actor_id=actor, actor_role='admin',
                                    operation='version_rubric', 
                                    reason='Major update required'
                                )
                                new_version = service.create_rubric_version(rubric['id'], audit)
                                success(f"Created version {new_version}")
                                st.cache_data.clear()
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {str(e)}")

                # Handle lock/unlock dialogs
                if st.session_state.get(f'locking_{rubric["id"]}'):
                    reason = st.text_input("Lock Reason*", key=f"lock_reason_{rubric['id']}")
                    col_a, col_b = st.columns(2)
                    with col_a:
                        if st.button("Confirm Lock", key=f"confirm_lock_{rubric['id']}"):
                            if reason:
                                try:
                                    audit = AuditEntry(
                                        actor_id=actor, actor_role='admin',
                                        operation='lock_rubric', reason=reason
                                    )
                                    service.lock_rubric(rubric['id'], reason, audit)
                                    success("Rubric locked")
                                    del st.session_state[f'locking_{rubric["id"]}']
                                    st.cache_data.clear()
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                    with col_b:
                        if st.button("Cancel", key=f"cancel_lock_{rubric['id']}"):
                            del st.session_state[f'locking_{rubric["id"]}']
                            st.rerun()

                if st.session_state.get(f'unlocking_{rubric["id"]}'):
                    reason = st.text_input("Unlock Reason*", key=f"unlock_reason_{rubric['id']}")
                    st.warning("⚠️ Step-up authentication required")
                    col_a, col_b = st.columns(2)
                    with col_a:
                        if st.button("Confirm Unlock", key=f"confirm_unlock_{rubric['id']}"):
                            if reason:
                                try:
                                    audit = AuditEntry(
                                        actor_id=actor, actor_role='admin',
                                        operation='unlock_rubric', reason=reason,
                                        step_up_performed=1
                                    )
                                    service.unlock_rubric(rubric['id'], reason, audit)
                                    success("Rubric unlocked")
                                    del st.session_state[f'unlocking_{rubric["id"]}']
                                    st.cache_data.clear()
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                    with col_b:
                        if st.button("Cancel", key=f"cancel_unlock_{rubric['id']}"):
                            del st.session_state[f'unlocking_{rubric["id"]}']
                            st.rerun()

    # Create new rubric
    if can_edit:
        st.markdown("---")
        st.markdown("#### Create New Rubric Configuration")

        with st.form("create_rubric_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                scope = st.selectbox(
                    "Scope*",
                    options=['subject', 'component'],
                    help="Subject-level or component-level rubric"
                )
                
                component_key = None
                if scope == 'component':
                    component_key = st.text_input(
                        "Component Key*",
                        help="e.g., internal.assignment"
                    )
                
                mode = st.selectbox(
                    "Mode*",
                    options=['analytic_points', 'analytic_levels'],
                    format_func=lambda x: 'Analytic Points' if x == 'analytic_points' else 'Analytic Levels'
                )
            
            with col2:
                co_linking = st.checkbox("Enable CO Linking", value=False)
                normalization = st.checkbox("Enable Normalization", value=True)
                visible_students = st.checkbox("Visible to Students", value=True)
                show_before = st.checkbox("Show Before Assessment", value=True)

            submitted = st.form_submit_button("Create Rubric Configuration", type="primary")

            if submitted:
                try:
                    config = RubricConfig(
                        offering_id=offering_id,
                        scope=scope,
                        component_key=component_key,
                        mode=mode,
                        co_linking_enabled=1 if co_linking else 0,
                        normalization_enabled=1 if normalization else 0,
                        visible_to_students=1 if visible_students else 0,
                        show_before_assessment=1 if show_before else 0,
                        version=1,
                        is_locked=0,
                        status='draft'
                    )
                    
                    audit = AuditEntry(
                        actor_id=actor,
                        actor_role='admin',
                        operation='create_rubric'
                    )
                    
                    config_id = service.create_rubric_config(config, audit)
                    success(f"Rubric configuration created (ID: {config_id})")
                    st.cache_data.clear()
                    st.rerun()
                
                except Exception as e:
                    st.error(f"Error: {str(e)}")
                    st.code(traceback.format_exc())


# ===========================================================================
# TAB 2: ASSESSMENTS MANAGER
# ===========================================================================

def render_assessments_tab(engine, service: RubricsService, actor: str, can_edit: bool):
    """Tab for managing assessments within rubrics."""
    st.subheader("📊 Assessments Manager")

    # Select rubric config
    offerings = fetch_offerings(engine)
    if not offerings:
        st.info("No published offerings found.")
        return

    offering_id = st.selectbox(
        "Select Offering*",
        options=[o['id'] for o in offerings],
        format_func=lambda x: get_offering_label(
            next(o for o in offerings if o['id'] == x)
        ),
        key="assess_offering"
    )

    if not offering_id:
        return

    rubrics = service.list_rubrics_for_offering(offering_id)
    
    if not rubrics:
        st.warning("No rubric configurations found for this offering.")
        st.info("Create a rubric configuration first in the 'Rubric Configuration' tab.")
        return

    rubric_id = st.selectbox(
        "Select Rubric*",
        options=[r['id'] for r in rubrics],
        format_func=lambda x: f"{next(r['scope'] for r in rubrics if r['id'] == x)} - "
                             f"v{next(r['version'] for r in rubrics if r['id'] == x)}",
        key="assess_rubric"
    )

    if not rubric_id:
        return

    rubric = service.get_rubric_config(rubric_id)
    
    st.markdown("---")
    st.markdown(f"#### Assessments ({rubric['mode']})")

    # Get complete rubric with assessments
    complete_rubric = service.get_rubric(rubric['offering_id'], rubric['scope'], 
                                        rubric['component_key'])

    # Display existing assessments
    if complete_rubric and complete_rubric.get('assessments'):
        for idx, assessment in enumerate(complete_rubric['assessments']):
            with st.expander(f"📝 {assessment['code']} - {assessment['title']}", expanded=idx==0):
                st.write(f"**Max Marks:** {assessment['max_marks']}")
                st.write(f"**Mode:** {assessment['mode']}")
                if assessment.get('component_key'):
                    st.write(f"**Component:** {assessment['component_key']}")

                if assessment['mode'] == 'analytic_points':
                    if assessment.get('criteria'):
                        st.markdown("##### Criteria Weights")
                        criteria_df = pd.DataFrame(assessment['criteria'])
                        if not criteria_df.empty:
                            display_cols = ['criterion_key', 'weight_pct']
                            if 'linked_cos' in criteria_df.columns:
                                display_cols.append('linked_cos')
                            st.dataframe(criteria_df[display_cols], use_container_width=True)
                            
                            # Validate
                            total = criteria_df['weight_pct'].sum()
                            if abs(total - 100.0) < 0.01:
                                st.success(f"✓ Weights sum to 100%")
                            else:
                                st.error(f"⚠ Weights sum to {total}%, not 100%")
                    else:
                        st.info("No criteria defined yet")
                
                elif assessment['mode'] == 'analytic_levels':
                    if assessment.get('levels'):
                        st.markdown("##### Level Descriptors")
                        levels_df = pd.DataFrame(assessment['levels'])
                        if not levels_df.empty:
                            # Group by criterion
                            for criterion_key in levels_df['criterion_key'].unique():
                                st.markdown(f"**{criterion_key}**")
                                criterion_levels = levels_df[
                                    levels_df['criterion_key'] == criterion_key
                                ].sort_values('level_sequence')
                                
                                for _, level in criterion_levels.iterrows():
                                    st.write(
                                        f"- {level['level_label']} "
                                        f"(Score: {level['level_score']}): "
                                        f"{level.get('level_descriptor', 'N/A')}"
                                    )
                    else:
                        st.info("No levels defined yet")

                # Actions
                if can_edit and not rubric['is_locked']:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        if st.button("Edit", key=f"edit_assess_{assessment['id']}"):
                            st.session_state[f'editing_{assessment["id"]}'] = True
                            st.rerun()
                    with col2:
                        if st.button("Add Criteria/Levels", key=f"add_crit_{assessment['id']}"):
                            st.session_state[f'adding_criteria_{assessment["id"]}'] = True
                            st.rerun()
                    with col3:
                        if st.button("Delete", key=f"del_assess_{assessment['id']}"):
                            if st.session_state.get(f'confirm_del_{assessment["id"]}'):
                                try:
                                    audit = AuditEntry(
                                        actor_id=actor, actor_role='admin',
                                        operation='delete_assessment'
                                    )
                                    service.delete_assessment(assessment['id'], audit)
                                    success("Assessment deleted")
                                    st.cache_data.clear()
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                            else:
                                st.session_state[f'confirm_del_{assessment["id"]}'] = True
                                st.warning("Click again to confirm deletion")
    else:
        st.info("No assessments defined yet")

    # Add new assessment
    if can_edit and not rubric['is_locked']:
        st.markdown("---")
        st.markdown("#### Add New Assessment")

        with st.form("add_assessment_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                assess_code = st.text_input("Assessment Code*", placeholder="e.g., A1, QUIZ1")
                assess_title = st.text_input("Title*", placeholder="e.g., Assignment 1 - Data Structures")
            
            with col2:
                assess_max = st.number_input("Max Marks*", min_value=0.0, value=10.0, step=0.5)
                assess_component = st.text_input("Component Key (optional)", 
                                                placeholder="e.g., internal.assignment")

            submitted = st.form_submit_button("Add Assessment", type="primary")

            if submitted:
                if not assess_code or not assess_title:
                    st.error("Code and Title are required")
                else:
                    try:
                        assessment = RubricAssessment(
                            rubric_config_id=rubric_id,
                            code=assess_code,
                            title=assess_title,
                            max_marks=assess_max,
                            mode=rubric['mode'],
                            component_key=assess_component if assess_component else None
                        )
                        
                        audit = AuditEntry(
                            actor_id=actor, actor_role='admin',
                            operation='add_assessment'
                        )
                        
                        assess_id = service.add_assessment(assessment, audit)
                        success(f"Assessment added (ID: {assess_id})")
                        st.cache_data.clear()
                        st.rerun()
                    
                    except Exception as e:
                        st.error(f"Error: {str(e)}")


# ===========================================================================
# TAB 3: CRITERIA/LEVELS EDITOR
# ===========================================================================

def render_criteria_editor_tab(engine, service: RubricsService, actor: str, can_edit: bool):
    """Tab for editing criteria weights or level descriptors."""
    st.subheader("⚙️ Criteria & Levels Editor")

    # Select assessment
    offerings = fetch_offerings(engine)
    if not offerings:
        st.info("No published offerings found.")
        return

    offering_id = st.selectbox(
        "Select Offering*",
        options=[o['id'] for o in offerings],
        format_func=lambda x: get_offering_label(
            next(o for o in offerings if o['id'] == x)
        ),
        key="crit_offering"
    )

    if not offering_id:
        return

    rubrics = service.list_rubrics_for_offering(offering_id)
    if not rubrics:
        st.info("No rubrics found")
        return

    rubric_id = st.selectbox(
        "Select Rubric*",
        options=[r['id'] for r in rubrics],
        format_func=lambda x: f"{next(r['scope'] for r in rubrics if r['id'] == x)} - "
                             f"v{next(r['version'] for r in rubrics if r['id'] == x)}",
        key="crit_rubric"
    )

    if not rubric_id:
        return

    rubric = service.get_rubric_config(rubric_id)
    complete_rubric = service.get_rubric(rubric['offering_id'], rubric['scope'], 
                                        rubric['component_key'])

    if not complete_rubric or not complete_rubric.get('assessments'):
        st.info("No assessments found in this rubric")
        return

    assessment = st.selectbox(
        "Select Assessment*",
        options=[a['id'] for a in complete_rubric['assessments']],
        format_func=lambda x: next(
            f"{a['code']} - {a['title']}" 
            for a in complete_rubric['assessments'] if a['id'] == x
        ),
        key="crit_assessment"
    )

    if not assessment:
        return

    selected_assessment = next(
        a for a in complete_rubric['assessments'] if a['id'] == assessment
    )

    st.markdown("---")

    # Get criteria catalog
    criteria_catalog = service.get_criteria_catalog()

    if selected_assessment['mode'] == 'analytic_points':
        st.markdown("#### Criteria Weights (Analytic Points)")
        
        # Show existing
        if selected_assessment.get('criteria'):
            st.markdown("##### Current Criteria")
            df = pd.DataFrame(selected_assessment['criteria'])
            st.dataframe(df[['criterion_key', 'weight_pct']], use_container_width=True)
            
            total = df['weight_pct'].sum()
            if abs(total - 100.0) < 0.01:
                st.success(f"✓ Total: {total}%")
            else:
                st.error(f"⚠ Total: {total}% (must be 100%)")

        # Add new criteria
        if can_edit and not rubric['is_locked']:
            st.markdown("##### Add Criteria Weights")
            
            with st.form("add_criteria_form"):
                st.write("Select criteria from catalog or add custom")
                
                num_criteria = st.number_input(
                    "Number of Criteria", min_value=1, max_value=10, value=3
                )
                
                criteria_data = {}
                for i in range(num_criteria):
                    col1, col2 = st.columns(2)
                    with col1:
                        criterion_key = st.selectbox(
                            f"Criterion {i+1}",
                            options=[c['key'] for c in criteria_catalog] + ['custom'],
                            key=f"crit_key_{i}"
                        )
                        if criterion_key == 'custom':
                            criterion_key = st.text_input(
                                "Custom Criterion Key",
                                key=f"crit_custom_{i}"
                            )
                    with col2:
                        weight = st.number_input(
                            f"Weight %",
                            min_value=0.0, max_value=100.0, value=0.0,
                            key=f"crit_weight_{i}"
                        )
                    
                    if criterion_key:
                        criteria_data[criterion_key] = weight

                submitted = st.form_submit_button("Add Criteria", type="primary")

                if submitted:
                    if not criteria_data:
                        st.error("No criteria specified")
                    else:
                        total = sum(criteria_data.values())
                        if abs(total - 100.0) > 0.01:
                            st.error(f"Weights must sum to 100%, got {total}%")
                        else:
                            try:
                                audit = AuditEntry(
                                    actor_id=actor, actor_role='admin',
                                    operation='add_criteria'
                                )
                                service.add_criteria_weights(
                                    assessment, criteria_data, audit_entry=audit
                                )
                                success("Criteria added successfully")
                                st.cache_data.clear()
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {str(e)}")

    elif selected_assessment['mode'] == 'analytic_levels':
        st.markdown("#### Level Descriptors (Analytic Levels)")
        
        # Show existing
        if selected_assessment.get('levels'):
            st.markdown("##### Current Levels")
            df = pd.DataFrame(selected_assessment['levels'])
            
            for criterion_key in df['criterion_key'].unique():
                with st.expander(f"📌 {criterion_key}"):
                    criterion_df = df[df['criterion_key'] == criterion_key].sort_values('level_sequence')
                    st.dataframe(
                        criterion_df[['level_label', 'level_score', 'level_descriptor']],
                        use_container_width=True
                    )

        # Add new levels
        if can_edit and not rubric['is_locked']:
            st.markdown("##### Add Level Descriptors")
            
            with st.form("add_levels_form"):
                criterion_key = st.selectbox(
                    "Criterion*",
                    options=[c['key'] for c in criteria_catalog] + ['custom']
                )
                
                if criterion_key == 'custom':
                    criterion_key = st.text_input("Custom Criterion Key*")
                
                criterion_weight = st.number_input(
                    "Criterion Weight %*",
                    min_value=0.0, max_value=100.0, value=25.0
                )
                
                num_levels = st.number_input(
                    "Number of Levels", min_value=2, max_value=6, value=4
                )
                
                levels_data = []
                for i in range(num_levels):
                    st.markdown(f"**Level {i+1}**")
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        label = st.text_input(
                            "Label", value=f"Level {i+1}",
                            key=f"level_label_{i}"
                        )
                    with col2:
                        score = st.number_input(
                            "Score", min_value=0.0, value=float(i+1),
                            key=f"level_score_{i}"
                        )
                    with col3:
                        descriptor = st.text_area(
                            "Descriptor", height=60,
                            key=f"level_desc_{i}"
                        )
                    
                    levels_data.append({
                        'criterion_key': criterion_key,
                        'criterion_weight_pct': criterion_weight,
                        'level_label': label,
                        'level_score': score,
                        'level_descriptor': descriptor,
                        'level_sequence': i
                    })

                submitted = st.form_submit_button("Add Levels", type="primary")

                if submitted:
                    if not criterion_key:
                        st.error("Criterion key required")
                    else:
                        try:
                            audit = AuditEntry(
                                actor_id=actor, actor_role='admin',
                                operation='add_levels'
                            )
                            service.add_assessment_levels(
                                assessment, levels_data, audit
                            )
                            success("Levels added successfully")
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {str(e)}")


# ===========================================================================
# TAB 4: CRITERIA CATALOG
# ===========================================================================

def render_criteria_catalog_tab(engine, service: RubricsService, actor: str, can_edit: bool):
    """Tab for managing global criteria catalog."""
    st.subheader("📚 Criteria Catalog")
    st.caption("Global reusable criteria for all rubrics")

    # Get catalog
    catalog = service.get_criteria_catalog(active_only=False)

    if catalog:
        df = pd.DataFrame(catalog)
        st.dataframe(
            df[['key', 'label', 'description', 'active']],
            use_container_width=True
        )
    else:
        st.info("No criteria in catalog")

    # Add new criterion
    if can_edit:
        st.markdown("---")
        st.markdown("#### Add New Criterion")

        with st.form("add_catalog_criterion"):
            col1, col2 = st.columns(2)
            
            with col1:
                key = st.text_input("Key*", placeholder="e.g., creativity")
                label = st.text_input("Label*", placeholder="e.g., Creativity")
            
            with col2:
                description = st.text_area("Description", height=80)

            submitted = st.form_submit_button("Add Criterion", type="primary")

            if submitted:
                if not key or not label:
                    st.error("Key and Label are required")
                else:
                    try:
                        criterion_id = service.add_catalog_criterion(key, label, description)
                        success(f"Criterion added (ID: {criterion_id})")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {str(e)}")


# ===========================================================================
# TAB 5: VALIDATION & PREVIEW
# ===========================================================================

def render_validation_tab(engine, service: RubricsService, actor: str, can_edit: bool):
    """Tab for validating rubrics before publishing."""
    st.subheader("✅ Validation & Preview")

    # Select rubric
    offerings = fetch_offerings(engine)
    if not offerings:
        st.info("No published offerings found.")
        return

    offering_id = st.selectbox(
        "Select Offering*",
        options=[o['id'] for o in offerings],
        format_func=lambda x: get_offering_label(
            next(o for o in offerings if o['id'] == x)
        ),
        key="val_offering"
    )

    if not offering_id:
        return

    rubrics = service.list_rubrics_for_offering(offering_id)
    if not rubrics:
        st.info("No rubrics found")
        return

    rubric_id = st.selectbox(
        "Select Rubric*",
        options=[r['id'] for r in rubrics],
        format_func=lambda x: f"{next(r['scope'] for r in rubrics if r['id'] == x)} - "
                             f"v{next(r['version'] for r in rubrics if r['id'] == x)}",
        key="val_rubric"
    )

    if not rubric_id:
        return

    # Run validation
    st.markdown("---")
    
    if st.button("Validate Rubric", type="primary"):
        result = service.validate_rubric_complete(rubric_id)
        
        st.markdown("#### Validation Results")
        
        if result['is_valid']:
            st.success("✅ Rubric is valid and ready to publish!")
        else:
            st.error("❌ Validation failed")
        
        if result.get('errors'):
            st.markdown("##### Errors")
            for error in result['errors']:
                st.error(f"• {error}")
        
        if result.get('warnings'):
            st.markdown("##### Warnings")
            for warning in result['warnings']:
                st.warning(f"• {warning}")

    # Preview
    st.markdown("---")
    st.markdown("#### Rubric Preview")
    
    complete_rubric = service.get_rubric(
        rubrics[0]['offering_id'],
        next(r['scope'] for r in rubrics if r['id'] == rubric_id),
        next((r['component_key'] for r in rubrics if r['id'] == rubric_id), None)
    )

    if complete_rubric:
        st.json(complete_rubric, expanded=False)


# ===========================================================================
# TAB 6: AUDIT TRAIL
# ===========================================================================

def render_audit_tab(engine, service: RubricsService, actor: str, can_edit: bool):
    """Tab for viewing rubric audit trail."""
    st.subheader("📜 Audit Trail")

    # Select offering
    offerings = fetch_offerings(engine)
    if not offerings:
        st.info("No published offerings found.")
        return

    offering_id = st.selectbox(
        "Select Offering*",
        options=[o['id'] for o in offerings],
        format_func=lambda x: get_offering_label(
            next(o for o in offerings if o['id'] == x)
        ),
        key="audit_offering"
    )

    if not offering_id:
        return

    # Get audit records
    audit_records = service._fetch_all("""
    SELECT * FROM rubrics_audit
    WHERE offering_id = :offering_id
    ORDER BY occurred_at_utc DESC
    LIMIT 100
    """, {'offering_id': offering_id})

    if audit_records:
        df = pd.DataFrame(audit_records)
        display_cols = [
            'occurred_at_utc', 'action', 'scope', 
            'actor_id', 'operation', 'reason'
        ]
        st.dataframe(df[display_cols], use_container_width=True)
    else:
        st.info("No audit records found")


# ===========================================================================
# MAIN RENDER
# ===========================================================================

@require_page("Rubrics Management")
def render():
    """Main render function for Rubrics Management."""
    st.title(PAGE_TITLE)
    tagline()

    settings = load_settings()
    engine = get_engine(settings.db.url)

    # Initialize service
    service = RubricsService(engine)

    user = st.session_state.get("user") or {}
    actor = user.get("email", "system")
    roles = user_roles()
    can_edit = can_edit_page("Rubrics Management", roles)

    if not can_edit:
        st.info("📖 Read-only mode: You have view access but cannot modify data.")

    # Create tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "Rubric Configuration",
        "Assessments Manager",
        "Criteria & Levels",
        "Criteria Catalog",
        "Validation",
        "Audit Trail"
    ])

    with tab1:
        render_rubric_config_tab(engine, service, actor, can_edit)

    with tab2:
        render_assessments_tab(engine, service, actor, can_edit)

    with tab3:
        render_criteria_editor_tab(engine, service, actor, can_edit)

    with tab4:
        render_criteria_catalog_tab(engine, service, actor, can_edit)

    with tab5:
        render_validation_tab(engine, service, actor, can_edit)

    with tab6:
        render_audit_tab(engine, service, actor, can_edit)


if __name__ == "__main__":
    render()
