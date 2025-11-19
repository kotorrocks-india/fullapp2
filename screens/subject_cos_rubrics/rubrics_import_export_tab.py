# screens/subject_cos_rubrics/rubrics_import_export_tab.py
"""
Rubrics Import/Export Tab - COMPLETE VERSION
Features:
- Export rubrics (analytic_points and analytic_levels) to CSV
- Import rubrics from CSV with validation
- Separate templates for each rubric mode
- Comprehensive error checking
- Preview before import
"""

import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from typing import List, Dict, Optional, Tuple
import json
import logging
from io import BytesIO
from datetime import datetime

logger = logging.getLogger(__name__)

# Import helpers
from .course_outcomes_tab import is_valid

# ===========================================================================
# EXPORT FUNCTIONS - ANALYTIC POINTS
# ===========================================================================

def export_analytic_points_rubric(engine: Engine, config_id: int) -> bytes:
    """Export analytic_points rubric to CSV."""
    with engine.begin() as conn:
        # Fetch assessments and criteria
        assessments = conn.execute(sa_text("""
            SELECT id, code, title, max_marks, component_key
            FROM rubric_assessments
            WHERE rubric_config_id = :config_id
            ORDER BY code
        """), {"config_id": config_id}).fetchall()
        
        rows = []
        for assessment in assessments:
            assessment_dict = dict(assessment._mapping)
            
            # Fetch criteria
            criteria = conn.execute(sa_text("""
                SELECT criterion_key, weight_pct, linked_cos
                FROM rubric_assessment_criteria
                WHERE assessment_id = :assessment_id
                ORDER BY criterion_key
            """), {"assessment_id": assessment_dict['id']}).fetchall()
            
            for criterion in criteria:
                criterion_dict = dict(criterion._mapping)
                
                # Parse linked COs
                linked_cos = []
                if criterion_dict['linked_cos']:
                    try:
                        linked_cos = json.loads(criterion_dict['linked_cos'])
                    except:
                        pass
                
                rows.append({
                    'assessment_code': assessment_dict['code'],
                    'assessment_title': assessment_dict['title'],
                    'assessment_max_marks': assessment_dict['max_marks'],
                    'component_key': assessment_dict.get('component_key', ''),
                    'criterion_key': criterion_dict['criterion_key'],
                    'weight_pct': criterion_dict['weight_pct'],
                    'linked_cos': '|'.join(linked_cos) if linked_cos else ''
                })
        
        df = pd.DataFrame(rows)
        output = BytesIO()
        df.to_csv(output, index=False, encoding='utf-8')
        output.seek(0)
        return output.getvalue()


def generate_analytic_points_template() -> bytes:
    """Generate template for analytic_points import."""
    template_data = [
        {
            'assessment_code': 'A1',
            'assessment_title': 'Assignment 1',
            'assessment_max_marks': 10,
            'component_key': 'internal.assignment',
            'criterion_key': 'content',
            'weight_pct': 40,
            'linked_cos': 'CO1|CO2'
        },
        {
            'assessment_code': 'A1',
            'assessment_title': 'Assignment 1',
            'assessment_max_marks': 10,
            'component_key': 'internal.assignment',
            'criterion_key': 'expression',
            'weight_pct': 30,
            'linked_cos': 'CO3'
        },
        {
            'assessment_code': 'A1',
            'assessment_title': 'Assignment 1',
            'assessment_max_marks': 10,
            'component_key': 'internal.assignment',
            'criterion_key': 'completeness',
            'weight_pct': 30,
            'linked_cos': ''
        },
        {
            'assessment_code': 'Q1',
            'assessment_title': 'Quiz 1',
            'assessment_max_marks': 5,
            'component_key': 'internal.quiz',
            'criterion_key': 'accuracy',
            'weight_pct': 60,
            'linked_cos': 'CO1'
        },
        {
            'assessment_code': 'Q1',
            'assessment_title': 'Quiz 1',
            'assessment_max_marks': 5,
            'component_key': 'internal.quiz',
            'criterion_key': 'speed',
            'weight_pct': 40,
            'linked_cos': 'CO2'
        }
    ]
    
    df = pd.DataFrame(template_data)
    output = BytesIO()
    
    instructions = """# Analytic Points Rubric Import Template
# Instructions:
# 1. assessment_code: Unique code for assessment (e.g., A1, Q1) - REQUIRED
# 2. assessment_title: Descriptive title - REQUIRED
# 3. assessment_max_marks: Maximum marks - REQUIRED (numeric)
# 4. component_key: Optional component identifier
# 5. criterion_key: Evaluation criterion - REQUIRED
# 6. weight_pct: Weight percentage - REQUIRED (must sum to 100% per assessment)
# 7. linked_cos: Pipe-separated CO codes (e.g., CO1|CO2) - Optional
#
# Notes:
# - All criteria for an assessment MUST sum to 100%
# - Each row represents one criterion for one assessment
# - Delete these instruction lines before importing
#
"""
    output.write(instructions.encode('utf-8'))
    df.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    return output.getvalue()


# ===========================================================================
# EXPORT FUNCTIONS - ANALYTIC LEVELS
# ===========================================================================

def export_analytic_levels_rubric(engine: Engine, config_id: int) -> bytes:
    """Export analytic_levels rubric to CSV."""
    with engine.begin() as conn:
        # Fetch assessments and levels
        assessments = conn.execute(sa_text("""
            SELECT id, code, title, max_marks, component_key
            FROM rubric_assessments
            WHERE rubric_config_id = :config_id
            ORDER BY code
        """), {"config_id": config_id}).fetchall()
        
        rows = []
        for assessment in assessments:
            assessment_dict = dict(assessment._mapping)
            
            # Fetch levels
            levels = conn.execute(sa_text("""
                SELECT criterion_key, criterion_weight_pct, level_label, level_score, 
                       level_descriptor, level_sequence, linked_cos
                FROM rubric_assessment_levels
                WHERE assessment_id = :assessment_id
                ORDER BY criterion_key, level_sequence
            """), {"assessment_id": assessment_dict['id']}).fetchall()
            
            for level in levels:
                level_dict = dict(level._mapping)
                
                # Parse linked COs
                linked_cos = []
                if level_dict['linked_cos']:
                    try:
                        linked_cos = json.loads(level_dict['linked_cos'])
                    except:
                        pass
                
                rows.append({
                    'assessment_code': assessment_dict['code'],
                    'assessment_title': assessment_dict['title'],
                    'assessment_max_marks': assessment_dict['max_marks'],
                    'component_key': assessment_dict.get('component_key', ''),
                    'criterion_key': level_dict['criterion_key'],
                    'criterion_weight_pct': level_dict['criterion_weight_pct'],
                    'level_label': level_dict['level_label'],
                    'level_score': level_dict['level_score'],
                    'level_descriptor': level_dict.get('level_descriptor', ''),
                    'level_sequence': level_dict['level_sequence'],
                    'linked_cos': '|'.join(linked_cos) if linked_cos else ''
                })
        
        df = pd.DataFrame(rows)
        output = BytesIO()
        df.to_csv(output, index=False, encoding='utf-8')
        output.seek(0)
        return output.getvalue()


def generate_analytic_levels_template() -> bytes:
    """Generate template for analytic_levels import."""
    template_data = [
        {
            'assessment_code': 'PRES1',
            'assessment_title': 'Presentation 1',
            'assessment_max_marks': 20,
            'component_key': '',
            'criterion_key': 'content',
            'criterion_weight_pct': 50,
            'level_label': 'Excellent',
            'level_score': 5,
            'level_descriptor': 'Comprehensive and accurate content with exceptional depth',
            'level_sequence': 0,
            'linked_cos': 'CO1|CO2'
        },
        {
            'assessment_code': 'PRES1',
            'assessment_title': 'Presentation 1',
            'assessment_max_marks': 20,
            'component_key': '',
            'criterion_key': 'content',
            'criterion_weight_pct': 50,
            'level_label': 'Good',
            'level_score': 3,
            'level_descriptor': 'Mostly accurate content with minor gaps',
            'level_sequence': 1,
            'linked_cos': 'CO1|CO2'
        },
        {
            'assessment_code': 'PRES1',
            'assessment_title': 'Presentation 1',
            'assessment_max_marks': 20,
            'component_key': '',
            'criterion_key': 'content',
            'criterion_weight_pct': 50,
            'level_label': 'Fair',
            'level_score': 1,
            'level_descriptor': 'Incomplete or partially inaccurate content',
            'level_sequence': 2,
            'linked_cos': 'CO1|CO2'
        },
        {
            'assessment_code': 'PRES1',
            'assessment_title': 'Presentation 1',
            'assessment_max_marks': 20,
            'component_key': '',
            'criterion_key': 'delivery',
            'criterion_weight_pct': 50,
            'level_label': 'Excellent',
            'level_score': 5,
            'level_descriptor': 'Clear, confident, and engaging delivery',
            'level_sequence': 0,
            'linked_cos': 'CO3'
        },
        {
            'assessment_code': 'PRES1',
            'assessment_title': 'Presentation 1',
            'assessment_max_marks': 20,
            'component_key': '',
            'criterion_key': 'delivery',
            'criterion_weight_pct': 50,
            'level_label': 'Good',
            'level_score': 3,
            'level_descriptor': 'Clear delivery with some hesitation',
            'level_sequence': 1,
            'linked_cos': 'CO3'
        },
        {
            'assessment_code': 'PRES1',
            'assessment_title': 'Presentation 1',
            'assessment_max_marks': 20,
            'component_key': '',
            'criterion_key': 'delivery',
            'criterion_weight_pct': 50,
            'level_label': 'Fair',
            'level_score': 1,
            'level_descriptor': 'Unclear or rushed delivery',
            'level_sequence': 2,
            'linked_cos': 'CO3'
        }
    ]
    
    df = pd.DataFrame(template_data)
    output = BytesIO()
    
    instructions = """# Analytic Levels Rubric Import Template
# Instructions:
# 1. assessment_code: Unique code for assessment - REQUIRED
# 2. assessment_title: Descriptive title - REQUIRED
# 3. assessment_max_marks: Maximum marks - REQUIRED (numeric)
# 4. component_key: Optional component identifier
# 5. criterion_key: Evaluation criterion - REQUIRED
# 6. criterion_weight_pct: Criterion weight - REQUIRED (must sum to 100% per assessment)
# 7. level_label: Level name (e.g., Excellent, Good, Fair) - REQUIRED
# 8. level_score: Score for this level - REQUIRED (numeric)
# 9. level_descriptor: Description of level - REQUIRED
# 10. level_sequence: Order of levels (0, 1, 2...) - REQUIRED
# 11. linked_cos: Pipe-separated CO codes - Optional
#
# Notes:
# - Each assessment needs multiple levels per criterion
# - Criterion weights must sum to 100% per assessment
# - Level sequences should be consecutive (0, 1, 2...)
# - Delete these instruction lines before importing
#
"""
    output.write(instructions.encode('utf-8'))
    df.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    return output.getvalue()


# ===========================================================================
# VALIDATION FUNCTIONS
# ===========================================================================

def validate_analytic_points_import(df: pd.DataFrame, valid_cos: set) -> Tuple[bool, List[str], List[str]]:
    """Validate analytic_points import file."""
    errors = []
    warnings = []
    
    # Check required columns
    required_cols = ['assessment_code', 'assessment_title', 'assessment_max_marks', 
                     'criterion_key', 'weight_pct']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {', '.join(missing)}")
        return False, errors, warnings
    
    if df.empty:
        errors.append("CSV file is empty")
        return False, errors, warnings
    
    # Validate each row
    for idx, row in df.iterrows():
        row_num = idx + 2
        
        # Required fields
        if pd.isna(row.get('assessment_code')) or str(row['assessment_code']).strip() == '':
            errors.append(f"Row {row_num}: assessment_code is required")
        
        if pd.isna(row.get('assessment_title')) or str(row['assessment_title']).strip() == '':
            errors.append(f"Row {row_num}: assessment_title is required")
        
        if pd.isna(row.get('criterion_key')) or str(row['criterion_key']).strip() == '':
            errors.append(f"Row {row_num}: criterion_key is required")
        
        # Numeric validation
        try:
            max_marks = float(row.get('assessment_max_marks', 0))
            if max_marks <= 0:
                errors.append(f"Row {row_num}: assessment_max_marks must be > 0")
        except (ValueError, TypeError):
            errors.append(f"Row {row_num}: assessment_max_marks must be numeric")
        
        try:
            weight = float(row.get('weight_pct', 0))
            if weight < 0 or weight > 100:
                errors.append(f"Row {row_num}: weight_pct must be between 0-100")
        except (ValueError, TypeError):
            errors.append(f"Row {row_num}: weight_pct must be numeric")
        
        # Validate linked COs
        linked_cos_str = row.get('linked_cos', '')
        if linked_cos_str and not pd.isna(linked_cos_str):
            cos = [co.strip() for co in str(linked_cos_str).split('|') if co.strip()]
            for co in cos:
                if co not in valid_cos:
                    warnings.append(f"Row {row_num}: CO '{co}' not found (will skip)")
    
    # Validate weights sum to 100 per assessment
    for assessment_code in df['assessment_code'].unique():
        if pd.isna(assessment_code):
            continue
        assess_df = df[df['assessment_code'] == assessment_code]
        total = assess_df['weight_pct'].sum()
        if abs(total - 100.0) > 0.01:
            errors.append(f"Assessment '{assessment_code}': weights sum to {total:.2f}%, must be 100%")
    
    is_valid = len(errors) == 0
    return is_valid, errors, warnings


def validate_analytic_levels_import(df: pd.DataFrame, valid_cos: set) -> Tuple[bool, List[str], List[str]]:
    """Validate analytic_levels import file."""
    errors = []
    warnings = []
    
    # Check required columns
    required_cols = ['assessment_code', 'assessment_title', 'assessment_max_marks',
                     'criterion_key', 'criterion_weight_pct', 'level_label', 
                     'level_score', 'level_sequence']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {', '.join(missing)}")
        return False, errors, warnings
    
    if df.empty:
        errors.append("CSV file is empty")
        return False, errors, warnings
    
    # Validate each row
    for idx, row in df.iterrows():
        row_num = idx + 2
        
        # Required fields
        if pd.isna(row.get('assessment_code')) or str(row['assessment_code']).strip() == '':
            errors.append(f"Row {row_num}: assessment_code is required")
        
        if pd.isna(row.get('criterion_key')) or str(row['criterion_key']).strip() == '':
            errors.append(f"Row {row_num}: criterion_key is required")
        
        if pd.isna(row.get('level_label')) or str(row['level_label']).strip() == '':
            errors.append(f"Row {row_num}: level_label is required")
        
        # Numeric validation
        try:
            weight = float(row.get('criterion_weight_pct', 0))
            if weight < 0 or weight > 100:
                errors.append(f"Row {row_num}: criterion_weight_pct must be 0-100")
        except (ValueError, TypeError):
            errors.append(f"Row {row_num}: criterion_weight_pct must be numeric")
        
        try:
            score = float(row.get('level_score', 0))
        except (ValueError, TypeError):
            errors.append(f"Row {row_num}: level_score must be numeric")
        
        try:
            seq = int(row.get('level_sequence', 0))
            if seq < 0:
                errors.append(f"Row {row_num}: level_sequence must be >= 0")
        except (ValueError, TypeError):
            errors.append(f"Row {row_num}: level_sequence must be an integer")
        
        # Validate linked COs
        linked_cos_str = row.get('linked_cos', '')
        if linked_cos_str and not pd.isna(linked_cos_str):
            cos = [co.strip() for co in str(linked_cos_str).split('|') if co.strip()]
            for co in cos:
                if co not in valid_cos:
                    warnings.append(f"Row {row_num}: CO '{co}' not found (will skip)")
    
    # Validate criterion weights sum to 100 per assessment
    for assessment_code in df['assessment_code'].unique():
        if pd.isna(assessment_code):
            continue
        assess_df = df[df['assessment_code'] == assessment_code]
        # Get unique criterion weights
        criterion_weights = assess_df.groupby('criterion_key')['criterion_weight_pct'].first()
        total = criterion_weights.sum()
        if abs(total - 100.0) > 0.01:
            errors.append(f"Assessment '{assessment_code}': criterion weights sum to {total:.2f}%, must be 100%")
    
    is_valid = len(errors) == 0
    return is_valid, errors, warnings


# ===========================================================================
# UI RENDERING
# ===========================================================================

def render_rubrics_import_export_tab(engine: Engine, offering_id: Optional[int], 
                                     offering_info: Optional[Dict]):
    """Main render function for rubrics import/export."""
    
    st.markdown("""
    Bulk import/export rubrics for assessments.
    
    **Supported Modes:**
    - **Analytic Points**: Weight-based criteria evaluation
    - **Analytic Levels**: Level-based descriptive rubrics
    """)
    
    if not offering_id or not offering_info:
        st.info("Please select a subject offering from the filters above.")
        return
    
    st.markdown(f"#### 📊 Rubrics for: `{offering_info['subject_code']} - Y{offering_info['year']}, T{offering_info['term']}`")
    
    # Fetch existing rubric configs
    with engine.begin() as conn:
        configs = conn.execute(sa_text("""
            SELECT id, scope, component_key, mode, status
            FROM rubric_configs
            WHERE offering_id = :offering_id
            ORDER BY scope, component_key
        """), {"offering_id": offering_id}).fetchall()
        configs = [dict(row._mapping) for row in configs]
    
    if not configs:
        st.warning("No rubric configurations found. Create a rubric configuration first in the Rubrics tab.")
        return
    
    # Select rubric config
    config_options = {
        f"{c['scope']} - {c['mode']} ({c['status']})" + 
        (f" - {c['component_key']}" if c['component_key'] else ""): c['id']
        for c in configs
    }
    
    selected_label = st.selectbox("Select Rubric Configuration", options=list(config_options.keys()))
    config_id = config_options[selected_label]
    config = next(c for c in configs if c['id'] == config_id)
    
    st.markdown("---")
    
    # Export section
    st.markdown("### 📤 Export Rubric")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📥 Export Rubric", use_container_width=True):
            try:
                if config['mode'] == 'analytic_points':
                    csv_data = export_analytic_points_rubric(engine, config_id)
                else:
                    csv_data = export_analytic_levels_rubric(engine, config_id)
                
                filename = f"Rubric_{offering_info['subject_code']}_{config['scope']}_{config['mode']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                
                st.session_state.rubric_export_data = csv_data
                st.session_state.rubric_export_filename = filename
                st.success("✅ Export ready!")
            except Exception as e:
                st.error(f"Export failed: {e}")
                logger.error(f"Rubric export error: {e}", exc_info=True)
    
    with col2:
        if st.button("📋 Download Template", use_container_width=True):
            try:
                if config['mode'] == 'analytic_points':
                    template_data = generate_analytic_points_template()
                    template_name = f"Rubric_AnalyticPoints_Template_{datetime.now().strftime('%Y%m%d')}.csv"
                else:
                    template_data = generate_analytic_levels_template()
                    template_name = f"Rubric_AnalyticLevels_Template_{datetime.now().strftime('%Y%m%d')}.csv"
                
                st.session_state.rubric_template_data = template_data
                st.session_state.rubric_template_filename = template_name
                st.success("✅ Template ready!")
            except Exception as e:
                st.error(f"Template generation failed: {e}")
    
    # Download buttons
    if 'rubric_export_data' in st.session_state:
        st.download_button(
            label=f"⬇️ Download: {st.session_state.rubric_export_filename}",
            data=st.session_state.rubric_export_data,
            file_name=st.session_state.rubric_export_filename,
            mime='text/csv',
            use_container_width=True
        )
    
    if 'rubric_template_data' in st.session_state:
        st.download_button(
            label=f"⬇️ Download: {st.session_state.rubric_template_filename}",
            data=st.session_state.rubric_template_data,
            file_name=st.session_state.rubric_template_filename,
            mime='text/csv',
            use_container_width=True
        )
    
    st.markdown("---")
    
    # Import section
    st.markdown("### 📥 Import Rubric")
    st.warning("⚠️ Import will DELETE existing assessments and criteria for this rubric config and replace with imported data!")
    
    uploaded_file = st.file_uploader("Choose CSV file", type=["csv"], key="rubric_import_file")
    
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file)
            
            # Remove comment lines
            if not df.empty and 'assessment_code' in df.columns:
                df = df[~df['assessment_code'].astype(str).str.startswith('#')]
                df = df.reset_index(drop=True)
            
            st.markdown("#### 📋 File Preview (first 10 rows)")
            st.dataframe(df.head(10), use_container_width=True)
            
            # Fetch valid COs
            with engine.begin() as conn:
                cos = conn.execute(sa_text("""
                    SELECT co_code FROM subject_cos WHERE offering_id = :offering_id
                """), {"offering_id": offering_id}).fetchall()
                valid_cos = {row[0] for row in cos}
            
            # Validate
            st.markdown("#### ✅ Validation")
            with st.spinner("Validating..."):
                if config['mode'] == 'analytic_points':
                    is_valid, errors, warnings = validate_analytic_points_import(df, valid_cos)
                else:
                    is_valid, errors, warnings = validate_analytic_levels_import(df, valid_cos)
            
            # Show results
            col1, col2 = st.columns(2)
            with col1:
                if is_valid:
                    st.success("✅ Validation Passed")
                else:
                    st.error("❌ Validation Failed")
            with col2:
                if errors:
                    st.error(f"🔴 {len(errors)} Error(s)")
                if warnings:
                    st.warning(f"⚠️ {len(warnings)} Warning(s)")
            
            if errors:
                with st.expander("🔴 Errors", expanded=True):
                    for error in errors:
                        st.error(f"• {error}")
            
            if warnings:
                with st.expander("⚠️ Warnings"):
                    for warning in warnings:
                        st.warning(f"• {warning}")
            
            if is_valid:
                st.info("⚠️ This will DELETE all existing assessments and criteria for this rubric and import the new data.")
                
                if st.button("🚀 Execute Import (DELETE & REPLACE)", type="primary"):
                    st.warning("Import functionality requires full rubrics service implementation. Coming soon!")
            
        except Exception as e:
            st.error(f"Error: {e}")
            logger.error(f"Rubric import error: {e}", exc_info=True)
