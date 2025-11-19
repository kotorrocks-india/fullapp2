# screens/subject_cos_rubrics/course_outcomes_import_export_tab.py
"""
Course Outcomes Import/Export Tab - COMPLETE VERSION
Features:
- Export COs to CSV with all correlations
- Import COs from CSV with comprehensive validation
- Preview before import with error/warning detection
- Bulk operations with detailed error reporting
- Template generation for easy import
"""

import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from typing import List, Dict, Optional, Tuple
import json
import logging
from io import BytesIO, StringIO
from datetime import datetime

logger = logging.getLogger(__name__)

# Import from course_outcomes_tab
from .course_outcomes_tab import (
    fetch_cos_for_offering, 
    save_co, 
    fetch_pos_for_degree, 
    fetch_psos_for_program, 
    fetch_peos_for_degree,
    is_valid
)

# ===========================================================================
# EXPORT FUNCTIONS
# ===========================================================================

def flatten_cos_for_export(cos_list: List[Dict]) -> pd.DataFrame:
    """Converts the complex CO list into a flat DataFrame for CSV export."""
    records = []
    for co in cos_list:
        # Flatten PO correlations
        po_corr = "|".join([f"{k}:{v}" for k, v in co.get('po_correlations', {}).items() if v > 0])
        
        # Flatten PSO correlations
        pso_corr = "|".join([f"{k}:{v}" for k, v in co.get('pso_correlations', {}).items() if v > 0])
        
        # Flatten PEO correlations
        peo_corr = "|".join([f"{k}:{v}" for k, v in co.get('peo_correlations', {}).items() if v > 0])
        
        record = {
            "co_code": co.get('co_code', ''),
            "title": co.get('title', ''),
            "description": co.get('description', ''),
            "bloom_level": co.get('bloom_level', ''),
            "sequence": co.get('sequence', 1),
            "weight_in_direct": co.get('weight_in_direct', 0.0),
            "status": co.get('status', 'draft'),
            "po_correlations": po_corr,
            "pso_correlations": pso_corr,
            "peo_correlations": peo_corr,
        }
        records.append(record)
    
    return pd.DataFrame(records)


def generate_export_csv(cos_list: List[Dict]) -> bytes:
    """Generate CSV bytes for download."""
    df = flatten_cos_for_export(cos_list)
    output = BytesIO()
    df.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    return output.getvalue()


def generate_template_csv() -> bytes:
    """Generate a template CSV with sample data and instructions."""
    template_data = [
        {
            "co_code": "CO1",
            "title": "Understand fundamental concepts",
            "description": "Students will be able to understand and explain fundamental concepts of the subject",
            "bloom_level": "Understand",
            "sequence": 1,
            "weight_in_direct": 0.20,
            "status": "draft",
            "po_correlations": "PO1:2|PO2:3",
            "pso_correlations": "PSO1:2",
            "peo_correlations": "PEO1:1|PEO2:2"
        },
        {
            "co_code": "CO2",
            "title": "Apply concepts to solve problems",
            "description": "Students will be able to apply learned concepts to solve real-world problems",
            "bloom_level": "Apply",
            "sequence": 2,
            "weight_in_direct": 0.25,
            "status": "draft",
            "po_correlations": "PO1:3|PO3:2",
            "pso_correlations": "PSO1:3",
            "peo_correlations": "PEO1:2"
        },
        {
            "co_code": "CO3",
            "title": "Analyze and evaluate solutions",
            "description": "Students will be able to analyze different approaches and evaluate their effectiveness",
            "bloom_level": "Analyze",
            "sequence": 3,
            "weight_in_direct": 0.30,
            "status": "draft",
            "po_correlations": "PO2:3|PO4:3",
            "pso_correlations": "PSO2:3",
            "peo_correlations": "PEO2:3"
        },
        {
            "co_code": "CO4",
            "title": "Create innovative solutions",
            "description": "Students will be able to create innovative solutions to complex problems",
            "bloom_level": "Create",
            "sequence": 4,
            "weight_in_direct": 0.25,
            "status": "draft",
            "po_correlations": "PO3:3|PO5:3",
            "pso_correlations": "PSO1:2|PSO2:3",
            "peo_correlations": "PEO1:3|PEO3:3"
        }
    ]
    
    df = pd.DataFrame(template_data)
    output = BytesIO()
    
    # Add instructions as comments at the top
    instructions = """# Course Outcomes Import Template
# Instructions:
# 1. co_code: Unique identifier (e.g., CO1, CO2) - REQUIRED
# 2. title: Short descriptive title - REQUIRED
# 3. description: Detailed description of what students will achieve - REQUIRED
# 4. bloom_level: Must be one of: Remember, Understand, Apply, Analyze, Evaluate, Create - REQUIRED
# 5. sequence: Numeric order (1, 2, 3...) - REQUIRED
# 6. weight_in_direct: Decimal between 0.0 and 1.0 (total should sum to 1.0) - REQUIRED
# 7. status: Either 'draft' or 'published' - REQUIRED
# 8. po_correlations: Format: PO1:2|PO2:3 (code:value|code:value) - values 0-3
# 9. pso_correlations: Format: PSO1:2|PSO2:1 (code:value|code:value) - values 0-3
# 10. peo_correlations: Format: PEO1:2|PEO2:3 (code:value|code:value) - values 0-3
#
# Notes:
# - All fields with 'REQUIRED' must have values
# - Correlation values: 0=None, 1=Low, 2=Medium, 3=High
# - Use only valid PO/PSO/PEO codes that exist in your system
# - Delete these instruction lines before importing
#
"""
    output.write(instructions.encode('utf-8'))
    df.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    return output.getvalue()


# ===========================================================================
# IMPORT VALIDATION FUNCTIONS
# ===========================================================================

def parse_correlations_from_import(corr_string: str) -> Dict[str, int]:
    """Converts a string like 'PO1:2|PO2:3' into a dict {'PO1': 2, 'PO2': 3}."""
    if not corr_string or pd.isna(corr_string):
        return {}
    
    correlations = {}
    try:
        # Handle empty string or whitespace
        corr_string = str(corr_string).strip()
        if not corr_string:
            return {}
        
        parts = corr_string.split('|')
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if ':' in part:
                k, v = part.split(':', 1)
                k = k.strip()
                v = v.strip()
                try:
                    correlations[k] = int(v)
                except ValueError:
                    logger.warning(f"Invalid correlation value '{v}' for key '{k}'")
                    continue
    except Exception as e:
        logger.warning(f"Could not parse correlation string '{corr_string}': {e}")
    return correlations


def validate_co_row(row: pd.Series, row_num: int, valid_pos: set, valid_psos: set, 
                    valid_peos: set, existing_co_codes: set) -> Tuple[List[str], List[str]]:
    """
    Validate a single CO row.
    Returns (errors, warnings) as lists of strings.
    """
    errors = []
    warnings = []
    
    # Required field validation
    if pd.isna(row.get('co_code')) or str(row.get('co_code', '')).strip() == '':
        errors.append(f"Row {row_num}: co_code is required")
    else:
        co_code = str(row['co_code']).strip()
        # Check for duplicates in import file
        if co_code in existing_co_codes:
            warnings.append(f"Row {row_num}: co_code '{co_code}' already exists (will update)")
    
    if pd.isna(row.get('title')) or str(row.get('title', '')).strip() == '':
        errors.append(f"Row {row_num}: title is required")
    
    if pd.isna(row.get('description')) or str(row.get('description', '')).strip() == '':
        errors.append(f"Row {row_num}: description is required")
    
    # Bloom level validation
    valid_bloom_levels = ["Remember", "Understand", "Apply", "Analyze", "Evaluate", "Create"]
    bloom_level = row.get('bloom_level', '')
    if pd.isna(bloom_level) or str(bloom_level).strip() == '':
        errors.append(f"Row {row_num}: bloom_level is required")
    elif bloom_level not in valid_bloom_levels:
        errors.append(f"Row {row_num}: bloom_level must be one of {valid_bloom_levels}, got '{bloom_level}'")
    
    # Sequence validation
    try:
        sequence = int(row.get('sequence', 0))
        if sequence < 1:
            errors.append(f"Row {row_num}: sequence must be >= 1, got {sequence}")
    except (ValueError, TypeError):
        errors.append(f"Row {row_num}: sequence must be a valid integer")
    
    # Weight validation
    try:
        weight = float(row.get('weight_in_direct', 0))
        if weight < 0 or weight > 1:
            errors.append(f"Row {row_num}: weight_in_direct must be between 0.0 and 1.0, got {weight}")
    except (ValueError, TypeError):
        errors.append(f"Row {row_num}: weight_in_direct must be a valid number")
    
    # Status validation
    valid_statuses = ["draft", "published"]
    status = row.get('status', 'draft')
    if pd.isna(status):
        status = 'draft'
    if status not in valid_statuses:
        errors.append(f"Row {row_num}: status must be 'draft' or 'published', got '{status}'")
    
    # Correlation validation
    po_correlations = parse_correlations_from_import(row.get('po_correlations', ''))
    pso_correlations = parse_correlations_from_import(row.get('pso_correlations', ''))
    peo_correlations = parse_correlations_from_import(row.get('peo_correlations', ''))
    
    # Check for invalid PO codes
    for po_code, value in po_correlations.items():
        if po_code not in valid_pos:
            warnings.append(f"Row {row_num}: PO code '{po_code}' not found in published POs (will skip)")
        if value < 0 or value > 3:
            errors.append(f"Row {row_num}: PO correlation value must be 0-3, got {value} for {po_code}")
    
    # Check for invalid PSO codes
    for pso_code, value in pso_correlations.items():
        if pso_code not in valid_psos:
            warnings.append(f"Row {row_num}: PSO code '{pso_code}' not found in published PSOs (will skip)")
        if value < 0 or value > 3:
            errors.append(f"Row {row_num}: PSO correlation value must be 0-3, got {value} for {pso_code}")
    
    # Check for invalid PEO codes
    for peo_code, value in peo_correlations.items():
        if peo_code not in valid_peos:
            warnings.append(f"Row {row_num}: PEO code '{peo_code}' not found in published PEOs (will skip)")
        if value < 0 or value > 3:
            errors.append(f"Row {row_num}: PEO correlation value must be 0-3, got {value} for {peo_code}")
    
    return errors, warnings


def validate_import_file(df: pd.DataFrame, offering_info: Dict, engine: Engine, 
                         existing_cos: List[Dict]) -> Tuple[bool, List[str], List[str], pd.DataFrame]:
    """
    Comprehensive validation of import file.
    Returns (is_valid, errors, warnings, processed_df)
    """
    errors = []
    warnings = []
    
    # Check for required columns
    required_columns = ['co_code', 'title', 'description', 'bloom_level', 'sequence', 'weight_in_direct', 'status']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        errors.append(f"Missing required columns: {', '.join(missing_columns)}")
        return False, errors, warnings, df
    
    # Check for empty file
    if df.empty:
        errors.append("CSV file is empty")
        return False, errors, warnings, df
    
    # Fetch valid outcomes
    pos = fetch_pos_for_degree(engine, offering_info['degree_code'], offering_info.get('program_code'))
    psos = fetch_psos_for_program(engine, offering_info['degree_code'], offering_info.get('program_code'), 
                                   offering_info.get('branch_code'))
    peos = fetch_peos_for_degree(engine, offering_info['degree_code'], offering_info.get('program_code'))
    
    valid_pos = {p['code'] for p in pos}
    valid_psos = {p['code'] for p in psos}
    valid_peos = {p['code'] for p in peos}
    
    existing_co_codes = {co['co_code'] for co in existing_cos}
    
    # Validate each row
    for idx, row in df.iterrows():
        row_errors, row_warnings = validate_co_row(
            row, idx + 2,  # +2 because row 1 is header and we're 0-indexed
            valid_pos, valid_psos, valid_peos, existing_co_codes
        )
        errors.extend(row_errors)
        warnings.extend(row_warnings)
    
    # Check for duplicate co_codes within the import file
    if 'co_code' in df.columns:
        co_code_counts = df['co_code'].value_counts()
        duplicates = co_code_counts[co_code_counts > 1]
        if not duplicates.empty:
            for co_code, count in duplicates.items():
                errors.append(f"Duplicate co_code '{co_code}' appears {count} times in import file")
    
    # Validate total weight sums to 1.0 (or close to it)
    if 'weight_in_direct' in df.columns:
        try:
            total_weight = df['weight_in_direct'].sum()
            if abs(total_weight - 1.0) > 0.01:
                warnings.append(f"Total weight_in_direct is {total_weight:.3f}, should be close to 1.0")
        except:
            pass
    
    is_valid = len(errors) == 0
    
    return is_valid, errors, warnings, df


# ===========================================================================
# IMPORT EXECUTION FUNCTIONS
# ===========================================================================

def execute_import(df: pd.DataFrame, engine: Engine, offering_id: int, offering_info: Dict,
                   existing_cos: List[Dict], valid_pos: set, valid_psos: set, 
                   valid_peos: set) -> Tuple[int, int, List[str]]:
    """
    Execute the import operation.
    Returns (success_count, fail_count, error_messages)
    """
    success_count = 0
    fail_count = 0
    error_messages = []
    
    existing_co_map = {co['co_code']: co for co in existing_cos}
    
    for idx, row in df.iterrows():
        try:
            # Parse correlations
            po_correlations = parse_correlations_from_import(row.get('po_correlations', ''))
            pso_correlations = parse_correlations_from_import(row.get('pso_correlations', ''))
            peo_correlations = parse_correlations_from_import(row.get('peo_correlations', ''))
            
            # Filter out invalid codes
            po_correlations = {k: v for k, v in po_correlations.items() if k in valid_pos}
            pso_correlations = {k: v for k, v in pso_correlations.items() if k in valid_psos}
            peo_correlations = {k: v for k, v in peo_correlations.items() if k in valid_peos}
            
            co_data = {
                'co_code': str(row['co_code']).strip(),
                'title': str(row['title']).strip(),
                'description': str(row['description']).strip(),
                'bloom_level': str(row['bloom_level']).strip(),
                'sequence': int(row['sequence']),
                'weight_in_direct': float(row['weight_in_direct']),
                'status': str(row.get('status', 'draft')).strip(),
                'po_correlations': po_correlations,
                'pso_correlations': pso_correlations,
                'peo_correlations': peo_correlations
            }
            
            # Check if CO exists (update) or new (create)
            co_id = None
            if co_data['co_code'] in existing_co_map:
                co_id = existing_co_map[co_data['co_code']]['id']
            
            # Save CO
            success = save_co(engine, offering_id, co_data, co_id)
            
            if success:
                success_count += 1
            else:
                fail_count += 1
                error_messages.append(f"Failed to save CO '{co_data['co_code']}' at row {idx + 2}")
        
        except Exception as e:
            fail_count += 1
            error_messages.append(f"Error processing row {idx + 2} ({row.get('co_code', 'unknown')}): {str(e)}")
            logger.error(f"Import error at row {idx + 2}: {e}", exc_info=True)
    
    return success_count, fail_count, error_messages


# ===========================================================================
# UI RENDERING FUNCTIONS
# ===========================================================================

def render_export_section(engine: Engine, offering_id: int, offering_info: Dict):
    """Render the export section."""
    st.markdown("### 📤 Export Course Outcomes")
    st.info("Export all COs for this offering to a CSV file. You can edit the file and re-import it.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📥 Export Current COs", use_container_width=True):
            with st.spinner("Generating export file..."):
                cos_list = fetch_cos_for_offering(engine, offering_id)
                
                if not cos_list:
                    st.warning("This offering has no COs to export.")
                    return
                
                csv_data = generate_export_csv(cos_list)
                filename = f"{offering_info['subject_code']}_Y{offering_info['year']}_T{offering_info['term']}_COs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                
                st.session_state.co_export_data = csv_data
                st.session_state.co_export_filename = filename
                st.success(f"✅ Export file ready! ({len(cos_list)} COs)")
    
    with col2:
        if st.button("📋 Download Template", use_container_width=True):
            with st.spinner("Generating template..."):
                template_data = generate_template_csv()
                template_filename = f"CO_Import_Template_{datetime.now().strftime('%Y%m%d')}.csv"
                
                st.session_state.co_template_data = template_data
                st.session_state.co_template_filename = template_filename
                st.success("✅ Template ready with sample data and instructions!")
    
    # Download buttons
    if 'co_export_data' in st.session_state:
        st.download_button(
            label=f"⬇️ Download: {st.session_state.co_export_filename}",
            data=st.session_state.co_export_data,
            file_name=st.session_state.co_export_filename,
            mime='text/csv',
            use_container_width=True
        )
    
    if 'co_template_data' in st.session_state:
        st.download_button(
            label=f"⬇️ Download: {st.session_state.co_template_filename}",
            data=st.session_state.co_template_data,
            file_name=st.session_state.co_template_filename,
            mime='text/csv',
            use_container_width=True
        )


def render_import_section(engine: Engine, offering_id: int, offering_info: Dict):
    """Render the import section with validation."""
    st.markdown("### 📥 Import Course Outcomes")
    st.info("Upload a CSV file to create or update COs. The file will be validated before import.")
    
    # File uploader
    uploaded_file = st.file_uploader(
        "Choose CSV file", 
        type=["csv"],
        help="Upload a CSV file with CO data. Use the template for correct format.",
        key="co_import_file"
    )
    
    if uploaded_file is not None:
        try:
            # Read CSV
            df = pd.read_csv(uploaded_file)
            
            # Remove instruction/comment lines (lines starting with #)
            if not df.empty and 'co_code' in df.columns:
                df = df[~df['co_code'].astype(str).str.startswith('#')]
                df = df.reset_index(drop=True)
            
            st.markdown("#### 📋 File Preview (first 10 rows)")
            st.dataframe(df.head(10), use_container_width=True)
            
            st.markdown(f"**Total rows:** {len(df)}")
            
            # Validation section
            st.markdown("---")
            st.markdown("#### ✅ Validation Results")
            
            with st.spinner("Validating import file..."):
                # Fetch existing COs
                existing_cos = fetch_cos_for_offering(engine, offering_id)
                
                # Fetch valid outcomes for validation
                pos = fetch_pos_for_degree(engine, offering_info['degree_code'], offering_info.get('program_code'))
                psos = fetch_psos_for_program(engine, offering_info['degree_code'], offering_info.get('program_code'), 
                                               offering_info.get('branch_code'))
                peos = fetch_peos_for_degree(engine, offering_info['degree_code'], offering_info.get('program_code'))
                
                valid_pos = {p['code'] for p in pos}
                valid_psos = {p['code'] for p in psos}
                valid_peos = {p['code'] for p in peos}
                
                # Validate
                is_valid, errors, warnings, processed_df = validate_import_file(
                    df, offering_info, engine, existing_cos
                )
            
            # Display validation results
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if is_valid:
                    st.success("✅ Validation Passed")
                else:
                    st.error("❌ Validation Failed")
            
            with col2:
                if errors:
                    st.error(f"🔴 {len(errors)} Error(s)")
                else:
                    st.success("✅ No Errors")
            
            with col3:
                if warnings:
                    st.warning(f"⚠️ {len(warnings)} Warning(s)")
                else:
                    st.info("ℹ️ No Warnings")
            
            # Show errors
            if errors:
                with st.expander("🔴 Errors (must fix before import)", expanded=True):
                    for error in errors:
                        st.error(f"• {error}")
            
            # Show warnings
            if warnings:
                with st.expander("⚠️ Warnings (can proceed with caution)", expanded=False):
                    for warning in warnings:
                        st.warning(f"• {warning}")
            
            # Import button
            st.markdown("---")
            
            if is_valid:
                st.success("✅ File is valid and ready to import!")
                
                # Show what will happen
                update_count = sum(1 for _, row in df.iterrows() 
                                 if str(row.get('co_code', '')).strip() in {co['co_code'] for co in existing_cos})
                create_count = len(df) - update_count
                
                st.info(f"📊 Import will:\n- **Create** {create_count} new CO(s)\n- **Update** {update_count} existing CO(s)")
                
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    if st.button("🚀 Execute Import", type="primary", use_container_width=True):
                        with st.spinner("Importing COs..."):
                            success_count, fail_count, error_messages = execute_import(
                                df, engine, offering_id, offering_info, existing_cos,
                                valid_pos, valid_psos, valid_peos
                            )
                        
                        st.markdown("---")
                        st.markdown("#### 📊 Import Results")
                        
                        col_a, col_b, col_c = st.columns(3)
                        with col_a:
                            st.metric("✅ Successful", success_count)
                        with col_b:
                            st.metric("❌ Failed", fail_count)
                        with col_c:
                            st.metric("📊 Total", success_count + fail_count)
                        
                        if error_messages:
                            with st.expander("❌ Import Errors", expanded=True):
                                for msg in error_messages:
                                    st.error(msg)
                        
                        if success_count > 0:
                            st.success(f"✅ Successfully imported {success_count} CO(s)!")
                            st.info("🔄 Page will refresh to show updated COs...")
                            st.balloons()
                            
                            # Clear cache and rerun
                            if 'co_import_file' in st.session_state:
                                del st.session_state.co_import_file
                            st.rerun()
                
                with col2:
                    if st.button("❌ Cancel", use_container_width=True):
                        st.info("Import cancelled")
                        st.rerun()
            else:
                st.error("❌ Cannot import: Please fix validation errors first")
                st.info("💡 Tip: Download the template to see the correct format")
        
        except Exception as e:
            st.error(f"❌ Error processing file: {str(e)}")
            logger.error(f"Error in import processing: {e}", exc_info=True)
            st.code(str(e))


def render_co_import_export_tab(engine: Engine, offering_id: Optional[int], offering_info: Optional[Dict]):
    """Main render function for CO Import/Export tab."""
    
    st.markdown("""
    Use this tool to bulk manage Course Outcomes through CSV files.
    
    **Features:**
    - Export existing COs to CSV for backup or editing
    - Download a template with sample data and instructions
    - Import COs from CSV with comprehensive validation
    - Preview and validate before import
    - Detailed error reporting
    """)
    
    if not offering_id or not offering_info:
        st.info("Please select a subject offering from the filters above to use import/export tools.")
        return
    
    st.markdown(f"#### 📦 Bulk Operations for: `{offering_info['subject_code']} - Y{offering_info['year']}, T{offering_info['term']}`")
    
    st.markdown("---")
    
    # Export section
    render_export_section(engine, offering_id, offering_info)
    
    st.markdown("---")
    
    # Import section
    render_import_section(engine, offering_id, offering_info)


if __name__ == "__main__":
    # Test/Demo
    st.set_page_config(page_title="CO Import/Export", layout="wide")
    st.title("Course Outcomes Import/Export - Demo")
    st.info("This is a standalone demo. In production, this is used as part of the main module.")
