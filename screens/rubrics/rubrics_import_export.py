# screens/rubrics/rubrics_import_export.py
"""Rubrics Import/Export Module"""

import pandas as pd
import json
from typing import Dict, List, Optional, Tuple
from io import StringIO, BytesIO
import logging

logger = logging.getLogger(__name__)


class RubricsImportExport:
    """Handle import/export operations for rubrics."""

    def __init__(self, service):
        """Initialize with rubrics service."""
        self.service = service

    # ========================================================================
    # EXPORT OPERATIONS
    # ========================================================================

    def export_rubric_to_csv(self, config_id: int) -> str:
        """Export complete rubric to CSV format."""
        config = self.service.get_rubric_config(config_id)
        if not config:
            raise ValueError(f"Rubric config {config_id} not found")

        complete_rubric = self.service.get_rubric(
            config['offering_id'], config['scope'], config['component_key']
        )

        if config['mode'] == 'analytic_points':
            return self._export_analytic_points_csv(complete_rubric)
        else:
            return self._export_analytic_levels_csv(complete_rubric)

    def _export_analytic_points_csv(self, rubric: Dict) -> str:
        """Export analytic_points rubric to CSV."""
        rows = []
        
        for assessment in rubric.get('assessments', []):
            for criterion in assessment.get('criteria', []):
                linked_cos = criterion.get('linked_cos')
                if linked_cos:
                    try:
                        cos_list = json.loads(linked_cos) if isinstance(linked_cos, str) else linked_cos
                        cos_str = '|'.join(cos_list)
                    except:
                        cos_str = str(linked_cos)
                else:
                    cos_str = ''

                rows.append({
                    'assessment_code': assessment['code'],
                    'assessment_title': assessment['title'],
                    'assessment_max_marks': assessment['max_marks'],
                    'component_key': assessment.get('component_key', ''),
                    'criterion_key': criterion['criterion_key'],
                    'weight_pct': criterion['weight_pct'],
                    'linked_cos': cos_str
                })

        df = pd.DataFrame(rows)
        return df.to_csv(index=False)

    def _export_analytic_levels_csv(self, rubric: Dict) -> str:
        """Export analytic_levels rubric to CSV."""
        rows = []
        
        for assessment in rubric.get('assessments', []):
            for level in assessment.get('levels', []):
                linked_cos = level.get('linked_cos')
                if linked_cos:
                    try:
                        cos_list = json.loads(linked_cos) if isinstance(linked_cos, str) else linked_cos
                        cos_str = '|'.join(cos_list)
                    except:
                        cos_str = str(linked_cos)
                else:
                    cos_str = ''

                rows.append({
                    'assessment_code': assessment['code'],
                    'assessment_title': assessment['title'],
                    'assessment_max_marks': assessment['max_marks'],
                    'component_key': assessment.get('component_key', ''),
                    'criterion_key': level['criterion_key'],
                    'criterion_weight_pct': level['criterion_weight_pct'],
                    'level_label': level['level_label'],
                    'level_score': level['level_score'],
                    'level_descriptor': level.get('level_descriptor', ''),
                    'level_sequence': level['level_sequence'],
                    'linked_cos': cos_str
                })

        df = pd.DataFrame(rows)
        return df.to_csv(index=False)

    def export_criteria_catalog_csv(self) -> str:
        """Export criteria catalog to CSV."""
        catalog = self.service.get_criteria_catalog(active_only=False)
        df = pd.DataFrame(catalog)
        return df.to_csv(index=False)

    def export_template_analytic_points(self) -> str:
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
            }
        ]
        df = pd.DataFrame(template_data)
        return df.to_csv(index=False)

    def export_template_analytic_levels(self) -> str:
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
                'level_descriptor': 'Comprehensive and accurate content',
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
            }
        ]
        df = pd.DataFrame(template_data)
        return df.to_csv(index=False)

    # ========================================================================
    # IMPORT OPERATIONS
    # ========================================================================

    def preview_import(self, csv_content: str, mode: str) -> Tuple[bool, List[str], pd.DataFrame]:
        """Preview import and validate."""
        errors = []
        
        try:
            df = pd.read_csv(StringIO(csv_content))
        except Exception as e:
            return False, [f"Failed to parse CSV: {str(e)}"], pd.DataFrame()

        # Validate columns
        if mode == 'analytic_points':
            required_cols = [
                'assessment_code', 'assessment_title', 'assessment_max_marks',
                'criterion_key', 'weight_pct'
            ]
        else:  # analytic_levels
            required_cols = [
                'assessment_code', 'assessment_title', 'assessment_max_marks',
                'criterion_key', 'criterion_weight_pct', 'level_label',
                'level_score', 'level_sequence'
            ]

        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            errors.append(f"Missing required columns: {missing_cols}")
            return False, errors, df

        # Validate data
        if df.empty:
            errors.append("CSV file is empty")
            return False, errors, df

        # Check for required fields
        for col in required_cols:
            if df[col].isnull().any():
                null_rows = df[df[col].isnull()].index.tolist()
                errors.append(f"Column '{col}' has null values in rows: {null_rows}")

        # Validate weights sum to 100 per assessment
        if mode == 'analytic_points':
            for assessment_code in df['assessment_code'].unique():
                assess_df = df[df['assessment_code'] == assessment_code]
                total = assess_df['weight_pct'].sum()
                if abs(total - 100.0) > 0.01:
                    errors.append(
                        f"Assessment '{assessment_code}': weights sum to {total}%, not 100%"
                    )
        
        elif mode == 'analytic_levels':
            # Validate criterion weights sum to 100 per assessment
            for assessment_code in df['assessment_code'].unique():
                assess_df = df[df['assessment_code'] == assessment_code]
                criterion_weights = assess_df.groupby('criterion_key')['criterion_weight_pct'].first()
                total = criterion_weights.sum()
                if abs(total - 100.0) > 0.01:
                    errors.append(
                        f"Assessment '{assessment_code}': criterion weights sum to {total}%, not 100%"
                    )

        is_valid = len(errors) == 0
        return is_valid, errors, df

    def import_rubric_from_csv(self, csv_content: str, rubric_config_id: int,
                               mode: str, actor: str) -> Dict:
        """Import rubric from CSV."""
        from sc_models import RubricAssessment, AuditEntry

        # Preview and validate
        is_valid, errors, df = self.preview_import(csv_content, mode)
        
        if not is_valid:
            return {
                'success': False,
                'errors': errors,
                'assessments_created': 0
            }

        audit = AuditEntry(
            actor_id=actor,
            actor_role='admin',
            operation='import_rubric',
            source='csv_import'
        )

        assessments_created = 0
        
        try:
            # Group by assessment
            for assessment_code in df['assessment_code'].unique():
                assess_df = df[df['assessment_code'] == assessment_code].iloc[0]
                
                # Create assessment
                assessment = RubricAssessment(
                    rubric_config_id=rubric_config_id,
                    code=assessment_code,
                    title=assess_df['assessment_title'],
                    max_marks=float(assess_df['assessment_max_marks']),
                    mode=mode,
                    component_key=assess_df.get('component_key') if pd.notna(assess_df.get('component_key')) else None
                )
                
                assessment_id = self.service.add_assessment(assessment, audit)
                assessments_created += 1

                # Add criteria/levels
                assess_rows = df[df['assessment_code'] == assessment_code]
                
                if mode == 'analytic_points':
                    criteria = {}
                    linked_cos = {}
                    
                    for _, row in assess_rows.iterrows():
                        criterion_key = row['criterion_key']
                        criteria[criterion_key] = float(row['weight_pct'])
                        
                        if pd.notna(row.get('linked_cos')) and row['linked_cos']:
                            cos_list = str(row['linked_cos']).split('|')
                            linked_cos[criterion_key] = cos_list
                    
                    self.service.add_criteria_weights(
                        assessment_id, criteria, linked_cos, audit
                    )
                
                elif mode == 'analytic_levels':
                    levels_data = []
                    
                    for _, row in assess_rows.iterrows():
                        cos_list = None
                        if pd.notna(row.get('linked_cos')) and row['linked_cos']:
                            cos_list = str(row['linked_cos']).split('|')
                        
                        levels_data.append({
                            'criterion_key': row['criterion_key'],
                            'criterion_weight_pct': float(row['criterion_weight_pct']),
                            'level_label': row['level_label'],
                            'level_score': float(row['level_score']),
                            'level_descriptor': row.get('level_descriptor', ''),
                            'level_sequence': int(row['level_sequence']),
                            'linked_cos': cos_list
                        })
                    
                    self.service.add_assessment_levels(assessment_id, levels_data, audit)

            return {
                'success': True,
                'errors': [],
                'assessments_created': assessments_created
            }

        except Exception as e:
            logger.error(f"Import failed: {e}", exc_info=True)
            return {
                'success': False,
                'errors': [str(e)],
                'assessments_created': assessments_created
            }

    def import_criteria_catalog_csv(self, csv_content: str) -> Dict:
        """Import criteria catalog from CSV."""
        try:
            df = pd.read_csv(StringIO(csv_content))
            
            required_cols = ['key', 'label']
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                return {
                    'success': False,
                    'errors': [f"Missing columns: {missing}"]
                }

            criteria_added = 0
            errors = []

            for _, row in df.iterrows():
                try:
                    self.service.add_catalog_criterion(
                        key=row['key'],
                        label=row['label'],
                        description=row.get('description') if pd.notna(row.get('description')) else None
                    )
                    criteria_added += 1
                except Exception as e:
                    errors.append(f"Row {_}: {str(e)}")

            return {
                'success': len(errors) == 0,
                'errors': errors,
                'criteria_added': criteria_added
            }

        except Exception as e:
            return {
                'success': False,
                'errors': [str(e)],
                'criteria_added': 0
            }
