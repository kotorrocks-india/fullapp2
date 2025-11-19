# screens/rubrics/rubrics_service.py
"""Complete Rubrics Service Layer (Slide 21) - All Operations"""

from typing import List, Dict, Optional, Tuple
from dataclasses import asdict
import json
import logging
from sqlalchemy.engine import Engine
from sqlalchemy import text as sa_text

from screens.subject_cos.base_service import BaseService
from screens.subject_cos.models import RubricConfig, RubricAssessment, AuditEntry

logger = logging.getLogger(__name__)


class RubricsService(BaseService):
    """Complete service for managing rubrics (Slide 21)."""

    # ========================================================================
    # RUBRIC CONFIG OPERATIONS
    # ========================================================================

    def create_rubric_config(self, config: RubricConfig, audit_entry: AuditEntry) -> int:
        """Create rubric configuration."""
        # Check for duplicates
        existing = self._fetch_one("""
        SELECT id FROM rubric_configs
        WHERE offering_id = :offering_id
        AND scope = :scope
        AND (component_key = :component_key OR (component_key IS NULL AND :component_key IS NULL))
        """, {
            'offering_id': config.offering_id,
            'scope': config.scope,
            'component_key': config.component_key
        })

        if existing:
            raise ValueError("Rubric config already exists for this scope")

        sql = """
        INSERT INTO rubric_configs (
            offering_id, scope, component_key, mode, co_linking_enabled,
            normalization_enabled, visible_to_students, show_before_assessment,
            version, is_locked, status, created_by
        ) VALUES (
            :offering_id, :scope, :component_key, :mode, :co_linking_enabled,
            :normalization_enabled, :visible_to_students, :show_before_assessment,
            :version, :is_locked, :status, :created_by
        )
        """
        params = asdict(config)
        params['created_by'] = audit_entry.actor_id
        config_id = self._insert(sql, params, audit_entry)

        self._audit_rubric('RUBRIC_CREATED', config_id, config.offering_id, 
                          config.scope, audit_entry)
        logger.info(f"Created rubric config {config_id}")
        return config_id

    def update_rubric_config(self, config_id: int, updates: Dict,
                            audit_entry: AuditEntry) -> bool:
        """Update rubric configuration."""
        config = self.get_rubric_config(config_id)
        if not config:
            raise ValueError(f"Rubric config {config_id} not found")
        
        if config['is_locked'] and not self._is_minor_update(updates):
            raise ValueError("Rubric is locked - only minor updates allowed")

        allowed_fields = [
            'co_linking_enabled', 'normalization_enabled', 
            'visible_to_students', 'show_before_assessment'
        ]
        
        set_clause = ', '.join([f"{k} = :{k}" for k in updates.keys() 
                               if k in allowed_fields])
        
        if not set_clause:
            return False

        sql = f"""
        UPDATE rubric_configs 
        SET {set_clause}, updated_at = CURRENT_TIMESTAMP, updated_by = :updated_by
        WHERE id = :config_id
        """
        
        with self.engine.begin() as conn:
            params = {k: v for k, v in updates.items() if k in allowed_fields}
            params['config_id'] = config_id
            params['updated_by'] = audit_entry.actor_id
            self._exec(conn, sql, params)

        self._audit_rubric('RUBRIC_UPDATED', config_id, config['offering_id'],
                          config['scope'], audit_entry, 
                          changed_fields=json.dumps(list(updates.keys())))
        return True

    def lock_rubric(self, config_id: int, reason: str, audit_entry: AuditEntry) -> bool:
        """Lock rubric (called when marks exist)."""
        sql = """
        UPDATE rubric_configs 
        SET is_locked = 1, locked_reason = :reason, 
            updated_at = CURRENT_TIMESTAMP, updated_by = :updated_by
        WHERE id = :config_id
        """
        
        with self.engine.begin() as conn:
            self._exec(conn, sql, {
                'config_id': config_id,
                'reason': reason,
                'updated_by': audit_entry.actor_id
            })

        config = self.get_rubric_config(config_id)
        self._audit_rubric('RUBRIC_LOCKED', config_id, config['offering_id'],
                          config['scope'], audit_entry, note=reason)
        logger.info(f"Locked rubric {config_id}: {reason}")
        return True

    def unlock_rubric(self, config_id: int, reason: str, 
                     audit_entry: AuditEntry) -> bool:
        """Unlock rubric (requires step-up auth)."""
        if not audit_entry.step_up_performed:
            raise ValueError("Step-up authentication required to unlock rubric")

        sql = """
        UPDATE rubric_configs 
        SET is_locked = 0, locked_reason = NULL,
            updated_at = CURRENT_TIMESTAMP, updated_by = :updated_by
        WHERE id = :config_id
        """
        
        with self.engine.begin() as conn:
            self._exec(conn, sql, {
                'config_id': config_id,
                'updated_by': audit_entry.actor_id
            })

        config = self.get_rubric_config(config_id)
        self._audit_rubric('RUBRIC_UNLOCKED', config_id, config['offering_id'],
                          config['scope'], audit_entry, note=reason)
        logger.info(f"Unlocked rubric {config_id}: {reason}")
        return True

    def publish_rubric(self, config_id: int, audit_entry: AuditEntry) -> bool:
        """Publish rubric."""
        config = self.get_rubric_config(config_id)
        if not config:
            raise ValueError(f"Rubric config {config_id} not found")
        
        # Validate before publishing
        validation_result = self.validate_rubric_complete(config_id)
        if not validation_result['is_valid']:
            raise ValueError(f"Validation failed: {validation_result['errors']}")

        sql = """
        UPDATE rubric_configs 
        SET status = 'published', updated_at = CURRENT_TIMESTAMP, updated_by = :updated_by
        WHERE id = :config_id
        """
        
        with self.engine.begin() as conn:
            self._exec(conn, sql, {
                'config_id': config_id,
                'updated_by': audit_entry.actor_id
            })

        self._audit_rubric('RUBRIC_PUBLISHED', config_id, config['offering_id'],
                          config['scope'], audit_entry)
        logger.info(f"Published rubric {config_id}")
        return True

    def create_rubric_version(self, config_id: int, audit_entry: AuditEntry) -> int:
        """Create new version of rubric (for major changes)."""
        old_config = self.get_rubric_config(config_id)
        if not old_config:
            raise ValueError(f"Rubric config {config_id} not found")

        # Create snapshot
        self._create_version_snapshot(config_id, 'version_created', audit_entry)

        # Increment version
        new_version = old_config['version'] + 1
        
        sql = """
        UPDATE rubric_configs 
        SET version = :version, updated_at = CURRENT_TIMESTAMP, updated_by = :updated_by
        WHERE id = :config_id
        """
        
        with self.engine.begin() as conn:
            self._exec(conn, sql, {
                'config_id': config_id,
                'version': new_version,
                'updated_by': audit_entry.actor_id
            })

        self._audit_rubric('RUBRIC_VERSION_CREATED', config_id, 
                          old_config['offering_id'], old_config['scope'], audit_entry)
        logger.info(f"Created version {new_version} of rubric {config_id}")
        return new_version

    # ========================================================================
    # ASSESSMENT OPERATIONS
    # ========================================================================

    def add_assessment(self, assessment: RubricAssessment, audit_entry: AuditEntry) -> int:
        """Add assessment to rubric."""
        config = self.get_rubric_config(assessment.rubric_config_id)
        if not config:
            raise ValueError(f"Rubric config {assessment.rubric_config_id} not found")
        if config['is_locked']:
            raise ValueError("Rubric is locked - cannot add assessments")

        # Check for duplicate code
        existing = self._fetch_one("""
        SELECT id FROM rubric_assessments
        WHERE rubric_config_id = :config_id AND code = :code
        """, {'config_id': assessment.rubric_config_id, 'code': assessment.code})

        if existing:
            raise ValueError(f"Assessment {assessment.code} already exists")

        sql = """
        INSERT INTO rubric_assessments (
            rubric_config_id, code, title, max_marks, mode, component_key
        ) VALUES (
            :rubric_config_id, :code, :title, :max_marks, :mode, :component_key
        )
        """
        assessment_id = self._insert(sql, asdict(assessment), audit_entry)
        logger.info(f"Added assessment {assessment.code} (ID: {assessment_id})")
        return assessment_id

    def update_assessment(self, assessment_id: int, updates: Dict,
                         audit_entry: AuditEntry) -> bool:
        """Update assessment."""
        assessment = self._fetch_one("""
        SELECT ra.*, rc.is_locked 
        FROM rubric_assessments ra
        JOIN rubric_configs rc ON ra.rubric_config_id = rc.id
        WHERE ra.id = :id
        """, {'id': assessment_id})

        if not assessment:
            raise ValueError(f"Assessment {assessment_id} not found")
        
        if assessment['is_locked']:
            # Only allow minor updates
            allowed_fields = ['title']
            updates = {k: v for k, v in updates.items() if k in allowed_fields}
            if not updates:
                raise ValueError("Rubric is locked - only title updates allowed")

        set_clause = ', '.join([f"{k} = :{k}" for k in updates.keys()])
        sql = f"""
        UPDATE rubric_assessments 
        SET {set_clause}, updated_at = CURRENT_TIMESTAMP
        WHERE id = :assessment_id
        """
        
        with self.engine.begin() as conn:
            params = updates.copy()
            params['assessment_id'] = assessment_id
            self._exec(conn, sql, params)

        logger.info(f"Updated assessment {assessment_id}")
        return True

    def delete_assessment(self, assessment_id: int, audit_entry: AuditEntry) -> bool:
        """Delete assessment."""
        assessment = self._fetch_one("""
        SELECT ra.*, rc.is_locked 
        FROM rubric_assessments ra
        JOIN rubric_configs rc ON ra.rubric_config_id = rc.id
        WHERE ra.id = :id
        """, {'id': assessment_id})

        if not assessment:
            raise ValueError(f"Assessment {assessment_id} not found")
        
        if assessment['is_locked']:
            raise ValueError("Rubric is locked - cannot delete assessments")

        sql = "DELETE FROM rubric_assessments WHERE id = :assessment_id"
        
        with self.engine.begin() as conn:
            self._exec(conn, sql, {'assessment_id': assessment_id})

        logger.info(f"Deleted assessment {assessment_id}")
        return True

    # ========================================================================
    # CRITERIA OPERATIONS (ANALYTIC_POINTS MODE)
    # ========================================================================

    def add_criteria_weights(self, assessment_id: int, criteria: Dict[str, float],
                            linked_cos: Optional[Dict[str, List[str]]] = None,
                            audit_entry: AuditEntry = None) -> bool:
        """Add criteria weights (analytic_points mode)."""
        # Validate weights sum to 100
        total = sum(criteria.values())
        if abs(total - 100.0) > 0.01:
            raise ValueError(f"Criteria weights must sum to 100%, got {total}%")

        with self.engine.begin() as conn:
            for criterion_key, weight_pct in criteria.items():
                cos_json = None
                if linked_cos and criterion_key in linked_cos:
                    cos_json = json.dumps(linked_cos[criterion_key])
                
                self._exec(conn, """
                INSERT INTO rubric_assessment_criteria (
                    assessment_id, criterion_key, weight_pct, linked_cos
                ) VALUES (
                    :assessment_id, :criterion_key, :weight_pct, :linked_cos
                )
                """, {
                    'assessment_id': assessment_id,
                    'criterion_key': criterion_key,
                    'weight_pct': weight_pct,
                    'linked_cos': cos_json
                })

        logger.info(f"Added {len(criteria)} criteria to assessment {assessment_id}")
        return True

    def update_criterion_weight(self, assessment_id: int, criterion_key: str,
                               new_weight: float, audit_entry: AuditEntry) -> bool:
        """Update criterion weight."""
        sql = """
        UPDATE rubric_assessment_criteria 
        SET weight_pct = :weight, updated_at = CURRENT_TIMESTAMP
        WHERE assessment_id = :assessment_id AND criterion_key = :criterion_key
        """
        
        with self.engine.begin() as conn:
            self._exec(conn, sql, {
                'assessment_id': assessment_id,
                'criterion_key': criterion_key,
                'weight': new_weight
            })

        # Validate total still equals 100
        is_valid, total = self.validate_rubric_weights(assessment_id)
        if not is_valid:
            raise ValueError(f"Weight update results in invalid total: {total}%")

        return True

    def delete_criterion(self, assessment_id: int, criterion_key: str,
                        audit_entry: AuditEntry) -> bool:
        """Delete criterion."""
        sql = """
        DELETE FROM rubric_assessment_criteria 
        WHERE assessment_id = :assessment_id AND criterion_key = :criterion_key
        """
        
        with self.engine.begin() as conn:
            self._exec(conn, sql, {
                'assessment_id': assessment_id,
                'criterion_key': criterion_key
            })

        logger.info(f"Deleted criterion {criterion_key} from assessment {assessment_id}")
        return True

    def link_criterion_to_cos(self, assessment_id: int, criterion_key: str,
                             co_codes: List[str], audit_entry: AuditEntry) -> bool:
        """Link criterion to COs."""
        # Validate CO codes exist
        assessment = self._fetch_one("""
        SELECT ra.*, rc.offering_id
        FROM rubric_assessments ra
        JOIN rubric_configs rc ON ra.rubric_config_id = rc.id
        WHERE ra.id = :id
        """, {'id': assessment_id})

        if not assessment:
            raise ValueError(f"Assessment {assessment_id} not found")

        valid_cos = self._fetch_all("""
        SELECT co_code FROM subject_cos 
        WHERE offering_id = :offering_id
        """, {'offering_id': assessment['offering_id']})
        
        valid_co_codes = {co['co_code'] for co in valid_cos}
        invalid = [co for co in co_codes if co not in valid_co_codes]
        
        if invalid:
            raise ValueError(f"Invalid CO codes: {invalid}")

        sql = """
        UPDATE rubric_assessment_criteria 
        SET linked_cos = :cos_json, updated_at = CURRENT_TIMESTAMP
        WHERE assessment_id = :assessment_id AND criterion_key = :criterion_key
        """
        
        with self.engine.begin() as conn:
            self._exec(conn, sql, {
                'assessment_id': assessment_id,
                'criterion_key': criterion_key,
                'cos_json': json.dumps(co_codes)
            })

        logger.info(f"Linked criterion {criterion_key} to COs: {co_codes}")
        return True

    # ========================================================================
    # LEVELS OPERATIONS (ANALYTIC_LEVELS MODE)
    # ========================================================================

    def add_assessment_levels(self, assessment_id: int, levels_data: List[Dict],
                             audit_entry: AuditEntry) -> bool:
        """Add level descriptors for analytic_levels mode."""
        # Validate structure
        criteria_weights = {}
        for level in levels_data:
            criterion_key = level['criterion_key']
            if criterion_key not in criteria_weights:
                criteria_weights[criterion_key] = level.get('criterion_weight_pct', 0)

        # Validate weights sum to 100
        total = sum(criteria_weights.values())
        if abs(total - 100.0) > 0.01:
            raise ValueError(f"Criterion weights must sum to 100%, got {total}%")

        with self.engine.begin() as conn:
            for level in levels_data:
                cos_json = None
                if 'linked_cos' in level and level['linked_cos']:
                    cos_json = json.dumps(level['linked_cos'])
                
                self._exec(conn, """
                INSERT INTO rubric_assessment_levels (
                    assessment_id, criterion_key, criterion_weight_pct,
                    level_label, level_score, level_descriptor, level_sequence, linked_cos
                ) VALUES (
                    :assessment_id, :criterion_key, :criterion_weight_pct,
                    :level_label, :level_score, :level_descriptor, :level_sequence, :linked_cos
                )
                """, {
                    'assessment_id': assessment_id,
                    'criterion_key': level['criterion_key'],
                    'criterion_weight_pct': level.get('criterion_weight_pct', 0),
                    'level_label': level['level_label'],
                    'level_score': level['level_score'],
                    'level_descriptor': level.get('level_descriptor'),
                    'level_sequence': level.get('level_sequence', 0),
                    'linked_cos': cos_json
                })

        logger.info(f"Added {len(levels_data)} levels to assessment {assessment_id}")
        return True

    def update_level_descriptor(self, level_id: int, new_descriptor: str,
                               audit_entry: AuditEntry) -> bool:
        """Update level descriptor (allowed minor edit when locked)."""
        sql = """
        UPDATE rubric_assessment_levels 
        SET level_descriptor = :descriptor, updated_at = CURRENT_TIMESTAMP
        WHERE id = :level_id
        """
        
        with self.engine.begin() as conn:
            self._exec(conn, sql, {
                'level_id': level_id,
                'descriptor': new_descriptor
            })

        return True

    # ========================================================================
    # RETRIEVAL OPERATIONS
    # ========================================================================

    def get_rubric_config(self, config_id: int) -> Optional[Dict]:
        """Get rubric configuration by ID."""
        return self._fetch_one("""
        SELECT * FROM rubric_configs WHERE id = :id
        """, {'id': config_id})

    def get_rubric(self, offering_id: int, scope: str = 'subject',
                  component_key: Optional[str] = None) -> Optional[Dict]:
        """Get complete rubric with assessments."""
        config = self._fetch_one("""
        SELECT * FROM rubric_configs
        WHERE offering_id = :offering_id AND scope = :scope
        AND (component_key = :component_key OR (component_key IS NULL AND :component_key IS NULL))
        """, {
            'offering_id': offering_id,
            'scope': scope,
            'component_key': component_key
        })

        if not config:
            return None

        # Get assessments
        assessments = self._fetch_all("""
        SELECT * FROM rubric_assessments
        WHERE rubric_config_id = :config_id
        ORDER BY code
        """, {'config_id': config['id']})

        # Get criteria/levels for each assessment
        for assessment in assessments:
            if assessment['mode'] == 'analytic_points':
                criteria = self._fetch_all("""
                SELECT * FROM rubric_assessment_criteria
                WHERE assessment_id = :assessment_id
                ORDER BY criterion_key
                """, {'assessment_id': assessment['id']})
                assessment['criteria'] = criteria
            else:  # analytic_levels
                levels = self._fetch_all("""
                SELECT * FROM rubric_assessment_levels
                WHERE assessment_id = :assessment_id
                ORDER BY criterion_key, level_sequence
                """, {'assessment_id': assessment['id']})
                assessment['levels'] = levels

        config['assessments'] = assessments
        return config

    def list_rubrics_for_offering(self, offering_id: int) -> List[Dict]:
        """List all rubrics for an offering."""
        return self._fetch_all("""
        SELECT * FROM rubric_configs
        WHERE offering_id = :offering_id
        ORDER BY scope, component_key
        """, {'offering_id': offering_id})

    # ========================================================================
    # CRITERIA CATALOG OPERATIONS
    # ========================================================================

    def add_catalog_criterion(self, key: str, label: str, description: str = None) -> int:
        """Add criterion to global catalog."""
        sql = """
        INSERT INTO rubric_criteria_catalog (key, label, description, active)
        VALUES (:key, :label, :description, 1)
        """
        
        with self.engine.begin() as conn:
            self._exec(conn, sql, {
                'key': key,
                'label': label,
                'description': description
            })
            result = self._exec(conn, "SELECT last_insert_rowid()").fetchone()
            return result[0]

    def get_criteria_catalog(self, active_only: bool = True) -> List[Dict]:
        """Get criteria catalog."""
        where = "WHERE active = 1" if active_only else ""
        return self._fetch_all(f"""
        SELECT * FROM rubric_criteria_catalog {where}
        ORDER BY label
        """)

    def update_catalog_criterion(self, criterion_id: int, updates: Dict) -> bool:
        """Update catalog criterion."""
        allowed = ['label', 'description', 'active']
        updates = {k: v for k, v in updates.items() if k in allowed}
        
        if not updates:
            return False

        set_clause = ', '.join([f"{k} = :{k}" for k in updates.keys()])
        sql = f"""
        UPDATE rubric_criteria_catalog 
        SET {set_clause}, updated_at = CURRENT_TIMESTAMP
        WHERE id = :criterion_id
        """
        
        with self.engine.begin() as conn:
            params = updates.copy()
            params['criterion_id'] = criterion_id
            self._exec(conn, sql, params)

        return True

    # ========================================================================
    # VALIDATION OPERATIONS
    # ========================================================================

    def validate_rubric_weights(self, assessment_id: int) -> Tuple[bool, float]:
        """Validate rubric criteria weights sum to 100."""
        result = self._fetch_one("""
        SELECT SUM(weight_pct) as total
        FROM rubric_assessment_criteria
        WHERE assessment_id = :assessment_id
        """, {'assessment_id': assessment_id})

        total = result['total'] if result and result['total'] else 0.0
        is_valid = abs(total - 100.0) < 0.01
        return is_valid, total

    def validate_rubric_complete(self, config_id: int) -> Dict:
        """Validate rubric is complete and ready to publish."""
        errors = []
        warnings = []

        config = self.get_rubric(config_id)
        if not config:
            return {'is_valid': False, 'errors': ['Rubric not found']}

        # Check has assessments
        if not config.get('assessments'):
            errors.append("No assessments defined")
        
        # Validate each assessment
        for assessment in config.get('assessments', []):
            if assessment['mode'] == 'analytic_points':
                # Check criteria exist
                if not assessment.get('criteria'):
                    errors.append(f"Assessment {assessment['code']}: No criteria defined")
                else:
                    # Validate weights
                    total = sum(c['weight_pct'] for c in assessment['criteria'])
                    if abs(total - 100.0) > 0.01:
                        errors.append(
                            f"Assessment {assessment['code']}: Weights sum to {total}%, not 100%"
                        )
            
            elif assessment['mode'] == 'analytic_levels':
                # Check levels exist
                if not assessment.get('levels'):
                    errors.append(f"Assessment {assessment['code']}: No levels defined")
                else:
                    # Check each criterion has at least 2 levels
                    criteria_levels = {}
                    for level in assessment['levels']:
                        key = level['criterion_key']
                        criteria_levels[key] = criteria_levels.get(key, 0) + 1
                    
                    for criterion_key, count in criteria_levels.items():
                        if count < 2:
                            warnings.append(
                                f"Assessment {assessment['code']}, {criterion_key}: "
                                f"Only {count} level(s), minimum 2 recommended"
                            )

        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }

    def check_marks_exist(self, config_id: int) -> bool:
        """Check if any marks exist for this rubric."""
        # This would check marks tables when implemented
        # For now, return False
        return False

    # ========================================================================
    # NORMALIZATION
    # ========================================================================

    def normalize_scores(self, assessment_id: int, raw_scores: Dict[str, float],
                        target_max: float) -> Dict[str, float]:
        """Normalize rubric scores to target max marks."""
        assessment = self._fetch_one("""
        SELECT max_marks FROM rubric_assessments WHERE id = :id
        """, {'id': assessment_id})

        if not assessment:
            raise ValueError(f"Assessment {assessment_id} not found")

        raw_max = assessment['max_marks']
        
        normalized = {}
        for student_id, raw_score in raw_scores.items():
            normalized[student_id] = (raw_score / raw_max) * target_max

        return normalized

    # ========================================================================
    # HELPER METHODS
    # ========================================================================

    def _is_minor_update(self, updates: Dict) -> bool:
        """Check if updates are minor (allowed when locked)."""
        minor_fields = [
            'visible_to_students', 'show_before_assessment',
            'normalization_enabled'
        ]
        return all(k in minor_fields for k in updates.keys())

    def _audit_rubric(self, action: str, config_id: int, offering_id: int,
                     scope: str, audit_entry: AuditEntry, note: str = None,
                     changed_fields: str = None):
        """Record rubric audit entry."""
        with self.engine.begin() as conn:
            self._exec(conn, """
            INSERT INTO rubrics_audit (
                rubric_config_id, offering_id, scope, action, note, changed_fields,
                actor_id, actor_role, operation, reason, source, step_up_performed
            ) VALUES (
                :config_id, :offering_id, :scope, :action, :note, :changed_fields,
                :actor_id, :actor_role, :operation, :reason, :source, :step_up
            )
            """, {
                'config_id': config_id,
                'offering_id': offering_id,
                'scope': scope,
                'action': action,
                'note': note,
                'changed_fields': changed_fields,
                'actor_id': audit_entry.actor_id,
                'actor_role': audit_entry.actor_role,
                'operation': audit_entry.operation,
                'reason': audit_entry.reason,
                'source': audit_entry.source,
                'step_up': audit_entry.step_up_performed
            })

    def _create_version_snapshot(self, config_id: int, reason: str, 
                                audit_entry: AuditEntry):
        """Create version snapshot."""
        rubric_data = self.get_rubric(config_id)
        
        with self.engine.begin() as conn:
            self._exec(conn, """
            INSERT INTO version_snapshots (
                entity_type, entity_id, snapshot_reason, actor, snapshot_data, version_number
            ) VALUES (
                'rubric_config', :config_id, :reason, :actor, :data, :version
            )
            """, {
                'config_id': config_id,
                'reason': reason,
                'actor': audit_entry.actor_id,
                'data': json.dumps(rubric_data),
                'version': rubric_data['version']
            })
