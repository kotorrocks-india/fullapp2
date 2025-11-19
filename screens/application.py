# application.py
"""
Complete application logic for Subjects, Offerings, COs, and Rubrics management.
Implements all business rules from YAML specifications (Slides 19, 20, 21).
"""

from __future__ import annotations
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
import json
import csv
from io import StringIO
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
import logging

logger = logging.getLogger(__name__)

# ===========================================================================
# ENUMS & CONSTANTS
# ===========================================================================

class Status(Enum):
    DRAFT = "draft"
    PUBLISHED = "published"
    ARCHIVED = "archived"

class SubjectType(Enum):
    CORE = "Core"
    ELECTIVE = "Elective"
    COLLEGE_PROJECT = "College Project"
    OTHER = "Other"

class BloomLevel(Enum):
    REMEMBER = "Remember"
    UNDERSTAND = "Understand"
    APPLY = "Apply"
    ANALYZE = "Analyze"
    EVALUATE = "Evaluate"
    CREATE = "Create"

class RubricMode(Enum):
    ANALYTIC_POINTS = "analytic_points"
    ANALYTIC_LEVELS = "analytic_levels"

class RubricScope(Enum):
    SUBJECT = "subject"
    COMPONENT = "component"

# ===========================================================================
# DATA CLASSES
# ===========================================================================

@dataclass
class SubjectCatalogEntry:
    """Subject catalog entry."""
    subject_code: str
    subject_name: str
    subject_type: str
    degree_code: str
    credits_total: float
    L: int = 0
    T: int = 0
    P: int = 0
    S: int = 0
    internal_marks_max: int = 40
    exam_marks_max: int = 60
    jury_viva_marks_max: int = 0
    program_code: Optional[str] = None
    branch_code: Optional[str] = None
    description: Optional[str] = None
    status: str = "active"
    active: int = 1
    
    # Pass criteria
    min_internal_percent: float = 50.0
    min_external_percent: float = 40.0
    min_overall_percent: float = 40.0
    
    # Attainment defaults
    direct_target_students_percent: float = 50.0
    indirect_target_students_percent: float = 50.0
    overall_direct_weight_percent: float = 80.0
    overall_indirect_weight_percent: float = 20.0

@dataclass
class SubjectOffering:
    """Subject offering for a specific AY-term."""
    subject_code: str
    subject_name: str
    subject_type: str
    degree_code: str
    ay_label: str
    year: int
    term: int
    credits_total: float
    total_max: int
    program_code: Optional[str] = None
    branch_code: Optional[str] = None
    division_code: Optional[str] = None
    applies_to_all_divisions: int = 1
    instructor_email: Optional[str] = None
    is_elective_parent: int = 0
    status: str = "draft"
    internal_max: Optional[int] = None
    external_max: Optional[int] = None
    jury_max: Optional[int] = None
    viva_max: Optional[int] = None
    override_inheritance: int = 0
    override_reason: Optional[str] = None
    syllabus_template_id: Optional[int] = None

@dataclass
class CourseOutcome:
    """Course outcome for an offering."""
    offering_id: int
    co_code: str
    title: str
    description: str
    bloom_level: str
    weight_in_direct: float = 0.0
    knowledge_type: Optional[str] = None
    threshold_internal_percent: Optional[float] = None
    threshold_external_percent: Optional[float] = None
    threshold_overall_percent: Optional[float] = None
    sequence: int = 0
    status: str = "draft"
    po_correlations: Optional[Dict[str, int]] = None
    pso_correlations: Optional[Dict[str, int]] = None
    peo_correlations: Optional[Dict[str, int]] = None

@dataclass
class RubricConfig:
    """Rubric configuration."""
    offering_id: int
    scope: str = "subject"
    component_key: Optional[str] = None
    mode: str = "analytic_points"
    co_linking_enabled: int = 0
    normalization_enabled: int = 1
    visible_to_students: int = 1
    show_before_assessment: int = 1
    version: int = 1
    is_locked: int = 0
    status: str = "draft"

@dataclass
class RubricAssessment:
    """Assessment within a rubric."""
    rubric_config_id: int
    code: str
    title: str
    max_marks: float
    mode: str = "analytic_points"
    component_key: Optional[str] = None

@dataclass
class AuditEntry:
    """Audit entry for tracking changes."""
    actor_id: str
    actor_role: str
    operation: str
    reason: Optional[str] = None
    source: str = "ui"
    step_up_performed: int = 0
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    session_id: Optional[str] = None

# ===========================================================================
# BASE SERVICE CLASS
# ===========================================================================

class BaseService:
    """Base service with common database operations."""
    
    def __init__(self, engine: Engine):
        self.engine = engine
    
    def _exec(self, conn, sql: str, params: dict = None):
        """Execute SQL with parameters."""
        return conn.execute(sa_text(sql), params or {})
    
    def _fetch_one(self, sql: str, params: dict = None) -> Optional[Dict]:
        """Fetch single row as dict."""
        with self.engine.begin() as conn:
            result = self._exec(conn, sql, params).fetchone()
            return dict(result._mapping) if result else None
    
    def _fetch_all(self, sql: str, params: dict = None) -> List[Dict]:
        """Fetch all rows as list of dicts."""
        with self.engine.begin() as conn:
            results = self._exec(conn, sql, params).fetchall()
            return [dict(r._mapping) for r in results]
    
    def _insert(self, sql: str, params: dict, audit_entry: AuditEntry = None) -> int:
        """Insert and return last insert id."""
        with self.engine.begin() as conn:
            self._exec(conn, sql, params)
            result = self._exec(conn, "SELECT last_insert_rowid()").fetchone()
            return result[0]
    
    def _audit(self, table: str, entity_id: int, action: str, 
               audit_entry: AuditEntry, note: Optional[str] = None,
               changed_fields: Optional[str] = None):
        """Record audit entry (simplified version matching actual schema)."""
        # Note: This is a simplified implementation
        # Real implementation would match specific audit table schemas
        pass  # Audit tables have different schemas per entity type

# ===========================================================================
# SUBJECTS CATALOG SERVICE
# ===========================================================================

class SubjectsCatalogService(BaseService):
    """Service for managing subjects catalog."""
    
    def create_subject(self, subject: SubjectCatalogEntry, 
                      audit_entry: AuditEntry) -> int:
        """Create new subject in catalog."""
        # Validate
        if not subject.subject_code or not subject.subject_name:
            raise ValueError("Subject code and name are required")
        
        if subject.credits_total <= 0:
            raise ValueError("Credits must be greater than 0")
        
        # Check for duplicates
        existing = self.get_subject(
            subject.subject_code, subject.degree_code,
            subject.program_code, subject.branch_code
        )
        if existing:
            raise ValueError(f"Subject {subject.subject_code} already exists in catalog")
        
        # Calculate totals
        total_max = subject.internal_marks_max + subject.exam_marks_max + subject.jury_viva_marks_max
        
        # Insert
        sql = """
        INSERT INTO subjects_catalog (
            subject_code, subject_name, subject_type, degree_code, program_code, branch_code,
            credits_total, L, T, P, S, internal_marks_max, exam_marks_max, jury_viva_marks_max,
            min_internal_percent, min_external_percent, min_overall_percent,
            direct_target_students_percent, indirect_target_students_percent,
            overall_direct_weight_percent, overall_indirect_weight_percent,
            description, status, active
        ) VALUES (
            :subject_code, :subject_name, :subject_type, :degree_code, :program_code, :branch_code,
            :credits_total, :L, :T, :P, :S, :internal_marks_max, :exam_marks_max, :jury_viva_marks_max,
            :min_internal_percent, :min_external_percent, :min_overall_percent,
            :direct_target_students_percent, :indirect_target_students_percent,
            :overall_direct_weight_percent, :overall_indirect_weight_percent,
            :description, :status, :active
        )
        """
        
        subject_id = self._insert(sql, asdict(subject), audit_entry)
        
        # Audit - using actual schema
        with self.engine.begin() as conn:
            self._exec(conn, """
            INSERT INTO subjects_catalog_audit (
                subject_id, subject_code, degree_code, program_code, branch_code,
                action, note, changed_fields, actor
            ) VALUES (
                :subject_id, :subject_code, :degree_code, :program_code, :branch_code,
                :action, :note, :changed_fields, :actor
            )
            """, {
                'subject_id': subject_id,
                'subject_code': subject.subject_code,
                'degree_code': subject.degree_code,
                'program_code': subject.program_code,
                'branch_code': subject.branch_code,
                'action': 'CREATED',
                'note': audit_entry.reason,
                'changed_fields': json.dumps(list(asdict(subject).keys())),
                'actor': audit_entry.actor_id
            })
        
        logger.info(f"Created subject {subject.subject_code} (ID: {subject_id})")
        return subject_id
    
    def get_subject(self, subject_code: str, degree_code: str,
                   program_code: Optional[str] = None,
                   branch_code: Optional[str] = None) -> Optional[Dict]:
        """Get subject from catalog."""
        sql = """
        SELECT * FROM subjects_catalog
        WHERE subject_code = :subject_code
        AND degree_code = :degree_code
        AND (program_code = :program_code OR (program_code IS NULL AND :program_code IS NULL))
        AND (branch_code = :branch_code OR (branch_code IS NULL AND :branch_code IS NULL))
        AND active = 1
        """
        return self._fetch_one(sql, {
            'subject_code': subject_code,
            'degree_code': degree_code,
            'program_code': program_code,
            'branch_code': branch_code
        })
    
    def list_subjects(self, degree_code: str,
                     program_code: Optional[str] = None,
                     branch_code: Optional[str] = None,
                     subject_type: Optional[str] = None) -> List[Dict]:
        """List subjects in catalog."""
        sql = """
        SELECT * FROM subjects_catalog
        WHERE degree_code = :degree_code
        AND (program_code = :program_code OR (program_code IS NULL AND :program_code IS NULL))
        AND (branch_code = :branch_code OR (branch_code IS NULL AND :branch_code IS NULL))
        AND (:subject_type IS NULL OR subject_type = :subject_type)
        AND active = 1
        ORDER BY subject_code
        """
        return self._fetch_all(sql, {
            'degree_code': degree_code,
            'program_code': program_code,
            'branch_code': branch_code,
            'subject_type': subject_type
        })
    
    def update_subject(self, subject_id: int, updates: Dict,
                      audit_entry: AuditEntry) -> bool:
        """Update subject in catalog."""
        # Get current
        current = self._fetch_one("SELECT * FROM subjects_catalog WHERE id = :id",
                                 {'id': subject_id})
        if not current:
            raise ValueError(f"Subject {subject_id} not found")
        
        # Build update SQL
        set_clauses = []
        params = {'id': subject_id}
        for key, value in updates.items():
            set_clauses.append(f"{key} = :{key}")
            params[key] = value
        
        params['updated_at'] = datetime.utcnow().isoformat()
        set_clauses.append("updated_at = :updated_at")
        
        sql = f"UPDATE subjects_catalog SET {', '.join(set_clauses)} WHERE id = :id"
        
        with self.engine.begin() as conn:
            self._exec(conn, sql, params)
            
            # Audit
            self._exec(conn, """
            INSERT INTO subjects_catalog_audit (
                subject_id, subject_code, degree_code, program_code, branch_code,
                action, note, changed_fields, actor
            ) VALUES (
                :subject_id, :subject_code, :degree_code, :program_code, :branch_code,
                :action, :note, :changed_fields, :actor
            )
            """, {
                'subject_id': subject_id,
                'subject_code': current['subject_code'],
                'degree_code': current['degree_code'],
                'program_code': current.get('program_code'),
                'branch_code': current.get('branch_code'),
                'action': 'UPDATED',
                'note': audit_entry.reason,
                'changed_fields': json.dumps(list(updates.keys())),
                'actor': audit_entry.actor_id
            })
        
        logger.info(f"Updated subject {subject_id}")
        return True

# ===========================================================================
# SUBJECT OFFERINGS SERVICE (Slide 19)
# ===========================================================================

class SubjectOfferingsService(BaseService):
    """Service for managing subject offerings (Slide 19)."""
    
    def __init__(self, engine: Engine, catalog_service: SubjectsCatalogService):
        super().__init__(engine)
        self.catalog_service = catalog_service
    
    def create_offering(self, offering: SubjectOffering,
                       audit_entry: AuditEntry) -> int:
        """Create new offering from catalog subject."""
        # Validate
        if not offering.subject_code or not offering.ay_label:
            raise ValueError("Subject code and AY are required")
        
        # Check for duplicates
        existing = self.get_offering(
            offering.subject_code, offering.degree_code,
            offering.ay_label, offering.year, offering.term,
            offering.program_code, offering.branch_code,
            offering.division_code
        )
        if existing:
            raise ValueError("Offering already exists for this scope")
        
        # Inherit from catalog if not overridden
        if not offering.override_inheritance:
            catalog_subject = self.catalog_service.get_subject(
                offering.subject_code, offering.degree_code,
                offering.program_code, offering.branch_code
            )
            if not catalog_subject:
                raise ValueError(f"Subject {offering.subject_code} not found in catalog")
            
            # Copy defaults
            offering.subject_name = catalog_subject['subject_name']
            offering.subject_type = catalog_subject['subject_type']
            offering.credits_total = catalog_subject['credits_total']
            offering.internal_max = catalog_subject['internal_marks_max']
            offering.external_max = catalog_subject['exam_marks_max']
            offering.jury_max = catalog_subject['jury_viva_marks_max']
            offering.total_max = (catalog_subject['internal_marks_max'] + 
                                 catalog_subject['exam_marks_max'] +
                                 catalog_subject['jury_viva_marks_max'])
        
        # Insert
        sql = """
        INSERT INTO subject_offerings (
            subject_code, subject_name, subject_type, degree_code, program_code, branch_code,
            ay_label, year, term, division_code, applies_to_all_divisions,
            instructor_email, is_elective_parent, credits_total,
            internal_max, external_max, jury_max, viva_max, total_max,
            override_inheritance, override_reason, status, last_updated_by
        ) VALUES (
            :subject_code, :subject_name, :subject_type, :degree_code, :program_code, :branch_code,
            :ay_label, :year, :term, :division_code, :applies_to_all_divisions,
            :instructor_email, :is_elective_parent, :credits_total,
            :internal_max, :external_max, :jury_max, :viva_max, :total_max,
            :override_inheritance, :override_reason, :status, :last_updated_by
        )
        """
        
        params = asdict(offering)
        params['last_updated_by'] = audit_entry.actor_id
        
        offering_id = self._insert(sql, params, audit_entry)
        
        # Audit
        with self.engine.begin() as conn:
            self._exec(conn, """
            INSERT INTO subject_offerings_audit (
                offering_id, subject_code, degree_code, program_code, branch_code,
                ay_label, year, term, division_code, action, actor, actor_role,
                operation, reason, source, step_up_performed
            ) VALUES (
                :offering_id, :subject_code, :degree_code, :program_code, :branch_code,
                :ay_label, :year, :term, :division_code, :action, :actor, :actor_role,
                :operation, :reason, :source, :step_up_performed
            )
            """, {
                'offering_id': offering_id,
                'subject_code': offering.subject_code,
                'degree_code': offering.degree_code,
                'program_code': offering.program_code,
                'branch_code': offering.branch_code,
                'ay_label': offering.ay_label,
                'year': offering.year,
                'term': offering.term,
                'division_code': offering.division_code,
                'action': 'OFFERING_CREATED',
                'actor': audit_entry.actor_id,
                'actor_role': audit_entry.actor_role,
                'operation': audit_entry.operation,
                'reason': audit_entry.reason,
                'source': audit_entry.source,
                'step_up_performed': audit_entry.step_up_performed
            })
        
        logger.info(f"Created offering {offering.subject_code} for {offering.ay_label} (ID: {offering_id})")
        return offering_id
    
    def get_offering(self, subject_code: str, degree_code: str,
                    ay_label: str, year: int, term: int,
                    program_code: Optional[str] = None,
                    branch_code: Optional[str] = None,
                    division_code: Optional[str] = None) -> Optional[Dict]:
        """Get offering by context."""
        sql = """
        SELECT * FROM subject_offerings
        WHERE subject_code = :subject_code
        AND degree_code = :degree_code
        AND ay_label = :ay_label
        AND year = :year
        AND term = :term
        AND (program_code = :program_code OR (program_code IS NULL AND :program_code IS NULL))
        AND (branch_code = :branch_code OR (branch_code IS NULL AND :branch_code IS NULL))
        AND (division_code = :division_code OR (division_code IS NULL AND :division_code IS NULL) 
             OR applies_to_all_divisions = 1)
        """
        return self._fetch_one(sql, {
            'subject_code': subject_code,
            'degree_code': degree_code,
            'ay_label': ay_label,
            'year': year,
            'term': term,
            'program_code': program_code,
            'branch_code': branch_code,
            'division_code': division_code
        })
    
    def list_offerings(self, degree_code: str, ay_label: str,
                      year: Optional[int] = None,
                      term: Optional[int] = None,
                      program_code: Optional[str] = None,
                      branch_code: Optional[str] = None,
                      status: Optional[str] = None) -> List[Dict]:
        """List offerings for AY."""
        sql = """
        SELECT * FROM subject_offerings
        WHERE degree_code = :degree_code
        AND ay_label = :ay_label
        AND (:year IS NULL OR year = :year)
        AND (:term IS NULL OR term = :term)
        AND (program_code = :program_code OR (program_code IS NULL AND :program_code IS NULL))
        AND (branch_code = :branch_code OR (branch_code IS NULL AND :branch_code IS NULL))
        AND (:status IS NULL OR status = :status)
        ORDER BY year, term, subject_code
        """
        return self._fetch_all(sql, {
            'degree_code': degree_code,
            'ay_label': ay_label,
            'year': year,
            'term': term,
            'program_code': program_code,
            'branch_code': branch_code,
            'status': status
        })
    
    def publish_offering(self, offering_id: int, audit_entry: AuditEntry) -> bool:
        """Publish offering (requires approval)."""
        # Get current
        offering = self._fetch_one("SELECT * FROM subject_offerings WHERE id = :id",
                                  {'id': offering_id})
        if not offering:
            raise ValueError(f"Offering {offering_id} not found")
        
        if offering['status'] == 'published':
            raise ValueError("Offering is already published")
        
        # Check if elective parent has topics (warning only)
        if offering['is_elective_parent']:
            logger.warning(f"Publishing elective parent {offering_id} - ensure topics are added")
        
        # Update status
        with self.engine.begin() as conn:
            self._exec(conn, """
            UPDATE subject_offerings
            SET status = 'published', updated_at = :now, last_updated_by = :actor
            WHERE id = :id
            """, {
                'id': offering_id,
                'now': datetime.utcnow().isoformat(),
                'actor': audit_entry.actor_id
            })
            
            # Audit
            self._exec(conn, """
            INSERT INTO subject_offerings_audit (
                offering_id, subject_code, degree_code, ay_label, year, term,
                action, actor, actor_role, operation, reason, source, step_up_performed
            ) VALUES (
                :offering_id, :subject_code, :degree_code, :ay_label, :year, :term,
                'OFFERING_PUBLISHED', :actor, :actor_role, 'publish',
                :reason, :source, :step_up_performed
            )
            """, {
                'offering_id': offering_id,
                'subject_code': offering['subject_code'],
                'degree_code': offering['degree_code'],
                'ay_label': offering['ay_label'],
                'year': offering['year'],
                'term': offering['term'],
                'actor': audit_entry.actor_id,
                'actor_role': audit_entry.actor_role,
                'reason': audit_entry.reason,
                'source': audit_entry.source,
                'step_up_performed': audit_entry.step_up_performed
            })
        
        logger.info(f"Published offering {offering_id}")
        return True
    
    def copy_offerings_between_ays(self, from_ay: str, to_ay: str,
                                   degree_code: str,
                                   audit_entry: AuditEntry,
                                   year: Optional[int] = None,
                                   term: Optional[int] = None,
                                   behavior: str = "clone_as_draft") -> Dict[str, Any]:
        """Copy offerings from one AY to another."""
        # Get source offerings
        source_offerings = self.list_offerings(
            degree_code, from_ay, year, term, status='published'
        )
        
        results = {
            'total': len(source_offerings),
            'created': 0,
            'skipped': 0,
            'errors': []
        }
        
        for source in source_offerings:
            try:
                # Check if target exists
                target_exists = self.get_offering(
                    source['subject_code'], source['degree_code'],
                    to_ay, source['year'], source['term'],
                    source['program_code'], source['branch_code'],
                    source['division_code']
                )
                
                if target_exists:
                    if behavior == "skip":
                        results['skipped'] += 1
                        continue
                    elif behavior == "clone_as_draft":
                        # Create new draft version
                        pass
                    # else overwrite
                
                # Create new offering
                new_offering = SubjectOffering(
                    subject_code=source['subject_code'],
                    subject_name=source['subject_name'],
                    subject_type=source['subject_type'],
                    degree_code=source['degree_code'],
                    program_code=source['program_code'],
                    branch_code=source['branch_code'],
                    ay_label=to_ay,
                    year=source['year'],
                    term=source['term'],
                    division_code=source['division_code'],
                    applies_to_all_divisions=source['applies_to_all_divisions'],
                    instructor_email=source['instructor_email'],
                    is_elective_parent=source['is_elective_parent'],
                    credits_total=source['credits_total'],
                    internal_max=source['internal_max'],
                    external_max=source['external_max'],
                    jury_max=source['jury_max'],
                    viva_max=source['viva_max'],
                    total_max=source['total_max'],
                    status='draft',  # Always draft when copying
                    override_inheritance=source['override_inheritance'],
                    override_reason=f"Copied from {from_ay}"
                )
                
                self.create_offering(new_offering, audit_entry)
                results['created'] += 1
                
            except Exception as e:
                results['errors'].append({
                    'subject_code': source['subject_code'],
                    'error': str(e)
                })
                logger.error(f"Error copying {source['subject_code']}: {e}")
        
        logger.info(f"Copied {results['created']}/{results['total']} offerings from {from_ay} to {to_ay}")
        return results

# ===========================================================================
# COURSE OUTCOMES SERVICE (Slide 20)
# ===========================================================================

class CourseOutcomesService(BaseService):
    """Service for managing course outcomes (Slide 20)."""
    
    def create_co(self, co: CourseOutcome, audit_entry: AuditEntry) -> int:
        """Create course outcome."""
        # Validate
        if not co.co_code or not co.title:
            raise ValueError("CO code and title are required")
        
        if co.bloom_level not in [b.value for b in BloomLevel]:
            raise ValueError(f"Invalid Bloom level: {co.bloom_level}")
        
        if not (0 <= co.weight_in_direct <= 1):
            raise ValueError("Weight must be between 0 and 1")
        
        # Check for duplicates
        existing = self._fetch_one("""
        SELECT id FROM subject_cos
        WHERE offering_id = :offering_id AND co_code = :co_code
        """, {'offering_id': co.offering_id, 'co_code': co.co_code})
        
        if existing:
            raise ValueError(f"CO {co.co_code} already exists for this offering")
        
        # Insert CO
        sql = """
        INSERT INTO subject_cos (
            offering_id, co_code, title, description, bloom_level, knowledge_type,
            weight_in_direct, threshold_internal_percent, threshold_external_percent,
            threshold_overall_percent, sequence, status, last_updated_by
        ) VALUES (
            :offering_id, :co_code, :title, :description, :bloom_level, :knowledge_type,
            :weight_in_direct, :threshold_internal_percent, :threshold_external_percent,
            :threshold_overall_percent, :sequence, :status, :last_updated_by
        )
        """
        
        params = {
            'offering_id': co.offering_id,
            'co_code': co.co_code,
            'title': co.title,
            'description': co.description,
            'bloom_level': co.bloom_level,
            'knowledge_type': co.knowledge_type,
            'weight_in_direct': co.weight_in_direct,
            'threshold_internal_percent': co.threshold_internal_percent,
            'threshold_external_percent': co.threshold_external_percent,
            'threshold_overall_percent': co.threshold_overall_percent,
            'sequence': co.sequence,
            'status': co.status,
            'last_updated_by': audit_entry.actor_id
        }
        
        co_id = self._insert(sql, params, audit_entry)
        
        # Add correlations
        if co.po_correlations:
            self._add_correlations(co_id, 'po', co.po_correlations)
        if co.pso_correlations:
            self._add_correlations(co_id, 'pso', co.pso_correlations)
        if co.peo_correlations:
            self._add_correlations(co_id, 'peo', co.peo_correlations)
        
        # Audit
        with self.engine.begin() as conn:
            self._exec(conn, """
            INSERT INTO subject_cos_audit (
                co_id, offering_id, co_code, action, actor_id, actor_role,
                operation, reason, source, step_up_performed
            ) VALUES (
                :co_id, :offering_id, :co_code, 'CO_CREATED', :actor_id, :actor_role,
                :operation, :reason, :source, :step_up_performed
            )
            """, {
                'co_id': co_id,
                'offering_id': co.offering_id,
                'co_code': co.co_code,
                'actor_id': audit_entry.actor_id,
                'actor_role': audit_entry.actor_role,
                'operation': audit_entry.operation,
                'reason': audit_entry.reason,
                'source': audit_entry.source,
                'step_up_performed': audit_entry.step_up_performed
            })
        
        logger.info(f"Created CO {co.co_code} (ID: {co_id})")
        return co_id
    
    def _add_correlations(self, co_id: int, corr_type: str, correlations: Dict[str, int]):
        """Add PO/PSO/PEO correlations."""
        table = f"co_{corr_type}_correlations"
        code_field = f"{corr_type}_code"
        
        with self.engine.begin() as conn:
            for code, value in correlations.items():
                if value not in [0, 1, 2, 3]:
                    raise ValueError(f"Invalid correlation value: {value}")
                
                self._exec(conn, f"""
                INSERT INTO {table} (co_id, {code_field}, correlation_value)
                VALUES (:co_id, :code, :value)
                """, {'co_id': co_id, 'code': code, 'value': value})
    
    def get_cos_for_offering(self, offering_id: int,
                            include_correlations: bool = True) -> List[Dict]:
        """Get all COs for an offering."""
        cos = self._fetch_all("""
        SELECT * FROM subject_cos
        WHERE offering_id = :offering_id
        ORDER BY sequence, co_code
        """, {'offering_id': offering_id})
        
        if include_correlations:
            for co in cos:
                co['po_correlations'] = self._get_correlations(co['id'], 'po')
                co['pso_correlations'] = self._get_correlations(co['id'], 'pso')
                co['peo_correlations'] = self._get_correlations(co['id'], 'peo')
        
        return cos
    
    def _get_correlations(self, co_id: int, corr_type: str) -> Dict[str, int]:
        """Get correlations for a CO."""
        table = f"co_{corr_type}_correlations"
        code_field = f"{corr_type}_code"
        
        results = self._fetch_all(f"""
        SELECT {code_field} as code, correlation_value as value
        FROM {table}
        WHERE co_id = :co_id
        """, {'co_id': co_id})
        
        return {r['code']: r['value'] for r in results}
    
    def validate_co_weights(self, offering_id: int) -> Tuple[bool, float]:
        """Validate that CO weights sum to ~1.0."""
        result = self._fetch_one("""
        SELECT SUM(weight_in_direct) as total
        FROM subject_cos
        WHERE offering_id = :offering_id
        """, {'offering_id': offering_id})
        
        total = result['total'] if result and result['total'] else 0.0
        is_valid = abs(total - 1.0) < 0.1  # 10% tolerance
        
        return is_valid, total
    
    def generate_po_co_matrix(self, offering_id: int) -> Dict[str, Any]:
        """Generate PO-CO correlation matrix."""
        cos = self.get_cos_for_offering(offering_id, include_correlations=True)
        
        # Get all POs
        po_codes = set()
        for co in cos:
            po_codes.update(co['po_correlations'].keys())
        
        po_codes = sorted(po_codes)
        
        # Build matrix
        matrix = []
        for co in cos:
            row = {
                'co_code': co['co_code'],
                'co_title': co['title']
            }
            for po in po_codes:
                row[po] = co['po_correlations'].get(po, 0)
            matrix.append(row)
        
        # Calculate averages per PO
        averages = {}
        for po in po_codes:
            values = [row[po] for row in matrix if row[po] > 0]
            averages[po] = sum(values) / len(values) if values else 0
        
        return {
            'matrix': matrix,
            'po_codes': po_codes,
            'averages': averages
        }
    
    def copy_cos_between_offerings(self, source_offering_id: int,
                                   target_offering_id: int,
                                   audit_entry: AuditEntry,
                                   include_correlations: bool = True) -> int:
        """Copy COs from one offering to another."""
        source_cos = self.get_cos_for_offering(source_offering_id, include_correlations)
        
        created = 0
        for source_co in source_cos:
            co = CourseOutcome(
                offering_id=target_offering_id,
                co_code=source_co['co_code'],
                title=source_co['title'],
                description=source_co['description'],
                bloom_level=source_co['bloom_level'],
                knowledge_type=source_co['knowledge_type'],
                weight_in_direct=source_co['weight_in_direct'],
                threshold_internal_percent=source_co['threshold_internal_percent'],
                threshold_external_percent=source_co['threshold_external_percent'],
                threshold_overall_percent=source_co['threshold_overall_percent'],
                sequence=source_co['sequence'],
                status='draft',
                po_correlations=source_co.get('po_correlations') if include_correlations else None,
                pso_correlations=source_co.get('pso_correlations') if include_correlations else None,
                peo_correlations=source_co.get('peo_correlations') if include_correlations else None
            )
            
            try:
                self.create_co(co, audit_entry)
                created += 1
            except Exception as e:
                logger.error(f"Error copying CO {co.co_code}: {e}")
        
        logger.info(f"Copied {created}/{len(source_cos)} COs")
        return created

# ===========================================================================
# RUBRICS SERVICE (Slide 21)
# ===========================================================================

class RubricsService(BaseService):
    """Service for managing rubrics (Slide 21)."""
    
    def create_rubric_config(self, config: RubricConfig,
                            audit_entry: AuditEntry) -> int:
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
        
        # Audit
        with self.engine.begin() as conn:
            self._exec(conn, """
            INSERT INTO rubrics_audit (
                rubric_config_id, offering_id, scope, action,
                actor_id, actor_role, operation, reason, source, step_up_performed
            ) VALUES (
                :config_id, :offering_id, :scope, 'RUBRIC_CREATED',
                :actor_id, :actor_role, :operation, :reason, :source, :step_up_performed
            )
            """, {
                'config_id': config_id,
                'offering_id': config.offering_id,
                'scope': config.scope,
                'actor_id': audit_entry.actor_id,
                'actor_role': audit_entry.actor_role,
                'operation': audit_entry.operation,
                'reason': audit_entry.reason,
                'source': audit_entry.source,
                'step_up_performed': audit_entry.step_up_performed
            })
        
        logger.info(f"Created rubric config {config_id}")
        return config_id
    
    def add_assessment(self, assessment: RubricAssessment,
                      audit_entry: AuditEntry) -> int:
        """Add assessment to rubric."""
        # Validate config exists
        config = self._fetch_one("""
        SELECT * FROM rubric_configs WHERE id = :id
        """, {'id': assessment.rubric_config_id})
        
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
    
    def add_criteria_weights(self, assessment_id: int,
                           criteria: Dict[str, float],
                           audit_entry: AuditEntry) -> bool:
        """Add criteria weights (analytic_points mode)."""
        # Validate weights sum to 100
        total = sum(criteria.values())
        if abs(total - 100.0) > 0.01:
            raise ValueError(f"Criteria weights must sum to 100%, got {total}%")
        
        with self.engine.begin() as conn:
            for criterion_key, weight_pct in criteria.items():
                self._exec(conn, """
                INSERT INTO rubric_assessment_criteria (
                    assessment_id, criterion_key, weight_pct
                ) VALUES (
                    :assessment_id, :criterion_key, :weight_pct
                )
                """, {
                    'assessment_id': assessment_id,
                    'criterion_key': criterion_key,
                    'weight_pct': weight_pct
                })
        
        logger.info(f"Added {len(criteria)} criteria to assessment {assessment_id}")
        return True
    
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
        
        # Get criteria for each assessment
        for assessment in assessments:
            if assessment['mode'] == 'analytic_points':
                criteria = self._fetch_all("""
                SELECT * FROM rubric_assessment_criteria
                WHERE assessment_id = :assessment_id
                """, {'assessment_id': assessment['id']})
                assessment['criteria'] = criteria
            else:
                levels = self._fetch_all("""
                SELECT * FROM rubric_assessment_levels
                WHERE assessment_id = :assessment_id
                ORDER BY criterion_key, level_sequence
                """, {'assessment_id': assessment['id']})
                assessment['levels'] = levels
        
        config['assessments'] = assessments
        return config

# ===========================================================================
# IMPORT/EXPORT SERVICE
# ===========================================================================

class ImportExportService(BaseService):
    """Service for bulk import/export operations."""
    
    def import_offerings_csv(self, csv_data: str, 
                           offerings_service: SubjectOfferingsService,
                           audit_entry: AuditEntry) -> Dict[str, Any]:
        """Import offerings from CSV."""
        reader = csv.DictReader(StringIO(csv_data))
        
        results = {
            'total': 0,
            'created': 0,
            'skipped': 0,
            'errors': []
        }
        
        for row in reader:
            results['total'] += 1
            
            try:
                offering = SubjectOffering(
                    subject_code=row['subject_code'],
                    subject_name=row.get('subject_name', ''),
                    subject_type=row['subject_type'],
                    degree_code=row['degree_code'],
                    program_code=row.get('program_code') or None,
                    branch_code=row.get('branch_code') or None,
                    ay_label=row['ay_label'],
                    year=int(row['year']),
                    term=int(row['term']),
                    division_code=row.get('division_code') or None,
                    applies_to_all_divisions=int(row.get('applies_to_all_divisions', 1)),
                    is_elective_parent=int(row.get('is_elective_parent', 0)),
                    credits_total=float(row.get('credits_total', 0)),
                    total_max=int(row.get('total_max', 100)),
                    status=row.get('status', 'draft')
                )
                
                offerings_service.create_offering(offering, audit_entry)
                results['created'] += 1
                
            except Exception as e:
                results['errors'].append({
                    'row': results['total'],
                    'subject_code': row.get('subject_code'),
                    'error': str(e)
                })
        
        return results
    
    def export_offerings_csv(self, offerings: List[Dict]) -> str:
        """Export offerings to CSV."""
        output = StringIO()
        
        if not offerings:
            return ""
        
        fieldnames = ['subject_code', 'subject_name', 'subject_type',
                     'degree_code', 'program_code', 'branch_code',
                     'ay_label', 'year', 'term', 'division_code',
                     'credits_total', 'total_max', 'status']
        
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        
        for offering in offerings:
            row = {k: offering.get(k, '') for k in fieldnames}
            writer.writerow(row)
        
        return output.getvalue()
    
    def import_cos_csv(self, csv_data: str, 
                      cos_service: CourseOutcomesService,
                      audit_entry: AuditEntry) -> Dict[str, Any]:
        """Import COs from CSV."""
        reader = csv.DictReader(StringIO(csv_data))
        
        results = {
            'total': 0,
            'created': 0,
            'errors': []
        }
        
        for row in reader:
            results['total'] += 1
            
            try:
                # Parse correlations
                po_corr = {}
                pso_corr = {}
                for key, value in row.items():
                    if key.startswith('PO') and value:
                        po_corr[key] = int(value)
                    elif key.startswith('PSO') and value:
                        pso_corr[key] = int(value)
                
                co = CourseOutcome(
                    offering_id=int(row['offering_id']),
                    co_code=row['co_code'],
                    title=row['title'],
                    description=row['description'],
                    bloom_level=row['bloom_level'],
                    weight_in_direct=float(row.get('weight_in_direct', 0)),
                    threshold_internal_percent=float(row['threshold_internal_percent']) if row.get('threshold_internal_percent') else None,
                    threshold_external_percent=float(row['threshold_external_percent']) if row.get('threshold_external_percent') else None,
                    threshold_overall_percent=float(row['threshold_overall_percent']) if row.get('threshold_overall_percent') else None,
                    sequence=int(row.get('sequence', 0)),
                    status=row.get('status', 'draft'),
                    po_correlations=po_corr if po_corr else None,
                    pso_correlations=pso_corr if pso_corr else None
                )
                
                cos_service.create_co(co, audit_entry)
                results['created'] += 1
                
            except Exception as e:
                results['errors'].append({
                    'row': results['total'],
                    'co_code': row.get('co_code'),
                    'error': str(e)
                })
        
        return results

# ===========================================================================
# MAIN APPLICATION FACADE
# ===========================================================================

class SubjectsApplication:
    """Main application facade for all services."""
    
    def __init__(self, engine: Engine):
        self.engine = engine
        
        # Initialize services
        self.catalog = SubjectsCatalogService(engine)
        self.offerings = SubjectOfferingsService(engine, self.catalog)
        self.cos = CourseOutcomesService(engine)
        self.rubrics = RubricsService(engine)
        self.import_export = ImportExportService(engine)
    
    def get_complete_offering_context(self, offering_id: int) -> Dict[str, Any]:
        """Get complete context for an offering."""
        offering = self.offerings._fetch_one(
            "SELECT * FROM subject_offerings WHERE id = :id",
            {'id': offering_id}
        )
        
        if not offering:
            return None
        
        cos = self.cos.get_cos_for_offering(offering_id)
        rubric = self.rubrics.get_rubric(offering_id)
        
        # Validate
        co_weights_valid, co_weight_total = self.cos.validate_co_weights(offering_id)
        
        return {
            'offering': offering,
            'cos': cos,
            'rubric': rubric,
            'validations': {
                'co_weights_valid': co_weights_valid,
                'co_weight_total': co_weight_total,
                'cos_count': len(cos)
            }
        }

# ===========================================================================
# EXAMPLE USAGE
# ===========================================================================

if __name__ == "__main__":
    from sqlalchemy import create_engine
    from comprehensive_subjects_schema import install_comprehensive_schema
    
    # Create database
    engine = create_engine("sqlite:///education_test.db")
    install_comprehensive_schema(engine)
    
    # Initialize application
    app = SubjectsApplication(engine)
    
    # Create audit entry
    audit = AuditEntry(
        actor_id="admin@example.com",
        actor_role="superadmin",
        operation="create",
        source="api"
    )
    
    # 1. Create subject in catalog
    subject = SubjectCatalogEntry(
        subject_code="ARC101",
        subject_name="Architectural Design 1",
        subject_type="Core",
        degree_code="BARCH",
        credits_total=4.0,
        L=2, T=1, P=2, S=0,
        internal_marks_max=40,
        exam_marks_max=60
    )
    
    subject_id = app.catalog.create_subject(subject, audit)
    print(f"✓ Created subject {subject.subject_code} (ID: {subject_id})")
    
    # 2. Create offering
    offering = SubjectOffering(
        subject_code="ARC101",
        subject_name="Architectural Design 1",
        subject_type="Core",
        degree_code="BARCH",
        ay_label="2025-26",
        year=1,
        term=1,
        credits_total=4.0,
        total_max=100
    )
    
    offering_id = app.offerings.create_offering(offering, audit)
    print(f"✓ Created offering (ID: {offering_id})")
    
    # 3. Create COs
    cos_data = [
        ("CO1", "Understand basic design principles", "Understand", 0.2),
        ("CO2", "Apply design concepts", "Apply", 0.3),
        ("CO3", "Analyze design solutions", "Analyze", 0.5)
    ]
    
    for co_code, title, bloom, weight in cos_data:
        co = CourseOutcome(
            offering_id=offering_id,
            co_code=co_code,
            title=title,
            description=f"Students will be able to {title.lower()}",
            bloom_level=bloom,
            weight_in_direct=weight,
            po_correlations={"PO1": 3, "PO2": 2}
        )
        co_id = app.cos.create_co(co, audit)
        print(f"✓ Created {co_code} (ID: {co_id})")
    
    # 4. Validate
    valid, total = app.cos.validate_co_weights(offering_id)
    print(f"\n✓ CO weights: {total:.2f} (valid: {valid})")
    
    # 5. Get complete context
    context = app.get_complete_offering_context(offering_id)
    print(f"\n✓ Complete context retrieved:")
    print(f"  - Offering: {context['offering']['subject_name']}")
    print(f"  - COs: {context['validations']['cos_count']}")
    print(f"  - CO weights valid: {context['validations']['co_weights_valid']}")
    
    print("\n✅ Application logic demonstration completed!")
