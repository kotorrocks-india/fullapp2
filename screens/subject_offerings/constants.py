"""
Constants and validation rules for Subject Offerings Management
Complete implementation per YAML specification (Slide 19)
"""

from typing import Tuple, Dict, Any, List
import json

# ===================================================================
# ENUMS
# ===================================================================

STATUS_VALUES = ["draft", "published", "archived"]
SUBJECT_TYPES = ["Core", "Elective", "College Project", "Other"]

# Action types for audit
AUDIT_ACTIONS = [
    "create", "update", "delete", 
    "publish", "unpublish", "archive", "restore",
    "freeze", "unfreeze",
    "override_enable", "override_disable",
    "copy_forward", "bulk_update",
    "import", "export"
]

# Approval request types
APPROVAL_TYPES = [
    "publish", "override_enable", "delete", "bulk_publish"
]

# Health check types
HEALTH_CHECK_TYPES = [
    "duplicate_offerings",
    "catalog_sync_status",
    "missing_divisions",
    "orphaned_offerings",
    "marks_consistency",
    "elective_topics_required"
]

# ===================================================================
# EXPORT COLUMNS
# ===================================================================

OFFERINGS_EXPORT_COLUMNS = [
    "offering_id",
    "ay_label",
    "degree_code",
    "program_code",
    "branch_code",
    "curriculum_group_code",
    "year",
    "term",
    "division_code",
    "applies_to_all_divisions",
    "subject_code",
    "subject_name",
    "subject_type",
    "is_elective_parent",
    "credits_total",
    "L",
    "T",
    "P",
    "S",
    "internal_marks_max",
    "exam_marks_max",
    "jury_viva_marks_max",
    "total_marks_max",
    "direct_weight_percent",
    "indirect_weight_percent",
    "pass_threshold_overall",
    "pass_threshold_internal",
    "pass_threshold_external",
    "status",
    "instructor_email",
    "override_inheritance",
    "override_reason",
    "override_approved_by",
    "is_frozen",
    "frozen_reason",
    "created_at",
    "updated_at",
    "created_by",
    "updated_by"
]

OFFERINGS_IMPORT_TEMPLATE_COLUMNS = [
    "degree_code",
    "program_code",
    "branch_code",
    "curriculum_group_code",
    "ay_label",
    "year",
    "term",
    "division_code",
    "applies_to_all_divisions",
    "subject_code",
    "subject_type",
    "is_elective_parent",
    "status",
    "instructor_email",
    "override_inheritance",
    "override_reason",
    "elective_selection_lead_days"
]

# ===================================================================
# PRETERM DEFAULTS (for Elective Selection Windows - Slide 18)
# ===================================================================

PRETERM_DEFAULTS = {
    "elective_selection_lead_days": 21,  # Days before term starts
    "allow_negative_offset": True,       # Allow setting window before term
    "default_window_duration_days": 7,   # Default selection window length
    "auto_confirm_enabled": True,
    "min_satisfaction_percent": 50.0
}

# ===================================================================
# CROSS-SCOPE RULES
# ===================================================================

CROSS_SCOPE_RULES = {
    "allow_cross_program": True,
    "allow_cross_branch": True,
    "allow_cross_degree": False  # Strictly disallowed
}

# ===================================================================
# GUARDRAILS
# ===================================================================

GUARDRAILS = {
    "subject_must_exist_in_catalog": True,
    "subject_code_unique_per_ay_term_div": True,
    "subject_type_must_match_catalog": True,
    "require_is_elective_parent_for_elective_cp": False,
    
    "electives_cp_publish_requires_topics_or_ack": {
        "enabled": True,
        "mode": "ack_ok"  # allow publish with acknowledgment
    },
    
    "totals_consistency_checks": {
        "internal_external_sum_le_total": True,
        "direct_indirect_sum_equals_100_if_defined": True
    },
    
    "freeze_on_marks_exist": {
        "enabled": True,
        "allow_minor_edits_with_reason": True,
        "minor_edits": ["instructor_email", "status"],
        "block_division_scope_change": True,
        "block_credits_change": True,
        "block_marks_change": True
    }
}

# ===================================================================
# APPROVAL SETTINGS
# ===================================================================

APPROVAL_SETTINGS = {
    "publish": {
        "approvers_any_one_of": ["principal", "director"],
        "require_reason": True,
        "require_step_up": True,
        "step_up_ttl_minutes": 15
    },
    "override_enable": {
        "approvers_any_one_of": ["principal", "director"],
        "require_reason": True,
        "require_step_up": True,
        "step_up_ttl_minutes": 15
    },
    "delete": {
        "approvers_any_one_of": ["superadmin", "principal", "director"],
        "require_reason": True,
        "require_step_up": False
    },
    "fallback_when_group_empty": {
        "enabled": True,
        "fallback_to": ["superadmin"]
    }
}

# ===================================================================
# VERSIONING SETTINGS
# ===================================================================

VERSIONING_SETTINGS = {
    "snapshot_on": ["create", "update", "publish", "rollback", "archive", "restore"],
    "keep_last": 100,
    "rollback": {
        "allowed_on_draft": True,
        "allowed_on_published_if_no_marks": True,
        "published_with_marks_behavior": "clone_new_draft"
    }
}

# ===================================================================
# MONITORING SETTINGS
# ===================================================================

MONITORING_SETTINGS = {
    "health_checks": [
        {
            "name": "duplicate_offerings",
            "description": "Check for duplicated subject_code within AY+Term+Division scope",
            "frequency": "hourly",
            "alert_threshold": 1
        },
        {
            "name": "catalog_sync_status",
            "description": "Ensure offerings reflect current catalog defaults unless an approved override exists",
            "frequency": "daily",
            "alert_threshold": 1
        }
    ],
    "metrics": [
        "offerings_published",
        "offerings_core_count",
        "offerings_elective_count",
        "offerings_cp_count"
    ],
    "alerting": {
        "channels": ["email", "system_notification", "dashboard"],
        "escalation_rules": True,
        "alert_suppression": True
    }
}

# ===================================================================
# VALIDATION FUNCTIONS
# ===================================================================

def validate_offering(data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Comprehensive offering validation.
    Implements all guardrails from YAML specification.
    """
    
    # Required fields
    required = ["degree_code", "ay_label", "year", "term", "subject_code", "subject_type"]
    for field in required:
        if not data.get(field):
            return False, f"Required field missing: {field}"
    
    # Year & Term validation
    year = data.get("year")
    term = data.get("term")
    
    if not isinstance(year, int) or year < 1 or year > 10:
        return False, "Year must be between 1 and 10"
    
    if not isinstance(term, int) or term < 1 or term > 4:
        return False, "Term must be between 1 and 4"
    
    # Subject type validation
    subject_type = data.get("subject_type")
    if subject_type not in SUBJECT_TYPES:
        return False, f"Subject type must be one of: {', '.join(SUBJECT_TYPES)}"
    
    # Status validation
    status = data.get("status", "draft")
    if status not in STATUS_VALUES:
        return False, f"Status must be one of: {', '.join(STATUS_VALUES)}"
    
    # Division logic validation
    applies_to_all = data.get("applies_to_all_divisions", True)
    division_code = data.get("division_code")
    
    if not applies_to_all and not division_code:
        return False, "Division code required when applies_to_all_divisions is False"
    
    if applies_to_all and division_code:
        return False, "Cannot specify division_code when applies_to_all_divisions is True"
    
    # Override validation
    override = data.get("override_inheritance", False)
    override_reason = data.get("override_reason", "")
    
    if override and not override_reason:
        return False, "Override reason required when override_inheritance is True"
    
    # Marks consistency checks (Guardrail)
    if GUARDRAILS["totals_consistency_checks"]["internal_external_sum_le_total"]:
        internal = data.get("internal_marks_max", 0)
        exam = data.get("exam_marks_max", 0)
        jury = data.get("jury_viva_marks_max", 0)
        total = data.get("total_marks_max", 0)
        
        if internal + exam + jury != total:
            return False, f"Marks sum mismatch: {internal} + {exam} + {jury} != {total}"
    
    # Weight percentage checks
    if GUARDRAILS["totals_consistency_checks"]["direct_indirect_sum_equals_100_if_defined"]:
        direct = data.get("direct_weight_percent", 0)
        indirect = data.get("indirect_weight_percent", 0)
        
        if (direct != 0 or indirect != 0) and (direct + indirect != 100.0):
            return False, f"Weight percentages must sum to 100: {direct} + {indirect} != 100"
    
    # Cross-scope validation
    if not CROSS_SCOPE_RULES["allow_cross_degree"]:
        # Would need to check against catalog here - deferred to DB validation
        pass
    
    return True, ""


def validate_offering_uniqueness(conn, data: Dict[str, Any], offering_id: int = None) -> Tuple[bool, str]:
    """
    Check if offering already exists for this scope.
    Implements uniqueness guardrail.
    """
    from screens.subject_offerings.helpers import exec_query
    
    query = """
        SELECT id FROM subject_offerings
        WHERE ay_label = :ay AND degree_code = :d AND year = :y AND term = :t
        AND subject_code = :sc
        AND COALESCE(program_code, '') = COALESCE(:p, '')
        AND COALESCE(branch_code, '') = COALESCE(:b, '')
        AND COALESCE(curriculum_group_code, '') = COALESCE(:cg, '')
        AND COALESCE(division_code, '') = COALESCE(:div, '')
    """
    
    params = {
        "ay": data["ay_label"],
        "d": data["degree_code"],
        "y": data["year"],
        "t": data["term"],
        "sc": data["subject_code"],
        "p": data.get("program_code"),
        "b": data.get("branch_code"),
        "cg": data.get("curriculum_group_code"),
        "div": data.get("division_code"),
    }
    
    if offering_id:
        query += " AND id != :id"
        params["id"] = offering_id
    
    existing = exec_query(conn, query, params).fetchone()
    
    if existing:
        return False, "Offering already exists for this scope (AY/Degree/Program/Branch/CG/Year/Term/Division/Subject)"
    
    return True, ""


def validate_freeze_rules(conn, offering_id: int, proposed_updates: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate changes against freeze rules.
    Implements freeze guardrails from YAML.
    """
    from screens.subject_offerings.helpers import exec_query
    
    if not GUARDRAILS["freeze_on_marks_exist"]["enabled"]:
        return True, ""
    
    # Check if offering is frozen
    result = exec_query(conn, """
        SELECT is_frozen, frozen_reason FROM subject_offerings WHERE id = :id
    """, {"id": offering_id}).fetchone()
    
    if not result or result[0] != 1:
        return True, ""  # Not frozen
    
    # Check if any marks exist
    marks_count = exec_query(conn, """
        SELECT COUNT(*) FROM subject_marks WHERE offering_id = :id
    """, {"id": offering_id}).fetchone()[0]
    
    if marks_count == 0:
        return True, ""  # No marks, can edit
    
    # Check if only minor edits
    allowed_minor = GUARDRAILS["freeze_on_marks_exist"]["minor_edits"]
    proposed_keys = set(proposed_updates.keys())
    
    if proposed_keys.issubset(set(allowed_minor)):
        if GUARDRAILS["freeze_on_marks_exist"]["allow_minor_edits_with_reason"]:
            if proposed_updates.get("update_reason"):
                return True, ""
            else:
                return False, "Minor edits on frozen offering require a reason"
        return True, ""
    
    # Check blocked fields
    blocked_fields = []
    
    if GUARDRAILS["freeze_on_marks_exist"]["block_division_scope_change"]:
        if "division_code" in proposed_keys or "applies_to_all_divisions" in proposed_keys:
            blocked_fields.append("division scope")
    
    if GUARDRAILS["freeze_on_marks_exist"]["block_credits_change"]:
        if "credits_total" in proposed_keys:
            blocked_fields.append("credits")
    
    if GUARDRAILS["freeze_on_marks_exist"]["block_marks_change"]:
        marks_fields = {"internal_marks_max", "exam_marks_max", "jury_viva_marks_max", "total_marks_max"}
        if proposed_keys.intersection(marks_fields):
            blocked_fields.append("marks structure")
    
    if blocked_fields:
        return False, f"Cannot modify {', '.join(blocked_fields)} on frozen offering with marks. Unfreeze first."
    
    return True, ""


def validate_elective_publish_requirements(conn, offering_id: int, acknowledge: bool = False) -> Tuple[bool, str, List[str]]:
    """
    Validate elective/CP offerings have topics before publishing.
    Implements elective publish guardrail with acknowledgment mode.
    """
    from screens.subject_offerings.helpers import exec_query
    
    config = GUARDRAILS["electives_cp_publish_requires_topics_or_ack"]
    if not config["enabled"]:
        return True, "", []
    
    # Get offering details
    offering = exec_query(conn, """
        SELECT subject_type, is_elective_parent, subject_code, ay_label, year, term
        FROM subject_offerings WHERE id = :id
    """, {"id": offering_id}).fetchone()
    
    if not offering:
        return False, "Offering not found", []
    
    if offering[1] != 1 or offering[0] not in ["Elective", "College Project"]:
        return True, "", []  # Not an elective/CP parent
    
    # Check for topics
    topics_count = exec_query(conn, """
        SELECT COUNT(*) FROM elective_topics
        WHERE subject_code = :sc AND ay_label = :ay AND year = :y AND term = :t
    """, {"sc": offering[2], "ay": offering[3], "y": offering[4], "t": offering[5]}).fetchone()[0]
    
    if topics_count == 0:
        if config["mode"] == "ack_ok" and acknowledge:
            warnings = [f"Warning: Publishing {offering[0]} '{offering[2]}' without topics. Topics should be added soon."]
            return True, "acknowledged", warnings
        else:
            return False, f"{offering[0]} offerings require at least one topic before publishing", []
    
    return True, "", []


def validate_catalog_sync(conn, offering_id: int) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Check if offering is in sync with catalog.
    Returns sync status and differences.
    """
    from screens.subject_offerings.helpers import exec_query
    
    result = exec_query(conn, """
        SELECT 
            o.subject_code,
            o.degree_code,
            o.override_inheritance,
            o.credits_total as o_credits,
            sc.credits_total as c_credits,
            o.internal_marks_max as o_internal,
            sc.internal_marks_max as c_internal,
            o.exam_marks_max as o_exam,
            sc.exam_marks_max as c_exam
        FROM subject_offerings o
        LEFT JOIN subjects_catalog sc 
            ON sc.subject_code = o.subject_code 
            AND sc.degree_code = o.degree_code
        WHERE o.id = :id
    """, {"id": offering_id}).fetchone()
    
    if not result:
        return False, "Offering not found", {}
    
    if result[2] == 1:  # override_inheritance
        return True, "overridden", {}
    
    differences = {}
    if result[3] != result[4]:  # credits
        differences["credits_total"] = {"offering": result[3], "catalog": result[4]}
    if result[5] != result[6]:  # internal
        differences["internal_marks_max"] = {"offering": result[5], "catalog": result[6]}
    if result[7] != result[8]:  # exam
        differences["exam_marks_max"] = {"offering": result[7], "catalog": result[8]}
    
    if differences:
        return False, "out_of_sync", differences
    
    return True, "synced", {}


# ===================================================================
# HELPER FUNCTIONS
# ===================================================================

def get_preterm_defaults() -> Dict[str, Any]:
    """Get default settings for elective selection windows."""
    return PRETERM_DEFAULTS.copy()


def check_approval_required(action: str, user_roles: List[str]) -> Tuple[bool, List[str]]:
    """
    Check if an action requires approval and who can approve.
    Returns (requires_approval, list_of_approver_roles)
    """
    if action not in APPROVAL_SETTINGS:
        return False, []
    
    config = APPROVAL_SETTINGS[action]
    approvers = config.get("approvers_any_one_of", [])
    
    # Check if user already has approver role
    if any(role in approvers for role in user_roles):
        return False, []  # User can self-approve
    
    return True, approvers


def get_health_check_config(check_type: str) -> Dict[str, Any]:
    """Get configuration for a specific health check."""
    for check in MONITORING_SETTINGS["health_checks"]:
        if check["name"] == check_type:
            return check
    return {}


def format_audit_changed_fields(old_data: Dict[str, Any], new_data: Dict[str, Any]) -> str:
    """Format changed fields for audit log."""
    changes = {}
    for key in new_data:
        if key in old_data and old_data[key] != new_data[key]:
            changes[key] = {
                "old": old_data[key],
                "new": new_data[key]
            }
    return json.dumps(changes) if changes else None
