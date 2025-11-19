"""
Constants and validation rules for Subjects & Syllabus Management
"""

import re
from typing import Tuple, Dict, Any

# ===================================================================
# REGEX PATTERNS
# ===================================================================

SUBJECT_CODE_RE = re.compile(r"^[A-Z0-9_-]+$")
SUBJECT_NAME_RE = re.compile(r"^[A-Za-z0-9 &/\\-\\.,()]+$")

# ===================================================================
# LISTS
# ===================================================================

DEFAULT_SUBJECT_TYPES = ["Core", "Elective", "Audit", "Honors", "Project", "Internship"]
STATUS_VALUES = ["active", "inactive", "archived"]
OVERRIDE_TYPES = ["replace", "append", "hide"]

# ===================================================================
# EXPORT COLUMNS
# ===================================================================

SUBJECT_CATALOG_EXPORT_COLUMNS = [
    "subject_code",
    "subject_name",
    "subject_type",
    "degree_code",
    "program_code",
    "branch_code",
    "curriculum_group_code",
    "semester_id",
    "credits_total",
    "L",
    "T",
    "P",
    "S",
    "student_credits",
    "teaching_credits",
    "internal_marks_max",
    "exam_marks_max",
    "jury_viva_marks_max",
    "min_internal_percent",
    "min_external_percent",
    "min_overall_percent",
    "direct_source_mode",
    "direct_internal_threshold_percent",
    "direct_external_threshold_percent",
    "direct_internal_weight_percent",
    "direct_external_weight_percent",
    "direct_target_students_percent",
    "indirect_target_students_percent",
    "indirect_min_response_rate_percent",
    "overall_direct_weight_percent",
    "overall_indirect_weight_percent",
    "description",
    "status",
    "active",
    "sort_order",
    "workload_breakup_json",
    "created_at",
    "updated_at",
]

SUBJECT_IMPORT_TEMPLATE_COLUMNS = [
    "subject_code",
    "subject_name",
    "subject_type",
    "degree_code",
    "program_code",
    "branch_code",
    "curriculum_group_code",
    "semester_number",
    "credits_total",
    "L",
    "T",
    "P",
    "S",
    "student_credits",
    "teaching_credits",
    "internal_marks_max",
    "exam_marks_max",
    "jury_viva_marks_max",
    "min_internal_percent",
    "min_external_percent",
    "min_overall_percent",
    "direct_source_mode",
    "direct_internal_threshold_percent",
    "direct_external_threshold_percent",
    "direct_internal_weight_percent",
    "direct_external_weight_percent",
    "direct_target_students_percent",
    "indirect_target_students_percent",
    "indirect_min_response_rate_percent",
    "overall_direct_weight_percent",
    "overall_indirect_weight_percent",
    "description",
    "status",
    "active",
    "sort_order",
    "workload_breakup_json",
]

SIMPLE_SUBJECT_IMPORT_TEMPLATE_COLUMNS = [
    col for col in SUBJECT_IMPORT_TEMPLATE_COLUMNS
    if col != "workload_breakup_json"
]

SUBJECTS_ALL_YEARS_EXPORT_COLUMNS = [
    "offering_id",
    "degree_code",
    "program_code",
    "branch_code",
    "curriculum_group_code",
    "ay_label",
    "year",
    "term",
    "subject_code",
    "subject_name",
    "subject_type",
    "credits_total",
    "L",
    "T",
    "P",
    "S",
    "status",
    "active",
    "instructor_email",
    "syllabus_template_id",
    "syllabus_customized",
    "created_at",
    "updated_at",
]

# ===================================================================
# VALIDATION
# ===================================================================

def validate_subject(data: Dict[str, Any]) -> Tuple[bool, str]:
    """Validate subject catalog data."""
    code = (data.get("subject_code") or "").strip().upper()
    name = (data.get("subject_name") or "").strip()
    degree_code = (data.get("degree_code") or "").strip().upper()

    if not code or not SUBJECT_CODE_RE.match(code):
        return False, "Subject code must match ^[A-Z0-9_-]+$"

    if not name or not SUBJECT_NAME_RE.match(name):
        return False, "Subject name contains invalid characters"

    if not degree_code:
        return False, "Degree code is required"

    try:
        credits_total = float(data.get("credits_total", 0))
        if credits_total < 0 or credits_total > 40:
            return False, "Credits total must be between 0 and 40"

        L = float(data.get("L", 0))
        T = float(data.get("T", 0))
        P = float(data.get("P", 0))
        S = float(data.get("S", 0))

        if any(x < 0 for x in [L, T, P, S]):
            return False, "L/T/P/S values cannot be negative"

        # RELAXED: Removed strict upper limits
        # Architecture programs often have high practical/studio hours
        # If limits are needed, they should be configurable per degree
        if L > 500 or T > 500 or P > 500 or S > 500:
            return False, "L/T/P/S values seem unreasonably high (max 500 each)"

    except (ValueError, TypeError):
        return False, "Invalid numeric values for credits or workload"

    return True, ""
