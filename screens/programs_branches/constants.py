"""
Constants and configuration for Programs/Branches module
"""
import re


# Column definitions for import/export
PROGRAM_IMPORT_COLS = ["program_code", "program_name", "active", "sort_order", "description"]
BRANCH_IMPORT_COLS = ["branch_code", "branch_name", "program_code", "active", "sort_order", "description"]
CG_IMPORT_COLS = ["group_code", "group_name", "kind", "active", "sort_order", "description"]
CGL_IMPORT_COLS = ["group_code", "program_code", "branch_code"]

# Validation Regex
CODE_RE = re.compile(r"^[A-Z0-9_-]+$")

# Cohort modes
COHORT_BOTH = "both"
COHORT_PROGRAM_OR_BRANCH = "program_or_branch"
COHORT_PROGRAM_ONLY = "program_only"
COHORT_BRANCH_ONLY = "branch_only"
COHORT_NONE = "none"

def allow_programs_for(mode: str) -> bool:
    return mode in {COHORT_BOTH, COHORT_PROGRAM_OR_BRANCH, COHORT_PROGRAM_ONLY}

def allow_branches_for(mode: str) -> bool:
    return mode in {COHORT_BOTH, COHORT_PROGRAM_OR_BRANCH, COHORT_BRANCH_ONLY}

def branches_require_program(mode: str) -> bool:
    return mode == COHORT_BOTH
