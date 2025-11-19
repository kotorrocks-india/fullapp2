# ==================================================================
# screens/subject_offerings/__init__.py
# ==================================================================
"""
Subject Offerings Module (Slide 19)
Manages AY-specific subject offerings with inheritance from catalog
"""

from .helpers import (
    safe_int,
    safe_float,
    to_bool,
    exec_query,
    table_exists,
    dict_from_row,
    rows_to_dicts,
)

from .db_helpers import (
    fetch_degrees,
    fetch_programs,
    fetch_branches,
    fetch_curriculum_groups,
    fetch_academic_years,
    fetch_divisions,
    fetch_catalog_subjects,
    fetch_offerings,
    fetch_catalog_subject_details,
)

from .constants import (
    STATUS_VALUES,
    SUBJECT_TYPES,
    OFFERINGS_EXPORT_COLUMNS,
    OFFERINGS_IMPORT_TEMPLATE_COLUMNS,
    validate_offering,
    validate_offering_uniqueness,
)

from .offerings_crud import (
    create_offering_from_catalog,
    update_offering,
    delete_offering,
    publish_offering,
    archive_offering,
    copy_offerings_forward,
    audit_offering,
    bulk_update_offerings,
)

# --- NEWLY ADDED ---
from .imports_offerings import (
    import_offerings_from_df,
)

__all__ = [
    # Helpers
    "safe_int",
    "safe_float",
    "to_bool",
    "exec_query",
    "table_exists",
    "dict_from_row",
    "rows_to_dicts",
    
    # DB Helpers
    "fetch_degrees",
    "fetch_programs",
    "fetch_branches",
    "fetch_curriculum_groups",
    "fetch_academic_years",
    "fetch_divisions",
    "fetch_catalog_subjects",
    "fetch_offerings",
    "fetch_catalog_subject_details",
    
    # Constants
    "STATUS_VALUES",
    "SUBJECT_TYPES",
    "OFFERINGS_EXPORT_COLUMNS",
    "OFFERINGS_IMPORT_TEMPLATE_COLUMNS",
    "validate_offering",
    "validate_offering_uniqueness",
    
    # CRUD Operations
    "create_offering_from_catalog",
    "update_offering",
    "delete_offering",
    "publish_offering",
    "archive_offering",
    "copy_offerings_forward",
    "audit_offering",
    "bulk_update_offerings",
    "import_offerings_from_df",
]
