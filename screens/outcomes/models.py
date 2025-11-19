# screens/outcomes/models.py
"""
Data models for Program Outcomes (PEOs, POs, PSOs).
Contains enums, dataclasses, and validation logic.
"""

from __future__ import annotations
from typing import Optional, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


# ============================================================================
# ENUMS
# ============================================================================

class ScopeLevel(str, Enum):
    """Defines at what level outcomes are managed."""
    PER_DEGREE = "per_degree"
    PER_PROGRAM = "per_program"
    PER_BRANCH = "per_branch"


class SetType(str, Enum):
    """Types of outcome sets."""
    PEOS = "peos"  # Program Educational Objectives
    POS = "pos"    # Program Outcomes
    PSOS = "psos"  # Program Specific Outcomes


class Status(str, Enum):
    """Workflow status of outcome sets."""
    DRAFT = "draft"
    PUBLISHED = "published"
    ARCHIVED = "archived"


class BloomLevel(str, Enum):
    """Bloom's Taxonomy cognitive levels."""
    REMEMBER = "Remember"
    UNDERSTAND = "Understand"
    APPLY = "Apply"
    ANALYZE = "Analyze"
    EVALUATE = "Evaluate"
    CREATE = "Create"


class OperationKey(str, Enum):
    """Keys for approval operations."""
    CREATE = "OUTCOMES_CREATE"
    PUBLISH = "OUTCOMES_PUBLISH"
    UNPUBLISH = "OUTCOMES_UNPUBLISH"
    MAJOR_EDIT = "OUTCOMES_MAJOR_EDIT"
    SCOPE_CHANGE = "OUTCOMES_SCOPE_CHANGE"


class ImportStatus(str, Enum):
    """Status of import operations."""
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class OutcomeItem:
    """Represents a single PEO/PO/PSO item."""
    code: str
    description: str
    title: Optional[str] = None
    bloom_level: Optional[BloomLevel] = None
    timeline_years: Optional[int] = None
    tags: List[str] = field(default_factory=list)
    sort_order: int = 100
    id: Optional[int] = None
    set_id: Optional[int] = None
    created_by: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_by: Optional[str] = None
    updated_at: Optional[datetime] = None

    def validate(self) -> List[str]:
        """Validate item fields and return list of errors."""
        errors = []
        
        if not self.code or not self.code.strip():
            errors.append("Code is required")
        elif len(self.code) > 16:
            errors.append("Code must be 16 characters or less")
        
        if not self.description or not self.description.strip():
            errors.append("Description is required")
        elif len(self.description) > 4000:
            errors.append("Description must be 4000 characters or less")
        
        if self.title and len(self.title) > 200:
            errors.append("Title must be 200 characters or less")
        
        if self.timeline_years is not None:
            if self.timeline_years < 1 or self.timeline_years > 10:
                errors.append("Timeline must be between 1 and 10 years")
        
        return errors


@dataclass
class OutcomeSet:
    """Represents a collection of outcomes (PEOs, POs, or PSOs)."""
    degree_code: str
    set_type: SetType
    status: Status = Status.DRAFT
    program_code: Optional[str] = None
    branch_code: Optional[str] = None
    version: int = 1
    is_current: bool = True
    items: List[OutcomeItem] = field(default_factory=list)
    id: Optional[int] = None
    created_by: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_by: Optional[str] = None
    updated_at: Optional[datetime] = None
    published_by: Optional[str] = None
    published_at: Optional[datetime] = None
    archived_by: Optional[str] = None
    archived_at: Optional[datetime] = None
    archive_reason: Optional[str] = None

    def validate(self) -> List[str]:
        """Validate set and return list of errors.

        Business rule (from YAML / slide16):
        - POs are compulsory.
        - PEOs and PSOs are optional.
        """
        errors: List[str] = []

        if not self.degree_code or not self.degree_code.strip():
            errors.append("Degree code is required")

        # ---------------------------------------------------------------------
        # Item count rules
        # ---------------------------------------------------------------------
        # POs (POS) must have at least 1 item.
        # PEOs (PEOS) and PSOs (PSOS) are allowed to have 0 items.
        if self.set_type == SetType.POS:
            min_items = 1
        else:
            min_items = 0

        max_items = 50  # hard safety cap

        if len(self.items) < min_items:
            errors.append(
                f"{self.set_type.value} requires at least {min_items} outcome(s)"
            )

        if len(self.items) > max_items:
            errors.append(
                f"{self.set_type.value} cannot have more than {max_items} outcome(s)"
            )

        # ---------------------------------------------------------------------
        # Validate each item
        # ---------------------------------------------------------------------
        for i, item in enumerate(self.items):
            item_errors = item.validate()
            for err in item_errors:
                errors.append(f"Item {i + 1} ({item.code}): {err}")

        # ---------------------------------------------------------------------
        # Duplicate codes
        # ---------------------------------------------------------------------
        codes = [item.code.upper() for item in self.items]
        duplicates = [code for code in set(codes) if codes.count(code) > 1]
        if duplicates:
            errors.append(f"Duplicate outcome codes found: {', '.join(duplicates)}")

        return errors

@dataclass
class ImportRow:
    """Represents a single row from import CSV."""
    degree_code: str
    set_type: str
    code: str
    description: str
    program_code: Optional[str] = None
    branch_code: Optional[str] = None
    status: str = "draft"
    title: Optional[str] = None
    bloom_level: Optional[str] = None
    timeline_years: Optional[int] = None
    tags: Optional[str] = None
    
    row_number: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class ImportResult:
    """Results of an import operation."""
    session_id: str
    total_rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    imported_rows: int = 0
    failed_rows: int = 0
    errors: List[dict] = field(default_factory=list)
    warnings: List[dict] = field(default_factory=list)
    dry_run: bool = True
    preview_data: List[ImportRow] = field(default_factory=list)


# ============================================================================
# CONSTANTS
# ============================================================================

SET_TYPE_LABELS = {
    SetType.PEOS: "PEOs (Program Educational Objectives)",
    SetType.POS: "POs (Program Outcomes)",
    SetType.PSOS: "PSOs (Program Specific Outcomes)"
}

BLOOM_LEVEL_DESCRIPTIONS = {
    BloomLevel.REMEMBER: "Recall facts and basic concepts",
    BloomLevel.UNDERSTAND: "Explain ideas or concepts",
    BloomLevel.APPLY: "Use information in new situations",
    BloomLevel.ANALYZE: "Draw connections among ideas",
    BloomLevel.EVALUATE: "Justify a decision or course of action",
    BloomLevel.CREATE: "Produce new or original work"
}

SCOPE_LEVEL_DESCRIPTIONS = {
    ScopeLevel.PER_DEGREE: "Outcomes defined once per degree (all programs share)",
    ScopeLevel.PER_PROGRAM: "Outcomes defined per program within a degree",
    ScopeLevel.PER_BRANCH: "Outcomes defined per branch/specialization"
}
