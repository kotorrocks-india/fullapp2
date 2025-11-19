# screens/outcomes/manager.py
"""
Outcomes Manager - Business logic for outcomes operations.
Handles complex operations like import/export, version control, and workflows.
"""

from __future__ import annotations
from typing import Optional, Tuple, List, Dict, Any
import json
import csv
import io
from dataclasses import asdict
from sqlalchemy.engine import Engine
from sqlalchemy import text as sa_text

from .models import (
    OutcomeSet, OutcomeItem, ImportRow, ImportResult,
    ScopeLevel, SetType, Status, BloomLevel,
)
from .helpers import (
    table_exists, get_scope_config, validate_degree_exists,
    validate_program_exists, validate_branch_exists,
    create_outcome_set, add_outcome_item, get_set_by_id,
    get_outcome_items, audit_operation, check_mappings
)


class OutcomesManager:
    """Main manager for outcomes operations."""
    
    def __init__(self, engine: Engine, actor: str, actor_role: str):
        self.engine = engine
        self.actor = actor
        self.actor_role = actor_role
    
    # ========================================================================
    # SCOPE CONFIGURATION
    # ========================================================================

    def _normalise_set_type(self, raw: str) -> str | None:
        """
        Accepts:
          - labels:  PEO / PO / PSO (any case)
          - db vals: peos / pos / psos
        and returns canonical labels: 'PEO', 'PO', 'PSO'
        """
        if not raw:
            return None
        normalised = raw.strip().lower()
        mapping = {
            "peo": "PEO", "peos": "PEO",
            "po": "PO",   "pos": "PO",
            "pso": "PSO", "psos": "PSO",
        }
        return mapping.get(normalised)





    def get_scope_config(self, degree_code: str) -> ScopeLevel:
        """Get the scope level configuration for a degree."""
        with self.engine.connect() as conn:
            scope_str = get_scope_config(conn, degree_code)
            return ScopeLevel(scope_str)
    
    def set_scope_config(self, degree_code: str, scope_level: ScopeLevel,
                        reason: str) -> Tuple[bool, str]:
        """Set scope configuration for a degree."""
        try:
            with self.engine.begin() as conn:
                # Check if degree exists
                if not validate_degree_exists(conn, degree_code):
                    return False, f"Degree '{degree_code}' not found"
                
                # Get old scope if exists
                old_scope = get_scope_config(conn, degree_code)
                
                # Upsert scope config
                conn.execute(sa_text("""
                    INSERT INTO outcomes_scope_config 
                    (degree_code, scope_level, changed_by, changed_at, change_reason)
                    VALUES (:dc, :sl, :actor, CURRENT_TIMESTAMP, :reason)
                    ON CONFLICT(degree_code) DO UPDATE SET
                        scope_level = excluded.scope_level,
                        changed_by = excluded.changed_by,
                        changed_at = excluded.changed_at,
                        change_reason = excluded.change_reason
                """), {
                    "dc": degree_code,
                    "sl": scope_level.value,
                    "actor": self.actor,
                    "reason": reason
                })
                
                # Audit the change
                conn.execute(sa_text("""
                    INSERT INTO outcomes_scope_config_audit
                    (degree_code, action, old_scope, new_scope, reason, actor, at)
                    VALUES (:dc, 'scope_change', :old, :new, :reason, :actor, CURRENT_TIMESTAMP)
                """), {
                    "dc": degree_code,
                    "old": old_scope,
                    "new": scope_level.value,
                    "reason": reason,
                    "actor": self.actor
                })
                
                return True, "Scope configuration updated successfully"
        except Exception as e:
            return False, f"Error setting scope: {str(e)}"
    
    # ========================================================================
    # CREATE OPERATIONS
    # ========================================================================
    
    def create_set(self, outcome_set: OutcomeSet, reason: str) -> Tuple[bool, Optional[int], List[str]]:
        """Create a new outcome set with items."""
        # Validate
        errors = outcome_set.validate()
        if errors:
            return False, None, errors
        
        try:
            with self.engine.begin() as conn:
                # Validate scope
                scope_level = get_scope_config(conn, outcome_set.degree_code)
                
                if scope_level == ScopeLevel.PER_DEGREE.value:
                    if outcome_set.program_code or outcome_set.branch_code:
                        return False, None, ["Degree uses per_degree scope; program/branch not allowed"]
                
                elif scope_level == ScopeLevel.PER_PROGRAM.value:
                    if not outcome_set.program_code:
                        return False, None, ["Degree uses per_program scope; program_code required"]
                    if outcome_set.branch_code:
                        return False, None, ["Degree uses per_program scope; branch not allowed"]
                
                elif scope_level == ScopeLevel.PER_BRANCH.value:
                    if not outcome_set.program_code or not outcome_set.branch_code:
                        return False, None, ["Degree uses per_branch scope; both program and branch required"]
                
                # Validate entities exist
                if not validate_degree_exists(conn, outcome_set.degree_code):
                    return False, None, [f"Degree '{outcome_set.degree_code}' not found"]
                
                if outcome_set.program_code:
                    if not validate_program_exists(conn, outcome_set.degree_code, outcome_set.program_code):
                        return False, None, [f"Program '{outcome_set.program_code}' not found"]
                
                if outcome_set.branch_code:
                    if not validate_branch_exists(conn, outcome_set.branch_code, outcome_set.program_code):
                        return False, None, [f"Branch '{outcome_set.branch_code}' not found"]
                
                # Create set
                set_id = create_outcome_set(
                    conn,
                    outcome_set.degree_code,
                    outcome_set.set_type.value,
                    outcome_set.program_code,
                    outcome_set.branch_code,
                    self.actor
                )
                
                # Add items
                for item in outcome_set.items:
                    add_outcome_item(
                        conn, set_id,
                        item.code,
                        item.description,
                        item.title,
                        item.bloom_level.value if item.bloom_level else None,
                        item.timeline_years,
                        "|".join(item.tags) if item.tags else "",
                        item.sort_order,
                        self.actor
                    )
                
                # Audit
                audit_operation(
                    conn, "create_set", self.actor, self.actor_role,
                    set_id=set_id, reason=reason,
                    after_data=self._serialize_set(outcome_set)
                )
                
                return True, set_id, []
        
        except Exception as e:
            return False, None, [f"Error creating set: {str(e)}"]
    
    # ========================================================================
    # READ OPERATIONS
    # ========================================================================
    
    def get_set(self, set_id: int) -> Optional[OutcomeSet]:
        """Get an outcome set by ID."""
        try:
            with self.engine.connect() as conn:
                set_row = get_set_by_id(conn, set_id)
                if not set_row:
                    return None
                
                items_rows = get_outcome_items(conn, set_id)
                
                items = [
                    OutcomeItem(
                        id=row[0],
                        code=row[1],
                        title=row[2],
                        description=row[3],
                        bloom_level=row[4],
                        timeline_years=row[5],
                        tags=row[6].split("|") if row[6] else [],
                        sort_order=row[7],
                        created_by=row[8],
                        created_at=row[9],
                        updated_by=row[10],
                        updated_at=row[11]
                    )
                    for row in items_rows
                ]
                
                return OutcomeSet(
                    id=set_row[0],
                    degree_code=set_row[1],
                    program_code=set_row[2],
                    branch_code=set_row[3],
                    set_type=SetType(set_row[4]),
                    status=Status(set_row[5]),
                    version=set_row[6],
                    is_current=bool(set_row[7]),
                    created_by=set_row[8],
                    created_at=set_row[9],
                    updated_by=set_row[10],
                    updated_at=set_row[11],
                    published_by=set_row[12],
                    published_at=set_row[13],
                    archived_by=set_row[14],
                    archived_at=set_row[15],
                    archive_reason=set_row[16],
                    items=items
                )
        
        except Exception:
            return None
    
    # ========================================================================
    # IMPORT/EXPORT
    # ========================================================================
    
   
    def export_outcomes(self, degree_code: str,
                        program_code: Optional[str] = None,
                        branch_code: Optional[str] = None) -> str:
        """Export outcomes to CSV format (matches YAML export columns)."""
        try:
            with self.engine.connect() as conn:
                # Build query
                conditions = ["lower(os.degree_code) = lower(:dc)", "os.status != 'archived'"]
                params: Dict[str, Any] = {"dc": degree_code}

                if program_code:
                    conditions.append("lower(os.program_code) = lower(:pc)")
                    params["pc"] = program_code

                if branch_code:
                    conditions.append("lower(os.branch_code) = lower(:bc)")
                    params["bc"] = branch_code

                where_clause = " AND ".join(conditions)

                rows = conn.execute(sa_text(f"""
                    SELECT os.degree_code,
                           os.program_code,
                           os.branch_code,
                           os.set_type,
                           os.status,
                           oi.code,
                           oi.title,
                           oi.description,
                           oi.bloom_level,
                           oi.timeline_years,
                           oi.tags,
                           oi.updated_at,
                           oi.updated_by
                    FROM outcomes_sets os
                    JOIN outcomes_items oi ON oi.set_id = os.id
                    WHERE {where_clause}
                    ORDER BY os.set_type, oi.sort_order, oi.code
                """), params).fetchall()

                # Generate CSV
                output = io.StringIO()
                writer = csv.writer(output)

                # Header (see slide16_POS.yaml export columns)
                writer.writerow([
                    "degree_code",
                    "program_code",
                    "branch_code",
                    "set_type",
                    "status",
                    "code",
                    "title",
                    "description",
                    "bloom_level",
                    "timeline_years",
                    "tags",
                    "updated_at",
                    "updated_by",
                ])

                # Data
                for row in rows:
                    writer.writerow(row)

                return output.getvalue()

        except Exception as e:
            return f"# Error exporting: {str(e)}\n"

    def import_preview(self, csv_content: str, degree_code: str,
                       session_id: str) -> ImportResult:
        """Preview CSV import without committing. Performs schema & enum validation."""
        result = ImportResult(session_id=session_id, dry_run=True)

        try:
            reader = csv.DictReader(io.StringIO(csv_content))
            row_num = 1

            for row_dict in reader:
                row_num += 1
                result.total_rows += 1

                raw_set_type = (row_dict.get("set_type") or "").strip()

                import_row = ImportRow(
                    degree_code=row_dict.get("degree_code", "").strip(),
                    program_code=row_dict.get("program_code", "").strip() or None,
                    branch_code=row_dict.get("branch_code", "").strip() or None,
                    set_type=raw_set_type,
                    status=row_dict.get("status", "draft").strip(),
                    code=row_dict.get("code", "").strip(),
                    title=row_dict.get("title", "").strip() or None,
                    description=row_dict.get("description", "").strip(),
                    bloom_level=row_dict.get("bloom_level", "").strip() or None,
                    timeline_years=int(row_dict.get("timeline_years", 0))
                    if row_dict.get("timeline_years") else None,
                    tags=row_dict.get("tags", "").strip() or None,
                    row_number=row_num,
                )

                # Basic required field checks
                if not import_row.degree_code:
                    import_row.errors.append("degree_code is required")
                if not raw_set_type:
                    import_row.errors.append("set_type is required")
                if not import_row.code:
                    import_row.errors.append("code is required")
                if not import_row.description:
                    import_row.errors.append("description is required")

                # Normalise set_type (accept peos/pos/psos OR PEO/PO/PSO)
                canonical = self._normalise_set_type(raw_set_type)
                if canonical is None:
                    import_row.errors.append(
                        "set_type must be one of PEO, PO, PSO (or peos/pos/psos)"
                    )
                else:
                    # Keep canonical label if you want to show it in the preview
                    import_row.set_type = canonical

                # Status validation
                if import_row.status:
                    allowed_status = {"draft", "published", "archived"}
                    status_norm = import_row.status.lower()
                    if status_norm not in allowed_status:
                        import_row.errors.append(
                            "status must be one of draft, published, archived"
                        )
                    else:
                        import_row.status = status_norm
                else:
                    import_row.status = "draft"

                if import_row.errors:
                    result.invalid_rows += 1
                    result.errors.append(
                        {
                            "row": row_num,
                            "code": import_row.code,
                            "errors": import_row.errors,
                        }
                    )
                else:
                    result.valid_rows += 1

                result.preview_data.append(import_row)

            return result

        except Exception as e:
            result.errors.append({"row": 0, "code": "", "errors": [str(e)]})
            return result
    def import_apply(self, csv_content: str, degree_code: str,
                     session_id: str) -> ImportResult:
        """Apply CSV import.

        Groups rows by (degree, program, branch, set_type) and creates new sets
        using create_set. Intended for bulk initial creation, not in-place edits.
        """
        result = ImportResult(session_id=session_id, dry_run=False)

        try:
            reader = csv.DictReader(io.StringIO(csv_content))
            row_num = 1

            # First pass: validate & group rows (similar to preview)
            groups: Dict[tuple, List[ImportRow]] = {}
            for row_dict in reader:
                row_num += 1
                result.total_rows += 1

                raw_set_type = (row_dict.get("set_type") or "").strip()

                import_row = ImportRow(
                    degree_code=row_dict.get("degree_code", "").strip(),
                    program_code=row_dict.get("program_code", "").strip() or None,
                    branch_code=row_dict.get("branch_code", "").strip() or None,
                    set_type=raw_set_type,
                    status=row_dict.get("status", "draft").strip(),
                    code=row_dict.get("code", "").strip(),
                    title=row_dict.get("title", "").strip() or None,
                    description=row_dict.get("description", "").strip(),
                    bloom_level=row_dict.get("bloom_level", "").strip() or None,
                    timeline_years=int(row_dict.get("timeline_years", 0))
                    if row_dict.get("timeline_years") else None,
                    tags=row_dict.get("tags", "").strip() or None,
                    row_number=row_num,
                )

                # Basic required field checks
                if not import_row.degree_code:
                    import_row.errors.append("degree_code is required")
                if not raw_set_type:
                    import_row.errors.append("set_type is required")
                if not import_row.code:
                    import_row.errors.append("code is required")
                if not import_row.description:
                    import_row.errors.append("description is required")

                canonical = self._normalise_set_type(raw_set_type)
                if canonical is None:
                    import_row.errors.append(
                        "set_type must be one of PEO, PO, PSO (or peos/pos/psos)"
                    )

                # Status validation
                if import_row.status:
                    allowed_status = {"draft", "published", "archived"}
                    status_norm = import_row.status.lower()
                    if status_norm not in allowed_status:
                        import_row.errors.append(
                            "status must be one of draft, published, archived"
                        )
                    else:
                        import_row.status = status_norm
                else:
                    import_row.status = "draft"

                if import_row.errors:
                    result.invalid_rows += 1
                    result.errors.append(
                        {
                            "row": row_num,
                            "code": import_row.code,
                            "errors": import_row.errors,
                        }
                    )
                    continue  # skip invalid rows

                result.valid_rows += 1
                import_row.set_type = canonical  # store the canonical label

                key = (
                    import_row.degree_code,
                    import_row.program_code,
                    import_row.branch_code,
                    canonical,  # PEO / PO / PSO
                )
                groups.setdefault(key, []).append(import_row)

            # Second pass: create sets per group
            for (deg, prog, branch, canonical), rows in groups.items():
                if canonical == "PEO":
                    set_type_enum = SetType.PEOS
                elif canonical == "PO":
                    set_type_enum = SetType.POS
                elif canonical == "PSO":
                    set_type_enum = SetType.PSOS
                else:
                    # Should not happen after validation
                    result.failed_rows += len(rows)
                    result.errors.append(
                        {
                            "row": rows[0].row_number,
                            "code": rows[0].code,
                            "errors": [f"Unsupported set_type {canonical!r}"],
                        }
                    )
                    continue

                status_text = (rows[0].status or "draft").lower()
                try:
                    status_enum = Status(status_text)
                except Exception:
                    status_enum = Status.DRAFT

                items: List[OutcomeItem] = []
                sort = 10
                for r in rows:
                    bloom_enum = None
                    if r.bloom_level:
                        for b in BloomLevel:
                            if b.value == r.bloom_level:
                                bloom_enum = b
                                break

                    tags = [t.strip() for t in (r.tags or "").split("|")]
                    if tags == [""]:
                        tags = []

                    items.append(
                        OutcomeItem(
                            code=r.code,
                            description=r.description,
                            title=r.title,
                            bloom_level=bloom_enum,
                            timeline_years=r.timeline_years,
                            tags=tags,
                            sort_order=sort,
                        )
                    )
                    sort += 10

                outcome_set = OutcomeSet(
                    degree_code=deg,
                    set_type=set_type_enum,
                    status=status_enum,
                    program_code=prog,
                    branch_code=branch,
                    items=items,
                )

                success, set_id, errors = self.create_set(
                    outcome_set,
                    reason=f"Import from CSV session {session_id}",
                )

                if not success:
                    result.failed_rows += len(rows)
                    result.errors.append(
                        {
                            "row": rows[0].row_number,
                            "code": rows[0].code,
                            "errors": errors,
                        }
                    )
                else:
                    result.imported_rows += len(rows)

            return result

        except Exception as e:
            result.errors.append({"row": 0, "code": "", "errors": [str(e)]})
            return result
        
    # ========================================================================
    # HELPERS
    # ========================================================================
    
    def _serialize_set(self, outcome_set: OutcomeSet) -> str:
        """Serialize outcome set to JSON."""
        data = asdict(outcome_set)
        # Convert enums to strings
        data["set_type"] = data["set_type"].value if hasattr(data["set_type"], "value") else data["set_type"]
        data["status"] = data["status"].value if hasattr(data["status"], "value") else data["status"]
        for item in data.get("items", []):
            if item.get("bloom_level") and hasattr(item["bloom_level"], "value"):
                item["bloom_level"] = item["bloom_level"].value
        return json.dumps(data, default=str)
