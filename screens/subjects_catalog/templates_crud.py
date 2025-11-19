"""
Syllabus template operations with audit logging
UPDATED to match subjects_syllabus_schema.py structure
"""

from typing import Dict, Any, List, Optional
import json
from sqlalchemy import text as sa_text
from screens.subjects_syllabus.helpers import exec_query, rows_to_dicts


def audit_template(conn, template_id: int, template_code: str, action: str,
                   actor: str, note: str = ""):
    """Write audit log for template changes."""
    exec_query(conn, """
        INSERT INTO syllabus_templates_audit
        (template_id, template_code, action, note, actor)
        VALUES (:tid, :code, :act, :note, :actor)
    """, {
        "tid": template_id,
        "code": template_code,
        "act": action,
        "note": note or "",
        "actor": actor or "system"
    })


def create_syllabus_template(
    engine, subject_code: str, version: str, name: str,
    points: List[Dict[str, Any]], actor: str,
    description: str = None, effective_from_ay: str = None,
    degree_code: str = None, program_code: str = None, branch_code: str = None
) -> int:
    """Create a new syllabus template with points."""
    with engine.begin() as conn:
        # Check if version already exists
        existing = exec_query(conn, """
            SELECT id FROM syllabus_templates
            WHERE subject_code = :sc AND version = :v
        """, {"sc": subject_code, "v": version}).fetchone()

        if existing:
            raise ValueError(
                f"Template version {version} already exists for {subject_code}"
            )

        # Generate unique code
        template_code = f"{subject_code}_{version}"
        if degree_code:
            template_code = f"{degree_code}_{template_code}"

        # Get version number
        max_ver = exec_query(conn, """
            SELECT COALESCE(MAX(version_number), 0)
            FROM syllabus_templates
            WHERE subject_code = :sc
        """, {"sc": subject_code}).fetchone()[0]

        version_number = max_ver + 1

        # Insert template (UPDATED: removed created_by)
        result = exec_query(conn, """
            INSERT INTO syllabus_templates (
                code, subject_code, version, version_number,
                name, description, effective_from_ay,
                degree_code, program_code, branch_code,
                is_current
            ) VALUES (
                :code, :sc, :ver, :vnum,
                :name, :desc, :ay,
                :dc, :pc, :bc,
                1
            )
        """, {
            "code": template_code,
            "sc": subject_code,
            "ver": version,
            "vnum": version_number,
            "name": name,
            "desc": description,
            "ay": effective_from_ay,
            "dc": degree_code,
            "pc": program_code,
            "bc": branch_code
        })

        template_id = result.lastrowid

        # Insert points (UPDATED: using new schema with point_type and metadata_json)
        for point in points:
            if not point.get("title"):  # Skip empty points
                continue

            # Build metadata_json from tags, resources, hours_weight
            metadata = {}
            if point.get("tags"):
                metadata["tags"] = point["tags"]
            if point.get("resources"):
                metadata["resources"] = point["resources"]
            if point.get("hours_weight") is not None:
                metadata["hours_weight"] = point["hours_weight"]
            
            metadata_json = json.dumps(metadata) if metadata else None

            exec_query(conn, """
                INSERT INTO syllabus_template_points (
                    template_id, sequence, point_type, code, title, description,
                    metadata_json
                ) VALUES (
                    :tid, :seq, :ptype, :code, :title, :desc, :metadata
                )
            """, {
                "tid": template_id,
                "seq": point["sequence"],
                "ptype": point.get("point_type", "unit"),  # Default to 'unit'
                "code": point.get("code"),
                "title": point["title"],
                "desc": point.get("description"),
                "metadata": metadata_json
            })

        audit_template(
            conn, template_id, template_code, "create", actor,
            f"Created template {name} with {len(points)} points"
        )

        return template_id


def get_template_points(conn, template_id: int) -> List[Dict]:
    """Get all points for a template."""
    rows = exec_query(conn, """
        SELECT id, template_id, sequence, point_type, code, title, description, 
               metadata_json, created_at, updated_at
        FROM syllabus_template_points
        WHERE template_id = :tid
        ORDER BY sequence
    """, {"tid": template_id}).fetchall()
    
    points = rows_to_dicts(rows)
    
    # Parse metadata_json for each point
    for point in points:
        if point.get("metadata_json"):
            try:
                metadata = json.loads(point["metadata_json"])
                point["tags"] = metadata.get("tags")
                point["resources"] = metadata.get("resources")
                point["hours_weight"] = metadata.get("hours_weight")
            except (json.JSONDecodeError, TypeError):
                pass
    
    return points


def list_templates_for_subject(conn, subject_code: str,
                               include_deprecated: bool = False) -> List[Dict]:
    """List all template versions for a subject."""
    query = """
        SELECT t.*,
               (SELECT COUNT(*) FROM syllabus_template_points 
                WHERE template_id = t.id) as point_count,
               (SELECT COUNT(*) FROM subject_offerings 
                WHERE syllabus_template_id = t.id) as usage_count
        FROM syllabus_templates t
        WHERE t.subject_code = :sc
    """
    params = {"sc": subject_code}

    if not include_deprecated:
        query += " AND t.deprecated_from_ay IS NULL"

    query += " ORDER BY t.version_number DESC"

    rows = exec_query(conn, query, params).fetchall()
    return rows_to_dicts(rows)


def get_current_template_for_subject(
    conn, subject_code: str, degree_code: str = None
) -> Optional[Dict]:
    """Get the current active template for a subject."""
    query = """
        SELECT * FROM syllabus_templates
        WHERE subject_code = :sc
        AND is_current = 1
        AND deprecated_from_ay IS NULL
    """
    params = {"sc": subject_code}

    if degree_code:
        query += " AND (degree_code = :dc OR degree_code IS NULL)"
        params["dc"] = degree_code

    query += " ORDER BY degree_code IS NOT NULL DESC LIMIT 1"

    row = exec_query(conn, query, params).fetchone()
    return dict(row._mapping) if row else None


def clone_template(engine, source_template_id: int, new_version: str,
                  new_name: str, actor: str) -> int:
    """Clone an existing template to create a new version."""
    with engine.begin() as conn:
        # Get source template
        source = exec_query(conn, """
            SELECT * FROM syllabus_templates WHERE id = :id
        """, {"id": source_template_id}).fetchone()

        if not source:
            raise ValueError("Source template not found")

        source = dict(source._mapping)

        # Get source points
        source_points = get_template_points(conn, source_template_id)

    # Create new template (outside transaction)
    new_template_id = create_syllabus_template(
        engine,
        subject_code=source["subject_code"],
        version=new_version,
        name=new_name,
        points=source_points,
        actor=actor,
        description=f"Cloned from {source['code']}",
        degree_code=source.get("degree_code"),
        program_code=source.get("program_code"),
        branch_code=source.get("branch_code")
    )

    return new_template_id


def bulk_assign_template(engine, template_id: int, subject_code: str,
                        degree_code: str, from_ay: str, actor: str) -> int:
    """Bulk assign a template to all matching offerings from a specific AY onwards."""
    with engine.begin() as conn:
        query = """
            UPDATE subject_offerings
            SET syllabus_template_id = :tid,
                syllabus_customized = 0
            WHERE subject_code = :sc
            AND degree_code = :dc
            AND ay_label >= :ay
        """
        params = {
            "tid": template_id,
            "sc": subject_code,
            "dc": degree_code,
            "ay": from_ay
        }

        result = exec_query(conn, query, params)
        return result.rowcount


def create_syllabus_override(
    engine, offering_id: int, sequence: int, override_type: str, actor: str,
    title: str = None, description: str = None, tags: str = None,
    resources: str = None, hours_weight: float = None, reason: str = None
) -> int:
    """Create or update an override for a specific point in an offering."""
    with engine.begin() as conn:
        # Check if override exists
        existing = exec_query(conn, """
            SELECT id FROM syllabus_point_overrides
            WHERE offering_id = :oid AND sequence = :seq
        """, {"oid": offering_id, "seq": sequence}).fetchone()

        # Build metadata_json
        metadata = {}
        if tags:
            metadata["tags"] = tags
        if resources:
            metadata["resources"] = resources
        if hours_weight is not None:
            metadata["hours_weight"] = hours_weight
        if reason:
            metadata["override_reason"] = reason
        
        metadata_json = json.dumps(metadata) if metadata else None

        if existing:
            # Update (UPDATED: using new schema)
            exec_query(conn, """
                UPDATE syllabus_point_overrides
                SET override_type = :type,
                    title = :title, description = :desc,
                    metadata_json = :metadata,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :id
            """, {
                "id": existing[0], "type": override_type,
                "title": title, "desc": description,
                "metadata": metadata_json
            })
            override_id = existing[0]
        else:
            # Insert (UPDATED: using new schema)
            result = exec_query(conn, """
                INSERT INTO syllabus_point_overrides (
                    offering_id, sequence, override_type,
                    point_type, code, title, description,
                    metadata_json
                ) VALUES (
                    :oid, :seq, :type,
                    :ptype, :code, :title, :desc,
                    :metadata
                )
            """, {
                "oid": offering_id, "seq": sequence, "type": override_type,
                "ptype": "unit",  # Default point type
                "code": None,
                "title": title, "desc": description,
                "metadata": metadata_json
            })
            override_id = result.lastrowid

        # Mark offering as customized
        exec_query(conn, """
            UPDATE subject_offerings
            SET syllabus_customized = 1
            WHERE id = :oid
        """, {"oid": offering_id})

        return override_id


def get_effective_syllabus_for_offering(conn, offering_id: int) -> List[Dict[str, Any]]:
    """Get the effective syllabus for an offering (template + overrides merged)."""
    # Get offering and its template
    offering = exec_query(conn, """
        SELECT syllabus_template_id, syllabus_customized
        FROM subject_offerings
        WHERE id = :oid
    """, {"oid": offering_id}).fetchone()

    if not offering or not offering[0]:
        return []

    template_id = offering[0]

    # Get template points (UPDATED: using new schema)
    template_points = exec_query(conn, """
        SELECT sequence, point_type, code, title, description, metadata_json
        FROM syllabus_template_points
        WHERE template_id = :tid
        ORDER BY sequence
    """, {"tid": template_id}).fetchall()

    # Get overrides (UPDATED: using new schema)
    overrides = exec_query(conn, """
        SELECT sequence, override_type, point_type, code, title, description,
               metadata_json
        FROM syllabus_point_overrides
        WHERE offering_id = :oid
    """, {"oid": offering_id}).fetchall()

    # Build override map
    override_map = {}
    for ov in overrides:
        metadata = {}
        if ov[6]:  # metadata_json
            try:
                metadata = json.loads(ov[6])
            except (json.JSONDecodeError, TypeError):
                pass
        
        override_map[ov[0]] = {
            "type": ov[1], "point_type": ov[2], "code": ov[3],
            "title": ov[4], "description": ov[5],
            "tags": metadata.get("tags"),
            "resources": metadata.get("resources"),
            "hours_weight": metadata.get("hours_weight")
        }

    # Merge template + overrides
    result = []
    for tp in template_points:
        seq = tp[0]
        
        # Parse template metadata
        tp_metadata = {}
        if tp[5]:  # metadata_json
            try:
                tp_metadata = json.loads(tp[5])
            except (json.JSONDecodeError, TypeError):
                pass
        
        if seq in override_map:
            ov = override_map[seq]
            if ov["type"] == "hide":
                continue
            elif ov["type"] == "replace":
                result.append({
                    "sequence": seq,
                    "point_type": ov["point_type"] or tp[1],
                    "code": ov["code"] or tp[2],
                    "title": ov["title"] or tp[3],
                    "description": ov["description"] or tp[4],
                    "tags": ov["tags"] or tp_metadata.get("tags"),
                    "resources": ov["resources"] or tp_metadata.get("resources"),
                    "hours_weight": (
                        ov["hours_weight"] if ov["hours_weight"] is not None 
                        else tp_metadata.get("hours_weight")
                    ),
                    "is_overridden": True
                })
            elif ov["type"] == "append":
                result.append({
                    "sequence": seq,
                    "point_type": tp[1],
                    "code": tp[2],
                    "title": tp[3],
                    "description": (tp[4] or "") + "\n\n" + (ov["description"] or ""),
                    "tags": tp_metadata.get("tags"),
                    "resources": (tp_metadata.get("resources", "") or "") + "\n" + (ov["resources"] or ""),
                    "hours_weight": tp_metadata.get("hours_weight"),
                    "is_overridden": True
                })
        else:
            result.append({
                "sequence": seq,
                "point_type": tp[1],
                "code": tp[2],
                "title": tp[3],
                "description": tp[4],
                "tags": tp_metadata.get("tags"),
                "resources": tp_metadata.get("resources"),
                "hours_weight": tp_metadata.get("hours_weight"),
                "is_overridden": False
            })

    return result
