"""
Import/Export functionality for Programs/Branches module - Part 1
"""
import pandas as pd
import streamlit as st
from typing import Tuple, List
from sqlalchemy import text as sa_text

from screens.programs_branches.constants import (
    PROGRAM_IMPORT_COLS, BRANCH_IMPORT_COLS, 
    CG_IMPORT_COLS, CGL_IMPORT_COLS, CODE_RE
)
from screens.programs_branches.db_helpers import (
    _fetch_program_by_code, _fetch_branch_by_code,
    _program_id_by_code, _table_cols
)
from screens.programs_branches.audit_helpers import (
    _audit_program, _audit_branch,
    _audit_curriculum_group, _audit_curriculum_group_link
)


def import_programs(
    conn, 
    df_import: pd.DataFrame, 
    degree_code: str, 
    actor: str,
    engine = None,
    dry_run: bool = False,
    debug: bool = False
) -> Tuple[int, int, List[str]]:
    """Imports Programs from a DataFrame, scoped to a specific Degree."""
    created_count = 0
    updated_count = 0
    errors = []
    debug_info = []
    
    if dry_run:
        debug_info.append("🔍 DRY-RUN MODE: No changes will be saved to database")
    
    df_import.columns = [col.strip() for col in df_import.columns]
    
    if debug:
        debug_info.append(f"📊 Input DataFrame: {len(df_import)} rows, {len(df_import.columns)} columns")
        debug_info.append(f"📋 Columns found: {list(df_import.columns)}")
    
    req_cols = set(PROGRAM_IMPORT_COLS)
    if not req_cols.issubset(df_import.columns):
        missing = list(req_cols - set(df_import.columns))
        errors.append(f"Import file is missing required columns: {', '.join(missing)}")
        return 0, 0, errors

    if debug:
        debug_info.append(f"✅ All required columns present: {PROGRAM_IMPORT_COLS}")
        debug_info.append(f"🎯 Target degree: {degree_code}")
        debug_info.append(f"👤 Actor: {actor}")
        debug_info.append("---")

    for idx, row in enumerate(df_import.itertuples(), start=1):
        code = ""
        try:
            code = str(getattr(row, "program_code", "")).strip().upper()
            name = str(getattr(row, "program_name", "")).strip()
            active = bool(int(getattr(row, "active", 1)))
            sort_order = int(getattr(row, "sort_order", 0))
            desc = str(getattr(row, "description", "")).strip()

            if debug:
                debug_info.append(f"🔍 Row {idx}: Processing '{code}'")

            if not code:
                errors.append(f"Skipped row {row.Index}: 'program_code' is missing.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: No program_code")
                continue
                
            if not CODE_RE.match(code):
                errors.append(f"Skipped row {row.Index} ({code}): 'program_code' contains invalid characters.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: Invalid code format '{code}'")
                continue
                
            if not name:
                errors.append(f"Skipped row {row.Index} ({code}): 'program_name' is missing.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: No program_name")
                continue

            existing = _fetch_program_by_code(conn, degree_code, code)
            
            new_data = {
                "program_name": name,
                "active": active,
                "sort_order": sort_order,
                "description": desc,
            }

            if existing:
                action = "update"
                old_data = {k: getattr(existing, k) for k in new_data}
                changes = {k: v for k, v in new_data.items() if str(v) != str(old_data[k])}
                
                if not changes:
                    if debug:
                        debug_info.append(f"  ⭐️ No changes needed (data identical)")
                    continue

                if debug:
                    debug_info.append(f"  📄 UPDATE: {len(changes)} field(s) changed")
                    for k, v in changes.items():
                        debug_info.append(f"     • {k}: '{old_data[k]}' → '{v}'")

                if not dry_run:
                    conn.execute(sa_text(f"""
                        UPDATE programs
                           SET {', '.join([f"{k} = :{k}" for k in changes])}
                         WHERE id = :id
                    """), {**changes, "id": existing.id})
                    
                    if debug:
                        debug_info.append(f"  ✅ SQL executed successfully")
                
                updated_count += 1
                audit_note = "Import: Updated" if not dry_run else "Import: Updated (DRY-RUN)"
                audit_payload = {"degree_code": degree_code, "program_code": code, **changes}

            else:
                action = "create"
                
                if debug:
                    debug_info.append(f"  ➕ CREATE: New program")
                    debug_info.append(f"     • program_code: {code}")
                    debug_info.append(f"     • program_name: {name}")
                    debug_info.append(f"     • active: {active}")
                    debug_info.append(f"     • sort_order: {sort_order}")

                if not dry_run:
                    conn.execute(sa_text("""
                        INSERT INTO programs (degree_code, program_code, program_name, active, sort_order, description)
                        VALUES(:dc, :pc, :name, :active, :sort, :desc)
                    """), {
                        "dc": degree_code,
                        "pc": code,
                        "name": name,
                        "active": active,
                        "sort": sort_order,
                        "desc": desc,
                    })
                    
                    if debug:
                        debug_info.append(f"  ✅ SQL executed successfully")
                
                created_count += 1
                audit_note = "Import: Created" if not dry_run else "Import: Created (DRY-RUN)"
                audit_payload = {"degree_code": degree_code, "program_code": code, **new_data}
            
            if not dry_run:
                _audit_program(conn, action, actor, audit_payload, note=audit_note)

        except Exception as e:
            error_msg = f"Error on row {row.Index} (Program '{code}'): {e}"
            errors.append(error_msg)
            if debug:
                debug_info.append(f"  ❌ ERROR: {e}")

    if debug or dry_run:
        st.info("**Import Debug Information:**")
        for line in debug_info:
            st.text(line)
    
    if not dry_run and debug:
        st.write("---")
        st.write("**Verification: Checking database...**")
        try:
            verify_count = conn.execute(sa_text(
                "SELECT COUNT(*) FROM programs WHERE degree_code = :dc"
            ), {"dc": degree_code}).fetchone()[0]
            st.success(f"✅ Database now has {verify_count} program(s) for degree '{degree_code}'")
        except Exception as e:
            st.error(f"❌ Verification failed: {e}")

    return created_count, updated_count, errors

"""
Import/Export functionality - Branches, Curriculum Groups, and Links
Add this content to import_export.py after the import_programs function
"""

def import_branches(
    conn, 
    df_import: pd.DataFrame, 
    degree_code: str, 
    actor: str,
    br_has_pid: bool,
    engine = None,
    dry_run: bool = False,
    debug: bool = False
) -> Tuple[int, int, List[str]]:
    """Imports Branches from a DataFrame, scoped to a specific Degree."""
    created_count = 0
    updated_count = 0
    errors = []
    debug_info = []
    
    if dry_run:
        debug_info.append("🔍 DRY-RUN MODE: No changes will be saved to database")
    
    df_import.columns = [col.strip() for col in df_import.columns]

    if debug:
        debug_info.append(f"📊 Input DataFrame: {len(df_import)} rows, {len(df_import.columns)} columns")
        debug_info.append(f"📋 Columns found: {list(df_import.columns)}")

    req_cols = set(BRANCH_IMPORT_COLS)
    if not req_cols.issubset(df_import.columns):
        missing = list(req_cols - set(df_import.columns))
        errors.append(f"Import file is missing required columns: {', '.join(missing)}")
        return 0, 0, errors

    if debug:
        debug_info.append(f"✅ All required columns present: {BRANCH_IMPORT_COLS}")
        debug_info.append(f"🎯 Target degree: {degree_code}")
        debug_info.append(f"👤 Actor: {actor}")
        debug_info.append(f"🔗 Schema uses program_id: {br_has_pid}")
        debug_info.append("---")

    for idx, row in enumerate(df_import.itertuples(), start=1):
        code = ""
        prog_code = ""
        try:
            code = str(getattr(row, "branch_code", "")).strip().upper()
            name = str(getattr(row, "branch_name", "")).strip()
            prog_code = str(getattr(row, "program_code", "")).strip().upper()
            active = bool(int(getattr(row, "active", 1)))
            sort_order = int(getattr(row, "sort_order", 0))
            desc = str(getattr(row, "description", "")).strip()

            if debug:
                debug_info.append(f"🔍 Row {idx}: Processing '{code}'")

            if not code:
                errors.append(f"Skipped row {row.Index}: 'branch_code' is missing.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: No branch_code")
                continue
                
            if not CODE_RE.match(code):
                errors.append(f"Skipped row {row.Index} ({code}): 'branch_code' contains invalid characters.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: Invalid code format '{code}'")
                continue
                
            if not name:
                errors.append(f"Skipped row {row.Index} ({code}): 'branch_name' is missing.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: No branch_name")
                continue
            
            if not prog_code:
                errors.append(f"Skipped row {row.Index} ({code}): 'program_code' is missing.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: No program_code")
                continue
            
            program_id = _program_id_by_code(conn, degree_code, prog_code)
            if br_has_pid and not program_id:
                errors.append(f"Skipped row {row.Index} ({code}): Program '{prog_code}' not found in degree '{degree_code}'. Import programs first.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: Parent program '{prog_code}' doesn't exist")
                continue

            if debug and program_id:
                debug_info.append(f"  🔗 Linked to program '{prog_code}' (ID: {program_id})")

            existing = _fetch_branch_by_code(conn, degree_code, code)
            
            new_data = {
                "branch_name": name,
                "active": active,
                "sort_order": sort_order,
                "description": desc,
            }
            if br_has_pid and program_id:
                new_data["program_id"] = program_id

            if existing:
                action = "update"
                old_data = {k: getattr(existing, k) for k in new_data if hasattr(existing, k)}
                changes = {k: v for k, v in new_data.items() if k not in old_data or str(v) != str(old_data[k])}
                
                if not changes:
                    if debug:
                        debug_info.append(f"  ⭐️ No changes needed (data identical)")
                    continue

                if debug:
                    debug_info.append(f"  📄 UPDATE: {len(changes)} field(s) changed")
                    for k, v in changes.items():
                        debug_info.append(f"     • {k}: '{old_data.get(k)}' → '{v}'")

                if not dry_run:
                    conn.execute(sa_text(f"""
                        UPDATE branches
                           SET {', '.join([f"{k} = :{k}" for k in changes])}
                         WHERE id = :id
                    """), {**changes, "id": existing.id})
                    
                    if debug:
                        debug_info.append(f"  ✅ SQL executed successfully")
                
                updated_count += 1
                audit_note = "Import: Updated" if not dry_run else "Import: Updated (DRY-RUN)"
                audit_payload = {"degree_code": degree_code, "branch_code": code, **changes}
            
            else:
                action = "create"
                
                if debug:
                    debug_info.append(f"  ➕ CREATE: New branch")
                    debug_info.append(f"     • branch_code: {code}")
                    debug_info.append(f"     • branch_name: {name}")
                    debug_info.append(f"     • program_code: {prog_code}")
                    debug_info.append(f"     • active: {active}")

                if not dry_run:
                    insert_data = new_data.copy()
                    insert_data["degree_code"] = degree_code
                    insert_data["branch_code"] = code
                    
                    bcols = _table_cols(engine if engine else conn.engine, "branches")
                    insert_cols = {k: v for k, v in insert_data.items() if k in bcols}
                    
                    col_names = ", ".join(insert_cols.keys())
                    col_params = ", ".join([f":{k}" for k in insert_cols.keys()])

                    conn.execute(sa_text(f"""
                        INSERT INTO branches ({col_names})
                        VALUES ({col_params})
                    """), insert_cols)
                    
                    if debug:
                        debug_info.append(f"  ✅ SQL executed successfully")
                
                created_count += 1
                audit_note = "Import: Created" if not dry_run else "Import: Created (DRY-RUN)"
                audit_payload = {"degree_code": degree_code, "branch_code": code, **new_data}

            if not dry_run:
                _audit_branch(conn, action, actor, audit_payload, note=audit_note)

        except Exception as e:
            error_msg = f"Error on row {row.Index} (Branch '{code}'): {e}"
            errors.append(error_msg)
            if debug:
                debug_info.append(f"  ❌ ERROR: {e}")

    if debug or dry_run:
        st.info("**Import Debug Information:**")
        for line in debug_info:
            st.text(line)
    
    if not dry_run and debug:
        st.write("---")
        st.write("**Verification: Checking database...**")
        try:
            if br_has_pid:
                verify_count = conn.execute(sa_text("""
                    SELECT COUNT(*) FROM branches b
                    JOIN programs p ON b.program_id = p.id
                    WHERE p.degree_code = :dc
                """), {"dc": degree_code}).fetchone()[0]
            else:
                verify_count = conn.execute(sa_text(
                    "SELECT COUNT(*) FROM branches WHERE degree_code = :dc"
                ), {"dc": degree_code}).fetchone()[0]
            
            st.success(f"✅ Database now has {verify_count} branch(es) for degree '{degree_code}'")
        except Exception as e:
            st.error(f"❌ Verification failed: {e}")

    return created_count, updated_count, errors


def import_cgs(
    conn, 
    df_import: pd.DataFrame, 
    degree_code: str, 
    actor: str, 
    cg_allowed: bool,
    engine = None,
    dry_run: bool = False,
    debug: bool = False
) -> Tuple[int, int, List[str]]:
    """Imports Curriculum Groups from a DataFrame."""
    created_count = 0
    updated_count = 0
    errors = []
    debug_info = []
    
    if dry_run:
        debug_info.append("🔍 DRY-RUN MODE: No changes will be saved to database")
    
    if not cg_allowed:
        errors.append("Import failed: This degree's cohort mode does not support curriculum groups.")
        return 0, 0, errors

    df_import.columns = [col.strip() for col in df_import.columns]
    
    if debug:
        debug_info.append(f"📊 Input DataFrame: {len(df_import)} rows, {len(df_import.columns)} columns")
        debug_info.append(f"📋 Columns found: {list(df_import.columns)}")
    
    req_cols = set(CG_IMPORT_COLS)
    if not req_cols.issubset(df_import.columns):
        missing = list(req_cols - set(df_import.columns))
        errors.append(f"Import file is missing required columns: {', '.join(missing)}")
        return 0, 0, errors

    if debug:
        debug_info.append(f"✅ All required columns present: {CG_IMPORT_COLS}")
        debug_info.append(f"🎯 Target degree: {degree_code}")
        debug_info.append(f"👤 Actor: {actor}")
        debug_info.append("---")

    for idx, row in enumerate(df_import.itertuples(), start=1):
        code = ""
        try:
            code = str(getattr(row, "group_code", "")).strip().upper()
            name = str(getattr(row, "group_name", "")).strip()
            kind = str(getattr(row, "kind", "")).strip()
            active = bool(int(getattr(row, "active", 1)))
            sort_order = int(getattr(row, "sort_order", 0))
            desc = str(getattr(row, "description", "")).strip()

            if debug:
                debug_info.append(f"🔍 Row {idx}: Processing '{code}'")

            if not code:
                errors.append(f"Skipped row {row.Index}: 'group_code' is missing.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: No group_code")
                continue
                
            if not CODE_RE.match(code):
                errors.append(f"Skipped row {row.Index} ({code}): 'group_code' contains invalid characters.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: Invalid code format '{code}'")
                continue
                
            if not name:
                errors.append(f"Skipped row {row.Index} ({code}): 'group_name' is missing.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: No group_name")
                continue
                
            if kind not in ("pseudo", "cohort"):
                errors.append(f"Skipped row {row.Index} ({code}): 'kind' must be 'pseudo' or 'cohort'.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: Invalid kind '{kind}'")
                continue

            existing = conn.execute(sa_text("""
                SELECT * FROM curriculum_groups WHERE degree_code = :dc AND group_code = :gc
            """), {"dc": degree_code, "gc": code}).fetchone()
            
            new_data = {
                "group_name": name,
                "kind": kind,
                "active": active,
                "sort_order": sort_order,
                "description": desc,
            }

            if existing:
                action = "update"
                old_data = {k: getattr(existing, k) for k in new_data}
                changes = {k: v for k, v in new_data.items() if str(v) != str(old_data[k])}
                
                if not changes:
                    if debug:
                        debug_info.append(f"  ⭐️ No changes needed (data identical)")
                    continue

                if debug:
                    debug_info.append(f"  📄 UPDATE: {len(changes)} field(s) changed")
                    for k, v in changes.items():
                        debug_info.append(f"     • {k}: '{old_data[k]}' → '{v}'")

                if not dry_run:
                    conn.execute(sa_text(f"""
                        UPDATE curriculum_groups
                           SET {', '.join([f"{k} = :{k}" for k in changes])}
                         WHERE id = :id
                    """), {**changes, "id": existing.id})
                    
                    if debug:
                        debug_info.append(f"  ✅ SQL executed successfully")
                
                updated_count += 1
                audit_note = "Import: Updated" if not dry_run else "Import: Updated (DRY-RUN)"
                audit_payload = {"degree_code": degree_code, "group_code": code, **changes}

            else:
                action = "create"
                
                if debug:
                    debug_info.append(f"  ➕ CREATE: New curriculum group")
                    debug_info.append(f"     • group_code: {code}")
                    debug_info.append(f"     • group_name: {name}")
                    debug_info.append(f"     • kind: {kind}")
                    debug_info.append(f"     • active: {active}")

                if not dry_run:
                    conn.execute(sa_text("""
                        INSERT INTO curriculum_groups (degree_code, group_code, group_name, kind, active, sort_order, description)
                        VALUES(:dc, :gc, :gn, :kind, :active, :sort_order, :desc)
                    """), {
                        "dc": degree_code,
                        "gc": code,
                        "gn": name,
                        "kind": kind,
                        "active": active,
                        "sort_order": sort_order,
                        "desc": desc,
                    })
                    
                    if debug:
                        debug_info.append(f"  ✅ SQL executed successfully")
                
                created_count += 1
                audit_note = "Import: Created" if not dry_run else "Import: Created (DRY-RUN)"
                audit_payload = {"degree_code": degree_code, "group_code": code, **new_data}
                
            if not dry_run:
                _audit_curriculum_group(conn, action, actor, audit_payload, note=audit_note)

        except Exception as e:
            error_msg = f"Error on row {row.Index} (Curriculum Group '{code}'): {e}"
            errors.append(error_msg)
            if debug:
                debug_info.append(f"  ❌ ERROR: {e}")

    if debug or dry_run:
        st.info("**Import Debug Information:**")
        for line in debug_info:
            st.text(line)
    
    if not dry_run and debug:
        st.write("---")
        st.write("**Verification: Checking database...**")
        try:
            verify_count = conn.execute(sa_text(
                "SELECT COUNT(*) FROM curriculum_groups WHERE degree_code = :dc"
            ), {"dc": degree_code}).fetchone()[0]
            st.success(f"✅ Database now has {verify_count} curriculum group(s) for degree '{degree_code}'")
        except Exception as e:
            st.error(f"❌ Verification failed: {e}")

    return created_count, updated_count, errors


def import_cg_links(
    conn, 
    df_import: pd.DataFrame, 
    degree_code: str, 
    actor: str, 
    cg_allowed: bool,
    group_codes: List[str],
    program_codes: List[str],
    branch_codes: List[str],
    engine = None,
    dry_run: bool = False,
    debug: bool = False
) -> Tuple[int, int, List[str]]:
    """Imports Curriculum Group Links from a DataFrame."""
    created_count = 0
    updated_count = 0
    errors = []
    debug_info = []
    
    if dry_run:
        debug_info.append("🔍 DRY-RUN MODE: No changes will be saved to database")
    
    if not cg_allowed:
        errors.append("Import failed: This degree's cohort mode does not support curriculum group links.")
        return 0, 0, errors

    df_import.columns = [col.strip() for col in df_import.columns]
    
    if debug:
        debug_info.append(f"📊 Input DataFrame: {len(df_import)} rows, {len(df_import.columns)} columns")
        debug_info.append(f"📋 Columns found: {list(df_import.columns)}")

    req_cols = set(CGL_IMPORT_COLS)
    if not req_cols.issubset(df_import.columns):
        missing = list(req_cols - set(df_import.columns))
        errors.append(f"Import file is missing required columns: {', '.join(missing)}")
        return 0, 0, errors

    if debug:
        debug_info.append(f"✅ All required columns present: {CGL_IMPORT_COLS}")
        debug_info.append(f"🎯 Target degree: {degree_code}")
        debug_info.append(f"👤 Actor: {actor}")
        debug_info.append(f"📊 Valid groups: {len(group_codes)}, programs: {len(program_codes)}, branches: {len(branch_codes)}")
        debug_info.append("---")

    for idx, row in enumerate(df_import.itertuples(), start=1):
        try:
            group_code = str(getattr(row, "group_code", "")).strip().upper()
            program_code = str(getattr(row, "program_code", "")).strip().upper()
            branch_code = str(getattr(row, "branch_code", "")).strip().upper()

            if debug:
                debug_info.append(f"🔍 Row {idx}: Processing link for group '{group_code}', program '{program_code}', branch '{branch_code}'")

            if not group_code:
                errors.append(f"Skipped row {row.Index}: 'group_code' is missing.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: No group_code")
                continue
                
            if group_code not in group_codes:
                errors.append(f"Skipped row {row.Index}: curriculum group '{group_code}' not found.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: Group '{group_code}' not in valid groups")
                continue
                
            if program_code and program_code not in program_codes:
                errors.append(f"Skipped row {row.Index}: program_code '{program_code}' not found.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: Program '{program_code}' not in valid programs")
                continue
                
            if branch_code and branch_code not in branch_codes:
                errors.append(f"Skipped row {row.Index}: branch_code '{branch_code}' not found.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: Branch '{branch_code}' not in valid branches")
                continue

            group_id_row = conn.execute(sa_text("""
                SELECT id FROM curriculum_groups WHERE degree_code = :dc AND group_code = :gc
            """), {"dc": degree_code, "gc": group_code}).fetchone()

            if not group_id_row:
                errors.append(f"Skipped row {row.Index}: curriculum group '{group_code}' not found in DB.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: Group '{group_code}' not found in database")
                continue

            duplicate = conn.execute(sa_text("""
                SELECT id FROM curriculum_group_links
                WHERE degree_code=:dc AND group_id=:gid AND program_code=:pc AND branch_code=:bc
            """), {
                "dc": degree_code, 
                "gid": group_id_row.id, 
                "pc": program_code or None, 
                "bc": branch_code or None
            }).fetchone()

            if duplicate:
                if debug:
                    debug_info.append(f"  ⭐️ Link already exists, skipping")
                continue

            if debug:
                debug_info.append(f"  ➕ CREATE: New link")
                debug_info.append(f"     • group_id: {group_id_row.id}")
                debug_info.append(f"     • program_code: {program_code}")
                debug_info.append(f"     • branch_code: {branch_code}")

            if not dry_run:
                conn.execute(sa_text("""
                    INSERT INTO curriculum_group_links (group_id, degree_code, program_code, branch_code)
                    VALUES (:gid, :dc, :pc, :bc)
                """), {
                    "gid": group_id_row.id,
                    "dc": degree_code,
                    "pc": program_code or None,
                    "bc": branch_code or None
                })
                
                if debug:
                    debug_info.append(f"  ✅ SQL executed successfully")
            
            created_count += 1
            
            if not dry_run:
                _audit_curriculum_group_link(
                    conn,
                    "create",
                    actor,
                    {
                        "group_id": group_id_row.id,
                        "degree_code": degree_code,
                        "program_code": program_code or None,
                        "branch_code": branch_code or None
                    },
                    note="Import: Created"
                )

        except Exception as e:
            error_msg = f"Error on row {row.Index}: {e}"
            errors.append(error_msg)
            if debug:
                debug_info.append(f"  ❌ ERROR: {e}")

    if debug or dry_run:
        st.info("**Import Debug Information:**")
        for line in debug_info:
            st.text(line)
    
    if not dry_run and debug:
        st.write("---")
        st.write("**Verification: Checking database...**")
        try:
            verify_count = conn.execute(sa_text(
                "SELECT COUNT(*) FROM curriculum_group_links WHERE degree_code = :dc"
            ), {"dc": degree_code}).fetchone()[0]
            st.success(f"✅ Database now has {verify_count} curriculum group link(s) for degree '{degree_code}'")
        except Exception as e:
            st.error(f"❌ Verification failed: {e}")

    return created_count, updated_count, errors

