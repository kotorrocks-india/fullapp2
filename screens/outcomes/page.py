# screens/outcomes/page.py
"""
Program Outcomes (PEOs, POs, PSOs) Screen
Main Streamlit interface for managing educational objectives and outcomes.

FIXES:
- Added detailed error reporting for CSV imports so users can see WHY rows failed.
"""

from __future__ import annotations

import json
import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text

from core.db import get_engine, init_db
from core.policy import require_page, can_edit_page, user_roles
from core.settings import load_settings

try:
    from screens.approvals.schema_helpers import (
        _cols as _appr_cols,
        _table_exists as _appr_table_exists,
    )
except ImportError:
    _appr_cols = None
    _appr_table_exists = None

from screens.outcomes.models import (
    OutcomeSet,
    OutcomeItem,
    SetType,
    Status,
    BloomLevel,
    SET_TYPE_LABELS,
    BLOOM_LEVEL_DESCRIPTIONS,
    SCOPE_LEVEL_DESCRIPTIONS,
    ScopeLevel,
)
from screens.outcomes.helpers import (
    table_exists,
    fetch_degrees,
    fetch_programs,
    fetch_branches,
    get_scope_config,
    get_outcome_sets,
    get_outcome_items,
    create_outcome_set,
    add_outcome_item,
    update_outcome_item,
    delete_outcome_item,
    publish_outcome_set,
    unpublish_outcome_set,
    archive_outcome_set,
    format_scope_display,
    format_set_type_display,
    truncate_text,
    audit_operation,
    check_mappings,
)
from screens.outcomes.manager import OutcomesManager


PAGE_KEY = "Outcomes"


# ============================================================================
# SCOPE SELECTION
# ============================================================================


def render_scope_selector(conn, engine, can_edit: bool):
    """Render scope selection UI (degree / program / branch)."""
    st.subheader("🎯 Select Scope")

    degrees = fetch_degrees(conn)
    if not degrees:
        st.warning("No degrees found. Please create degrees first.")
        return None

    # Degree selection
    degree_options = {d[0]: f"{d[0]} - {d[1]}" for d in degrees}
    selected_degree = st.selectbox(
        "Degree",
        options=list(degree_options.keys()),
        format_func=lambda x: degree_options[x],
        key="outcome_degree",
    )

    if not selected_degree:
        return None

    scope: dict[str, str] = {"degree_code": selected_degree}

    # Default configured scope (for info only)
    config_scope = get_scope_config(conn, selected_degree)
    try:
        config_scope_enum = ScopeLevel(config_scope)
    except Exception:
        config_scope_enum = ScopeLevel.PER_PROGRAM

    programs = fetch_programs(conn, selected_degree)
    has_programs = bool(programs)

    # Do we have any branches at all for this degree?
    has_branches = False
    if has_programs:
        for p in programs:
            program_id = p[0]
            brs = fetch_branches(conn, selected_degree, program_id)
            if brs:
                has_branches = True
                break

    col1, col2 = st.columns([3, 1])
    with col1:
        st.caption(f"**Default scope (configuration):** {format_scope_display(config_scope)}")
        st.caption(SCOPE_LEVEL_DESCRIPTIONS.get(config_scope_enum, ""))

        if not has_programs:
            st.caption(
                "This degree has no programs configured. "
                "Outcomes are effectively managed at **degree level only**."
            )

    with col2:
        if can_edit and has_programs and st.button("⚙️ Change default scope", key="change_scope"):
            st.session_state["show_scope_config"] = True

    # Optional default-scope configuration panel
    if has_programs and st.session_state.get("show_scope_config"):
        with st.expander("🔧 Default scope configuration", expanded=True):
            render_scope_configuration(conn, engine, selected_degree)
            if st.button("Done", key="scope_done"):
                del st.session_state["show_scope_config"]
                st.rerun()

    # Runtime scope choice: Degree / Program / Branch
    scope_options = ["degree"]
    if has_programs:
        scope_options.append("program")
    if has_branches:
        scope_options.append("branch")

    labels = {
        "degree": "Degree level (all programs & branches)",
        "program": "Program level (select program)",
        "branch": "Branch level (select program & branch)",
    }

    chosen_scope = st.radio(
        "Work at level",
        options=scope_options,
        index=0,
        format_func=lambda x: labels[x],
        key="outcome_runtime_scope",
    )

    if chosen_scope == "degree":
        return scope

    if chosen_scope in ("program", "branch") and has_programs:
        program_options = {p[1]: f"{p[1]} - {p[2]}" for p in programs}
        selected_program = st.selectbox(
            "Program",
            options=list(program_options.keys()),
            format_func=lambda x: program_options[x],
            key="outcome_program",
        )
        if selected_program:
            scope["program_code"] = selected_program

    if chosen_scope == "branch" and has_branches and "program_code" in scope:
        prog_row = [p for p in programs if p[1] == scope["program_code"]]
        if prog_row:
            program_id = prog_row[0][0]
            branches = fetch_branches(conn, selected_degree, program_id)
            if branches:
                branch_options = {b[1]: f"{b[1]} - {b[2]}" for b in branches}
                selected_branch = st.selectbox(
                    "Branch",
                    options=list(branch_options.keys()),
                    format_func=lambda x: branch_options[x],
                    key="outcome_branch",
                )
                if selected_branch:
                    scope["branch_code"] = selected_branch

    return scope


def render_scope_configuration(conn, engine, degree_code: str):
    """Render scope configuration interface for a degree."""
    programs = fetch_programs(conn, degree_code)
    if not programs:
        st.info(
            "This degree has no programs or branches configured. "
            "Scope is fixed to **Degree-level**."
        )
        st.caption(
            "You cannot switch to program-level or branch-level scope "
            "when there are no programs/branches for this degree."
        )
        return

    current_scope = get_scope_config(conn, degree_code)

    st.write("Change how outcomes are organized for this degree:")

    new_scope = st.radio(
        "Scope level",
        options=["per_degree", "per_program", "per_branch"],
        format_func=format_scope_display,
        index=["per_degree", "per_program", "per_branch"].index(current_scope),
        help="This determines whether outcomes are shared across all programs or unique per program/branch.",
    )

    reason = st.text_input("Reason for change:", key="scope_change_reason")

    if st.button("Update scope configuration", type="primary", key="update_scope_btn"):
        if not reason:
            st.error("Please provide a reason for the scope change.")
        elif new_scope != current_scope:
            actor = st.session_state.get("email", "unknown@example.com")
            manager = OutcomesManager(engine, actor, "admin")

            from .models import ScopeLevel
            success, msg = manager.set_scope_config(
                degree_code,
                ScopeLevel(new_scope),
                reason,
            )

            if success:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)
        else:
            st.info("Scope level unchanged.")


# ============================================================================
# LIST + IMPORT / EXPORT + PUBLISH / ARCHIVE
# ============================================================================


# ... (imports remain the same) ...

# ============================================================================
# LIST + IMPORT / EXPORT + PUBLISH / ARCHIVE
# ============================================================================


def render_outcome_list(conn, engine, scope: dict, actor: str, can_edit: bool):
    """Render list of outcome sets plus CSV import/export."""

    # Clear import state ONLY if an import was successfully applied previously
    if st.session_state.get("outcomes_import_applied"):
        st.session_state.pop("outcomes_import_preview", None)
        st.session_state.pop("outcomes_import_csv", None)
        
        if "outcomes_import_file" in st.session_state:
            del st.session_state.outcomes_import_file
            
        st.session_state.pop("outcomes_import_applied", None)
    
    st.subheader("📋 Outcome sets")

    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        set_type_filter = st.selectbox(
            "Type",
            options=["all"] + [t.value for t in SetType],
            format_func=lambda x: "All types" if x == "all" else format_set_type_display(x),
            key="outcome_type_filter",
        )

    with col2:
        status_filter = st.selectbox(
            "Status",
            options=["active", "all", Status.DRAFT.value, Status.PUBLISHED.value, Status.ARCHIVED.value],
            format_func=lambda x: x.title(),
            key="outcome_status_filter",
        )

    with col3:
        if can_edit:
            st.write("")
            if st.button("➕ Create new set", key="create_new_btn", type="primary"):
                st.session_state["show_create_form"] = True
                st.rerun()

    # CSV import / export toolbar
    manager = OutcomesManager(engine, actor, "admin")
    toolbar_cols = st.columns(2)

    with toolbar_cols[0]:
        csv_data = manager.export_outcomes(
            scope["degree_code"],
            scope.get("program_code"),
            scope.get("branch_code"),
        )
        st.download_button(
            "⬇ Export current outcomes (CSV)",
            data=csv_data.encode("utf-8"),
            file_name="outcomes_export.csv",
            mime="text/csv",
            help=(
                "Downloads all PEO / PO / PSO rows for the selected degree/program/branch "
                "using the POS CSV template columns."
            ),
        )

    with toolbar_cols[1]:
        if can_edit:
            with st.expander("⬆ Import outcomes from CSV", expanded=False):
                st.caption(
                    "Use the POS CSV template columns: "
                    "`degree_code, program_code, branch_code, set_type, status, "
                    "code, title, description, bloom_level, timeline_years, tags`."
                )
                uploaded = st.file_uploader(
                    "Choose a CSV file to preview",
                    type=["csv"],
                    key="outcomes_import_file",
                )

                session_id = st.session_state.get("session_id", "default")

                if uploaded is not None:
                    csv_text = uploaded.getvalue().decode("utf-8")
                    st.session_state["outcomes_import_csv"] = csv_text
                    preview = manager.import_preview(csv_text, scope["degree_code"], session_id)
                    st.session_state["outcomes_import_preview"] = preview

                preview = st.session_state.get("outcomes_import_preview")
                if preview:
                    st.markdown(
                        f"**Import preview:** {preview.total_rows} row(s) • "
                        f"{preview.valid_rows} valid • {preview.invalid_rows} with errors"
                    )

                    if preview.errors:
                        st.error(
                            "Some rows have errors. Please correct the CSV and re-upload. "
                            "Rows with errors will not be imported."
                        )
                        for err in preview.errors[:20]:
                            msg = "; ".join(err.get("errors", []))
                            st.write(f"Row {err.get('row', '?')}: {msg}")
                    else:
                        rows = [
                            {
                                "Row": r.row_number,
                                "Degree": r.degree_code,
                                "Program": r.program_code,
                                "Branch": r.branch_code,
                                "Type": r.set_type,
                                "Status": r.status,
                                "Code": r.code,
                                "Title": r.title,
                                "Description": r.description,
                                "Bloom": r.bloom_level,
                                "Years": r.timeline_years,
                                "Tags": r.tags,
                            }
                            for r in preview.preview_data
                        ]
                        if rows:
                            df_prev = pd.DataFrame(rows)
                            st.dataframe(df_prev, use_container_width=True)

                        st.caption(
                            "If the preview looks correct, click **Apply import** to create "
                            "new outcome sets from this CSV."
                        )

                        if st.button("Apply import", type="primary", key="apply_outcomes_import"):
                            csv_text = st.session_state.get("outcomes_import_csv", "")
                            if not csv_text:
                                st.error("No CSV content found in this session. Please upload again.")
                            else:
                                apply_result = manager.import_apply(
                                    csv_text, scope["degree_code"], session_id
                                )
                                
                                # --- FIX: HANDLING ERRORS PROPERLY WITHOUT RERUN ---
                                if apply_result.failed_rows or apply_result.errors:
                                    st.error(
                                        f"Import failed for {apply_result.failed_rows} row(s). "
                                        f"Only {apply_result.imported_rows} row(s) were imported."
                                    )
                                    
                                    # SHOW ACTUAL ERROR MESSAGES
                                    with st.container():
                                        st.write("### ❌ Error Details")
                                        for error_obj in apply_result.errors:
                                            row_num = error_obj.get("row", "?")
                                            code = error_obj.get("code", "N/A")
                                            # Handle list or string messages
                                            raw_msgs = error_obj.get("errors", [])
                                            if isinstance(raw_msgs, list):
                                                error_msg = " | ".join(raw_msgs)
                                            else:
                                                error_msg = str(raw_msgs)
                                                
                                            st.error(f"**Row {row_num} (Code: {code})**: {error_msg}")
                                    
                                    # DO NOT RERUN HERE. Let the user see the error.
                                            
                                else:
                                    # SUCCESS CASE
                                    st.success(
                                        f"✅ Successfully imported {apply_result.imported_rows} outcome row(s)."
                                    )
                                    st.session_state["outcomes_import_applied"] = True
                                    st.rerun()

    # Fetch outcome sets according to filters
    type_param = None if set_type_filter == "all" else set_type_filter
    status_param = None if status_filter == "active" else (status_filter if status_filter != "all" else None)
    include_archived = status_filter in ["all", Status.ARCHIVED.value]

    sets = get_outcome_sets(
        conn,
        scope["degree_code"],
        scope.get("program_code"),
        scope.get("branch_code"),
        type_param,
        status_param,
        include_archived,
    )

    if not sets:
        st.info("No outcome sets found for the selected scope and filters.")
        return
    
    # ... (rest of the function showing expanders remains the same) ...
    for outcome_set in sets:
        set_id = outcome_set[0]
        set_type = outcome_set[4]
        status = outcome_set[5]
        version = outcome_set[6]

        # Header with status badge
        status_emoji = {"draft": "📝", "published": "✅", "archived": "📦"}
        header = f"{status_emoji.get(status, '')} {format_set_type_display(set_type)} - Version {version}"

        with st.expander(header, expanded=(status == Status.DRAFT.value)):
            col_info, col_actions = st.columns([3, 1])

            with col_info:
                st.caption(f"**Status:** {status.title()}")
                st.caption(f"**Created by:** {outcome_set[8]} on {outcome_set[9]}")
                if outcome_set[10]:  # published_at
                    st.caption(f"**Published by:** {outcome_set[11]} on {outcome_set[10]}")
                if outcome_set[12]:  # archived_at
                    st.caption(f"**Archived by:** {outcome_set[13]} on {outcome_set[12]}")
                    if outcome_set[14]:  # archive_reason
                        st.caption(f"**Archive reason:** {outcome_set[14]}")

            # Get items
            items = get_outcome_items(conn, set_id)

            if items:
                df = pd.DataFrame(
                    [
                        {
                            "Code": item[1],
                            "Title": item[2] or "-",
                            "Description": truncate_text(item[3], 100),
                            "Bloom level": item[4] or "-",
                            "Timeline": f"{item[5]} years" if item[5] else "-",
                        }
                        for item in items
                    ]
                )
                st.dataframe(df, use_container_width=True, hide_index=True)

                # Actions
                if can_edit:
                    action_cols = st.columns(4)

                    with action_cols[0]:
                        if status == Status.DRAFT.value:
                            if st.button("✅ Publish", key=f"publish_{set_id}"):
                                with engine.begin() as trans_conn:
                                    publish_outcome_set(trans_conn, set_id, actor)
                                    audit_operation(
                                        trans_conn, 
                                        "publish_set", 
                                        actor, 
                                        "user", 
                                        set_id=set_id,
                                        reason="Published outcome set"
                                    )
                                st.success("Set published!")
                                st.rerun()

                    with action_cols[1]:
                        if status == Status.PUBLISHED.value:
                            if st.button("📝 Unpublish", key=f"unpublish_{set_id}"):
                                with engine.begin() as trans_conn:
                                    unpublish_outcome_set(trans_conn, set_id, actor)
                                    audit_operation(
                                        trans_conn, 
                                        "unpublish_set", 
                                        actor, 
                                        "user", 
                                        set_id=set_id,
                                        reason="Unpublished outcome set"
                                    )
                                st.success("Set unpublished!")
                                st.rerun()

                    with action_cols[2]:
                        if status != Status.ARCHIVED.value:
                            if st.button("📦 Archive", key=f"archive_{set_id}"):
                                st.session_state[f"archive_reason_{set_id}"] = True

                    with action_cols[3]:
                        if status == Status.DRAFT.value:
                            if st.button("✏️ Edit", key=f"edit_{set_id}"):
                                st.session_state[f"editing_set_{set_id}"] = True
                                st.rerun()

                    # Archive reason form
                    if st.session_state.get(f"archive_reason_{set_id}"):
                        with st.form(key=f"archive_form_{set_id}"):
                            reason = st.text_input(
                                "Archive reason:", key=f"archive_reason_input_{set_id}"
                            )
                            col_submit, col_cancel = st.columns(2)
                            with col_submit:
                                if st.form_submit_button("Confirm archive"):
                                    if reason:
                                        with engine.begin() as trans_conn:
                                            archive_outcome_set(trans_conn, set_id, reason, actor)
                                            audit_operation(
                                                trans_conn,
                                                "archive_set",
                                                actor,
                                                "user",
                                                set_id=set_id,
                                                reason=reason,
                                            )
                                        del st.session_state[f"archive_reason_{set_id}"]
                                        st.success("Set archived!")
                                        st.rerun()
                                    else:
                                        st.error("Please provide a reason.")
                            with col_cancel:
                                if st.form_submit_button("Cancel"):
                                    del st.session_state[f"archive_reason_{set_id}"]
                                    st.rerun()

                # Edit mode
                if st.session_state.get(f"editing_set_{set_id}"):
                    st.markdown("---")
                    render_edit_items(conn, engine, set_id, items, actor)
            else:
                st.info("No items in this set yet.")

# ============================================================================
# APPROVALS HOOK FOR MAJOR EDITS
# ============================================================================


def _enqueue_outcome_edit_approval(
    conn,
    scope: dict,
    set_id: int,
    item_id: int,
    code: str,
    before: dict,
    after: dict,
    change_reason: str,
    actor: str,
) -> int | None:
    """Create an approvals row for a major outcome edit."""
    if _appr_table_exists is None or not _appr_table_exists(conn, "approvals"):
        raise RuntimeError("Approvals table not available; cannot queue major change approval.")

    cols = _appr_cols(conn, "approvals")

    if "note" in cols:
        note_col = "note"
    elif "reason_note" in cols:
        note_col = "reason_note"
    else:
        note_col = None

    has_payload = "payload" in cols

    payload = {
        "degree_code": scope.get("degree_code"),
        "program_code": scope.get("program_code"),
        "branch_code": scope.get("branch_code"),
        "set_id": set_id,
        "item_id": item_id,
        "code": code,
        "before": before,
        "after": after,
        "change_type": "major",
        "reason": change_reason,
        "requested_by": actor,
    }

    col_names = ["object_type", "object_id", "action", "status", "requester"]
    params = {
        "object_type": "outcome",
        "object_id": str(item_id),
        "action": "edit",
        "status": "pending",
        "requester": actor,
    }

    if note_col:
        col_names.append(note_col)
        params[note_col] = change_reason or f"Major change requested for {code}"

    if has_payload:
        col_names.append("payload")
        params["payload"] = json.dumps(payload)

    placeholders = [f":{c}" for c in col_names]

    sql = f"""
        INSERT INTO approvals ({', '.join(col_names)})
        VALUES ({', '.join(placeholders)})
    """
    result = conn.execute(sa_text(sql), params)

    approval_id = getattr(result, "lastrowid", None)
    return approval_id


# ============================================================================
# ADVANCED EDITOR (REORDER + MINOR/MAJOR + AUDIT + CANCEL)
# ============================================================================


def render_edit_items(conn, engine, set_id: int, items: list, actor: str):
    """Render item editing interface with reordering + change type + reason + CANCEL."""
    st.subheader("✏️ Edit outcomes (advanced)")
    
    # FIX: Add cancel button at the top
    col_header, col_cancel = st.columns([4, 1])
    with col_header:
        st.caption("Edit individual outcomes below. Changes require reason and classification.")
    with col_cancel:
        if st.button("❌ Cancel editing", key="cancel_edit_mode", type="secondary"):
            # Clear editing state
            st.session_state.pop(f"editing_set_{set_id}", None)
            st.success("Editing cancelled.")
            st.rerun()

    for idx, item in enumerate(items):
        item_id = item[0]
        code = item[1]
        old_title = item[2] or ""
        old_desc = item[3] or ""
        old_bloom = item[4]
        old_years = item[5]
        old_tags = item[6] or ""

        with st.container():
            # Reorder controls (up / down)
            col_reorder, col_header = st.columns([1, 5])
            with col_reorder:
                up_disabled = idx == 0
                down_disabled = idx == len(items) - 1

                if st.button("▲", key=f"move_up_{item_id}", disabled=up_disabled, help="Move up"):
                    # FIX: Use begin() for transaction with audit
                    with engine.begin() as trans_conn:
                        if _move_outcome_item(trans_conn, set_id, item_id, "up", actor):
                            st.success(f"Outcome {code} moved up.")
                            st.rerun()

                if st.button("▼", key=f"move_down_{item_id}", disabled=down_disabled, help="Move down"):
                    # FIX: Use begin() for transaction with audit
                    with engine.begin() as trans_conn:
                        if _move_outcome_item(trans_conn, set_id, item_id, "down", actor):
                            st.success(f"Outcome {code} moved down.")
                            st.rerun()

            with col_header:
                st.markdown(f"**{code}** – {old_title or 'No title'}")

            # Edit form for this item
            with st.form(key=f"edit_form_{item_id}"):
                col1, col2 = st.columns([2, 1])

                with col1:
                    new_title = st.text_input(
                        "Title (optional)",
                        value=old_title,
                        key=f"title_{item_id}",
                    )
                    new_desc = st.text_area(
                        "Description",
                        value=old_desc,
                        key=f"desc_{item_id}",
                        height=100,
                    )

                with col2:
                    bloom_options = [None] + [b.value for b in BloomLevel]
                    bloom_index = bloom_options.index(old_bloom) if old_bloom in bloom_options else 0
                    new_bloom = st.selectbox(
                        "Bloom level",
                        options=bloom_options,
                        index=bloom_index,
                        key=f"bloom_{item_id}",
                    )

                    new_years = st.number_input(
                        "Timeline (years)",
                        min_value=1,
                        max_value=10,
                        value=old_years if old_years else 5,
                        key=f"years_{item_id}",
                    )

                    new_tags = st.text_input(
                        "Tags (optional, separated by |)",
                        value=old_tags,
                        key=f"tags_{item_id}",
                    )

                # Change type + reason
                st.markdown("**Change classification**")
                change_cols = st.columns([1, 3])
                with change_cols[0]:
                    change_type = st.radio(
                        "Type",
                        options=["Minor", "Major"],
                        index=0,
                        key=f"change_type_{item_id}",
                        horizontal=True,
                        help="Minor: small wording/clarity. Major: meaning or level of outcome changes.",
                    )
                with change_cols[1]:
                    change_reason = st.text_input(
                        "Reason for change",
                        key=f"change_reason_{item_id}",
                        help="Required for major changes; optional for minor edits.",
                    )

                col_update, col_delete = st.columns(2)
                with col_update:
                    clicked_update = st.form_submit_button("💾 Update")

                with col_delete:
                    clicked_delete = st.form_submit_button("🗑️ Delete", type="secondary")

                # Handle DELETE
                if clicked_delete:
                    # FIX: Use begin() for transaction with audit
                    with engine.begin() as trans_conn:
                        delete_outcome_item(trans_conn, item_id)
                        audit_operation(
                            trans_conn,
                            "delete_item",
                            actor,
                            "user",
                            item_id=item_id,
                            code=code,
                            reason="Item deleted by user",
                        )
                    st.success(f"Outcome {code} deleted.")
                    st.rerun()

                # Handle UPDATE
                if clicked_update:
                    if change_type == "Major" and not (change_reason or "").strip():
                        st.warning("Please provide a reason for a major change.")
                    else:
                        before = {
                            "title": old_title,
                            "description": old_desc,
                            "bloom_level": old_bloom,
                            "timeline_years": old_years,
                            "tags": old_tags,
                        }
                        after = {
                            "title": new_title.strip(),
                            "description": new_desc.strip(),
                            "bloom_level": new_bloom,
                            "timeline_years": int(new_years),
                            "tags": new_tags.strip(),
                        }

                        if before == after:
                            st.info("No changes detected for this outcome.")
                        else:
                            change_type_norm = change_type.lower()
                            reason_text = (change_reason or "").strip()

                            if change_type == "Minor":
                                # Minor = immediate update with transaction
                                with engine.begin() as trans_conn:
                                    update_outcome_item(
                                        trans_conn,
                                        item_id,
                                        after["title"],
                                        after["description"],
                                        after["bloom_level"],
                                        after["timeline_years"],
                                        after["tags"],
                                        actor,
                                    )

                                    audit_operation(
                                        trans_conn,
                                        "update_item",
                                        actor,
                                        "user",
                                        item_id=item_id,
                                        code=code,
                                        change_type=change_type_norm,
                                        reason=reason_text or "Minor edit",
                                        before_data=json.dumps(before),
                                        after_data=json.dumps(after),
                                    )

                                st.success(f"Outcome {code} updated as a minor change.")
                                st.rerun()

                            else:
                                # Major = queue approval, DO NOT change DB yet
                                try:
                                    scope = {
                                        "degree_code": st.session_state.get("outcome_degree"),
                                        "program_code": st.session_state.get("outcome_program"),
                                        "branch_code": st.session_state.get("outcome_branch"),
                                    }

                                    # FIX: Use begin() for transaction
                                    with engine.begin() as trans_conn:
                                        approval_id = _enqueue_outcome_edit_approval(
                                            trans_conn,
                                            scope,
                                            set_id,
                                            item_id,
                                            code,
                                            before,
                                            after,
                                            reason_text,
                                            actor,
                                        )

                                        # Log that we requested a major change
                                        audit_operation(
                                            trans_conn,
                                            "request_item_change",
                                            actor,
                                            "user",
                                            item_id=item_id,
                                            code=code,
                                            change_type=change_type_norm,
                                            reason=reason_text or None,
                                            before_data=json.dumps(before),
                                            after_data=json.dumps(after),
                                            approval_id=approval_id,
                                        )

                                    msg_id = f" #{approval_id}" if approval_id is not None else ""
                                    st.success(
                                        f"Major change for outcome {code} has been submitted for approval{msg_id}."
                                    )
                                    st.info(
                                        "The outcome will be updated after approvers approve this request "
                                        "via the Approvals inbox."
                                    )
                                    st.rerun()

                                except Exception as ex:
                                    st.error(f"Could not queue approval for major change: {ex}")


# ============================================================================
# QUICK CREATE WIZARD
# ============================================================================


def render_create_set(conn, engine, scope: dict, actor: str):
    """Render quick-entry form for creating a new outcome set."""
    st.subheader("Define Program Outcomes (PEOs, POs, PSOs)")

    max_items = 10
    num_items = st.session_state.get("outcome_rows", 1)
    if num_items < 1:
        num_items = 1
    if num_items > max_items:
        num_items = max_items

    with st.form("create_outcome_set"):
        set_type_value = st.selectbox(
            "Outcome set type",
            options=[t.value for t in SetType],
            format_func=lambda x: format_set_type_display(x),
            index=list(SetType).index(SetType.POS) if SetType.POS in list(SetType) else 0,
            help="PEOs and PSOs are optional. POs are compulsory as per the program outcome policy.",
        )

        st.markdown("#### Quick POS entry")
        st.caption(
            "Start by defining one outcome. You can add more rows as needed (maximum **10** in this wizard).\n"
        )

        items_data: list[dict] = []
        bloom_choices = [None] + [b.value for b in BloomLevel]

        for i in range(num_items):
            st.markdown(f"**Outcome {i + 1}**")
            col1, col2 = st.columns([2, 1])

            with col1:
                code = st.text_input(
                    "Code",
                    key=f"new_code_{i}",
                    placeholder="e.g., PEO1, PO1, PSO1",
                    help="Short outcome code like PEO1, PO2, PSO3. Leave blank to skip this row.",
                )
                title = st.text_input(
                    "Title (optional)",
                    key=f"new_title_{i}",
                    placeholder="Short label for this outcome (optional).",
                )
                description = st.text_area(
                    "Description",
                    key=f"new_desc_{i}",
                    height=100,
                    placeholder="Full outcome statement as per POS booklet.",
                )

            with col2:
                bloom = st.selectbox(
                    "Bloom level (optional)",
                    options=bloom_choices,
                    key=f"new_bloom_{i}",
                    help="Optional – pick the dominant Bloom level for this outcome.",
                )
                years = st.number_input(
                    "Timeline (years)",
                    min_value=1,
                    max_value=10,
                    value=5,
                    key=f"new_years_{i}",
                    help="Typical time frame (in years) over which this outcome is expected to be attained.",
                )

            if code and description:
                items_data.append(
                    {
                        "code": code.strip().upper(),
                        "title": title.strip() or None,
                        "description": description.strip(),
                        "bloom": bloom,
                        "years": int(years) if years else None,
                        "sort_order": (i + 1) * 10,
                    }
                )

        col_submit, col_cancel = st.columns(2)
        with col_submit:
            submitted = st.form_submit_button("Create outcome set", type="primary")
        with col_cancel:
            cancelled = st.form_submit_button("Cancel")

    # Add / remove rows controls (outside the form)
    col_add, col_remove = st.columns(2)
    with col_add:
        if num_items < max_items and st.button("➕ Add another outcome", key="add_outcome_row"):
            st.session_state["outcome_rows"] = num_items + 1
            st.rerun()
    with col_remove:
        if num_items > 1 and st.button("➖ Remove last outcome", key="remove_outcome_row"):
            st.session_state["outcome_rows"] = num_items - 1
            st.rerun()

    # Handle form actions
    if cancelled:
        st.session_state["outcome_rows"] = 1
        if "show_create_form" in st.session_state:
            del st.session_state["show_create_form"]
        st.rerun()

    if not submitted:
        return

    if not items_data:
        st.error("Please add at least one outcome with Code and Description.")
        return

    # Build OutcomeSet dataclass
    try:
        set_type_enum = SetType(set_type_value)
    except ValueError:
        st.error(f"Unknown set type: {set_type_value}")
        return

    items: list[OutcomeItem] = []
    for data in items_data:
        bloom_enum = None
        if data["bloom"]:
            for b in BloomLevel:
                if b.value == data["bloom"]:
                    bloom_enum = b
                    break

        items.append(
            OutcomeItem(
                code=data["code"],
                description=data["description"],
                title=data["title"],
                bloom_level=bloom_enum,
                timeline_years=data["years"],
                tags=[],
                sort_order=data["sort_order"],
            )
        )

    outcome_set = OutcomeSet(
        degree_code=scope["degree_code"],
        set_type=set_type_enum,
        status=Status.DRAFT,
        program_code=scope.get("program_code"),
        branch_code=scope.get("branch_code"),
        items=items,
    )

    manager = OutcomesManager(engine, actor, "admin")
    success, set_id, errors = manager.create_set(
        outcome_set, reason="Quick entry via Program Outcomes wizard"
    )

    if not success:
        st.error("Could not create outcome set:")
        for err in errors:
            st.write(f"- {err}")
        return

    st.session_state["outcome_rows"] = 1

    if "show_create_form" in st.session_state:
        del st.session_state["show_create_form"]

    # FIX: Audit the creation with proper connection
    with engine.begin() as trans_conn:
        audit_operation(
            trans_conn, 
            "create_set", 
            actor, 
            "user", 
            set_id=set_id,
            reason="Created via wizard"
        )
    
    st.success(
        f"Outcome set created successfully (ID {set_id}). "
        "You can now refine it using the advanced editor."
    )
    st.rerun()


# ============================================================================
# REORDER HELPER
# ============================================================================


def _move_outcome_item(conn, set_id: int, item_id: int, direction: str, actor: str) -> bool:
    """
    Move an outcome item up or down by swapping its sort_order
    with the previous/next item in the same set.

    direction: "up" or "down"
    """
    rows = conn.execute(
        sa_text(
            """
            SELECT id, sort_order
            FROM outcomes_items
            WHERE set_id = :sid
            ORDER BY sort_order, id
            """
        ),
        {"sid": set_id},
    ).fetchall()

    # Find this item in the ordered list
    index = None
    for i, row in enumerate(rows):
        if row[0] == item_id:
            index = i
            break

    if index is None:
        return False

    if direction == "up":
        if index == 0:
            return False
        swap_index = index - 1
    elif direction == "down":
        if index == len(rows) - 1:
            return False
        swap_index = index + 1
    else:
        return False

    this_id, this_sort = rows[index]
    other_id, other_sort = rows[swap_index]

    # Swap sort_order values
    conn.execute(
        sa_text("UPDATE outcomes_items SET sort_order = :s WHERE id = :id"),
        {"s": other_sort, "id": this_id},
    )
    conn.execute(
        sa_text("UPDATE outcomes_items SET sort_order = :s WHERE id = :id"),
        {"s": this_sort, "id": other_id},
    )

    # FIX: Enhanced audit trail for reordering
    audit_operation(
        conn,
        "reorder_item",
        actor,
        "user",
        set_id=set_id,
        item_id=item_id,
        reason=f"Moved {direction}",
        before_data=json.dumps({"position": index, "sort_order": this_sort}),
        after_data=json.dumps({"position": swap_index, "sort_order": other_sort}),
    )

    return True


# ============================================================================
# MAIN PAGE
# ============================================================================


def main():
    """Main page function."""
    require_page(PAGE_KEY)

    st.title("🎯 Program Outcomes")
    st.markdown(
        "Manage PEOs (Program Educational Objectives), "
        "POs (Program Outcomes), and PSOs (Program Specific Outcomes)"
    )

    # Get or create engine with proper db_url
    engine = st.session_state.get("engine")
    if engine is None:
        settings = load_settings()
        db_url = None

        # Try attribute-style access
        if hasattr(settings, "database_url"):
            db_url = getattr(settings, "database_url")
        elif hasattr(settings, "db_url"):
            db_url = getattr(settings, "db_url")

        # Try dict-style access
        if db_url is None and isinstance(settings, dict):
            db_url = (
                settings.get("database_url")
                or settings.get("db_url")
                or settings.get("DB_URL")
            )

        if db_url:
            engine = get_engine(db_url)
        else:
            st.error(
                "Database URL is not configured. "
                "Please check your application settings (database_url / db_url)."
            )
            return

        st.session_state["engine"] = engine

    # Initialize database
    init_db(engine)

    # Check if outcomes tables exist
    with engine.connect() as conn:
        if not table_exists(conn, "outcomes_sets"):
            st.error("⚠️ Outcomes tables not initialized. Please run schema initialization first.")
            if st.button("Initialize Outcomes Schema"):
                from schemas.outcomes_schema import ensure_outcomes_schema

                ensure_outcomes_schema(engine)
                st.success("✅ Schema initialized!")
                st.rerun()
            return

    # User and permissions
    actor = st.session_state.get("email", "unknown@example.com")
    roles = user_roles(engine=engine)
    can_edit = can_edit_page(PAGE_KEY, roles)

    # Main UI – use autocommit for read operations, begin() for writes
    with engine.connect().execution_options(autocommit=True) as conn:
        # Scope selector
        scope = render_scope_selector(conn, engine, can_edit)

        if not scope:
            return

        st.markdown("---")

        # Create form
        if st.session_state.get("show_create_form"):
            render_create_set(conn, engine, scope, actor)
            st.markdown("---")

        # Outcome list
        render_outcome_list(conn, engine, scope, actor, can_edit)


if __name__ == "__main__":
    main()
