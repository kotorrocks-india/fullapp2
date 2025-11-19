from __future__ import annotations

import sqlite3
from typing import Optional, List, Tuple

import streamlit as st

from core.electives_policy import (
    ElectivesPolicy,
    fetch_effective_policy,
    get_policy_for_scope,
    upsert_policy,
    deactivate_policy_for_scope,
)


# ------------- Role / permissions helper -------------


ALLOWED_POLICY_ROLES = {
    # global + tech + academic roles (you can tweak this list)
    "superadmin",
    "tech_admin",
    "tech-admin",
    "academic_admin",
    "academic-admin",
    "academic_admin_degree",
    "academic_admin_program",
    "academic_admin_branch",
    "electives_admin",
}



def _user_can_edit_policy() -> bool:
    """
    Decide whether current user can edit electives policy.
    Checks st.session_state["user"]["roles"] AND st.session_state["current_user_roles"].
    """
    # 1. Try to get roles from the standard 'user' object in session state
    user = st.session_state.get("user", {})
    roles = user.get("roles", [])

    # 2. Fallback: try the old key if 'user' didn't have it
    if not roles:
        roles = st.session_state.get("current_user_roles") or []
    
    # 3. Normalize to a set of lowercase strings
    try:
        # If 'roles' is a list of strings or set of strings
        roles_set = {str(r).lower() for r in roles}
    except Exception:
        return False

    # 4. Check if any user role matches the allowed list
    for allowed in ALLOWED_POLICY_ROLES:
        if allowed.lower() in roles_set:
            return True
            
    return False

def _load_degrees(conn: sqlite3.Connection) -> List[Tuple[str, str, bool]]:
    """
    Return list of (degree_code, label, is_active).
    Shows *all* degrees, but marks inactive ones in the label.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT code, title, active, sort_order
        FROM degrees
        ORDER BY active DESC, sort_order, code;
        """
    )
    rows = cur.fetchall()
    result: List[Tuple[str, str, bool]] = []
    for code, title, active, sort_order in rows:
        is_active = bool(active)
        base_label = f"{code} – {title}"
        label = base_label if is_active else f"[INACTIVE] {base_label}"
        result.append((code, label, is_active))
    return result


def _load_programs_for_degree(
    conn: sqlite3.Connection,
    degree_code: str,
) -> List[Tuple[str, str, bool]]:
    """
    Return list of (program_code, label, is_active) for a given degree.

    Uses programs table:
    - degree_code
    - program_code
    - program_name
    - active
    - sort_order
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT program_code, program_name, active, sort_order
        FROM programs
        WHERE degree_code = ?
        ORDER BY active DESC, sort_order, program_code;
        """,
        (degree_code,),
    )
    rows = cur.fetchall()
    result: List[Tuple[str, str, bool]] = []
    for code, name, active, sort_order in rows:
        is_active = bool(active)
        base_label = f"{code} – {name}"
        label = base_label if is_active else f"[INACTIVE] {base_label}"
        result.append((code, label, is_active))
    return result


def _load_branches_for_program(
    conn: sqlite3.Connection,
    degree_code: str,
    program_code: str,
) -> List[Tuple[str, str, bool]]:
    """
    Return list of (branch_code, label, is_active) for a given degree+program.

    branches table only has program_id; so we join programs -> branches.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            b.branch_code,
            b.branch_name,
            b.active,
            b.sort_order
        FROM branches b
        JOIN programs p ON b.program_id = p.id
        WHERE
            p.degree_code = ?
            AND p.program_code = ?
        ORDER BY
            b.active DESC,
            b.sort_order,
            b.branch_code;
        """,
        (degree_code, program_code),
    )
    rows = cur.fetchall()
    result: List[Tuple[str, str, bool]] = []
    for code, name, active, sort_order in rows:
        is_active = bool(active)
        base_label = f"{code} – {name}"
        label = base_label if is_active else f"[INACTIVE] {base_label}"
        result.append((code, label, is_active))
    return result


# ------------- UI helpers -------------


def _elective_mode_labels() -> dict:
    return {
        "topics_only": "Topics only (base subject + many topics, B.Arch-style studios)",
        "subject_only": "Subject only (students pick one subject, no sub-topics)",
        "both": "Both patterns allowed",
    }


def _allocation_mode_labels() -> dict:
    return {
        "upload_only": "Upload only (office/faculty upload final allocations)",
        "rank_and_auto": "Rank + auto allocation (students give choices)",
    }


def _capacity_strategy_labels() -> dict:
    return {
        "manual": "Manual per-topic capacity",
        "equal_split": "Equal split (total seats ÷ topics)",
        "unlimited": "Unlimited (no cap per topic)",
    }


def _new_default_policy(
    degree_code: str,
    program_code: Optional[str],
    branch_code: Optional[str],
    scope_level: str,
) -> ElectivesPolicy:
    """
    Default settings when no row exists yet.
    You can tweak these defaults if you like.
    """
    return ElectivesPolicy(
        id=None,
        degree_code=degree_code,
        program_code=program_code,
        branch_code=branch_code,
        scope_level=scope_level,
        elective_mode="topics_only",
        allocation_mode="upload_only",
        max_choices_per_slot=0,
        default_topic_capacity_strategy="manual",
        cross_batch_allowed=False,
        cross_branch_allowed=False,
        uses_timetable_clash_check=False,
        is_active=True,
        notes=None,
    )


def _apply_architecture_preset(policy: ElectivesPolicy) -> ElectivesPolicy:
    """
    Architecture / studio preset:

    - Topics-only studio-style electives
    - Upload-only allocation (office/faculty upload rosters)
    - Cross-batch + cross-branch allowed (vertical/open studios)
    - Manual capacities, no timetable clash checks
    """
    policy.elective_mode = "topics_only"
    policy.allocation_mode = "upload_only"
    policy.max_choices_per_slot = 0
    policy.default_topic_capacity_strategy = "manual"
    policy.cross_batch_allowed = True
    policy.cross_branch_allowed = True
    policy.uses_timetable_clash_check = False
    if not policy.notes:
        policy.notes = "Architecture-style studio electives preset."
    return policy


def _apply_engineering_preset(policy: ElectivesPolicy) -> ElectivesPolicy:
    """
    Engineering / rank-based preset:

    - Subject-only electives (or topics if you later wish)
    - Rank + auto allocation with 3 choices
    - No cross-batch/branch by default
    - Equal split capacities, timetable clash checks ON
    """
    policy.elective_mode = "subject_only"
    policy.allocation_mode = "rank_and_auto"
    policy.max_choices_per_slot = 3
    policy.default_topic_capacity_strategy = "equal_split"
    policy.cross_batch_allowed = False
    policy.cross_branch_allowed = False
    policy.uses_timetable_clash_check = True
    if not policy.notes:
        policy.notes = "Engineering-style rank-based electives preset."
    return policy


# ------------- Main entry point -------------


def render(conn: sqlite3.Connection) -> None:
    """
    Render the Electives Policy admin screen.

    Expected usage in your navigation:

        import screens.electives_policy_admin as ep_admin
        ...
        # conn should be a sqlite3.Connection (e.g. engine.raw_connection())
        ep_admin.render(conn)

    Permission model:
        - Editable only if current_user_roles (in st.session_state)
          intersects ALLOWED_POLICY_ROLES.
        - Otherwise, the page is read-only.
    """
    can_edit = _user_can_edit_policy()

    st.title("Electives Policy")

    st.caption(
        "Configure how electives behave per degree / program / branch. "
        "Use this to differentiate B.Arch (studio-style topics, vertical studios) "
        "vs. engineering rank-based electives without duplicating modules."
    )

    if not can_edit:
        st.info(
            "You have **read-only** access to electives policy. "
            "Contact a superadmin / academic admin / tech admin if you need edit rights."
        )

    st.divider()

    # ----- Scope selection -----

    scope_choice = st.radio(
        "Policy scope level",
        options=["degree", "program", "branch"],
        format_func=lambda v: {
            "degree": "Degree-wide policy",
            "program": "Program-level policy",
            "branch": "Branch-level policy",
        }[v],
        horizontal=True,
    )

    degrees = _load_degrees(conn)
    if not degrees:
        st.error("No degrees found. Please configure degrees before electives policy.")
        return

    degree_codes = [d[0] for d in degrees]
    degree_labels = {code: label for code, label, _ in degrees}
    degree_active = {code: is_active for code, _, is_active in degrees}

    selected_degree_code = st.selectbox(
        "Degree",
        options=degree_codes,
        format_func=lambda code: degree_labels.get(code, code),
    )
    degree_code = selected_degree_code

    if not degree_active.get(degree_code, True):
        st.warning(
            "Selected **degree** is currently inactive. "
            "You can still configure policy, but it won't affect new data unless the degree is re-activated."
        )

    program_code: Optional[str] = None
    branch_code: Optional[str] = None

    if scope_choice in ("program", "branch"):
        programs = _load_programs_for_degree(conn, degree_code)
        if not programs:
            st.warning(
                "No programs found for this degree. "
                "You can still use a **degree-level** policy."
            )
            program_code = None
        else:
            program_codes = [p[0] for p in programs]
            program_labels = {code: label for code, label, _ in programs}
            program_active = {code: is_active for code, _, is_active in programs}

            selected_program_code = st.selectbox(
                "Program",
                options=program_codes,
                format_func=lambda code: program_labels.get(code, code),
            )
            program_code = selected_program_code

            if program_code is not None and not program_active.get(program_code, True):
                st.warning(
                    "Selected **program** is currently inactive. "
                    "You can still configure policy for historical or future use."
                )

    if scope_choice == "branch" and program_code is not None:
        branches = _load_branches_for_program(conn, degree_code, program_code)
        if not branches:
            st.warning(
                "No branches found for this degree/program. "
                "You can still configure a **program-level** or **degree-level** policy."
            )
            branch_code = None
        else:
            branch_codes = [b[0] for b in branches]
            branch_labels = {code: label for code, label, _ in branches}
            branch_active = {code: is_active for code, _, is_active in branches}

            selected_branch_code = st.selectbox(
                "Branch",
                options=branch_codes,
                format_func=lambda code: branch_labels.get(code, code),
            )
            branch_code = selected_branch_code

            if branch_code is not None and not branch_active.get(branch_code, True):
                st.warning(
                    "Selected **branch** is currently inactive. "
                    "You can still configure policy if needed for archival or planning."
                )

    scope_level = scope_choice

    st.info(
        f"Editing policy for scope: "
        f"degree={degree_code!r}, "
        f"program={program_code!r}, "
        f"branch={branch_code!r} "
        f"({scope_level})"
    )

    # ----- Load existing policy (if any) for *this exact scope* -----

    existing_policy = get_policy_for_scope(
        conn,
        degree_code=degree_code,
        program_code=program_code,
        branch_code=branch_code,
    )

    if existing_policy and not existing_policy.is_active:
        st.warning(
            "An inactive policy exists for this scope. "
            "Saving will reactivate it."
        )

    if existing_policy is None:
        policy = _new_default_policy(
            degree_code=degree_code,
            program_code=program_code,
            branch_code=branch_code,
            scope_level=scope_level,
        )
    else:
        policy = existing_policy

    # ----- Quick presets (only when editing allowed) -----

    if can_edit:
        st.subheader("Quick presets")
        col_arch, col_eng = st.columns(2)
        preset_chosen: Optional[str] = None

        with col_arch:
            if st.button("🏛 Architecture / Studio preset", use_container_width=True):
                preset_chosen = "arch"
        with col_eng:
            if st.button("⚙️ Engineering / Rank-based preset", use_container_width=True):
                preset_chosen = "eng"

        if preset_chosen == "arch":
            policy = _apply_architecture_preset(policy)
            st.success("Applied Architecture / Studio preset. You can review & tweak before saving.")
        elif preset_chosen == "eng":
            policy = _apply_engineering_preset(policy)
            st.success("Applied Engineering / Rank-based preset. You can review & tweak before saving.")

    st.subheader("Policy settings")

    em_labels = _elective_mode_labels()
    am_labels = _allocation_mode_labels()
    cap_labels = _capacity_strategy_labels()

    # ----- Policy edit form -----

    with st.form("electives_policy_form"):
        elective_mode = st.selectbox(
            "Elective mode",
            options=list(em_labels.keys()),
            index=list(em_labels.keys()).index(policy.elective_mode)
            if policy.elective_mode in em_labels
            else 0,
            format_func=lambda k: em_labels.get(k, k),
            help=(
                "How electives exist in this context.\n\n"
                "• Topics only: base subject with multiple topics (studio, seminar, etc.).\n"
                "• Subject only: students pick one subject, no sub-topics.\n"
                "• Both: allow either pattern."
            ),
            disabled=not can_edit,
        )

        allocation_mode = st.selectbox(
            "Allocation mode",
            options=list(am_labels.keys()),
            index=list(am_labels.keys()).index(policy.allocation_mode)
            if policy.allocation_mode in am_labels
            else 0,
            format_func=lambda k: am_labels.get(k, k),
            help=(
                "How students get allocated to topics/subjects.\n\n"
                "• Upload only: office/faculty upload final rosters.\n"
                "• Rank + auto: students submit preferences, app allocates."
            ),
            disabled=not can_edit,
        )

        max_choices_per_slot = st.number_input(
            "Max choices per slot (for rank + auto)",
            min_value=0,
            max_value=10,
            value=policy.max_choices_per_slot,
            help="0 disables preference capture. For engineering, 3 is common.",
            disabled=not can_edit,
        )

        default_topic_capacity_strategy = st.selectbox(
            "Default topic capacity strategy",
            options=list(cap_labels.keys()),
            index=list(cap_labels.keys()).index(policy.default_topic_capacity_strategy)
            if policy.default_topic_capacity_strategy in cap_labels
            else 0,
            format_func=lambda k: cap_labels.get(k, k),
            disabled=not can_edit,
        )

        cross_batch_allowed = st.checkbox(
            "Allow cross-batch topics (vertical studios)",
            value=policy.cross_batch_allowed,
            help="Enable this for vertical/open electives spanning multiple batches/years.",
            disabled=not can_edit,
        )

        cross_branch_allowed = st.checkbox(
            "Allow cross-branch / cross-program topics",
            value=policy.cross_branch_allowed,
            help="Enable this if electives can mix branches/programs within the degree.",
            disabled=not can_edit,
        )

        uses_timetable_clash_check = st.checkbox(
            "Enable timetable clash checks",
            value=policy.uses_timetable_clash_check,
            help="Recommended for tightly scheduled engineering electives.",
            disabled=not can_edit,
        )

        notes = st.text_area(
            "Notes (optional)",
            value=policy.notes or "",
            help="Internal note, e.g. 'B.Arch studio electives; office uploads rosters.'",
            disabled=not can_edit,
        )

        col_save, col_deactivate = st.columns([2, 1])
        with col_save:
            save_clicked = st.form_submit_button(
                "💾 Save policy",
                use_container_width=True,
                disabled=not can_edit,
            )
        with col_deactivate:
            deactivate_clicked = st.form_submit_button(
                "🗑 Deactivate for this scope",
                use_container_width=True,
                disabled=not can_edit,
            )

    # ----- Handle actions -----

    if deactivate_clicked:
        if not can_edit:
            st.warning("You do not have permission to deactivate policies.")
        else:
            deactivate_policy_for_scope(
                conn,
                degree_code=degree_code,
                program_code=program_code,
                branch_code=branch_code,
            )
            st.success("Policy deactivated for this scope.")
        st.stop()

    if save_clicked:
        if not can_edit:
            st.warning("You do not have permission to save policies.")
        else:
            # Update in-memory object with form values
            policy.scope_level = scope_level
            policy.elective_mode = elective_mode
            policy.allocation_mode = allocation_mode
            policy.max_choices_per_slot = int(max_choices_per_slot)
            policy.default_topic_capacity_strategy = default_topic_capacity_strategy
            policy.cross_batch_allowed = bool(cross_batch_allowed)
            policy.cross_branch_allowed = bool(cross_branch_allowed)
            policy.uses_timetable_clash_check = bool(uses_timetable_clash_check)
            policy.is_active = True
            policy.notes = notes or None

            saved = upsert_policy(conn, policy)
            st.success(
                f"Policy saved for degree={saved.degree_code}, "
                f"program={saved.program_code}, branch={saved.branch_code}."
            )

    # ----- Show effective policy (with inheritance banner) -----

    st.divider()
    st.subheader("Effective policy for this degree context")

    effective = fetch_effective_policy(
        conn,
        degree_code=degree_code,
        program_code=program_code,
        branch_code=branch_code,
    )

    if effective is None:
        st.warning(
            "No effective policy found. Electives engine should fall back to code defaults, "
            "or you can define a policy above."
        )
        return

    # Inheritance banner: if effective scope != current scope, show where it's coming from.
    if (
        effective.scope_level != scope_level
        or effective.program_code != program_code
        or effective.branch_code != branch_code
    ):
        st.info(
            "Policy for this context is currently **inherited** from a broader scope:\n\n"
            f"- Effective scope level: `{effective.scope_level}`\n"
            f"- From degree={effective.degree_code!r}, "
            f"program={effective.program_code!r}, "
            f"branch={effective.branch_code!r}\n\n"
            "Saving a policy above will create/override the policy specifically for the "
            "scope you selected."
        )

    em = _elective_mode_labels()
    am = _allocation_mode_labels()
    cs = _capacity_strategy_labels()

    st.markdown(
        f"""
**Scope resolved:** `{effective.scope_level}`  
**Elective mode:** {em.get(effective.elective_mode, effective.elective_mode)}  
**Allocation mode:** {am.get(effective.allocation_mode, effective.allocation_mode)}  
**Max choices per slot:** `{effective.max_choices_per_slot}`  
**Capacity strategy:** {cs.get(effective.default_topic_capacity_strategy, effective.default_topic_capacity_strategy)}  

**Cross-batch allowed:** `{"Yes" if effective.cross_batch_allowed else "No"}`  
**Cross-branch allowed:** `{"Yes" if effective.cross_branch_allowed else "No"}`  
**Timetable clash checks:** `{"Enabled" if effective.uses_timetable_clash_check else "Disabled"}`  

**Notes:** {effective.notes or "_(none)_"}
"""
    )
# 1. Check if the engine exists in session state (it should be created by app.py)
if "engine" not in st.session_state:
    st.error("Database engine not found in session state. Please go to the login page.")
    st.stop()

# 2. Get the SQLAlchemy engine
engine = st.session_state["engine"]

# 3. The render() function expects a raw sqlite3.Connection.
#    We get one from the engine's raw_connection() method.
#    We use 'try...finally' to ensure the connection is always closed.
# ----- Main execution block for st.navigation -----

# This code runs when the page is selected in the sidebar

conn = None
try:
    # 1. Check if the engine exists
    if "engine" not in st.session_state:
        st.error("Database engine not found in session state. Please go to the login page.")
        st.stop()

    # 2. Get the SQLAlchemy engine
    engine = st.session_state["engine"]
    
    # 3. Get the raw connection
    #    This is now INSIDE the try block to catch connection errors
    conn = engine.raw_connection()

    # 4. Call the main render function for this page
    render(conn)

except Exception as e:
    # This should now catch ALL errors, including connection errors
    st.error(f"An error occurred while rendering the page: {e}")
    # Also print to the console for more details
    import traceback
    traceback.print_exc()

finally:
    # 5. Safely close the connection
    if conn:
        conn.close()
