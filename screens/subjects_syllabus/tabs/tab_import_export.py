"""
Import/Export Tab - Download and upload data
"""

import streamlit as st
import pandas as pd
from ..exports import (
    export_subjects,
    export_subject_offerings,
    export_templates,
)
from ..imports import import_subjects_from_df
from ..templates_import import import_templates_from_df, TEMPLATE_IMPORT_COLUMNS  # NEW
from ..constants import SUBJECT_IMPORT_TEMPLATE_COLUMNS, SIMPLE_SUBJECT_IMPORT_TEMPLATE_COLUMNS
from core.forms import success


def render(engine, actor: str, CAN_EDIT: bool):
    """Render the Import/Export tab."""
    st.subheader("📥📤 Import / Export")

    col1, col2 = st.columns(2)

    # =========================
    # EXPORT SUBJECTS & OFFERINGS
    # =========================
    with col1:
        st.markdown("### Export Subjects (Catalog Full)")

        if st.button("Export Subjects CSV", key="exp_sub_csv"):
            name, data = export_subjects(engine, fmt="csv")
            st.download_button(
                "Download subject_catalog_full.csv",
                data,
                file_name=name,
                mime="text/csv",
                key="exp_sub_csv_dl",
            )

        if st.button("Export Subjects Excel", key="exp_sub_xlsx"):
            name, data = export_subjects(engine, fmt="excel")
            st.download_button(
                "Download subject_catalog_full.xlsx",
                data,
                file_name=name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="exp_sub_xlsx_dl",
            )

        st.markdown("---")

        st.markdown("### Export Offerings (All AYs)")

        if st.button("Export Offerings CSV", key="exp_off_csv"):
            name, data = export_subject_offerings(engine, fmt="csv")
            st.download_button(
                "Download subjects_all_years_export.csv",
                data,
                file_name=name,
                mime="text/csv",
                key="exp_off_csv_dl",
            )

        if st.button("Export Offerings Excel", key="exp_off_xlsx"):
            name, data = export_subject_offerings(engine, fmt="excel")
            st.download_button(
                "Download subjects_all_years_export.xlsx",
                data,
                file_name=name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="exp_off_xlsx_dl",
            )

    with col2:
        st.markdown("### Export Templates")

        if st.button("Export Templates CSV", key="exp_tmpl_csv"):
            name, data = export_templates(engine, fmt="csv")
            st.download_button(
                "Download templates_export.csv",
                data,
                file_name=name,
                mime="text/csv",
                key="exp_tmpl_csv_dl",
            )

        if st.button("Export Templates Excel", key="exp_tmpl_xlsx"):
            name, data = export_templates(engine, fmt="excel")
            st.download_button(
                "Download templates_export.xlsx",
                data,
                file_name=name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="exp_tmpl_xlsx_dl",
            )

    # =========================
    # IMPORT SUBJECTS
    # =========================
    st.markdown("---")
    st.subheader("📥 Import Subjects Catalog")

    st.markdown(
        "Use this section to bulk-create subjects. "
        "Download a CSV template, fill it, and upload it for dry run / import."
    )

    # Prepare headers for simple and advanced templates
    simple_template_header = ",".join(SIMPLE_SUBJECT_IMPORT_TEMPLATE_COLUMNS) + "\n"
    advanced_template_header = ",".join(SUBJECT_IMPORT_TEMPLATE_COLUMNS) + "\n"

    col_t1, col_t2 = st.columns(2)
    with col_t1:
        st.download_button(
            label="Download Simple Subjects CSV Template",
            data=simple_template_header,
            file_name="subject_catalog_simple_template.csv",
            mime="text/csv",
            key="subj_template_simple_dl",
        )
    with col_t2:
        st.download_button(
            label="Download Advanced Subjects CSV Template (with workload_breakup_json)",
            data=advanced_template_header,
            file_name="subject_catalog_advanced_template.csv",
            mime="text/csv",
            key="subj_template_advanced_dl",
        )

    st.info(
        "**Simple template** – recommended for regular faculty. It has the usual columns "
        "(subject code, name, degree, semester, L/T/P/S, credits, etc.). The app will "
        "automatically build the workload breakup from L/T/P/S and validate the credits.\n\n"
        "**Advanced template** – adds one extra column `workload_breakup_json` for power users. "
        "A valid example value is:\n\n"
        "```json\n"
        "[\n"
        "  {\"code\": \"L\", \"name\": \"Lectures\", \"hours\": 1},\n"
        "  {\"code\": \"S\", \"name\": \"Studio\",   \"hours\": 4}\n"
        "]\n"
        "```\n\n"
        "If you are not sure which one to use, stick to the **Simple template**."
    )

    # Friendlier preview: two-column table "Column" / "Example value"
    example_pairs = [
        ("subject_code", "AD101"),
        ("subject_name", "Architectural Design Studio 1"),
        ("subject_type", "Core"),
        ("degree_code", "BARCH"),
        ("program_code", ""),
        ("branch_code", ""),
        ("curriculum_group_code", "DES"),
        ("semester_number", 1),
        ("credits_total", 4),
        ("L", 0),
        ("T", 0),
        ("P", 0),
        ("S", 4),
        ("student_credits", 4),
        ("teaching_credits", 4),
    ]
    example_df = pd.DataFrame(example_pairs, columns=["Column", "Example value"])
    st.markdown(
        "**Example (Simple template) for Sem 1 – Architectural Design Studio 1:**"
    )
    st.table(example_df)

    subjects_file = st.file_uploader(
        "Upload Subjects CSV",
        type="csv",
        key="subjects_import_csv",
    )

    if subjects_file is not None:
        try:
            subjects_file.seek(0)
            df_import = pd.read_csv(subjects_file)
            st.info(f"Loaded {len(df_import)} rows from CSV")

            col_a, col_b = st.columns(2)

            with col_a:
                if st.button("🧪 Dry Run Import", key="subj_dry_run"):
                    with st.spinner("Running dry run..."):
                        errors, ok_count = import_subjects_from_df(
                            engine, df_import, dry_run=True, actor=actor
                        )

                    if ok_count > 0:
                        st.success(f"✅ Dry run OK: {ok_count} subjects would be created/updated.")

                    if errors:
                        st.warning(f"{len(errors)} issue(s) found in dry run.")
                        err_df = pd.DataFrame(errors)
                        st.dataframe(err_df, use_container_width=True)

                        st.download_button(
                            "Download Errors CSV",
                            err_df.to_csv(index=False),
                            file_name="subjects_import_errors_dry_run.csv",
                            mime="text/csv",
                            key="subj_dry_errors_dl",
                        )

            with col_b:
                if st.button("🚀 Import Subjects", key="subj_do_import"):
                    errors, ok_count = import_subjects_from_df(
                        engine, df_import, dry_run=False, actor=actor
                    )

                    if ok_count > 0:
                        st.success(f"✅ Imported {ok_count} subjects successfully!")
                        st.info("💡 **Tip:** Switch to another tab and back to Subjects Catalog to see the updated data, or refresh the page (F5).")

                    if errors:
                        st.warning(f"{len(errors)} error(s) during import.")
                        err_df = pd.DataFrame(errors)
                        st.dataframe(err_df, use_container_width=True)

                        st.download_button(
                            "Download Errors CSV",
                            err_df.to_csv(index=False),
                            file_name="subjects_import_errors.csv",
                            mime="text/csv",
                            key="subj_import_errors_dl",
                        )

                    # Clear caches so new subjects show up when user navigates
                    st.cache_data.clear()
                    # REMOVED: st.rerun() - let user stay on current tab

        except Exception as e:
            st.error(f"Failed to process CSV: {e}")

    # =========================
    # IMPORT SYLLABUS TEMPLATES (NEW SECTION)
    # =========================
    st.markdown("---")
    st.subheader("📥 Import Syllabus Templates")

    st.markdown(
        "Use this section to bulk-create syllabus templates. "
        "Download a CSV template, fill it, and upload it for dry run / import."
    )

    # Prepare template header
    template_header = ",".join(TEMPLATE_IMPORT_COLUMNS) + "\n"

    st.download_button(
        label="Download Template Import CSV Template",
        data=template_header,
        file_name="syllabus_template_import_template.csv",
        mime="text/csv",
        key="template_template_dl",
    )

    st.info(
        "**Template Import Format:**\n\n"
        "Each template can have multiple points. Use one row per point, but repeat "
        "the template-level fields (subject_code, version, name, etc.) on each row.\n\n"
        "**Example:** A template with 3 points needs 3 rows, all with the same "
        "subject_code and version, but different point_sequence and point_title."
    )

    # Example preview
    example_data = [
        {
            "Column": "subject_code",
            "Example": "CS101",
            "Description": "Must exist in subjects_catalog"
        },
        {
            "Column": "version",
            "Example": "v1.0",
            "Description": "Unique version identifier"
        },
        {
            "Column": "name",
            "Example": "Standard Syllabus 2024",
            "Description": "Template name"
        },
        {
            "Column": "description",
            "Example": "Core CS curriculum",
            "Description": "Optional description"
        },
        {
            "Column": "effective_from_ay",
            "Example": "2024-25",
            "Description": "Optional AY"
        },
        {
            "Column": "degree_code",
            "Example": "BTECH",
            "Description": "Optional scope"
        },
        {
            "Column": "point_sequence",
            "Example": "1",
            "Description": "Point order (1, 2, 3...)"
        },
        {
            "Column": "point_title",
            "Example": "Introduction to Programming",
            "Description": "Point title (required)"
        },
        {
            "Column": "point_description",
            "Example": "Basic concepts...",
            "Description": "Optional details"
        },
        {
            "Column": "point_hours_weight",
            "Example": "4",
            "Description": "Hours allocated"
        },
    ]
    
    st.markdown("**Column Reference:**")
    st.table(pd.DataFrame(example_data))

    templates_file = st.file_uploader(
        "Upload Templates CSV",
        type="csv",
        key="templates_import_csv",
    )

    if templates_file is not None:
        try:
            templates_file.seek(0)
            df_import = pd.read_csv(templates_file)
            
            # Show preview
            unique_templates = df_import.groupby(['subject_code', 'version']).size().reset_index(name='points')
            st.info(f"Loaded {len(df_import)} rows → {len(unique_templates)} unique template(s)")
            
            with st.expander("Preview: Unique Templates"):
                st.dataframe(unique_templates, use_container_width=True)

            col_a, col_b, col_c = st.columns(3)

            with col_a:
                allow_updates = st.checkbox(
                    "Allow Updates",
                    help="If enabled, existing templates will be replaced. If disabled, duplicates will be skipped.",
                    key="template_allow_updates"
                )

            with col_b:
                if st.button("🧪 Dry Run Import", key="template_dry_run"):
                    with st.spinner("Running dry run..."):
                        errors, ok_count = import_templates_from_df(
                            engine, df_import, dry_run=True, actor=actor, allow_update=allow_updates
                        )

                    if ok_count > 0:
                        st.success(f"✅ Dry run OK: {ok_count} template(s) would be created/updated.")

                    if errors:
                        st.warning(f"{len(errors)} issue(s) found in dry run.")
                        err_df = pd.DataFrame(errors)
                        st.dataframe(err_df, use_container_width=True)

                        st.download_button(
                            "Download Errors CSV",
                            err_df.to_csv(index=False),
                            file_name="templates_import_errors_dry_run.csv",
                            mime="text/csv",
                            key="template_dry_errors_dl",
                        )

            with col_c:
                if st.button("🚀 Import Templates", key="template_do_import"):
                    errors, ok_count = import_templates_from_df(
                        engine, df_import, dry_run=False, actor=actor, allow_update=allow_updates
                    )

                    if ok_count > 0:
                        st.success(f"✅ Imported {ok_count} template(s) successfully!")
                        st.info("💡 **Tip:** Switch to Template Manager tab to see the new templates.")

                    if errors:
                        st.warning(f"{len(errors)} error(s) during import.")
                        err_df = pd.DataFrame(errors)
                        st.dataframe(err_df, use_container_width=True)

                        st.download_button(
                            "Download Errors CSV",
                            err_df.to_csv(index=False),
                            file_name="templates_import_errors.csv",
                            mime="text/csv",
                            key="template_import_errors_dl",
                        )

                    # Clear caches
                    st.cache_data.clear()

        except Exception as e:
            st.error(f"Failed to process CSV: {e}")
