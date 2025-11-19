"""
Import/Export Tab - Download and upload offerings data
"""

import streamlit as st
import pandas as pd
import io
from ..helpers import exec_query, rows_to_dicts
from ..constants import OFFERINGS_EXPORT_COLUMNS, OFFERINGS_IMPORT_TEMPLATE_COLUMNS
from core.forms import success
# --- NEW IMPORT ---
from ..imports_offerings import import_offerings_from_df


def export_offerings(engine, fmt: str = "csv"):
    """Export all offerings."""
    with engine.begin() as conn:
        rows = exec_query(conn, """
            SELECT o.*, sc.subject_name
            FROM subject_offerings o
            LEFT JOIN subjects_catalog sc 
                ON sc.subject_code = o.subject_code 
                AND sc.degree_code = o.degree_code
            ORDER BY o.ay_label DESC, o.degree_code, o.year, o.term, o.subject_code
        """).fetchall()
    
    df = pd.DataFrame(rows_to_dicts(rows))
    
    if not df.empty:
        ordered_cols = [c for c in OFFERINGS_EXPORT_COLUMNS if c in df.columns]
        ordered_cols += [c for c in df.columns if c not in OFFERINGS_EXPORT_COLUMNS]
        df = df[ordered_cols]
    
    if fmt == "excel":
        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        return "subject_offerings_export.xlsx", buf.getvalue()
    
    out = io.StringIO()
    df.to_csv(out, index=False)
    return "subject_offerings_export.csv", out.getvalue().encode("utf-8")


def render(engine, actor: str, CAN_EDIT: bool):
    """Render the Import/Export tab."""
    st.subheader("📥📤 Import / Export")
    
    col1, col2 = st.columns(2)
    
    # --- EXPORT ---
    with col1:
        st.markdown("### Export Offerings")
        
        if st.button("Export CSV", key="exp_csv"):
            name, data = export_offerings(engine, fmt="csv")
            st.download_button(
                "Download CSV",
                data,
                file_name=name,
                mime="text/csv",
                key="exp_csv_dl"
            )
        
        if st.button("Export Excel", key="exp_xlsx"):
            name, data = export_offerings(engine, fmt="excel")
            st.download_button(
                "Download Excel",
                data,
                file_name=name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="exp_xlsx_dl"
            )
    
    # --- IMPORT ---
    with col2:
        st.markdown("### Import Template")
        
        template_header = ",".join(OFFERINGS_IMPORT_TEMPLATE_COLUMNS) + "\n"
        
        st.download_button(
            label="Download Import Template CSV",
            data=template_header,
            file_name="subject_offerings_import_template.csv",
            mime="text/csv",
            key="template_dl"
        )
    
    st.markdown("---")
    
    # --- IMPORT SECTION ---
    if CAN_EDIT:
        st.subheader("📥 Import Offerings")
        
        st.info(
            "**Import Format:**\n\n"
            "Each row represents one offering. Required fields: "
            "degree_code, ay_label, year, term, subject_code, subject_type.\n\n"
            "The system will auto-populate credits, marks, and other fields from the catalog."
        )
        
        uploaded_file = st.file_uploader(
            "Upload Offerings CSV",
            type="csv",
            key="offerings_import_csv"
        )
        
        if uploaded_file is not None:
            try:
                uploaded_file.seek(0)
                df_import = pd.read_csv(uploaded_file)
                st.info(f"Loaded {len(df_import)} rows from CSV")
                
                # Preview
                st.markdown("**Preview (first 10 rows):**")
                st.dataframe(df_import.head(10), use_container_width=True)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("🧪 Dry Run Import", key="dry_run"):
                        # --- MODIFIED BLOCK ---
                        with st.spinner("Running dry run..."):
                            errors, ok_count = import_offerings_from_df(
                                engine, df_import, dry_run=True, actor=actor
                            )
                        
                        if ok_count > 0:
                            st.success(f"✅ Dry run OK: {ok_count} offerings would be created.")
                        
                        if errors:
                            st.warning(f"{len(errors)} issue(s) found in dry run.")
                            err_df = pd.DataFrame(errors)
                            st.dataframe(err_df, use_container_width=True)
                            
                            st.download_button(
                                "Download Errors CSV",
                                err_df.to_csv(index=False),
                                file_name="offerings_import_errors_dry_run.csv",
                                mime="text/csv",
                                key="off_dry_errors_dl",
                            )
                        # --- END MODIFIED BLOCK ---
                
                with col2:
                    if st.button("🚀 Import Offerings", key="do_import"):
                        # --- MODIFIED BLOCK ---
                        with st.spinner("Importing... this may take a moment."):
                            errors, ok_count = import_offerings_from_df(
                                engine, df_import, dry_run=False, actor=actor
                            )

                        if ok_count > 0:
                            st.success(f"✅ Imported {ok_count} offerings successfully!")
                            st.info("💡 Tip: Refresh the 'Offerings' tab to see the new data.")

                        if errors:
                            st.warning(f"{len(errors)} error(s) during import.")
                            err_df = pd.DataFrame(errors)
                            st.dataframe(err_df, use_container_width=True)

                            st.download_button(
                                "Download Errors CSV",
                                err_df.to_csv(index=False),
                                file_name="offerings_import_errors.csv",
                                mime="text/csv",
                                key="off_import_errors_dl",
                            )

                        # Clear caches so new data shows up
                        st.cache_data.clear()
                        # --- END MODIFIED BLOCK ---
                        
            except Exception as e:
                st.error(f"Failed to process CSV: {e}")
