"""
Export functions for subjects, offerings, and templates
"""

from typing import Tuple
import io
import pandas as pd
from sqlalchemy import text as sa_text
from screens.subjects_syllabus.helpers import exec_query, rows_to_dicts
from screens.subjects_syllabus.constants import (
    SUBJECT_CATALOG_EXPORT_COLUMNS,
    SUBJECTS_ALL_YEARS_EXPORT_COLUMNS
)


def export_subjects(engine, fmt: str = "csv") -> Tuple[str, bytes]:
    """Export subjects catalog in the 'subject_catalog_full' format."""
    with engine.begin() as conn:
        rows = exec_query(conn, """
            SELECT * FROM subjects_catalog ORDER BY degree_code, subject_code
        """).fetchall()

    df = pd.DataFrame(rows_to_dicts(rows))

    if not df.empty:
        ordered_cols = [
            c for c in SUBJECT_CATALOG_EXPORT_COLUMNS if c in df.columns
        ] + [c for c in df.columns if c not in SUBJECT_CATALOG_EXPORT_COLUMNS]
        df = df[ordered_cols]

    if fmt == "excel":
        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        return "subject_catalog_full.xlsx", buf.getvalue()

    out = io.StringIO()
    df.to_csv(out, index=False)
    return "subject_catalog_full.csv", out.getvalue().encode("utf-8")


def export_subject_offerings(engine, fmt: str = "csv") -> Tuple[str, bytes]:
    """
    Export subject offerings across all academic years,
    aligned with subjects_all_years_export_columns.csv.
    """
    with engine.begin() as conn:
        rows = exec_query(conn, """
            SELECT
                so.id AS offering_id,
                so.degree_code,
                so.program_code,
                so.branch_code,
                so.curriculum_group_code,
                so.ay_label,
                so.year,
                so.term,
                so.subject_code,
                sc.subject_name,
                sc.subject_type,
                sc.credits_total,
                sc.L,
                sc.T,
                sc.P,
                sc.S,
                so.status,
                so.active,
                so.instructor_email,
                so.syllabus_template_id,
                so.syllabus_customized,
                so.created_at,
                so.updated_at
            FROM subject_offerings so
            LEFT JOIN subjects_catalog sc
                ON sc.subject_code = so.subject_code
                AND sc.degree_code = so.degree_code
                AND (sc.program_code = so.program_code 
                     OR (sc.program_code IS NULL AND so.program_code IS NULL))
                AND (sc.branch_code = so.branch_code 
                     OR (sc.branch_code IS NULL AND so.branch_code IS NULL))
            ORDER BY so.degree_code, so.ay_label, so.year, so.term, so.subject_code
        """).fetchall()

    df = pd.DataFrame(rows_to_dicts(rows))

    if not df.empty:
        ordered_cols = [
            c for c in SUBJECTS_ALL_YEARS_EXPORT_COLUMNS if c in df.columns
        ] + [c for c in df.columns if c not in SUBJECTS_ALL_YEARS_EXPORT_COLUMNS]
        df = df[ordered_cols]

    if fmt == "excel":
        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        return "subjects_all_years_export.xlsx", buf.getvalue()

    out = io.StringIO()
    df.to_csv(out, index=False)
    return "subjects_all_years_export.csv", out.getvalue().encode("utf-8")


def export_templates(engine, subject_code: str = None,
                    fmt: str = "csv") -> Tuple[str, bytes]:
    """Export syllabus templates."""
    with engine.begin() as conn:
        query = """
            SELECT t.*, tp.sequence, tp.title, tp.description,
                   tp.tags, tp.resources, tp.hours_weight
            FROM syllabus_templates t
            LEFT JOIN syllabus_template_points tp ON tp.template_id = t.id
        """
        params = {}

        if subject_code:
            query += " WHERE t.subject_code = :sc"
            params["sc"] = subject_code

        query += " ORDER BY t.code, tp.sequence"

        rows = exec_query(conn, query, params).fetchall()

    df = pd.DataFrame(rows_to_dicts(rows))

    if fmt == "excel":
        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        return "templates_export.xlsx", buf.getvalue()

    out = io.StringIO()
    df.to_csv(out, index=False)
    return "templates_export.csv", out.getvalue().encode("utf-8")
