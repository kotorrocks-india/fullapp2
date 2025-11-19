# screens/subject_cos_rubrics/__init__.py
"""
Subject Course Outcomes and Rubrics Management Module

This module provides comprehensive management of:
- Subject Catalog per degree cohorts per AY per term
- Course Outcomes (COs) for published subjects
- Assessment Rubrics for subjects

All operations work on published subject offerings.
"""

from .main import render_subject_cos_rubrics_page

__all__ = ['render_subject_cos_rubrics_page']
