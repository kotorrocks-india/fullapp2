# screens/class_in_charge/__init__.py
"""
Class-in-Charge Management Module (Slide 17)

This module provides comprehensive management of faculty assignments as class-in-charge
across academic scopes (AY, Degree, Program, Branch, Year, Term, Division).

Features:
- Faculty-only CIC assignments per scope
- Warning system for admin positions (user can proceed)
- Bulk assignment for all divisions
- Complete approval workflows
- Import/Export functionality
- Comprehensive audit trail
- Integration with student schema's division_master table

Usage:
    from screens.class_in_charge import main
    
    # In your Streamlit app routing:
    if page == "Class-in-Charge":
        main.render()
"""

from .main import render

__all__ = ['render']
__version__ = '1.0.0'
