# screens/outcomes/__init__.py
"""
Program Outcomes (PEOs, POs, PSOs) Module

This module manages educational objectives and outcomes at degree/program/branch scope.

Main components:
- models: Data models and enums
- helpers: Database queries and utility functions
- manager: OutcomesManager for complex operations
- page: Streamlit UI (main entry point)

Usage:
    From Streamlit navigation:
        PAGES["Outcomes"] = "screens/outcomes/page.py"
    
    Programmatic access:
        from screens.outcomes.manager import OutcomesManager
        from screens.outcomes.models import OutcomeSet, OutcomeItem
"""

from .models import (
    OutcomeSet,
    OutcomeItem,
    ScopeLevel,
    SetType,
    Status,
    BloomLevel,
    SET_TYPE_LABELS,
)

from .manager import OutcomesManager

# Re-export main function for direct page access
from .page import main

__all__ = [
    # Data models
    'OutcomeSet',
    'OutcomeItem',
    
    # Enums
    'ScopeLevel',
    'SetType',
    'Status',
    'BloomLevel',
    
    # Constants
    'SET_TYPE_LABELS',
    
    # Manager
    'OutcomesManager',
    
    # Main function
    'main',
]

__version__ = '1.0.0'
