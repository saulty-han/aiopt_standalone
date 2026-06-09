"""
Dynamic Feature Detector Module

Detects database instance capabilities by checking system variable existence.
"""

from sqlalchemy import text
from ai_logger import aiopt_logger
from data_models import FeatureFlags


def _check_variable_exists(db_controller, var_name: str) -> bool:
    """Helper to check if a global system variable exists."""
    result = db_controller.execute(
        text(f"SHOW GLOBAL VARIABLES LIKE '{var_name}'")
    )
    exists = result.rowcount > 0
    status = "exists" if exists else "missing"
    aiopt_logger.debug(f"[FeatureDetector] Variable '{var_name}' {status}")
    return exists


def is_hints_extraction_supported(db_controller) -> bool:
    """Check if hints extraction (Outline Data in EXPLAIN) is supported."""
    return _check_variable_exists(db_controller, 'txsql_outlinedata_in_explain_enabled')


def is_spm_supported(db_controller) -> bool:
    """Check if SQL Plan Management (SPM) is supported."""
    return _check_variable_exists(db_controller, 'txsql_spm_enabled')


def is_statement_outline_supported(db_controller) -> bool:
    """Check if Statement Outline is supported."""
    return _check_variable_exists(db_controller, 'txsql_ai_rules_enabled')


def is_rows_examined_supported(db_controller) -> bool:
    """Check if rows_examined in EXPLAIN is supported."""
    return _check_variable_exists(db_controller, 'txsql_rows_examined_in_explain_enabled')


def detect_features(db_controller) -> FeatureFlags:
    """
    Detect database instance capabilities.
    Aggregates results from individual feature detection functions.
    
    Args:
        db_controller: Database connection controller
    
    Returns:
        FeatureFlags object with detected capabilities
    """
    flags = FeatureFlags()

    flags.supports_hints_extraction = is_hints_extraction_supported(db_controller)
    flags.supports_spm = is_spm_supported(db_controller)
    flags.supports_statement_outline = is_statement_outline_supported(db_controller)
    flags.supports_rows_examined = is_rows_examined_supported(db_controller)

    aiopt_logger.info(f"[FeatureDetector] Final detection result: {flags}")
    return flags
