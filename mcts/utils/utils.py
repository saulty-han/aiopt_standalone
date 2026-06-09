"""
mcts.utils.utils - General-purpose utility functions.
"""
from __future__ import annotations

import math
from typing import Optional


def round6(v):
    """Round a float to 6 significant digits. Pass through None."""
    if v is None:
        return None
    return float(f"{v:.6g}")


def compute_reward(baseline_time: Optional[float], current_time: Optional[float]) -> Optional[float]:
    """Compute tanh(ln(baseline_time / current_time)).

    Returns None if inputs are invalid.
    """
    if baseline_time is None or current_time is None:
        return None
    if baseline_time <= 0 or current_time <= 0:
        return None
    try:
        return math.tanh(math.log(baseline_time / current_time))
    except (ValueError, OverflowError):
        return None
