"""
mcts - MCTS-based SQL Optimizer

A clean, type-safe implementation of Monte Carlo Tree Search for SQL query optimization.
"""
import logging

# Use the same logger name ("mcts") as ai_logger.mcts_logger.
# When ai_logger is initialized (by the optimizer), file handlers are
# automatically attached to this logger via logging.getLogger("mcts").
logger = logging.getLogger("mcts")

__all__ = ["logger"]
