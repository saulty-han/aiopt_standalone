from typing import Type

from ai_logger import aiopt_logger
from optimizer.basic_optimizer import BasicOptimizer, OptimizationContext
from optimizer.hints_enum_optimizer import HintsEnumOptimizer
from optimizer.llm_optimizer import LLMOptimizer


class OptimizerFactory:
    """
    Factory for creating optimizer instances.
    Supports pluggable optimizers based on configuration.
    """

    _REGISTRY: dict[str, Type[BasicOptimizer]] = {
        "basic": BasicOptimizer,
        "small_model": HintsEnumOptimizer,  # backward compat
        "llm": LLMOptimizer,
    }

    @classmethod
    def create_optimizer(cls, name: str, context: OptimizationContext) -> BasicOptimizer:
        """
        Create an optimizer instance.

        Args:
            name: Optimizer name (e.g., "small_model", "basic", "llm")
            context: Optimization context

        Returns:
            Optimizer instance

        Raises:
            ValueError: If optimizer name is unknown
        """
        optimizer_cls = cls._REGISTRY.get(name)
        if not optimizer_cls:
            valid_names = list(cls._REGISTRY.keys())
            raise ValueError(f"Unknown optimizer type: '{name}'. Valid options: {valid_names}")

        aiopt_logger.info(f"[OptimizerFactory] Creating optimizer: {name}")
        return optimizer_cls(context)
