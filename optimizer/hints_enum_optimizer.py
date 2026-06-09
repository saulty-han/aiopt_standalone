"""
Hints Enumeration Optimizer

Extends BasicOptimizer with index hints enumeration strategy.
This is the original SmallModelOptimizer's enumeration logic extracted
into a subclass.
"""

from ai_logger import aiopt_logger
from optimizer.basic_optimizer import BasicOptimizer, CandidatePlan
from ai_exception import PlanCaptureError
import db_utils
import hints_generator
from ai_config import TrainingParameters


class HintsEnumOptimizer(BasicOptimizer):
    """
    Optimizer that enumerates candidate plans via index hints combinations.

    Extends BasicOptimizer by generating additional candidates through
    index hint enumeration on the representative SQL.
    """

    def get_optimizer_name(self) -> str:
        return "HintsEnumOptimizer"

    def _collect_additional_candidates(
        self,
        db: str,
        digest: str,
        sql_samples: list[str],
    ) -> list[CandidatePlan]:
        """
        Collect Enumerated candidate plans (Set P) via index hints combinations.

        Always performed, but hints_text source differs based on feature detection:
        - If hints extraction is supported: use extracted outline hints (canonical)
        - If hints extraction is NOT supported: use enumerated hints (from index combinations)
        """
        candidates = []
        representative_sql = sql_samples[0]
        supports_hints_extraction = self.context.feature_flags.supports_hints_extraction

        possible_keys = db_utils.get_possible_keys(self.context.training_controller, db, representative_sql)
        possible_keys = hints_generator.filter_temporary_tables(possible_keys)
        aiopt_logger.debug(f"[Candidates] Possible keys for enumeration: {possible_keys}")

        if not possible_keys:
            aiopt_logger.debug("[Candidates] No possible keys, skipping enumeration")
            return []

        index_combinations = hints_generator.generate_indexes(
            possible_keys, 20, TrainingParameters.index_hints_enumeration_limit,
            TrainingParameters.with_ignore_index_hints
        )
        aiopt_logger.debug(f"[Candidates] Generated {len(index_combinations)} index combinations")

        # Apply index hints to Rep SQL temporarily to extract canonical PlanID/Outline
        # combine_sql_with_indexes returns: (executed_sql, indexes_dict, hints_text)
        temp_sqls = hints_generator.combine_sql_with_indexes(
            representative_sql, index_combinations, self.context.outline_type
        )

        seen_pids = set()
        for (executed_sql, idx_dict, enumerated_hints) in temp_sqls:
            try:
                pid, extracted_outline = db_utils.get_plan_id_and_outline(
                    self.context.training_controller, executed_sql,
                    extract_outline=supports_hints_extraction
                )
            except PlanCaptureError as e:
                aiopt_logger.debug(f"[Candidates] Enum candidate skipped (plan capture failed): {e}")
                continue

            if pid and pid not in seen_pids:
                seen_pids.add(pid)
                # Decide hints_text source based on hints extraction support
                if supports_hints_extraction:
                    # Use extracted outline hints (canonical)
                    hints_text = extracted_outline
                else:
                    # Use enumerated hints (from index combinations)
                    hints_text = enumerated_hints

                candidates.append(CandidatePlan(
                    plan_id=pid,
                    hints_text=hints_text,
                    indexes_dict=idx_dict,
                    source_sql=executed_sql
                ))

        aiopt_logger.debug(f"[Candidates] Enumeration produced {len(candidates)} candidates")
        return candidates
