"""
mcts.utils.hint_utils - Utilities for parsing and deduplicating MySQL optimizer hints.

All functions are pure and operate on lists of hint strings.
"""
from __future__ import annotations

import re
from typing import List, Optional, Tuple, Set


# ---------------------------------------------------------------------------
# Extract hints from LLM text
# ---------------------------------------------------------------------------

_HINT_TYPES = ["INDEX", "JOIN_PREFIX", "JOIN_SUFFIX", "NO_INDEX", "SET_VAR"]
_HINT_PATTERN = re.compile(
    rf'\b(?:{"|".join(_HINT_TYPES)})\((?:[^()]*|\([^()]*\))*\)'
)
_HINT_BLOCK_PATTERN = re.compile(r'/\*\+\s*(.*?)\s*\*/', re.DOTALL)

_ANSWER_PATTERN = re.compile(r'<answer>(.*?)</answer>', re.DOTALL)


def extract_hints_from_text(text: str) -> List[str]:
    """Extract all MySQL optimizer hints (INDEX, JOIN_PREFIX, etc.) from text.

    Looks inside ``/*+ ... */`` blocks and extracts individual hint expressions.
    Returns deduplicated hints preserving first-occurrence order.
    """
    if not text:
        return []

    blocks = _HINT_BLOCK_PATTERN.findall(text)
    if not blocks:
        return []

    seen: Set[str] = set()
    result: List[str] = []
    for block in blocks:
        for match in _HINT_PATTERN.findall(block):
            h = match.strip()
            if h and h not in seen:
                seen.add(h)
                result.append(h)
    return result


def extract_final_answer(text: str) -> Optional[str]:
    """Extract text between <answer> and </answer> tags.

    Returns None if no match found.
    """
    m = _ANSWER_PATTERN.search(text)
    return m.group(1).strip() if m else None


# ---------------------------------------------------------------------------
# Hint deduplication utilities
# ---------------------------------------------------------------------------

def dedupe_join_hints(hints: List[str]) -> List[str]:
    """Keep one JOIN_PREFIX and one JOIN_SUFFIX; last occurrence wins.

    When a duplicate is encountered, the later value replaces the earlier one
    at the earlier slot. This lets new hints override ancestor hints (e.g.
    A2 "modify join order" can supersede a JOIN_PREFIX already in the path).
    """
    if not hints:
        return hints

    out: List[str] = []
    prefix_idx: Optional[int] = None
    suffix_idx: Optional[int] = None

    for h in hints:
        upper = (h or "").strip().upper()
        if upper.startswith("JOIN_PREFIX"):
            if prefix_idx is None:
                prefix_idx = len(out)
                out.append(h)
            else:
                out[prefix_idx] = h
        elif upper.startswith("JOIN_SUFFIX"):
            if suffix_idx is None:
                suffix_idx = len(out)
                out.append(h)
            else:
                out[suffix_idx] = h
        else:
            out.append(h)
    return out


_INDEX_TABLE_PATTERN = re.compile(
    r"^(?P<kind>NO_INDEX|INDEX)\s*\(\s*(?P<body>.*)\s*\)\s*$",
    re.IGNORECASE,
)


def _extract_index_table(hint: str) -> Optional[Tuple[str, str]]:
    """Parse INDEX/NO_INDEX hint and return (kind, table) if matched."""
    if not hint:
        return None
    m = _INDEX_TABLE_PATTERN.match(hint.strip())
    if not m:
        return None
    body = (m.group("body") or "").strip()
    if not body:
        return None
    table = re.split(r"[,\s]+", body, maxsplit=1)[0].strip()
    # strip quotes
    if len(table) >= 2 and table[0] in ('`', "'", '"') and table[-1] == table[0]:
        table = table[1:-1]
    return m.group("kind").upper(), table


def dedupe_index_hints_by_table(hints: List[str]) -> List[str]:
    """For the same table, keep one INDEX/NO_INDEX hint; last occurrence wins.

    A later INDEX/NO_INDEX on the same table overwrites the earlier one at
    the earlier slot, so new hints can override ancestor hints while
    preserving the ancestor ordering.
    """
    if not hints:
        return hints

    out: List[str] = []
    table_idx: dict = {}

    for h in hints:
        parsed = _extract_index_table(h)
        if parsed is not None:
            _, table = parsed
            key = table.lower()
            if key in table_idx:
                out[table_idx[key]] = h
                continue
            table_idx[key] = len(out)
        out.append(h)
    return out


_SETVAR_PATTERN = re.compile(r"^\s*SET_VAR\s*\(\s*(?P<body>.*)\s*\)\s*$", re.IGNORECASE)
_OPT_SWITCH_PATTERN = re.compile(
    r"(?:^|,)\s*optimizer_switch\s*=\s*'(?P<opts>[^']*)'\s*(?:,|$)",
    re.IGNORECASE,
)


def merge_set_var_hints(hints: List[str]) -> List[str]:
    """Merge multiple SET_VAR(optimizer_switch='...') hints into one.

    Last occurrence of each key wins. Non-optimizer_switch SET_VARs are kept as-is.
    This lets new hints override ancestor hints (e.g. an A-step can flip a
    previously-set optimizer_switch option).
    """
    if not hints:
        return hints

    first_setvar_idx: Optional[int] = None
    # Preserve insertion order for first-seen keys, but update value on later hits.
    merged_order: List[str] = []  # keys in insertion order (lowercase)
    merged_map: dict = {}          # key_lower -> (original_key, value)
    keep: List[str] = []

    for h in hints:
        m = _SETVAR_PATTERN.match(h or "")
        if not m:
            keep.append(h)
            continue

        body = m.group("body")
        opt_matches = list(_OPT_SWITCH_PATTERN.finditer(body))
        if not opt_matches:
            keep.append(h)
            continue

        if first_setvar_idx is None:
            first_setvar_idx = len(keep)

        for om in opt_matches:
            opts = (om.group("opts") or "").strip()
            if not opts:
                continue
            for part in (p.strip() for p in opts.split(",")):
                if not part or "=" not in part:
                    continue
                k, v = part.split("=", 1)
                k, v = k.strip(), v.strip()
                if not k:
                    continue
                key_lower = k.lower()
                if key_lower not in merged_map:
                    merged_order.append(key_lower)
                # last value wins
                merged_map[key_lower] = (k, v)

    if first_setvar_idx is None or not merged_order:
        return keep

    merged_pairs = [merged_map[key_lower] for key_lower in merged_order]
    merged_opts = ", ".join(f"{k}={v}" for k, v in merged_pairs)
    merged_hint = f"SET_VAR(optimizer_switch='{merged_opts}')"
    keep.insert(first_setvar_idx, merged_hint)
    return keep


def deduplicate_hints(hints: List[str]) -> List[str]:
    """Apply all deduplication rules in the correct order."""
    return dedupe_index_hints_by_table(merge_set_var_hints(dedupe_join_hints(hints)))


def build_sql_with_hints(query: str, hints: List[str]) -> str:
    """Insert hints after every SELECT keyword in the query.

    Returns the query with ``/*+ hint1 hint2 ... */`` after each SELECT.
    """
    if not hints:
        return query
    hint_str = " ".join(hints)
    pattern = re.compile(r'\bSELECT\b', re.IGNORECASE)
    return pattern.sub(lambda m: m.group(0) + f" /*+ {hint_str} */", query)
