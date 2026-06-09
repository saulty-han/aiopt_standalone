from ai_logger import aiopt_logger
import random
import re
from data_models import OutlineType
import itertools


def is_temporary_table(table_name: str):
    TEMPOREARY_TABLE_PREFIX_LIST = ["<temporary>", "<materialized_subquery>", "<subquery", "<derived", "/tmp"]
    TEMPOREARY_TABLE_SUFFIX_LIST = ["temporary>"]
    TEMPOREARY_TABLE_SUBSTR = ["/tmp"]
    # 如果 table_name 以某个字符串开头，则认为它是一个临时表
    for temp_table in TEMPOREARY_TABLE_PREFIX_LIST:
        if table_name.startswith(temp_table):
            return True
    for temp_table in TEMPOREARY_TABLE_SUFFIX_LIST:
        if table_name.endswith(temp_table):
            return True
    for temp_table in TEMPOREARY_TABLE_SUBSTR:
        if temp_table in table_name:
            return True
    return False


def filter_temporary_tables(possible_keys):
    """
    过滤掉可能的键中包含临时表的条目
    :param possible_keys: 每个 (query_block_number, table_name) 对应的索引列表
    :return: 过滤后的可能键
    """
    return {key: indexes for key, indexes in possible_keys.items() if not is_temporary_table(key[1])}


def estimate_enumerate_space(possible_keys):
    """
    估计索引提示的空间复杂度
    :param possible_keys: 每个 (query_block_number, table_name) 对应的索引列表
    :return: 空间复杂度
    """
    space = 1
    for _, indexes in possible_keys.items():
        space *= len(indexes) + 1  # +1 是因为可以选择不使用任何索引
    return space


def enumerate_index_combinations(possible_keys, with_ignore_index_hints):
    """
    枚举所有可能的索引组合
    :param possible_keys: 每个 (query_block_number, table_name) 对应的索引列表
    :param with_ignore_index_hints: 是否包含 NO_INDEX/IGNORE INDEX 提示
    :return: 所有可能的索引组合列表
    """
    # 为每个 (query_block_number, table_name) 创建一个包含其所有索引和 None 的列表
    # None 代表不选择任何索引
    options_for_each_key = {}
    for key, indexes in possible_keys.items():
        options_for_each_key[key] = indexes.copy()
        if with_ignore_index_hints:
            options_for_each_key[key] += [None]

    # 获取所有 key 的列表，以便保持顺序
    keys_order = list(options_for_each_key.keys())

    # 获取每个 key 对应的选项列表
    all_options_lists = [options_for_each_key[key] for key in keys_order]

    # 使用 itertools.product 生成所有组合
    all_combinations_values = itertools.product(*all_options_lists)

    # 将组合的值重新映射回字典形式
    result_combinations = []
    for combination_values in all_combinations_values:
        current_combination = {}
        for i, key in enumerate(keys_order):
            current_combination[key] = combination_values[i]
        result_combinations.append(current_combination)

    return result_combinations


def generate_indexes(possible_keys, limit_to, max_enumerate_space, with_ignore_index_hints):
    """
    生成索引提示
    :param possible_keys: 每个 (query_block_number, table_name) 对应的索引列表
    :param limit_to: 生成的 SQL 数量限制
    :param max_enumerate_space: 枚举空间大小限制
    :param with_ignore_index_hints: 是否包含 NO_INDEX/IGNORE INDEX 提示
    :return: 索引提示列表
    """
    estimated_space = estimate_enumerate_space(possible_keys)
    if estimated_space > max_enumerate_space:
        aiopt_logger.warning(f"Enumerate space {estimated_space} exceeds limit {max_enumerate_space}, skipping enumeration.")
        return []
    
    all_index_combinations = enumerate_index_combinations(possible_keys, with_ignore_index_hints)
    if len(all_index_combinations) > limit_to:
        limited_combinations = random.sample(all_index_combinations, limit_to)
        aiopt_logger.debug(f"Sample {len(limited_combinations)} index combinations out of {len(all_index_combinations)}")
    else:
        limited_combinations = all_index_combinations
        aiopt_logger.debug(f"Using all {len(all_index_combinations)} enumerated index combinations")
    aiopt_logger.debug(f"Index combinations: {limited_combinations}")

    return limited_combinations


def insert_optimizer_hints(sql_text, hint_str):
    # 使用 re.search 在整个字符串中寻找 'select' 关键字
    # (?i) 忽略大小写
    # 第一个捕获组 (select) 匹配 'select'
    # 第二个捕获组 (\s*) 匹配其后的任意空白符
    match = re.search(r'(?i)(select)(\s*)', sql_text)
    if match:
        # 获取匹配到的 'select' 关键字
        select_keyword = match.group(1)
        # 获取匹配到的 'select' 之后的空白符
        whitespace = match.group(2)
        # 拆分 SQL 语句
        start_pos, end_pos = match.span()
        before_select = sql_text[:start_pos]
        after_whitespace = sql_text[end_pos:]

        # 重新组合：'select' + 'hint' + 'whitespace' + 'rest'
        return f"{before_select}{select_keyword} /*+ {hint_str} */{whitespace}{after_whitespace}"
    else:
        return sql_text


def insert_raw_hints(sql_text, raw_hints):
    """
    Insert a raw hint string (which already includes /*+ ... */) into the SQL.
    :param sql_text: The original SQL
    :param raw_hints: The hint string including delimiters
    """
    # 使用 re.search 在整个字符串中寻找 'select' 关键字
    match = re.search(r'(?i)(select)(\s*)', sql_text)
    if match:
        select_keyword = match.group(1)
        whitespace = match.group(2)
        start_pos, end_pos = match.span()
        before_select = sql_text[:start_pos]
        after_whitespace = sql_text[end_pos:]
        
        # 插入 raw_hints (假设它已经格式化好了)
        # 注意：这里我们强制加一个空格在 hints 前后以防万一
        return f"{before_select}{select_keyword} {raw_hints}{whitespace}{after_whitespace}"
    else:
        return sql_text


def insert_multiple_raw_hints(sql_text, raw_hints):
    """
    Insert a raw hint string (which already includes /*+ ... */) after ALL SELECT keywords in the SQL.
    :param sql_text: The original SQL
    :param raw_hints: The hint string including delimiters
    """
    # 使用 re.sub 替换所有的 'select' 关键字，在每个后面插入 raw_hints
    def _replace(match):
        select_keyword = match.group(1)
        whitespace = match.group(2)
        return f"{select_keyword} {raw_hints}{whitespace}"

    return re.sub(r'(?i)(select)(\s*)', _replace, sql_text)


def combine_sql_with_optimizer_hints(sql, index_combinations):
    """
    Combine SQL with optimizer hints based on the provided combination.
    For statement outline, use optimizer hints syntax (/*+ INDEX(...) */, /*+ NO_INDEX(...) */).
    Returns list of tuples: (sql_with_hints, indexes_dict, hints_text)
    """
    sql_list_with_hints = []
    for indexes_dict in index_combinations:
        hints = []
        # todo: inject hints according to qb_number and table
        for (qb_number, table), index in indexes_dict.items():
            if is_temporary_table(table):
                continue
            if index is None:
                hints.append(f"NO_INDEX({table})")
            else:
                hints.append(f"INDEX({table} {index})")
        hint_str = " ".join(hints)
        hints_text = f"/*+ {hint_str} */" if hint_str else None
        sql_list_with_hints.append((insert_optimizer_hints(sql, hint_str), indexes_dict, hints_text))
    return sql_list_with_hints


def combine_sql_with_indexes(sql, index_combinations, outline_type: OutlineType):
    """
    Combine SQL with index hints based on the provided index combination and outline type.
    :param sql: The original SQL query.
    :param index_combinations: A list of index combinations to apply.
    :param outline_type: The type of outline to use (STATEMENT_OUTLINE or SPM).
    :return: List of tuples (sql_with_hints, indexes_dict, hints_text).
    """
    if outline_type == OutlineType.STATEMENT_OUTLINE or outline_type == OutlineType.SPM:
        # SPM and STATEMENT_OUTLINE both use optimizer hints syntax
        return combine_sql_with_optimizer_hints(sql, index_combinations)
    else:
        raise ValueError("Unsupported outline type")
