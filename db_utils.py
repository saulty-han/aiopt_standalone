from ai_logger import aiopt_logger
import enum
import json
from typing import List, Tuple, Optional, Dict
from sqlalchemy import text
from db_controller import DBController
from data_models import OutlineType
import re
from ai_exception import PlanCaptureError
class ExplainFormat(enum.Enum):
    JSON = 'json'
    JSON_V2 = 'json_v2'
    TRADITIONAL = 'traditional'


class ExplainFields:
    """
    Represents the field index of an EXPLAIN statement.
    """
    QUERYBLOCK_ID = 0
    SELECT_TYPE = 1
    TABLE = 2
    PARTITIONS = 3
    TYPE = 4
    POSSIBLE_KEYS = 5
    KEY = 6
    KEY_LEN = 7
    REF = 8
    ROWS = 9
    FILTERED = 10
    EXTRA = 11


def is_select_statement(sql_text):
    #########################################
    # Step 1
    #########################################
    # 先屏蔽包含 INSERT/DELETE/UPDATE/REPLACE/CREATE/DROP/ALTER 关键词的 SQL（不区分大小写）
    # 如果匹配到这些关键词，直接认为不是 SELECT
    dml_pattern = re.compile(r"\b(?:INSERT|DELETE|UPDATE|REPLACE|CREATE|DROP|ALTER)\b", re.IGNORECASE)
    if dml_pattern.search(sql_text):
        return False

    #########################################
    # Step 2
    #########################################
    # 优化后的正则表达式，强制要求 -- 后面有至少一个空格或控制字符
    # ^\s* # 匹配行首的任意空白字符
    # (?:                                  # 非捕获组，用于匹配不同类型的注释
    #    --\s+[^\n]*\n* # 匹配以 -- 开头的单行注释，强制要求 -- 后面有至少一个空格，直到行尾，并可选地匹配换行符
    #    |                                 # 或者
    #    #[^\n]*\n* # 匹配以 # 开头的单行注释，直到行尾，并可选地匹配换行符
    #    |                                 # 或者
    #    /\*[\s\S]*?\*/\s* # 匹配 /* ... */ 多行注释，[\s\S]*? 匹配所有字符（包括换行符）非贪婪模式
    # )* # 零次或多次匹配上述注释类型
    # \s* # 匹配注释后的任意空白字符
    # SELECT\b                             # 匹配 SELECT 关键字，\b 确保是整个单词匹配
    select_pattern = re.compile(
        r"\A\s*(?:--\s+[^\n]*\n*|#[^\n]*\n*|/\*[\s\S]*?\*/\s*)*\s*SELECT\b",
        re.IGNORECASE
    )
    return bool(select_pattern.search(sql_text))

def has_index_hints(sql_text):
    index_hints_pattern = re.compile(
        r"(USE\s+INDEX|FORCE\s+INDEX|IGNORE\s+INDEX)",
        re.IGNORECASE | re.MULTILINE
    )
    return index_hints_pattern.search(sql_text) is not None

def has_index_level_optimizer_hints(sql_text):
    # 匹配（1）/*+(任意符号)INDEX(任意符号)*/
    #    （2）/*+(任意符号)NO_INDEX(任意符号)*/
    index_level_hints_pattern = re.compile(
        r"/\*\+[^*]*\bINDEX\b[^*]*\*/|/\*\+[^*]*\bNO_INDEX\b[^*]*\*/",
        re.IGNORECASE | re.MULTILINE
    )
    return index_level_hints_pattern.search(sql_text) is not None

def contains_procedure_call(sql_text):
    # 匹配 CALL proc() ~~或 SELECT proc()~~
    patterns = [
        r"\bCALL\s+\w+\s*\(",  # CALL proc()
        # r"\bSELECT\s+[\w\.]+\s*\("  # SELECT func()
    ]
    return any(re.search(p, sql_text, re.IGNORECASE) for p in patterns)

def get_routine_list(controller: DBController):
    """
    Get a list of stored routines in the database.
    :param controller: the instance of DBController to execute the query
    :return: a list of routine names
    """
    query = text("SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES")
    result = controller.execute(query)
    return [row[0] for row in result.fetchall()]

def compute_statement_digest(controller: DBController, sql_text) -> str:
    """
    Compute the digest for the SQL statement.
    :param controller: the instance of DBController to execute the query
    :param sql_text: The SQL statement text
    :return: A string representing the digest of the SQL statement
    """
    query = text("SELECT statement_digest(:sql_text) AS digest")
    result = controller.execute(query, {"sql_text": sql_text})
    digest = result.scalar()
    assert digest is not None, "Digest computation returned None"
    return digest


def compute_statement_digest_text(controller: DBController, sql_text: str) -> str:
    """
    Compute the digest text (normalized/parameterized SQL template) for the SQL statement.

    Uses MySQL's statement_digest_text() function.
    The digest text is the human-readable form where literals are replaced with ? placeholders.

    :param controller: the instance of DBController to execute the query
    :param sql_text: The SQL statement text (with actual literal values)
    :return: A string representing the parameterized SQL template (literals replaced with ?)
    """
    query = text("SELECT statement_digest_text(:sql_text) AS digest_text")
    result = controller.execute(query, {"sql_text": sql_text})
    digest_text = result.scalar()
    assert digest_text is not None, "Digest text computation returned None"
    return digest_text


def execute_explain(controller: DBController, db: str, sql: str, fmt: ExplainFormat, mapping: bool = False, timeout_seconds=None):
    """
    Execute an EXPLAIN SQL statement for a given SQL query.
    :param controller: DBController object
    :param sql: The SQL query to explain
    :param fmt: The format of the EXPLAIN output (JSON, JSON_V2, or TRADITIONAL)
    :param timeout_seconds: Optional timeout in seconds for the EXPLAIN statement
    :return: The result of the EXPLAIN statement in the specified format.
    """
    if fmt == ExplainFormat.JSON_V2:
        raise ValueError("JSON_V2 format is not supported. Use JSON instead.")
    elif fmt == ExplainFormat.JSON:
        raise ValueError("JSON format is not supported. Use TRADITIONAL instead.")

    controller.use_db(db)
    kwargs = {}
    if timeout_seconds is not None:
        kwargs['timeout_seconds'] = timeout_seconds
    result = controller.execute(text(f"EXPLAIN format={fmt.value} {sql}"), **kwargs)

    if fmt == ExplainFormat.JSON and result:
        return result.scalar()
    elif mapping:
        return result.mappings().fetchall()
    else:
        return result.fetchall()
    

def set_explain_json_format_v2(controller: DBController):
    """
    Set the EXPLAIN format to JSON_V2 for the current session.
    :param controller: DBController object
    """
    controller.execute(text("SET SESSION explain_json_format_version = 2"))


def enable_planid_in_explain(controller: DBController):
    """
    Enable `txsql_planid_in_explain_enabled` for the current session.
    :param controller: DBController object
    """
    controller.execute(text("SET SESSION txsql_planid_in_explain_enabled = ON"))


def enable_planid_in_explain_and_set_json_format_v2(controller: DBController, enable_rows_examined: bool = False):
    """
    Enable `txsql_planid_in_explain_enabled`, set `explain_json_format_version` to 2,
    and optionally enable `txsql_rows_examined_in_explain_enabled`.

    :param controller: DBController object
    :param enable_rows_examined: If True, also enable rows_examined collection
    """
    enable_planid_in_explain(controller)
    set_explain_json_format_v2(controller)
    if enable_rows_examined:
        enable_rows_examined_in_explain(controller)


def enable_rows_examined_in_explain(controller: DBController):
    """
    Enable `txsql_rows_examined_in_explain_enabled` for the current session.
    :param controller: DBController object
    """
    controller.execute(text("SET SESSION txsql_rows_examined_in_explain_enabled = ON"))


def extract_rows_examined_from_json(analyze_json: str | None) -> int | None:
    """
    Extract rows_examined value from JSON V2 EXPLAIN ANALYZE result.

    Args:
        analyze_json: JSON string from EXPLAIN ANALYZE FORMAT=JSON (v2)

    Returns:
        rows_examined value as int, or None if not found or parsing fails
    """
    if not analyze_json:
        return None

    try:
        data = json.loads(analyze_json)
        if isinstance(data, dict) and "rows_examined" in data:
            value = data["rows_examined"]
            if isinstance(value, (int, float)):
                return int(value)
    except (json.JSONDecodeError, TypeError, AttributeError) as e:
        aiopt_logger.debug(f"[db_utils] Failed to extract rows_examined from JSON: {e}")

    return None


def get_possible_keys(controller: DBController, db: str, sql: str, with_empty_key: bool = False, explain_timeout_seconds=None):
    """
    Get possible keys (indexes) for tables in the SQL query.

    :param controller: DBController object
    :param db: Database name
    :param sql: The SQL query
    :param with_empty_key: If True, include entries with empty possible_keys (default: False)
    :param explain_timeout_seconds: Optional timeout in seconds for the EXPLAIN statement (default: None)
    :return: Dictionary mapping (queryblock_id, table) to lists of possible index names
    """
    possible_keys = dict()
    outputs = execute_explain(controller, db, sql, fmt=ExplainFormat.TRADITIONAL, timeout_seconds=explain_timeout_seconds)
    for output in outputs:
        if output[ExplainFields.QUERYBLOCK_ID] is None or output[ExplainFields.TABLE] is None:
            continue
        key_id = (output[ExplainFields.QUERYBLOCK_ID], output[ExplainFields.TABLE])
        if output[ExplainFields.POSSIBLE_KEYS] is not None:
            possible_keys[key_id] = [x for x in output[ExplainFields.POSSIBLE_KEYS].split(',')]
        else:
            # 如果 with_empty_key 为 True，添加空列表；否则跳过
            if with_empty_key:
                possible_keys[key_id] = []
    return possible_keys



def enable_outlinedata_in_explain(controller: DBController):
    """
    Enable `txsql_outlinedata_in_explain_enabled` for the current session.
    :param controller: DBController object
    """
    controller.execute(text("SET SESSION txsql_outlinedata_in_explain_enabled = ON"))


def get_plan_id_only(controller: DBController, sql: str, explain_timeout_seconds=None) -> str:
    """
    Execute EXPLAIN FORMAT=TREE and extract only PlanID (without enabling outline data).

    Use this for instances that don't support hints extraction.

    :param controller: DBController object
    :param sql: The SQL query to explain
    :param explain_timeout_seconds: Optional timeout in seconds for the EXPLAIN statement (default: None)
    :return: plan_id
    :raises: PlanCaptureError if extraction fails
    """
    enable_planid_in_explain(controller)
    # Note: Do NOT enable outlinedata_in_explain here

    kwargs = {}
    if explain_timeout_seconds is not None:
        kwargs['timeout_seconds'] = explain_timeout_seconds
    result = controller.execute(text(f"EXPLAIN FORMAT=TREE {sql}"), **kwargs)
    row = result.fetchone()
    if not row:
        raise PlanCaptureError("Empty result from EXPLAIN FORMAT=TREE")
    output = row[0]
    
    # Extract PlanID
    pid_match = re.search(r"QUERY_PLAN_ID:\s*(0x[0-9a-fA-F]+)", output)
    if not pid_match:
        raise PlanCaptureError(f"PlanID not found in EXPLAIN TREE output. Output: {output}")
    
    return pid_match.group(1)


def get_plan_id_and_outline(controller: DBController, sql: str, extract_outline: bool = True, explain_timeout_seconds=None) -> Tuple[str, str]:
    """
    Execute EXPLAIN FORMAT=TREE and extract PlanID and Outline Data hints.

    :param controller: DBController object
    :param sql: The SQL query to explain
    :param extract_outline: Whether to enable outline data extraction (default True)
    :param explain_timeout_seconds: Optional timeout in seconds for the EXPLAIN statement (default: None)
    :return: (plan_id, outline_hints) - outline_hints is empty string if extract_outline=False
    :raises: PlanCaptureError if extraction fails
    """
    enable_planid_in_explain(controller)
    if extract_outline:
        enable_outlinedata_in_explain(controller)

    # EXPLAIN FORMAT=TREE usually returns a single string containing the tree and notes
    kwargs = {}
    if explain_timeout_seconds is not None:
        kwargs['timeout_seconds'] = explain_timeout_seconds
    result = controller.execute(text(f"EXPLAIN FORMAT=TREE {sql}"), **kwargs)
    row = result.fetchone()
    if not row:
        raise PlanCaptureError("Empty result from EXPLAIN FORMAT=TREE")
    output = row[0] # The output is in the first column
    
    # Extract PlanID
    # Format: QUERY_PLAN_ID: 0x...
    pid_match = re.search(r"QUERY_PLAN_ID:\s*(0x[0-9a-fA-F]+)", output)
    if not pid_match:
        # Include output in exception for debugging
        raise PlanCaptureError(f"PlanID not found in EXPLAIN TREE output. Output: {output}")
    plan_id = pid_match.group(1)
    
    # Extract Outline Hints (only if requested)
    if extract_outline:
        # Look for the block containing BEGIN_OUTLINE_DATA ... END_OUTLINE_DATA
        # It is wrapped in /*+ ... */
        outline_match = re.search(r"/\*\+\s*BEGIN_OUTLINE_DATA.*END_OUTLINE_DATA\s*\*/", output, re.DOTALL)
        if not outline_match:
             raise PlanCaptureError(f"Outline Data not found in EXPLAIN TREE output. Output: {output}")
        outline_hints = outline_match.group(0).strip()
    else:
        outline_hints = ""
    
    return plan_id, outline_hints


def get_all_tables_indexes_info(controller: DBController, table_names: Optional[List[str]] = None) -> Dict:
    """
    Get index information for specified tables.
    
    :param controller: DBController object
    :param table_names: List of table names, or None to get all tables
    :return: Nested dictionary: {table_name: {index_name: {columns: [...], unique: ...}}}
    """
    # Get current database
    db_result = controller.execute(text("SELECT DATABASE()"))
    database_name = db_result.scalar()

    if not database_name:
        aiopt_logger.warning("No database selected")
        return {}

    # Build SQL query
    if table_names:
        table_list = list(table_names)
        table_names_str = ", ".join([f"'{table}'" for table in table_list])
        table_filter = f"AND TABLE_NAME IN ({table_names_str})"
    else:
        table_filter = ""

    sql = f"""
    SELECT
        TABLE_NAME,
        INDEX_NAME,
        COLUMN_NAME,
        NON_UNIQUE,
        EXPRESSION
    FROM information_schema.statistics
    WHERE TABLE_SCHEMA = '{database_name}' {table_filter}
    ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
    """

    results = controller.execute(text(sql))
    rows = results.fetchall()

    if not rows:
        return {}

    # Organize index information
    all_indexes_info = {}

    for row in rows:
        table_name = row[0]      # TABLE_NAME
        index_name = row[1]      # INDEX_NAME
        column_name = row[2]     # COLUMN_NAME (may be None for expression indexes)
        non_unique = row[3]      # NON_UNIQUE (0=unique, 1=non-unique)
        expression = row[4]      # EXPRESSION (functional index expression, or None)

        # Initialize table dict
        if table_name not in all_indexes_info:
            all_indexes_info[table_name] = {}

        # Initialize index dict
        if index_name not in all_indexes_info[table_name]:
            all_indexes_info[table_name][index_name] = {
                "unique": (non_unique == 0),
            }

        index_entry = all_indexes_info[table_name][index_name]

        # Add column name (skip if None)
        if column_name is not None:
            index_entry.setdefault("columns", []).append(column_name)

        # Add expression if present
        if expression is not None:
            index_entry.setdefault("expressions", []).append(expression)

    return all_indexes_info
    