import re


def is_use_statement(sql_text):
    use_pattern = re.compile(
        r"^\s*(?:--\s+[^\n]*\n*|#[^\n]*\n*|/\*[\s\S]*?\*/\s*)*\s*USE\b",
        re.IGNORECASE | re.MULTILINE
    )
    return use_pattern.search(sql_text)


def has_order_by_clause(sql_text):
    order_by_pattern = re.compile(
        r"\bORDER\s+BY\b",
        re.IGNORECASE | re.MULTILINE
    )
    return order_by_pattern.search(sql_text)

