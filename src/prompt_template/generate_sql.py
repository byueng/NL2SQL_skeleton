from runner.enum_aggretion import Task

def generate_sql(task: Task, schema: str):
    template = f"""
        You are a SQLite expert. You need to read and understand the following database schema description, as well as the evidence
        that may be used, and use your SQLite knowledge to generate SQL statements to answer user questions.
        1) Dialect: Use SQLite3 syntax only.
        2) Quoting:
        - Use double quotes (") for table/column identifiers, especially if they contain spaces or parentheses.
        - Use single quotes (') only for string literals.
        3) Numeric casting:
        - If a column stores numbers as text, CAST it to REAL before arithmetic: CAST(column AS REAL).
        4) Division:
        - Ensure floating-point division by multiplying with 1.0 or casting operands to REAL.
        - Avoid division by zero with NULLIF(denominator, 0) or by filtering WHERE denominator > 0.
        5) Aggregation & aliases:
        - Use clear AS aliases, e.g., AS highest_rate.
        6) Portability:
        - Do NOT use MySQL/SQL Server quoting or syntax (no backticks ` or [ ]). Use LIMIT instead of TOP.
        There's some information I give you:
        [DataBase_ID]:
        {task.db_id},
        [Schema]:
        {schema},
        [Evidence]:
        {task.evidence},
        [Question]:
        {task.question}

        The answer format that you must obey is as follows:
        [Answer]:
        ```sql
        <The generated SQL>
        ```
    """
    return template
