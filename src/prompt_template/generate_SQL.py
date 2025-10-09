from runner.enum_aggretion import Task

def generate_SQL(task: Task, schema: str):
    template = f"""
        You are a SQLite expert. You need to read and understand the following database schema description, as well as the evidence
        that may be used, and use your SQLite knowledge to generate SQL statements to answer user questions.
        [DataBase_ID]:
        {task.db_id},
        [Schema]:
        {schema},
        [Evidence]:
        {task.evidence},
        [Question]:
        {task.question},
        The answer format that you must obey is as follows:
        [Answer]:
        ```sql
        <The generated SQL>
        ```
    """
    return template
