import re

def parse_result(response) -> str:
    pattern = r'```sql(.*?)```'
    matches: str = re.findall(pattern, response, re.DOTALL)[0]    
    sql = matches.replace("\n", " ").strip().lstrip()
    return sql