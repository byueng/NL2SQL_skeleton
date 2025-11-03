################################
# Assumptions:
#   1. sql is correct
#   2. only table name has alias
#   3. only one intersect/union/except
#
# val: number(float)/string(str)/sql(dict)
# col_unit: (agg_id, col_id, isDistinct(bool))
# val_unit: (unit_op, col_unit1, col_unit2)
# table_unit: (table_type, col_unit/sql)
# cond_unit: (not_op, op_id, val_unit, val1, val2)
# condition: [cond_unit1, 'and'/'or', cond_unit2, ...]
# sql {
#   'select': (isDistinct(bool), [(agg_id, val_unit), (agg_id, val_unit), ...])
#   'from': {'table_units': [table_unit1, table_unit2, ...], 'conds': condition}
#   'where': condition
#   'groupBy': [col_unit1, col_unit2, ...]
#   'orderBy': ('asc'/'desc', [val_unit1, val_unit2, ...])
#   'having': condition
#   'limit': None/limit value
#   'intersect': None/sql
#   'except': None/sql
#   'union': None/sql
# }
################################

import json
import re
import sqlite3

from loguru import logger
from typing import Dict, List, Any, Tuple, Optional
from nltk import word_tokenize

from process_data.schema_generator import Schema

CLAUSE_KEYWORDS = ('select', 'from', 'where', 'group', 'order', 'limit', 'intersect', 'union', 'except')
JOIN_KEYWORDS = ('join', 'on', 'as')

WHERE_OPS = ('not', 'between', '=', '>', '<', '>=', '<=', '!=', 'in', 'like', 'is', 'exists')
UNIT_OPS = ('none', '-', '+', "*", '/')
AGG_OPS = ('none', 'max', 'min', 'count', 'sum', 'avg')
OTHER_OPS = ('nullif', 'cast')
TABLE_TYPE = {
    'sql': "sql",
    'table_unit': "table_unit",
}

COND_OPS = ('and', 'or')
SQL_OPS = ('intersect', 'union', 'except')
ORDER_OPS = ('desc', 'asc')
NUMBERS = ('1', '2', '3', '4', '5', '6', '7', '8', '9', '0')

def get_schema_from_json(fpath: str) -> Dict[str, List[str]]:
    with open(fpath) as f:
        data = json.load(f)

    schema: Dict[str, List[str]] = {}
    for entry in data:
        table: str = str(entry['table'].lower())
        cols: List[str] = [str(col['column_name'].lower()) for col in entry['col_data']]
        schema[table] = cols

    return schema


# Global error collector for tolerant parsing
PARSE_ERRORS: List[Dict[str, Any]] = []


def record_error(message: str, toks: "Optional[List[str]]" = None, idx: "Optional[int]" = None, exc: "Optional[Exception]" = None) -> None:
    info: Dict[str, Any] = {"message": message}
    if toks is not None:
        info["toks_sample"] = toks[max(0, (idx or 0) - 3):(idx or 0) + 3]
    if idx is not None:
        info["idx"] = idx
    if exc is not None:
        info["exception"] = str(exc)
    PARSE_ERRORS.append(info)


def tokenize(string: str) -> List[str]:
    string = str(string)
    string = string.replace("\'", "\"")  # ensures all string values wrapped by "" problem??
    quote_idxs: List[int] = [idx for idx, char in enumerate(string) if char == '"']
    if len(quote_idxs) % 2 != 0:
        record_error("tokenize: unexpected/unmatched quote count", list(string))
        # fallback: ignore quote grouping
        quote_idxs = []

    # keep string value as token
    vals: Dict[str, str] = {}
    for i in range(len(quote_idxs)-1, -1, -2):
        qidx1: int = quote_idxs[i-1]
        qidx2: int = quote_idxs[i]
        val: str = string[qidx1: qidx2+1]
        key: str = "__val_{}_{}__".format(qidx1, qidx2)
        string = string[:qidx1] + key + string[qidx2+1:]
        vals[key] = val

    toks: List[str] = [word.lower() for word in word_tokenize(string)]
    # replace with string value token
    for i in range(len(toks)):
        if toks[i] in vals:
            toks[i] = vals[toks[i]]
    
    # find if there exists !=, >=, <=
    eq_idxs: List[int] = [idx for idx, tok in enumerate(toks) if tok == "="]
    eq_idxs.reverse()
    prefix: Tuple[str, ...] = ('!', '>', '<')
    for eq_idx in eq_idxs:
        pre_tok: str = toks[eq_idx-1]
        if pre_tok in prefix:
            toks = toks[:eq_idx-1] + [pre_tok + "="] + toks[eq_idx+1: ]
    return toks

def get_brackets(prefix_toks):
    if prefix_toks is None:
        logger.info(f"prefix_tokens is empty!")
        return False, []

    status = False
    stack = []
    pairs = []
    lidx = None
    ridx = None

    for i, t in enumerate(prefix_toks):
        if t == "(":
            stack.append(i)
            pairs.append(i)
        elif t == ")":
            if stack:
                stack.pop()
            if pairs:
                lidx_candidate = pairs.pop()
            else:
                lidx_candidate = None
            if len(stack) == 0 and lidx_candidate is not None:
                lidx = lidx_candidate
                ridx = i
        else:
            continue

    if lidx is None or ridx is None:
        logger.info(f"brackets not found or unmatched, the sql parser failed.")
        return False, []

    if len(stack) != 0:
        logger.info(f"brackets stack not empty, the sql parser failed.")
        return False, []
    start = max(0, lidx - 1)
    return status, prefix_toks[start:ridx]


def scan_alias(toks):
    """Scan the index of 'as' and build the map for all alias"""
    as_idxs = [idx for idx, tok in enumerate(toks) if tok == 'as']
    alias = {}
    for idx in as_idxs:
        if toks[idx-1] != ")" and toks[idx-1] != "(":
            alias[toks[idx+1]] = toks[idx-1]
        else:
            status, bracket_toks_list = get_brackets(toks[:idx])
            if status:
                bracket_toks = " ".join(bracket_toks_list)
                alias[toks[idx+1]] = bracket_toks
            else:
                alias[toks[idx+1]] = "Invalid prefix tokens"
    return alias

def get_tables_with_alias(schema: Schema, toks):
    tables = scan_alias(toks)
    for key in schema.schema:
        if key in tables:
            record_error(f"get_tables_with_alias: alias '{key}' conflicts with real table name", toks)
        tables[key] = key
    return tables

def parse_col(toks, start_idx, tables_with_alias, schema, default_tables=None):
    """
        :returns next idx, column id
    """
    idx = start_idx
    if toks[idx] == '(':
        idx += 1
    try:
        tok_raw = toks[start_idx]
        # if column name is quoted like "Free Meal Count (K-12)", strip quotes
        if isinstance(tok_raw, str) and tok_raw.startswith('"') and tok_raw.endswith('"'):
            tok = tok_raw.strip('"').lower()
        else:
            tok = tok_raw
    except Exception as e:
        record_error("parse_col: failed to read token", toks, start_idx, e)
        return start_idx + 1, None
    if tok == "*":
        return start_idx + 1, schema.idMap[tok]

    if isinstance(tok, str) and '.' in tok:  # if token is a composite
        try:
            alias, col = tok.split('.')
        except Exception as e:
            record_error("parse_col: malformed composite token", toks, start_idx, e)
            return start_idx + 1, None
        # strip possible quotes around alias/col
        if alias.startswith('"') and alias.endswith('"'):
            alias = alias.strip('"').lower()
        if col.startswith('"') and col.endswith('"'):
            col = col.strip('"').lower()
        try:
            key = tables_with_alias.get(alias, alias) + "." + col
            return start_idx+1, schema.idMap.get(key)
        except Exception as e:
            record_error("parse_col: unknown alias/column", toks, start_idx, e)
            return start_idx + 1, None

    if default_tables is None or len(default_tables) == 0:
        record_error("parse_col: default_tables missing or empty", toks, start_idx)
        return start_idx+1, None

    for alias in default_tables:
        try:
            table = tables_with_alias[alias]
            # compare lowercased token to schema columns (which are stored lowercased)
            if isinstance(tok, str) and tok.lower() in schema.schema[table]:
                key = table + "." + tok.lower()
                return start_idx+1, schema.idMap.get(key)
        except Exception as e:
            # skip and continue trying other default tables
            record_error("parse_col: error checking default table columns", toks, start_idx, e)
            continue

    record_error("Error col: {}".format(tok), toks, start_idx)
    return start_idx+1, None


def parse_col_unit(toks, start_idx, tables_with_alias, schema, default_tables=None):
    """
        :returns next idx, (agg_op id, col_id)
    """
    idx = start_idx
    len_ = len(toks)
    isBlock = False
    isDistinct = False
    isNullif = False
    if toks[idx] == 'nullif':
        idx += 1
        isNullif = True

    if toks[idx] == '(':
        isBlock = True
        idx += 1

    if toks[idx] == 'cast':
        idx += 1

    if toks[idx] == '(':
        isBlock = True
        idx += 1

    if toks[idx] in AGG_OPS:
        agg_id = AGG_OPS.index(toks[idx])
        idx += 1
        if not (idx < len_ and toks[idx] == '('):
            record_error("parse_col_unit: expected '(' after agg op", toks, idx)
        else:
            idx += 1
        if toks[idx] == 'distinct':
            idx += 1
            isDistinct = True
        idx, col_id = parse_col(toks, idx, tables_with_alias, schema, default_tables)
        # "as" validation
        if toks[idx] == 'as':
            idx += 2
        if not (idx < len_ and toks[idx] == ')'):
            record_error("parse_col_unit: expected ')' after agg col", toks, idx)
        else:
            idx += 1
        return idx, (agg_id, col_id, isDistinct)

    if toks[idx] == 'distinct':
        idx += 1
        isDistinct = True

    agg_id = AGG_OPS.index("none")
    idx, col_id = parse_col(toks, idx, tables_with_alias, schema, default_tables)

    if toks[idx] == 'as':
        idx += 2

    if isBlock:
        if not (idx < len_ and toks[idx] == ')'):
            record_error("parse_col_unit: expected closing ')' for block", toks, idx)
        else:
            idx += 1  # skip ')'

    return idx, (agg_id, col_id, isDistinct)


def parse_col_unit_value(slice_toks, tables_with_alias, schema, default_tables=None):
        """
        尝试把一段 tokens 解析为 col_unit（使用 parse_col_unit），
        如果解析成功返回 col_unit，否则将该 tokens 列表作为实体值处理并返回一个字符串值。

        参数:
            - slice_toks: tokens 列表切片（list of str），代表待解析的 token 序列
            - tables_with_alias, schema, default_tables: 传递给 parse_col_unit 的相同上下文

        返回:
            - col_unit（三元组）或字符串（将 tokens 用空格拼接）
        """
        try:
                _, col_unit = parse_col_unit(slice_toks, 0, tables_with_alias, schema, default_tables)
                return col_unit
        except Exception:
                # 不能解析为列（例如未加引号的实体值），则把 tokens 合并为字符串作为值返回
                return ' '.join(slice_toks)


def parse_val_unit(toks, start_idx, tables_with_alias, schema, default_tables=None):
    idx = start_idx
    len_ = len(toks)
    isBlock = False
    if toks[idx] == '(':
        isBlock = True
        idx += 1

    col_unit1 = None
    col_unit2 = None
    unit_op = UNIT_OPS.index('none')

    idx, col_unit1 = parse_col_unit(toks, idx, tables_with_alias, schema, default_tables)
    if idx < len_ and toks[idx] in UNIT_OPS:
        unit_op = UNIT_OPS.index(toks[idx])
        idx += 1
        idx, col_unit2 = parse_col_unit(toks, idx, tables_with_alias, schema, default_tables)

    if idx < len_ and toks[idx] == ',':
        idx += 1

    if idx < len_ and toks[idx] in NUMBERS:
        idx += 2

    if isBlock:
        if not (idx < len_ and toks[idx] == ')'):
            record_error("parse_val_unit: expected closing ')' for block", toks, idx)
        else:
            while idx < len_ and toks[idx] == ')':
                idx += 1  # skip ')'

    return idx, (unit_op, col_unit1, col_unit2)


def parse_table_unit(toks, start_idx, tables_with_alias, schema):
    """
        :returns next idx, table id, table name
    """
    idx = start_idx
    len_ = len(toks)
    try:
        token = toks[idx]
    except Exception as e:
        record_error("parse_table_unit: missing token", toks, idx, e)
        return idx + 1, None, None

    # resolve alias mapping if possible
    try:
        key = tables_with_alias.get(token, token)
    except Exception as e:
        record_error("parse_table_unit: tables_with_alias lookup failed", toks, idx, e)
        key = token

    if idx + 1 < len_ and toks[idx+1] == "as":
        idx += 3
    else:
        idx += 1

    table_id = schema.idMap.get(key)
    if table_id is None:
        record_error("parse_table_unit: unknown table key", toks, idx, None)

    return idx, table_id, key


def parse_value(toks, start_idx, tables_with_alias, schema, default_tables=None):
    idx = start_idx
    len_ = len(toks)

    isBlock = False
    if toks[idx] == '(':
        isBlock = True
        idx += 1

    if toks[idx] == 'select':
        idx, val = parse_sql(toks, idx, tables_with_alias, schema)
    elif "\"" in toks[idx]:  # token is a string value
        val = toks[idx]
        idx += 1
    else:
        try:
            val = float(toks[idx])
            idx += 1
        except:
            end_idx = idx
            while end_idx < len_ and toks[end_idx] != ',' and toks[end_idx] != ')'\
                and toks[end_idx] != 'and' and toks[end_idx] not in CLAUSE_KEYWORDS and toks[end_idx] not in JOIN_KEYWORDS:
                    end_idx += 1
            # Try to parse as a column unit; if that fails, treat the token slice as a literal value
            slice_toks = toks[start_idx: end_idx]
            val = parse_col_unit_value(slice_toks, tables_with_alias, schema, default_tables)
            idx = end_idx

    if isBlock:
        if not (idx < len_ and toks[idx] == ')'):
            record_error("parse_val_unit: expected closing ')' for block", toks, idx)
        else:
            idx += 1

    return idx, val


def parse_condition(toks, start_idx, tables_with_alias, schema, default_tables=None):
    idx = start_idx
    len_ = len(toks)
    conds = []

    while idx < len_:
        idx, val_unit = parse_val_unit(toks, idx, tables_with_alias, schema, default_tables)
        not_op = False
        if idx < len_ and toks[idx] == 'not':
            not_op = True
            idx += 1

        if not (idx < len_ and toks[idx] in WHERE_OPS):
            record_error("parse_condition: expected where-op but not found", toks, idx)
            break
        op_id = WHERE_OPS.index(toks[idx])
        idx += 1
        val1 = val2 = None
        if op_id == WHERE_OPS.index('between'):  # between..and... special case: dual values
                idx, val1 = parse_value(toks, idx, tables_with_alias, schema, default_tables)
                if not (idx < len_ and toks[idx] == 'and'):
                    record_error("parse_condition: expected 'and' in between clause", toks, idx)
                else:
                    idx += 1
                idx, val2 = parse_value(toks, idx, tables_with_alias, schema, default_tables)
        else:  # normal case: single value
            idx, val1 = parse_value(toks, idx, tables_with_alias, schema, default_tables)
            val2 = None

        conds.append((not_op, op_id, val_unit, val1, val2))


        if idx < len_ and (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";") or toks[idx] in JOIN_KEYWORDS):
            break

        if idx < len_ and toks[idx] in COND_OPS:
            conds.append(toks[idx])
            idx += 1  # skip and/or

    return idx, conds


def parse_select(toks, start_idx, tables_with_alias, schema, default_tables=None):
    idx = start_idx
    len_ = len(toks)

    if not (idx < len(toks) and toks[idx] == 'select'):
        record_error("parse_select: 'select' not found", toks, idx)
        return start_idx, (False, [])
    idx += 1
    isDistinct = False
    isCast = False

    if idx < len_ and toks[idx] == 'distinct':
        idx += 1
        isDistinct = True

    val_units = []
    # "selct operation" -> "from operation" index range.
    while idx < len_ and toks[idx] not in CLAUSE_KEYWORDS:
        agg_id = AGG_OPS.index("none")                                 
        if toks[idx] in AGG_OPS:
            agg_id = AGG_OPS.index(toks[idx])
            idx += 1
        idx, val_unit = parse_val_unit(toks, idx, tables_with_alias, schema, default_tables)
        val_units.append((agg_id, val_unit))
        if idx < len_ and toks[idx] == ',':
            idx += 1  # skip ','

        elif idx < len_ and toks[idx] == 'as':
            idx += 2 # skip 'as'

    return idx, (isDistinct, val_units)


def parse_from(toks, start_idx, tables_with_alias, schema):
    """
    Assume in the from clause, all table units are combined with join
    """
    if 'from' not in toks[start_idx:]:
        record_error("parse_from: 'from' not found", toks, start_idx)
        return start_idx, [], [], []

    len_ = len(toks)
    idx = toks.index('from', start_idx) + 1
    default_tables = []
    table_units = []
    conds = []

    while idx < len_:
        isBlock = False
        if toks[idx] == '(':
            isBlock = True
            idx += 1
        
        # sub_sql idx+1.
        if toks[idx] == 'select':
            idx, sql = parse_sql(toks, idx, tables_with_alias, schema)
            table_units.append((TABLE_TYPE['sql'], sql))
        else:
            if idx < len_ and (toks[idx] == 'inner' and toks[idx+1] == 'join'):
                idx += 2 # skip inner join
            elif idx < len_ and toks[idx] == 'join':
                idx += 1
            idx, table_unit, table_name = parse_table_unit(toks, idx, tables_with_alias, schema)
            table_units.append((TABLE_TYPE['table_unit'], table_unit))
            default_tables.append(table_name)
        if idx < len_ and toks[idx] == "on":
            idx += 1  # skip on
            idx, this_conds = parse_condition(toks, idx, tables_with_alias, schema, default_tables)
            if len(conds) > 0:
                conds.append('and')
            conds.extend(this_conds)

        if isBlock:
            if not (idx < len_ and toks[idx] == ')'):
                record_error("parse_from: expected closing ')' for block", toks, idx)
            else:
                idx += 1
        if idx < len_ and (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";")):
            break

    return idx, table_units, conds, default_tables


def parse_where(toks, start_idx, tables_with_alias, schema, default_tables):
    idx = start_idx
    len_ = len(toks)

    if idx >= len_ or toks[idx] != 'where':
        return idx, []

    idx += 1
    idx, conds = parse_condition(toks, idx, tables_with_alias, schema, default_tables)
    return idx, conds


def parse_group_by(toks, start_idx, tables_with_alias, schema, default_tables):
    idx = start_idx
    len_ = len(toks)
    col_units = []

    if idx >= len_ or toks[idx] != 'group':
        return idx, col_units

    idx += 1
    if not (idx < len_ and toks[idx] == 'by'):
        record_error("parse_group_by: expected 'by' after 'group'", toks, idx)
        return idx, col_units
    idx += 1

    while idx < len_ and not (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";")):
        idx, col_unit = parse_col_unit(toks, idx, tables_with_alias, schema, default_tables)
        col_units.append(col_unit)
        if idx < len_ and toks[idx] == ',':
            idx += 1  # skip ','
        else:
            break

    return idx, col_units


def parse_order_by(toks, start_idx, tables_with_alias, schema, default_tables):
    idx = start_idx
    len_ = len(toks)
    val_units = []
    order_type = 'asc' # default type is 'asc'

    if idx >= len_ or toks[idx] != 'order':
        return idx, val_units

    idx += 1
    if not (idx < len_ and toks[idx] == 'by'):
        record_error("parse_order_by: expected 'by' after 'order'", toks, idx)
        return idx, val_units
    idx += 1

    while idx < len_ and not (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";")):
        idx, val_unit = parse_val_unit(toks, idx, tables_with_alias, schema, default_tables)
        val_units.append(val_unit)
        if idx < len_ and toks[idx] in ORDER_OPS:
            order_type = toks[idx]
            idx += 1
        if idx < len_ and toks[idx] == ',':
            idx += 1  # skip ','
        else:
            break

    return idx, (order_type, val_units)


def parse_having(toks, start_idx, tables_with_alias, schema, default_tables):
    idx = start_idx
    len_ = len(toks)

    if idx >= len_ or toks[idx] != 'having':
        return idx, []

    idx += 1
    idx, conds = parse_condition(toks, idx, tables_with_alias, schema, default_tables)
    return idx, conds


def parse_limit(toks, start_idx):
    idx = start_idx
    len_ = len(toks)

    if idx < len_ and toks[idx] == 'limit':
        idx += 2
        return idx, int(toks[idx-1])

    return idx, None


def parse_sql(toks, start_idx, tables_with_alias, schema):
    isBlock = False # indicate whether this is a block of sql/sub-sql
    len_ = len(toks)
    idx = start_idx

    sql = {}
    # initialize structured per-clause errors
    sql['_errors'] = {
        'from': [], 'select': [], 'where': [], 'groupBy': [], 'having': [],
        'orderBy': [], 'limit': [], 'intersect': [], 'union': [], 'except': []
    }
    if toks[idx] == '(':
        isBlock = True
        idx += 1

    # parse from clause in order to get default tables
    start_err_len = len(PARSE_ERRORS)
    from_end_idx, table_units, conds, default_tables = parse_from(toks, start_idx, tables_with_alias, schema)
    new_errs = PARSE_ERRORS[start_err_len:]
    if new_errs:
        sql['_errors']['from'].extend(new_errs)
    sql['from'] = {'table_units': table_units, 'conds': conds}
    # select clause
    start_err_len = len(PARSE_ERRORS)
    _, select_col_units = parse_select(toks, idx, tables_with_alias, schema, default_tables)
    new_errs = PARSE_ERRORS[start_err_len:]
    if new_errs:
        sql['_errors']['select'].extend(new_errs)
    idx = from_end_idx
    sql['select'] = select_col_units
    # where clause
    start_err_len = len(PARSE_ERRORS)
    idx, where_conds = parse_where(toks, idx, tables_with_alias, schema, default_tables)
    new_errs = PARSE_ERRORS[start_err_len:]
    if new_errs:
        sql['_errors']['where'].extend(new_errs)
    sql['where'] = where_conds
    # group by clause
    start_err_len = len(PARSE_ERRORS)
    idx, group_col_units = parse_group_by(toks, idx, tables_with_alias, schema, default_tables)
    new_errs = PARSE_ERRORS[start_err_len:]
    if new_errs:
        sql['_errors']['groupBy'].extend(new_errs)
    sql['groupBy'] = group_col_units
    # having clause
    start_err_len = len(PARSE_ERRORS)
    idx, having_conds = parse_having(toks, idx, tables_with_alias, schema, default_tables)
    new_errs = PARSE_ERRORS[start_err_len:]
    if new_errs:
        sql['_errors']['having'].extend(new_errs)
    sql['having'] = having_conds
    # order by clause
    start_err_len = len(PARSE_ERRORS)
    idx, order_col_units = parse_order_by(toks, idx, tables_with_alias, schema, default_tables)
    new_errs = PARSE_ERRORS[start_err_len:]
    if new_errs:
        sql['_errors']['orderBy'].extend(new_errs)
    sql['orderBy'] = order_col_units
    # limit clause
    start_err_len = len(PARSE_ERRORS)
    idx, limit_val = parse_limit(toks, idx)
    new_errs = PARSE_ERRORS[start_err_len:]
    if new_errs:
        sql['_errors']['limit'].extend(new_errs)
    sql['limit'] = limit_val

    idx = skip_semicolon(toks, idx)
    if isBlock:
        if not (idx < len_ and toks[idx] == ')'):
            record_error("parse_sql: expected closing ')' for block", toks, idx)
        else:
            idx += 1  # skip ')'
    idx = skip_semicolon(toks, idx)

    # intersect/union/except clause
    for op in SQL_OPS:  # initialize IUE
        sql[op] = None
    if idx < len_ and toks[idx] in SQL_OPS:
        sql_op = toks[idx]
        idx += 1
        start_err_len = len(PARSE_ERRORS)
        idx, IUE_sql = parse_sql(toks, idx, tables_with_alias, schema)
        new_errs = PARSE_ERRORS[start_err_len:]
        if new_errs:
            # attach under the IUE op name
            sql['_errors'][sql_op].extend(new_errs)
        sql[sql_op] = IUE_sql

    return idx, sql


def load_data(fpath):
    with open(fpath) as f:
        data = json.load(f)
    return data


def get_sql(schema, query):
    # reset errors for each run
    global PARSE_ERRORS
    PARSE_ERRORS = []

    toks = [token.replace("\"", "") for token in tokenize(query.replace('`', '\''))]
    tables_with_alias = get_tables_with_alias(schema, toks)
    try:
        _, sql = parse_sql(toks, 0, tables_with_alias, schema)
    except Exception as e:
        # record and return best-effort structure
        record_error("get_sql: parse_sql raised exception", toks, 0, e)
        sql = {}

    # attach collected parse errors so caller can inspect them
    # parse_sql already initializes a structured sql['_errors'] per-clause; preserve it
    if isinstance(sql, dict):
        sql.setdefault('_errors', {})
        # put the global flat error list under a 'global' key so callers can inspect both
        sql['_errors']['global'] = PARSE_ERRORS.copy()
    else:
        sql = {'_errors': PARSE_ERRORS.copy()}
    return sql

def skip_semicolon(toks, start_idx):
    idx = start_idx
    while idx < len(toks) and toks[idx] == ";":
        idx += 1
    return idx

def extract_sql(response: str) -> str:
    pattern: str = r'```sql(.*?)```'
    matches = re.findall(pattern, response, re.DOTALL)
    if matches:
        sql: str = matches[0].replace("\n", " ").strip().lstrip()
        return sql
    else:
        return response.strip()
    
