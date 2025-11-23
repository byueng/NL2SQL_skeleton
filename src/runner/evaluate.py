# -*- coding: utf-8 -*-
# @Time    : 2025-10-27 14:33
# @Author  : jwm
# @File    : evaluate.py
# @description: For the High King
import os
import json
import re
import uuid
from datetime import datetime
import process_data.parser_sql as psql

from typing import Dict, Any, List, Tuple, Optional
from loguru import logger

from runner.enum_aggretion import Task
from process_data.connection import DB_System
from process_data.parser_sql import tokenize, get_tables_with_alias, parse_sql, get_sql

class Evaluator:
    def __init__(self, schema, task: Task, pr_sql: Optional[str], sql_client: DB_System, output_name: str) -> None:
        self.schema = schema
        self.task: Task = task
        self.pr_sql: Optional[str] = pr_sql
        self.sql_client: DB_System = sql_client
        self.output_name: str = output_name

    def _run(self) -> None:
        if self.pr_sql is not None:
            pr_sql = self.pr_sql
            is_accuray = self.save_sql(pr_sql, self.task, self.output_name)
            # if not is_accuray:
            #     self.parser(pr_sql)
        else:
            logger.warning("Generated SQL is None, skipping save.")
    
    def parser(self, pr_sql):
        gt_parse_op_list = get_sql(self.schema, self.task.SQL)
        pr_parse_op_list = get_sql(self.schema, pr_sql)
        self.save_parse(self.output_name, gt_parse_op_list, pr_parse_op_list)


    def validate_sql(self, gold_sql: str, generate_sql: str) -> bool:
        self.sql_client.open()
        conn = self.sql_client.conn
        if conn is not None:
            cursor = conn.cursor()
        else:
            raise RuntimeError("Database connection not open")
        
        try:
            cursor.execute(gold_sql)
            gold_result: List[Tuple[Any, ...]] = cursor.fetchall()
            cursor.execute(generate_sql)
            generate_result: List[Tuple[Any, ...]] = cursor.fetchall()
            self.sql_client._close()
            if gold_result == generate_result:
                return True
            else:
                return False
            
        except Exception as e:
            logger.error(f"SQL validation error: {e}")
            return False

    def save_sql(self, pr_sql: str, task: Task, output_name: str) -> bool:
        file_path: str = f"./result/{output_name}/original_result.json"
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "a", encoding="utf-8") as f:

            accuracy: bool = False
            if task.SQL is not None:
                accuracy = self.validate_sql(task.SQL, pr_sql)
            
            result_data: Dict[str, Any] = {
                "db_id": task.db_id,
                "question": task.question,
                "ground_truth_sql": task.SQL,
                "answer_sql": pr_sql,
                "difficulty": task.difficulty,
                "accuracy": accuracy
            }
            f.write(json.dumps(result_data, indent=4, ensure_ascii=False))
            f.write(",\n")
        return accuracy

    def save_parse(self, output_name, gt_parse_op_list, pr_parse_op_list):
        folder_path: str = f"./result/{output_name}/parse/"
        dir_num = len(os.listdir(folder_path))
        file_path = folder_path + f"{output_name}_{self.task.question_id}.json"
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        # build output directory: ./result/{model_name}/parse/
        out_dir = os.path.join(".", "result", self.output_name, "parse")
        os.makedirs(out_dir, exist_ok=True)

        # prepare metadata
        db_id = getattr(self.task, "db_id", None)
        question = getattr(self.task, "question", None)
        ground_sql = getattr(self.task, "SQL", None)

        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        short_id = uuid.uuid4().hex[:8]
        safe_db = re.sub(r"[^0-9A-Za-z_-]", "", str(db_id)) if db_id is not None else "unknown_db"
        filename = f"{timestamp}_{safe_db}_{short_id}.json"
        file_path = os.path.join(out_dir, filename)

        # helper: humanize functions
        def humanize_col_unit(col_unit):
            # col_unit may be tuple (agg_id, col_id, isDistinct) or a string literal
            if col_unit is None:
                return None
            if isinstance(col_unit, str):
                return col_unit
            try:
                agg_id, col_id, isDistinct = col_unit
                agg = psql.AGG_OPS[agg_id] if isinstance(agg_id, int) and agg_id < len(psql.AGG_OPS) else str(agg_id)
                name = col_id if isinstance(col_id, str) else None
                return {"agg": agg, "col": name, "distinct": bool(isDistinct)}
            except Exception:
                return str(col_unit)

        def humanize_val_unit(val_unit):
            # val_unit may be a tuple (unit_op, col_unit1, col_unit2) or literal
            if val_unit is None:
                return None
            if isinstance(val_unit, str) or isinstance(val_unit, (int, float)):
                return val_unit
            try:
                unit_op_idx, cu1, cu2 = val_unit
                unit_op = psql.UNIT_OPS[unit_op_idx] if isinstance(unit_op_idx, int) and unit_op_idx < len(psql.UNIT_OPS) else str(unit_op_idx)
                return {"op": unit_op, "left": humanize_col_unit(cu1), "right": humanize_col_unit(cu2)}
            except Exception:
                return str(val_unit)

        def humanize_condition(cond_list):
            human = []
            for item in cond_list:
                if isinstance(item, tuple):
                    not_op, op_id, val_unit, val1, val2 = item
                    op = psql.WHERE_OPS[op_id] if isinstance(op_id, int) and op_id < len(psql.WHERE_OPS) else str(op_id)
                    human.append({
                        "not": bool(not_op),
                        "op": op,
                        "expr": humanize_val_unit(val_unit),
                        "val1": humanize_val_unit(val1) if val1 is not None else val1,
                        "val2": humanize_val_unit(val2) if val2 is not None else val2,
                    })
                else:
                    human.append(item)
            return human

        # build human-readable summary from the parse results
        def humanize_sql(sql):
            if not isinstance(sql, dict):
                return sql
            hr = {}
            # select
            try:
                sel = sql.get('select')
                if isinstance(sel, (list, tuple)) and len(sel) == 2:
                    isDistinct, val_units = sel
                    hr['select'] = {"distinct": bool(isDistinct), "cols": []}
                    for agg_id, val_unit in val_units:
                        agg = psql.AGG_OPS[agg_id] if isinstance(agg_id, int) and agg_id < len(psql.AGG_OPS) else str(agg_id)
                        hr['select']['cols'].append({"agg": agg, "expr": humanize_val_unit(val_unit)})
                else:
                    hr['select'] = sel
            except Exception:
                hr['select'] = str(sql.get('select'))

            # from
            try:
                frm = sql.get('from', {})
                hr['from'] = {"tables": [], "conds": humanize_condition(frm.get('conds', []))}
                for t in frm.get('table_units', []):
                    # t is (type, table_id_or_sql)
                    if isinstance(t, (list, tuple)) and len(t) == 2:
                        ttype, payload = t
                        if ttype == psql.TABLE_TYPE['table_unit']:
                            hr['from']['tables'].append(payload)
                        elif ttype == psql.TABLE_TYPE['sql']:
                            hr['from']['tables'].append(humanize_sql(payload))
                        else:
                            hr['from']['tables'].append(str(t))
                    else:
                        hr['from']['tables'].append(str(t))
            except Exception:
                hr['from'] = str(sql.get('from'))

            # where
            try:
                hr['where'] = humanize_condition(sql.get('where', []))
            except Exception:
                hr['where'] = str(sql.get('where'))

            # groupBy
            try:
                grp = sql.get('groupBy', [])
                hr['groupBy'] = [humanize_col_unit(cu) for cu in grp]
            except Exception:
                hr['groupBy'] = str(sql.get('groupBy'))

            # orderBy
            try:
                order = sql.get('orderBy')
                if isinstance(order, (list, tuple)) and len(order) == 2:
                    order_type, vals = order
                    hr['orderBy'] = {"type": order_type, "cols": [humanize_val_unit(v) for v in vals]}
                else:
                    hr['orderBy'] = order
            except Exception:
                hr['orderBy'] = str(sql.get('orderBy'))

            # having
            try:
                hr['having'] = humanize_condition(sql.get('having', []))
            except Exception:
                hr['having'] = str(sql.get('having'))

            # limit
            hr['limit'] = sql.get('limit')

            # errors
            hr['errors'] = sql.get('_errors', {})

            return hr

        payload = {
            "db_id": db_id,
            "question": question,
            "timestamp": timestamp,
            "ground_sql": ground_sql,
            "ground_parse": gt_parse_op_list,
            "predict_parse": pr_parse_op_list,
            "human_readable": humanize_sql(pr_parse_op_list),
        }

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved parse pair to {file_path}")
        except Exception as e:
            logger.error(f"Failed to save parse to {file_path}: {e}")
        return file_path
