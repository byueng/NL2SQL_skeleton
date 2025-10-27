# -*- coding: utf-8 -*-
# @Time    : 2025-10-27 14:33
# @Author  : jwm
# @File    : evaluate.py
# @description: For the High King
import os
import json 
from typing import Dict, Any, List, Tuple, Optional

from loguru import logger
from runner.enum_aggretion import Task
from process_data.connection import DB_System

class Evaluator:
    def __init__(self, task: Task, pr_sql: Optional[str], sql_client: DB_System, output_name: str) -> None:
        self.task: Task = task
        self.pr_sql: Optional[str] = pr_sql
        self.sql_client: DB_System = sql_client
        self.output_name: str = output_name

    def _run(self) -> None:
        if self.pr_sql is not None:
            self.save_sql(self.pr_sql, self.task, self.output_name)
        else:
            logger.warning("Generated SQL is None, skipping save.")
    
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

    def save_sql(self, pr_sql: str, task: Task, output_name: str) -> None:
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

