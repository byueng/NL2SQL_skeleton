# -*- coding: utf-8 -*-
# @Time    : 2025-07-29 14:46
# @Author  : jwm
# @File    : generator.py
# @description: Define the Generator.

import json, sys, importlib, os

from loguru import logger
from process_data.connection import DB_System
from process_data.schema_generator import Schema
from typing import List, Optional
from runner.enum_aggretion import Model, Request, Response
from workflow.agents.meta_agent import MetaAgent

# class FrameWork:
#     def __init__(
#             self,
#             db_system: DB_System,
#             schema: Schema,
#             agents: List[MetaAgent]
#             ) -> None:
#         self.db_system = db_system
#         self.schema = schema
#         self.agents = agents


class FrameWork:
    def __init__(self, args, sql_client, schema, task, agents: Optional[List[MetaAgent]]) -> None:
        self.args = args
        self.sql_client: DB_System = sql_client
        self.schema = schema
        self.task = task
        self.agents: Optional[List[MetaAgent]] = agents

    def get_template(self, template_name):
        """
            Load template function from config file.
        """
        template_module = importlib.import_module("prompt_template." + template_name)
        template_func = getattr(template_module, template_name)
        template = template_func(self.task, self.schema)
        return template

    def validate_sql(self, gold_sql, generate_sql) -> bool:
        self.sql_client.open()
        conn = self.sql_client.conn
        if conn is not None:
            cursor = conn.cursor()
        else:
            raise RuntimeError("Database connection not open")
        
        try:
            cursor.execute(gold_sql)
            gold_result = cursor.fetchall()
            cursor.execute(generate_sql)
            generate_result = cursor.fetchall()
            self.sql_client._close()
            if gold_result == generate_result:
                return True
            else:
                return False
            
        except Exception as e:
            print(e)
            return False

    def save_sql(self, sql, model_name):
        file_path = f"./result/{model_name}/original_result.json"
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "db_id": self.task.db_id,
                "question": self.task.question,
                "ground_truth_sql": self.task.SQL,
                "answer_sql": sql,
                "difficulty": self.task.difficulty,
                "accuray": self.validate_sql(self.task.SQL, sql)
            }, indent=4, ensure_ascii=False))
            f.write(",\n")

    # need modify
    def _run(self):
        if self.agents == None:
            logger.warning(f"Agent list is empty!")
        else:
            for agent in self.agents:
                request: Request = Request(**{
                    "template": self.get_template(agent.model_info.template_name),
                })
                agent._input = request
                result = agent._run()
                response: Response = Response(**{
                    "status": True,
                    "result": result
                })
                self.save_sql(response.result, agent.model_info.model_name)