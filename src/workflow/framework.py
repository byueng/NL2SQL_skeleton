# -*- coding: utf-8 -*-
# @Time    : 2025-07-29 14:46
# @Author  : jwm
# @File    : generator.py
# @description: Define the Generator.

import json
import sys
import importlib
import os
from typing import List, Optional, Any, Dict, Tuple

from loguru import logger
from process_data.connection import DB_System
from process_data.schema_generator import Schema
from runner.enum_aggretion import Model, Request, Response, Task
from workflow.agents.meta_agent import MetaAgent

class FrameWork:
    def __init__(self, args: Any, sql_client: DB_System, schema: Schema, task: Task, agents: Optional[List[MetaAgent]]) -> None:
        self.args = args
        self.sql_client: DB_System = sql_client
        self.schema: Schema = schema
        self.task: Task = task
        self.agents: Optional[List[MetaAgent]] = agents

    def get_template(self, template_name: str) -> str:
        """
            Load template function from config file.
        """
        template_module = importlib.import_module("prompt_template." + template_name)
        template_func = getattr(template_module, template_name)
        template: str = template_func(self.task, self.schema.schema)
        return template

    # need modify
    def _run(self) -> Optional[Response]:
        if self.agents is None:
            logger.warning(f"Agent list is empty!")
            return None
        else:
            for agent in self.agents:
                request: Request = Request(**{
                    "template": self.get_template(agent.model_info.template_name),
                })
                agent.input = request
                result: Optional[str] = agent._run()
                response: Response = Response(**{
                    "status": True,
                    "result": result
                })
                agent.output = response
            
            # 安全地访问最后一个agent的输出
            last_agent = self.agents[-1]
            if last_agent.output is not None:
                from runner.evaluate import Evaluator
                evaluator: Evaluator = Evaluator(
                    self.task, 
                    last_agent.output.result, 
                    self.sql_client, 
                    last_agent.model_info.output_name
                )
                evaluator._run()
                return last_agent.output
            else:
                logger.warning("Last agent output is None!")
                return None
        