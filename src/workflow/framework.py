# -*- coding: utf-8 -*-
# @Time    : 2025-07-29 14:46
# @Author  : jwm
# @File    : generator.py
# @description: Define the Generator.

import json, sys, importlib, os
import pydantic_settings
import dotenv
from loguru import logger
from process_data.connection import DB_System
from typing import List, Optional
from os import getenv
from ast import literal_eval
from runner.enum_aggretion import Model, Request, Response
from workflow.agents.Meta_Agent import MetaAgent
from workflow.agents.agent_factory import registry_agents


class FrameWork:
    def __init__(self, args, sql_client, schema, task) -> None:
        self.args = args
        self.sql_client: DB_System = sql_client
        self.schema = schema
        self.task = task
        self.agents: Optional[List[MetaAgent]] = None
        self._init()
 
    def _init(self):
        agents = self.bind_agents()
        self.agents_build(agents)

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

    def bind_agents(self) -> List[Model]:
        agents: List = list()
        model_path = self.args.model_path
        with open(model_path, 'r') as f:
            model_list = json.load(f)
        agents_str = getenv("AGENTS", "[]").lower()
        agents_list = literal_eval(agents_str)
        for agent in agents_list:
            try:
                agents.append(*[Model(**singal_model) for singal_model in model_list if agent == singal_model["corresponding_agent"] ])
            except Exception as e:
                logger.error(f"Bind failed, check the .env file and models.json(locates in llm folder)")
                sys.exit(1)

        if len(agents_list) != len(agents):
            logger.error(f"Can't bind all agents from file, Check out them. (include .env file, models.json)")
            sys.exit(1)
        return agents

    def agents_build(self, agents: List[Model]):
        """
            create instance each agents from agents list.
        """
        agents_cls: List[MetaAgent] = registry_agents(agents)
        self.agents = agents_cls