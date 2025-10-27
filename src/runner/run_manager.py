# -*- coding: utf-8 -*-
# @Time    : 2025-10-18 16:43
# @Author  : jwm
# @File    : run_manager.py
# @description: For the High King

import os 
import json
import sys

from os import getenv
from ast import literal_eval
from typing import List, Dict, Any, Optional, Callable
from loguru import logger
from dotenv import find_dotenv, load_dotenv

from runner.enum_aggretion import Task, Model, Response
from process_data.schema_generator import schema_list
from process_data.connection import DB_System
from process_data.schema_generator import Schema
from workflow.agents.agent_factory import registry_agents
from workflow.framework import FrameWork
from workflow.agents.meta_agent import MetaAgent


_DOTENV_PATH = find_dotenv(usecwd=True)
if _DOTENV_PATH:
    load_dotenv(_DOTENV_PATH, override=False, encoding="utf-8")
    logger.debug(f"Loaded .env from: {_DOTENV_PATH}")
else:
    logger.debug(".env not found via find_dotenv")

class RunManager:
    def __init__(self, args: Any) -> None:
        self.args = args
        self.tasks: List[Task] = []
        self.schema_generator: Callable = schema_list[args.schema_generator]
        self.total_task_num: int = 0
        with open(self.args.model_path, 'r') as f:
            self.model_list: List[Dict[str, Any]] = json.load(f)
        self.agents: Optional[List[MetaAgent]] = None
        if self.schema_generator is not None:
            logger.info(f"RunManager init correctly, chosen schema_generator: {args.schema_generator}")
        else:
            logger.warning(f"The schema_generator is None, add it in .env file")
    
    def agents_build(self, models: List[Model]) -> List[MetaAgent]:
        """
            create instance each agents from agents list.
        """
        agents_cls: List[MetaAgent] = registry_agents(models)
        return agents_cls

    def bind_agents(self, model_list: List[Dict[str, Any]]) -> List[MetaAgent]:
        """
            bind model.json llm config with .env AGENTS environment
        """
        agents: List[Model] = []

        agents_str: str = getenv("AGENTS", "[]") 
        agents_list: List[str] = literal_eval(agents_str)
        for agent in agents_list:
            try:
                # 使用大小写不敏感的匹配
                matched_models: List[Model] = [
                    Model(**singal_model) 
                    for singal_model in model_list 
                    if agent.lower() == singal_model["corresponding_agent"].lower()
                ]
                if matched_models:
                    agents.extend(matched_models)
            except Exception as e:
                logger.info(f"{e}")
                logger.error(f"Bind failed, check the .env file and models.json(locates in llm folder)")
                sys.exit(1)

        if len(agents_list) != len(agents):
            logger.error(f"Can't bind all agents from file, Check out them. (include .env file, models.json)")
            sys.exit(1)
        built_agents: List[MetaAgent] = self.agents_build(agents) 
        return built_agents
    
    def initialize_tasks(self, dataset: List[Dict[str, Any]]) -> None:
        """
        Initializes tasks from the provided dataset.
        
        Args:
            dataset (List[Dict[str, Any]]): The dataset containing task information.
        """
        for i, data in enumerate(dataset):
            if "question_id" not in data:
                data = {"question_id": i, **data}
            task: Task = Task(**data)
            self.tasks.append(task)
        self.total_task_num = len(self.tasks)
        logger.info(f"initialize task completed, total task number is {self.total_task_num}")
        # Avoid having only one model information in the models.josn.
        if isinstance(self.model_list, Dict):
            self.model_list = [self.model_list]

        self.agents = self.bind_agents(self.model_list)
        logger.info(f"Agents build successfully, builded agents: {[agent.model_info.model_name for agent in self.agents]}")

    def run_task(self) -> None:
        """
            NL2SQL work flow.
            Not completed, multithreaded asynchronous needs to be supplemented
        """
        for task in self.tasks:
            self.worker(task)
            break

    def worker(self, task: Task) -> None:
        """
        Worker function to process a single task.
        
        Args:
            task (Task): The task to be processed.
        """
        logger.info(f"begin task: {task.db_id} {task.question_id}")
        
        db_system: DB_System = DB_System(self.args, task)
        schema: Schema = Schema(self.schema_generator(db_system.conn))

        if self.agents is None:
            logger.warning(f"agents bind nothing")
            sys.exit(1)

        # nl2sql_framework = FrameWork(db_system, schema, self.agents)
        nl2sql_framework: FrameWork = FrameWork(self.args, db_system, schema, task, self.agents)
        nl2sql_framework._run()


