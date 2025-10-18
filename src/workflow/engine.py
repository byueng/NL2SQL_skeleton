# -*- coding: utf-8 -*-
# @Time    : 2025-07-29 14:18
# @Author  : jwm
# @File    : engine.py
# @description: Center Control to the whole workflow

import sys
import pydantic_settings

from os import getenv
from ast import literal_eval
from loguru import logger
from typing import List, Optional


from runner.enum_aggretion import Model
from workflow.framework import FrameWork
from workflow.agents.meta_agent import MetaAgent
from workflow.agents.agent_factory import registry_agents
from process_data.connection import DB_System
from process_data.schema_generator import Schema

def agents_build(models: List[Model]) -> List[MetaAgent]:
    """
        create instance each agents from agents list.
    """
    agents_cls: List[MetaAgent] = registry_agents(models)
    return agents_cls

def bind_agents(model_list) -> List[MetaAgent]:
    """
        bind model.json llm config with .env AGENTS environment
    """
    agents: List = list()
    agents_str = getenv("AGENTS", "[]")  # 移除 .lower()，保持原始大小写
    agents_list = literal_eval(agents_str)
    for agent in agents_list:
        try:
            # 使用大小写不敏感的匹配
            matched_models = [
                Model(**singal_model) 
                for singal_model in model_list 
                if agent.lower() == singal_model["corresponding_agent"].lower()
            ]
            if matched_models:
                agents.extend(matched_models)
        except Exception:
            logger.error(f"Bind failed, check the .env file and models.json(locates in llm folder)")
            sys.exit(1)

    if len(agents_list) != len(agents):
        logger.error(f"Can't bind all agents from file, Check out them. (include .env file, models.json)")
        sys.exit(1)
    agents = agents_build(agents) 
    return agents


def engine(args, task, schema_generator, model_list):
    agents = bind_agents(model_list)
    logger.info(f"Agents build successfully, the builded agents: {[agent.model_info.model_name for agent in agents]}")
    db_system = DB_System(args, task)
    schema: Schema = Schema(schema_generator(db_system.conn))
    
    return 

    