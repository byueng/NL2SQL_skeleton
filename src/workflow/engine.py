# -*- coding: utf-8 -*-
# @Time    : 2025-07-29 14:18
# @Author  : jwm
# @File    : engine.py
# @description: Center Control to the whole workflow

from workflow.framework import FrameWork
from workflow.data_preprogress import DataPreprogress
from process_data.connection import DB_System
from process_data.schema_generator import schema_list, Schema


def set_schema(sql_client, schema_generator):
    schema = schema_generator(sql_client)._run()
    return schema 


def engine(args, task) -> bool:
    """
    Create and compile the NL2SQL FrameWork running.
    
    Three class is used in Framework: DataPreprogressing, Generator, Selection
    """
    sql_client = DB_System(args, task)
    schema_generator = schema_list[args.schema_generator]
    schema: dict = set_schema(sql_client, schema_generator)
    framework = FrameWork(args, sql_client, schema, task)
    framework._run()
    return True
    