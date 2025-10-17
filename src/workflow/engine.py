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

def engine(args, task, schema_generator, model_list) -> bool:
    """
    Create and compile the NL2SQL FrameWork running.
    
    Three class is used in Framework: DataPreprogressing, Generator, Selection
    """
    sql_client = DB_System(args, task)
    sql_client.open()
    schema_dict: dict = schema_generator(sql_client.conn)
    schema: Schema = Schema(schema_dict)
    framework = FrameWork(args, sql_client, schema, task, model_list)
    framework._run()
    return True
    