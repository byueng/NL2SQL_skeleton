# -*- coding: utf-8 -*-
# @Time    : 2025-07-17 16:00
# @Author  : jwm
# @File    : schema_generator.py
# @description: For the High King
from abc import abstractmethod
from typing import Optional
from process_data.connection import DB_System

class Schema:
    """
    Simple schema which maps table&column to a unique identifier
    """
    def __init__(self, schema):
        self._schema = schema
        self._idMap = self._map(self._schema)

    @property
    def schema(self):
        return self._schema

    @property
    def idMap(self):
        return self._idMap

    def _map(self, schema):
        idMap = {'*': "__all__"}
        id = 1
        for key, vals in schema.items():
            for val in vals:
                idMap[key.lower() + "." + val.lower()] = "__" + key.lower() + "." + val.lower() + "__"
                id += 1

        for key in schema:
            idMap[key.lower()] = "__" + key.lower() + "__"
            id += 1

        return idMap

def ddl_schema(conn):
    """
    Get database's schema, which is a dict with table name as key
    and list of column names as value
    :param db: database path
    :return: schema dict
    """
    schema = {}
    cursor = conn.cursor()

    # fetch table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [str(table[0].lower()) for table in cursor.fetchall()]

    # fetch table info
    for table in tables:
        cursor.execute(f'PRAGMA table_info("{table}")')
        schema[table] = [str(col[1].lower()) for col in cursor.fetchall()]

    return schema

# class M_Schema(Schema):
#     def __init__(self, db_id: str) -> None:
#         super().__init__(db_id)

#     def _run(self):
#         return super()._run()


# class MAC_SQL_Schema(Schema):
#     def __init__(self, db_id: str) -> None:
#         super().__init__(db_id)

#     def _run(self):
#         return super()._run()


schema_list = {"DDL": ddl_schema}