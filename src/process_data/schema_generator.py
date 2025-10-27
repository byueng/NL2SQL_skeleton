# -*- coding: utf-8 -*-
# @Time    : 2025-07-17 16:00
# @Author  : jwm
# @File    : schema_generator.py
# @description: For the High King
from abc import abstractmethod
from typing import Optional, Dict, List, Any
from sqlite3 import Connection

class Schema:
    """
    Simple schema which maps table&column to a unique identifier
    """
    def __init__(self, schema: Dict[str, List[str]]) -> None:
        self._schema: Dict[str, List[str]] = schema
        self._idMap: Dict[str, str] = self._map(self._schema)

    @property
    def schema(self) -> Dict[str, List[str]]:
        return self._schema

    @property
    def idMap(self) -> Dict[str, str]:
        return self._idMap

    def _map(self, schema: Dict[str, List[str]]) -> Dict[str, str]:
        idMap: Dict[str, str] = {'*': "__all__"}
        id: int = 1
        for key, vals in schema.items():
            for val in vals:
                idMap[key.lower() + "." + val.lower()] = "__" + key.lower() + "." + val.lower() + "__"
                id += 1

        for key in schema:
            idMap[key.lower()] = "__" + key.lower() + "__"
            id += 1

        return idMap

def ddl_schema(conn: Connection) -> Dict[str, List[str]]:
    """
    Get database's schema, which is a dict with table name as key
    and list of column names as value
    :param conn: database connection
    :return: schema dict
    """
    schema: Dict[str, List[str]] = {}
    cursor = conn.cursor()

    # fetch table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables: List[str] = [str(table[0].lower()) for table in cursor.fetchall()]

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


schema_list: Dict[str, Any] = {"DDL": ddl_schema}