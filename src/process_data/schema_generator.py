# -*- coding: utf-8 -*-
# @Time    : 2025-07-17 16:00
# @Author  : jwm
# @File    : schema_generator.py
# @description: For the High King
import os, json
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
        # Use readable identifiers for columns and tables (no surrounding '__')
        idMap: Dict[str, str] = {'*': '*'}
        id: int = 1
        for key, vals in schema.items():
            for val in vals:
                idMap[key.lower() + "." + val.lower()] = key.lower() + "." + val.lower()
                id += 1

        for key in schema:
            idMap[key.lower()] = key.lower()
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

def m_schema(conn: Connection) -> Dict:
    data = None
    db_info = conn.execute("PRAGMA database_list").fetchall()
    db_path = db_info[0][2]
    db_base = os.path.basename(db_path)[:-7]
    schema: Dict = {}
    m_schema_path = f"../data/m_schema"
    m_schema_list = os.listdir(m_schema_path)
    for i in m_schema_list:
        if db_base in i:
            with open(os.path.join(m_schema_path, i), "r", encoding="utf-8") as f:
                data = json.load(f)
            break
    if data == None:
        logger.warning(f"db_name doesn't exist in M_Schema file.")
    else:
        schema = data
    return {k: schema[k] for k in schema.keys() if k != "schema"}

# class MAC_SQL_Schema(Schema):
#     def __init__(self, db_id: str) -> None:
#         super().__init__(db_id)

#     def _run(self):
#         return super()._run()


schema_list: Dict[str, Any] = {"DDL": ddl_schema, "M_Schema": m_schema}