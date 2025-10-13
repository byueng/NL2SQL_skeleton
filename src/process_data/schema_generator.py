# -*- coding: utf-8 -*-
# @Time    : 2025-07-17 16:00
# @Author  : jwm
# @File    : schema_generator.py
# @description: For the High King
from abc import abstractmethod
from typing import Optional
from process_data.connection import DB_System

class Schema:
    def __init__(self, sql_client) -> None:
        self.sql_client: DB_System = sql_client
        
    @abstractmethod
    def _run(self) -> dict:
        return {}

class DDL(Schema):
    def __init__(self, sql_client) -> None:
        super().__init__(sql_client)

    def _run(self) -> dict:
        self.sql_client.open()
        conn = self.sql_client.conn
        schema = {}
        if conn == None:
            raise Exception
        cursor = conn.cursor()

        # fetch table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [str(table[0].lower()) for table in cursor.fetchall()]

        # fetch table info
        for table in tables:
            cursor.execute("PRAGMA table_info({})".format(table))
            schema[table] = [str(col[1].lower()) for col in cursor.fetchall()]

        return schema

class M_Schema(Schema):
    def __init__(self, db_id: str) -> None:
        super().__init__(db_id)

    def _run(self):
        return super()._run()


class MAC_SQL_Schema(Schema):
    def __init__(self, db_id: str) -> None:
        super().__init__(db_id)

    def _run(self):
        return super()._run()


schema_list = {"DDL": DDL, "M_Schema": M_Schema, "MAC_SQL_Schema": MAC_SQL_Schema}