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
        if conn is not None:
            cursor = conn.cursor()
        else:
            raise RuntimeError("Database connection not open")

        # select all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = cursor.fetchall()

        ddl_statements = {}

        for (table_name,) in tables:
            # 查询建表语句
            cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            result = cursor.fetchone()
            if result and result[0]:
                ddl_statements[table_name] = result[0] + ";"
        
        self.sql_client._close()
        return ddl_statements


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