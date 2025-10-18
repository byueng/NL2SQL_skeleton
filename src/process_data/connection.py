# -*- coding: utf-8 -*-
# @Time    : 2025-07-17 16:00
# @Author  : jwm
# @File    : connection.py
# @description: For the High King

import os 
from contextlib import contextmanager

from loguru import logger
from sqlite3 import connect, Connection

from runner.enum_aggretion import Task

class DB_System:
    def __init__(self, args, task: Task) -> None:
        self.args = args
        self.task = task
        self._conn = None 

    @property
    def conn(self) -> Connection:
        if self._conn is None:
            self.open()
        return self._conn  # type: ignore
    
    @conn.setter
    def conn(self, value):

        if value is None and self._conn is not None:
            self._close()
        else:
            self._conn = value

    def open(self) -> None:
        if self._conn is not None:
            logger.warning("Connection already open, closing existing connection first")
            self._close()
            
        db_path = os.path.join(self.args.data_path, f"{self.args.data_mode}_databases", self.task.db_id, f"{self.task.db_id}.sqlite")
        self._conn = connect(db_path)
        logger.debug(f"Database connection opened for {self.task.db_id}")
    
    def _close(self):
        if self._conn is None:
            logger.warning(f"The Database wasn't open first, can't close!")
        else:
            self._conn.close()
            self._conn = None
            logger.debug(f"Database connection closed for {self.task.db_id}")

    @contextmanager
    def get_connection(self):

        try:
            conn = self.conn  
            yield conn
        finally:
            self.conn = None
    def execute_query(self, query: str, params=None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
