# -*- coding: utf-8 -*-
# @Time    : 2025-07-17 16:00
# @Author  : jwm
# @File    : connection.py
# @description: For the High King

import os 
from contextlib import contextmanager
from typing import Optional, Any, List, Tuple, Generator

from loguru import logger
from sqlite3 import connect, Connection

from runner.enum_aggretion import Task

class DB_System:
    def __init__(self, args: Any, task: Task) -> None:
        self.args = args
        self.task: Task = task
        self._conn: Optional[Connection] = None 

    @property
    def conn(self) -> Connection:
        if self._conn is None:
            self.open()
        return self._conn  # type: ignore
    
    @conn.setter
    def conn(self, value: Optional[Connection]) -> None:
        if value is None and self._conn is not None:
            self._close()
        else:
            self._conn = value

    def open(self) -> None:
        if self._conn is not None:
            # logger.warning("Connection already open, closing existing connection first")
            self._close()
            
        db_path: str = os.path.join(self.args.data_path, f"{self.args.data_mode}_databases", self.task.db_id, f"{self.task.db_id}.sqlite")
        self._conn = connect(db_path)
    
    def _close(self) -> None:
        if self._conn is None:
            logger.warning(f"The Database wasn't open first, can't close!")
        else:
            self._conn.close()
            self._conn = None

    @contextmanager
    def get_connection(self) -> Generator[Connection, None, None]:
        try:
            conn: Connection = self.conn  
            yield conn
        finally:
            self.conn = None
    
    def execute_query(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> List[Tuple[Any, ...]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
