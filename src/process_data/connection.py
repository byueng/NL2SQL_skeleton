# -*- coding: utf-8 -*-
# @Time    : 2025-07-17 16:00
# @Author  : jwm
# @File    : connection.py
# @description: For the High King

import json
import os 

from loguru import logger
from sqlite3 import connect, Connection

from runner.enum_aggretion import Task

class DB_System:
    def __init__(self, args, task) -> None:
        self.args = args
        self.task = task
        self.conn = None 

    def open(self) -> None:
        db_path = os.path.join(self.args.data_path, f"{self.args.data_mode}_databases", self.task.db_id, f"{self.task.db_id}.sqlite")
        conn = connect(db_path)
        self.conn = conn
    
    def _close(self):
        if self.conn == None:
            logger.warning(f"The Database wasn't open first, can't close!")
        else:
            self.conn.close()
            self.conn = None

