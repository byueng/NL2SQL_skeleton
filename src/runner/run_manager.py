# -*- coding: utf-8 -*-
# @Time    : 2025-07-17 17:00
# @Author  : jwm
# @File    : run_manager.py
# @description: Control center of working flow in the whole process.
import os 
import json

from typing import List, Dict, Any
from loguru import logger


from runner.enum_aggretion import Task, Work
from runner.visual_flow import Visual
from process_data.schema_generator import schema_list
from workflow.engine import engine


class RunManager:
    def __init__(self, args) -> None:
        self.args = args
        self.tasks: List = []
        self.schema_generator = schema_list[args.schema_generator]
        self.total_task_num: int = 0
        if self.schema_generator != None:
            logger.info(f"RunManager init correctly, chosen schema_generator: {args.schema_generator}")
        else:
            logger.warning(f"The schema_generator is None, add it in .env file")

    def initialize_tasks(self, dataset: List[Dict[str, Any]]):
        """
        Initializes tasks from the provided dataset.
        
        Args:
            dataset (List[Dict[str, Any]]): The dataset containing task information.
        """
        for i, data in enumerate(dataset):
            if "question_id" not in data:
                data = {"question_id": i, **data}
            task: Task = Task(**data)
            self.tasks.append(task)
        self.total_task_num = len(self.tasks)
        logger.info(f"initialize task completed, total task number is {self.total_task_num}")

    def run_task(self):
        """
            NL2SQL work flow.
        """
        # progress = Visual(self.total_task_num)
        for task in self.tasks:
            work = self.worker(task)
            # progress.update(1)
       
    def worker(self, task: Task) -> Work:
        """
        Worker function to process a single task.
        
        Args:
            task (Task): The task to be processed.
        
        Returns:
            tuple: The state of the task processing and task identifiers.
        """
        work = Work(task=task)
        logger.info(f"begin task: {task.db_id} {task.question_id}")
        work.status = engine(self.args, task)
        return work

