# -*- coding: utf-8 -*-
# @Time    : 2025-07-28 15:36
# @Author  : jwm
# @File    : task.py
# @description: Define Task Enum

from typing import Optional
from pydantic import BaseModel

class Task(BaseModel):
    """
    Represents a task with question and database details.

    Attributes:
        question_id (int): The unique identifier for the question.
        db_id (str): The database identifier.
        question (str): The question text.
        evidence (str): Supporting evidence for the question.
        SQL (Optional[str]): The SQL query associated with the task, if any.
        difficulty (Optional[str]): The difficulty level of the task, if specified.

    """
    question_id: int
    db_id: str
    question: str
    evidence: str
    SQL: Optional[str] = None
    difficulty: Optional[str] = None


class Model(BaseModel):
    """
        In the model <--> agent binding stage, a media variable to create.
        Convert json elements to python elements

        Attributes:
            model_name: test
            model_type: local or API
            model_path: if local, the path of model, else empty
            API_KEY: if API, the key of API, else empty
            BASE_URL: if API, the base url of API, else empty
            corresponding_agent: The corresponding agent used in the FrameWork
            description: This is a test model, not to be read
            template: The prompt templates of llm used in agent.

    """
    model_name: str 
    model_type: str 
    model_path: str
    API_KEY: Optional[str] = None
    BASE_URL: Optional[str] = None
    corresponding_agent: str 
    description: str
    template_name: str
    output_name: str


class Request(BaseModel):
    """
        The unified request body for agent communication in the framework
        
    """
    template: str
    _schema: Optional[str] = None


class Response(BaseModel):
    """
        The unified response body for agent communication in the framework

    """
    status: bool
    result: Optional[str]