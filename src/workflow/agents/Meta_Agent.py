# 延迟解析
from __future__ import annotations
import inspect, threading, importlib

from abc import ABCMeta, abstractmethod
from typing import Type, Dict, Iterable, Callable, Optional
from runner.enum_aggretion import Model, Request, Response

_REGISTRY: Dict[str, Type[MetaAgent]] = {}
_LOCK = threading.RLock()

class MetaAgent(metaclass=ABCMeta):
    __agent_name__: str
    __agent_aliases__: tuple[str, ...] 
    def __init__(self, model_info) -> None:
        self.model_info: Model = model_info
        self._input: Optional[Request] = None
        self._output: Optional[Response] = None        

    @abstractmethod
    def _run(self) -> str | None:
        pass

def _add(name: str, cls: Type[MetaAgent], *, override: bool):
    with _LOCK:
        if not override and name in _REGISTRY and _REGISTRY[name] is not cls:
            exist = _REGISTRY[name]
            raise ValueError(f"Agent name wrong: '{name}' has binded to {exist.__module__}.{exist.__name__}")
        _REGISTRY[name] = cls

def register(
    name: str | None = None,
    *, aliases: Iterable[str] = (),
    override: bool = False,
) -> Callable[[Type[MetaAgent]], Type[MetaAgent]]:
    """
      @register()                   
      @register("Generator")       
      @register(aliases=("Gen",))  # extra alias
    """
    def deco(cls: Type[MetaAgent]) -> Type[MetaAgent]:
        if not inspect.isclass(cls) or not issubclass(cls, MetaAgent):
            raise TypeError("@register only acts MetaAgent sub-class")
        
        # 跳过抽象类（中间基类不参与注册）
        if inspect.isabstract(cls):
            return cls

        final_name = name or getattr(cls, "NAME", None) or cls.__name__
        _add(final_name, cls, override=override)
        for a in aliases:
            _add(a, cls, override=override)

        # 帮助排查
        cls.__agent_name__ = final_name
        cls.__agent_aliases__ = tuple(aliases)
        return cls
    return deco

def get_registry() -> Dict[str, Type[MetaAgent]]:
    return dict(_REGISTRY)