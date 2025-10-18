import importlib, pkgutil, inspect, sys

from workflow import agents
from runner.enum_aggretion import Model
from typing import Iterable, Dict, Any, List
from workflow.agents.meta_agent import MetaAgent, get_registry

def autodiscover_agents() -> None:
    """
    Import all first-level submodules under the workflow.agents package and trigger the @register method.    
    """
    pkg_name = __package__  # "workflow.agents"

    # If __package__ is empty (very rare), fallback 
    if not pkg_name:
        pkg_name = MetaAgent.__module__.rsplit(".", 1)[0]

    pkg = sys.modules[pkg_name]

    for m in pkgutil.iter_modules(pkg.__path__):  # type: ignore[attr-defined]
        if m.name.startswith("_"):
            continue
        if m.name in {"meta_agent", "agent_factory"}:
            continue
        importlib.import_module(f"{pkg_name}.{m.name}")


def registry_agents(
    agents: List[Model],
    *,
    common_kwargs: Dict[str, Any] | None = None,
    per_agent_kwargs: Dict[str, Dict[str, Any]] | None = None,
    
) -> List[MetaAgent]:
    """
    names: The name or alias of the Agent to be instantiated
    common_kwargs: Input parameters shared by all agents
    per_agent_kwargs: Dedicated input parameter for a specific name, such as {"Generator": {"topk": 5}}
    """
    autodiscover_agents()
    reg = get_registry()

    common_kwargs = common_kwargs or {}
    per_agent_kwargs = per_agent_kwargs or {}

    out: List[MetaAgent] = []
    for i in range(len(agents)):
        n = agents[i].corresponding_agent
        # 大小写不敏感的查找
        cls = None
        for reg_name, reg_cls in reg.items():
            if reg_name.lower() == n.lower():
                cls = reg_cls
                break
        
        if cls is None:
            raise KeyError(f"Can't find registried agent: {n}; Could use: {list(reg.keys())}")

        if not inspect.isclass(cls) or not issubclass(cls, MetaAgent):
            raise TypeError(f"{n} Not MetaAgent sub class")

        kwargs = {**common_kwargs, **per_agent_kwargs.get(n, {})}
        out.append(cls(model_info=agents[i], **kwargs))
    return out
