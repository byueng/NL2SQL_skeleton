from typing import Optional
from workflow.agents.meta_agent import MetaAgent, register
from llm.llm_meta import Llm
from process_data.parser_sql import extract_sql
from runner.enum_aggretion import Model

@register()
class Generator(MetaAgent):
    def __init__(self, model_info: Model) -> None:
        super().__init__(model_info)

    def parse_result(self, result: Optional[str]) -> Optional[str]:
        if result is None:
            return None
        sql: str = extract_sql(result)
        return sql

    def _run(self) -> str | None:
        llm_instance: Llm = Llm(self.model_info, self._input)
        result: Optional[str] = llm_instance._run()
        return self.parse_result(result)