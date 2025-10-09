from workflow.agents.Meta_Agent import MetaAgent, register
from llm.llm_meta import Llm
from process_data.json_parser import parse_result

@register()
class Generator(MetaAgent):
    def __init__(self, model_info) -> None:
        super().__init__(model_info)

    def parse_result(self, result):
        sql = parse_result(result)
        return sql

    def _run(self) -> str | None:
        llm_instance = Llm(self.model_info, self._input)
        result = llm_instance._run()
        return self.parse_result(result)