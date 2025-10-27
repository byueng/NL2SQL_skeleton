from openai import OpenAI
from typing import Optional
from transformers import AutoTokenizer, AutoModelForCausalLM
from pathlib import Path
from loguru import logger

from runner.enum_aggretion import Model, Request

class Llm:
    def __init__(
            self,
            model_info: Model,
            request: Optional[Request] = None
        ) -> None:
        self.model_info = model_info
        self.request = request

    def _run(self) -> str | None:
        if self.model_info.model_type == "local":
            return self.llm_local_call()
        else:
            return self.llm_chain_call()

    def llm_chain_call(self) -> str | None:
        if self.request is None or not hasattr(self.request, "template"):
            raise ValueError("Request object is None or missing 'template' attribute.")
        
        client = OpenAI(api_key=self.model_info.API_KEY, base_url=self.model_info.BASE_URL)
        resp = client.chat.completions.create(
            model = self.model_info.model_name,
            messages = [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": self.request.template}
            ]
        )
        answer = resp.choices[0].message.content
        return answer 

    def llm_local_call(self) -> str:
        model_path: Path = Path(self.model_info.model_path + self.model_info.model_name)

        model = AutoModelForCausalLM.from_pretrained(
            model_path,
            torch_dtype="auto",
            device_map="auto",
            ignore_mismatched_sizes=True,
            output_loading_info=False  # 关键参数
        )
        tokenizer = AutoTokenizer.from_pretrained(model_path)

        if self.request is None or not hasattr(self.request, "template"):
            raise ValueError("Request object is None or missing 'template' attribute.")
        
        prompt: str = self.request.template
        messages = [
            {"role": "user", "content": prompt}
        ]
        text: str = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True
        )
        model_inputs = tokenizer([text], return_tensors="pt").to(model.device)

        generated_ids = model.generate(
            **model_inputs,
            max_new_tokens=32768
        )
        generated_ids = [
            output_ids[len(input_ids):] for input_ids, output_ids in zip(model_inputs.input_ids, generated_ids)
        ]
        response: str = tokenizer.batch_decode(generated_ids, skip_special_tokens=True)[0]
        return response
