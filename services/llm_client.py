import os
import json
from pydantic import BaseModel
from dotenv import load_dotenv

from .ai.service import AIService
from .ai.base import ProviderName

load_dotenv()

# Instantiate the service, optionally preferring Anthropic for complex reasoning 
# (which handles the JSON well due to Sonnet 3.5/3.7)
ACTIVE_PROVIDER = ProviderName(os.environ.get("LLM_PROVIDER", "openai"))
ai_service = AIService(preferred_provider=ACTIVE_PROVIDER)

def _patch_schema_for_openai(schema: dict) -> dict:
    """
    OpenAI strict JSON mode requires additionalProperties: false on EVERY object
    in the schema, including those defined in $defs.
    """
    import copy
    schema = copy.deepcopy(schema)

    def patch(node):
        if not isinstance(node, dict):
            return
        if node.get("type") == "object":
            node.setdefault("additionalProperties", False)
        # Recurse into all dict values and list items
        for v in node.values():
            if isinstance(v, dict):
                patch(v)
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        patch(item)

    if "schema" in schema:
        patch(schema["schema"])
    return schema

def generate_structured_response(prompt: str, response_format: type[BaseModel], system_prompt: str = "You are a senior data analyst.") -> BaseModel:
    """
    Calls the AI service and maps the JSON response back to a Pydantic model.
    """
    schema_dict = {
        "name": response_format.__name__,
        "schema": response_format.model_json_schema()
    }
    
    schema_dict = _patch_schema_for_openai(schema_dict)
    
    response = ai_service.complete_json(
        prompt=prompt,
        schema=schema_dict,
        system_prompt=system_prompt,
        max_tokens=4000
    )
    
    content = response.content
    try:
        return response_format.model_validate_json(content)
    except Exception as e:
        print(f"Failed to parse LLM Response: {content}")
        raise e

def generate_text_response(prompt: str, system_prompt: str = "You are a senior data analyst.", model: str = None) -> str:
    """
    Calls the LLM for a raw text response.
    """
    response = ai_service.complete(
        prompt=prompt,
        system_prompt=system_prompt,
        model=model
    )
    return response.content
