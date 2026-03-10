import os
import litellm
from pydantic import BaseModel

# Ensure we use an expected model, defaulting to a fast OpenAI model
DEFAULT_MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini")

def generate_structured_response(prompt: str, response_format: type[BaseModel], system_prompt: str = "You are a senior data analyst.", model: str = DEFAULT_MODEL):
    """
    Calls the LLM via litellm and forces the output to match the given Pydantic model.
    """
    response = litellm.completion(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        response_format=response_format
    )
    
    # Parse the structured JSON response into the Pydantic model
    content = response.choices[0].message.content
    return response_format.model_validate_json(content)

def generate_text_response(prompt: str, system_prompt: str = "You are a senior data analyst.", model: str = DEFAULT_MODEL) -> str:
    """
    Calls the LLM for a raw text response.
    """
    response = litellm.completion(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ]
    )
    return response.choices[0].message.content
