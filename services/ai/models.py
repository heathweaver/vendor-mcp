"""Concrete AI provider implementations."""

import base64
import json
import mimetypes
from typing import Any, Optional

import anthropic
from google import genai
from google.genai import types
from openai import OpenAI

from .base import AIProvider, AIResponse, ProviderName


_COMPLETION_TOKEN_MODELS = ("gpt-5", "o1", "o3", "o4")

def _uses_completion_tokens(model: str) -> bool:
    """Returns True for models that require max_completion_tokens instead of max_tokens."""
    return any(model.startswith(prefix) for prefix in _COMPLETION_TOKEN_MODELS)


class OpenAICompatibleProvider(AIProvider):
    """OpenAI-compatible provider with text and optional vision support."""

    provider_name: ProviderName
    default_model: str
    default_base_url: Optional[str] = None
    _supports_vision = True

    def __init__(self, api_key: str, **kwargs: Any):
        base_url = kwargs.get("base_url", self.default_base_url)
        self.client = OpenAI(api_key=api_key, base_url=base_url)

    @property
    def supports_vision(self) -> bool:
        return self._supports_vision

    def complete(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.2,
        max_tokens: int = 1000,
        model: Optional[str] = None,
    ) -> AIResponse:
        selected_model = model or self.default_model
        input_messages = []
        if system_prompt:
            input_messages.append(
                {
                    "role": "system",
                    "content": [{"type": "text", "text": system_prompt}],
                }
            )
        input_messages.append(
            {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
            }
        )
        token_kwarg = "max_completion_tokens" if _uses_completion_tokens(selected_model) else "max_tokens"
        request_args = {
            "model": selected_model,
            "messages": input_messages,
            token_kwarg: max_tokens,
        }
        if self._supports_temperature(selected_model):
            request_args["temperature"] = temperature
        response = self.client.chat.completions.create(**request_args)
        return self._build_response(response, selected_model)

    def analyze_image(
        self,
        *,
        prompt: str,
        image_path: str,
        schema: dict[str, Any],
        system_prompt: Optional[str] = None,
        model: Optional[str] = None,
        max_tokens: int = 3000,
    ) -> AIResponse:
        if not self.supports_vision:
            raise NotImplementedError(f"{self.provider_name.value} does not support vision input in this integration.")

        input_messages = []
        if system_prompt:
            input_messages.append(
                {
                    "role": "system",
                    "content": [{"type": "text", "text": system_prompt}],
                }
            )
        input_messages.append(
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {"url": self._image_as_data_url(image_path), "detail": "high"},
                    },
                ],
            }
        )
        _model = model or self.default_model
        token_kwarg = "max_completion_tokens" if _uses_completion_tokens(_model) else "max_tokens"
        response = self.client.chat.completions.create(
            model=_model,
            messages=input_messages,
            **{token_kwarg: max_tokens},
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": schema.get("name", "SchemaName"),
                    "schema": schema.get("schema", schema),
                    "strict": True,
                }
            },
        )
        return self._build_response(response, _model)

    def complete_json(
        self,
        *,
        prompt: str,
        schema: dict[str, Any],
        system_prompt: Optional[str] = None,
        max_tokens: int = 500,
        model: Optional[str] = None,
    ) -> AIResponse:
        selected_model = model or self.default_model
        input_messages = []
        if system_prompt:
            input_messages.append(
                {
                    "role": "system",
                    "content": [{"type": "text", "text": system_prompt}],
                }
            )
        input_messages.append(
            {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
            }
        )
        token_kwarg = "max_completion_tokens" if _uses_completion_tokens(selected_model) else "max_tokens"
        response = self.client.chat.completions.create(
            model=selected_model,
            messages=input_messages,
            **{token_kwarg: max_tokens},
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": schema.get("name", "SchemaName"),
                    "schema": schema.get("schema", schema),
                    "strict": True,
                }
            },
        )
        return self._build_response(response, selected_model)

    def is_available(self) -> bool:
        return bool(self.client.api_key)

    def _build_response(self, response: Any, model: str) -> AIResponse:
        usage = getattr(response, "usage", None)
        finish_reason = None
        content = ""
        if getattr(response, "choices", None):
            finish_reason = getattr(response.choices[0], "finish_reason", None)
            content = getattr(response.choices[0].message, "content", "")
        return AIResponse(
            content=content,
            model=model,
            provider=self.provider_name,
            raw_response=response,
            tokens_used=getattr(usage, "total_tokens", None),
            finish_reason=finish_reason,
        )

    def _image_as_data_url(self, image_path: str) -> str:
        mime_type, _ = mimetypes.guess_type(image_path)
        mime_type = mime_type or "image/png"
        with open(image_path, "rb") as handle:
            encoded = base64.b64encode(handle.read()).decode("ascii")
        return f"data:{mime_type};base64,{encoded}"

    def _supports_temperature(self, model: str) -> bool:
        # Avoid setting temp for models like o1 or o3
        return not model.startswith("o") and not model.startswith("gpt-5")


class OpenAIProvider(OpenAICompatibleProvider):
    provider_name = ProviderName.OPENAI
    default_model = "gpt-4o"


class XAIProvider(OpenAICompatibleProvider):
    provider_name = ProviderName.XAI
    default_model = "grok-3"
    default_base_url = "https://api.x.ai/v1"


class DeepSeekProvider(OpenAICompatibleProvider):
    provider_name = ProviderName.DEEPSEEK
    default_model = "deepseek-chat"
    default_base_url = "https://api.deepseek.com"
    _supports_vision = False


class AnthropicProvider(AIProvider):
    provider_name = ProviderName.ANTHROPIC
    default_model = "claude-3-7-sonnet-20250219"

    def __init__(self, api_key: str, **kwargs: Any):
        self.client = anthropic.Anthropic(api_key=api_key)

    @property
    def supports_vision(self) -> bool:
        return True

    def complete(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.2,
        max_tokens: int = 1000,
        model: Optional[str] = None,
    ) -> AIResponse:
        response = self.client.messages.create(
            model=model or self.default_model,
            system=system_prompt or "",
            temperature=temperature,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        )
        return self._build_response(response, model or self.default_model)

    def analyze_image(
        self,
        *,
        prompt: str,
        image_path: str,
        schema: dict[str, Any],
        system_prompt: Optional[str] = None,
        model: Optional[str] = None,
        max_tokens: int = 3000,
    ) -> AIResponse:
        schema_text = json.dumps(schema.get("schema", schema), indent=2)
        response = self.client.messages.create(
            model=model or self.default_model,
            system=(
                (system_prompt or "")
                + "\nReturn only valid JSON matching this schema exactly:\n"
                + schema_text
            ).strip(),
            max_tokens=max_tokens,
            temperature=0,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": self._image_source(image_path),
                        },
                        {
                            "type": "text",
                            "text": prompt,
                        },
                    ],
                }
            ],
        )
        return self._build_response(response, model or self.default_model)

    def complete_json(
        self,
        *,
        prompt: str,
        schema: dict[str, Any],
        system_prompt: Optional[str] = None,
        max_tokens: int = 500,
        model: Optional[str] = None,
    ) -> AIResponse:
        schema_text = json.dumps(schema.get("schema", schema), indent=2)
        combined_system = (
            (system_prompt or "")
            + "\nReturn ONLY valid JSON matching this schema exactly, with NO additional text or markdown:\n"
            + schema_text
        ).strip()
        response = self.client.messages.create(
            model=model or self.default_model,
            system=combined_system,
            max_tokens=max_tokens,
            temperature=0,
            messages=[{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        )
        return self._build_response(response, model or self.default_model)

    def is_available(self) -> bool:
        return bool(self.client.api_key)

    def _build_response(self, response: Any, model: str) -> AIResponse:
        text_parts = [block.text for block in response.content if getattr(block, "type", None) == "text"]
        usage = getattr(response, "usage", None)
        return AIResponse(
            content="".join(text_parts).strip(),
            model=model,
            provider=self.provider_name,
            raw_response=response,
            tokens_used=getattr(usage, "input_tokens", 0) + getattr(usage, "output_tokens", 0),
            finish_reason=getattr(response, "stop_reason", None),
        )

    def _image_source(self, image_path: str) -> dict[str, str]:
        mime_type, _ = mimetypes.guess_type(image_path)
        mime_type = mime_type or "image/png"
        with open(image_path, "rb") as handle:
            encoded = base64.b64encode(handle.read()).decode("ascii")
        return {
            "type": "base64",
            "media_type": mime_type,
            "data": encoded,
        }


class GeminiProvider(AIProvider):
    provider_name = ProviderName.GOOGLE
    default_model = "gemini-2.5-flash"

    def __init__(self, api_key: str, **kwargs: Any):
        self.client = genai.Client(api_key=api_key)
        self.api_key = api_key

    @property
    def supports_vision(self) -> bool:
        return True

    def complete(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.2,
        max_tokens: int = 1000,
        model: Optional[str] = None,
    ) -> AIResponse:
        config_kwargs = {
            "temperature": temperature,
            "max_output_tokens": max_tokens,
        }
        if system_prompt:
            config_kwargs["system_instruction"] = system_prompt

        response = self.client.models.generate_content(
            model=model or self.default_model,
            contents=prompt,
            config=types.GenerateContentConfig(**config_kwargs),
        )
        return self._build_response(response, model or self.default_model)

    def analyze_image(
        self,
        *,
        prompt: str,
        image_path: str,
        schema: dict[str, Any],
        system_prompt: Optional[str] = None,
        model: Optional[str] = None,
        max_tokens: int = 3000,
    ) -> AIResponse:
        with open(image_path, "rb") as handle:
            image_bytes = handle.read()

        config_kwargs = {
            "temperature": 0,
            "max_output_tokens": max_tokens,
            "response_mime_type": "application/json",
            "response_schema": schema.get("schema", schema),
        }
        if system_prompt:
            config_kwargs["system_instruction"] = system_prompt

        response = self.client.models.generate_content(
            model=model or self.default_model,
            contents=[
                prompt,
                types.Part.from_bytes(
                    data=image_bytes,
                    mime_type=mimetypes.guess_type(image_path)[0] or "image/png",
                ),
            ],
            config=types.GenerateContentConfig(**config_kwargs),
        )
        return self._build_response(response, model or self.default_model)

    def complete_json(
        self,
        *,
        prompt: str,
        schema: dict[str, Any],
        system_prompt: Optional[str] = None,
        max_tokens: int = 500,
        model: Optional[str] = None,
    ) -> AIResponse:
        config_kwargs = {
            "temperature": 0,
            "max_output_tokens": max_tokens,
            "response_mime_type": "application/json",
            "response_schema": schema.get("schema", schema),
        }
        if system_prompt:
            config_kwargs["system_instruction"] = system_prompt

        response = self.client.models.generate_content(
            model=model or self.default_model,
            contents=prompt,
            config=types.GenerateContentConfig(**config_kwargs),
        )
        return self._build_response(response, model or self.default_model)

    def is_available(self) -> bool:
        return bool(self.api_key)

    def _build_response(self, response: Any, model: str) -> AIResponse:
        usage = getattr(response, "usage_metadata", None)
        total_tokens = None
        if usage is not None:
            total_tokens = getattr(usage, "total_token_count", None)
        content = getattr(response, "text", None)
        if not content:
            parts = []
            for candidate in getattr(response, "candidates", []) or []:
                candidate_content = getattr(candidate, "content", None)
                for part in getattr(candidate_content, "parts", None) or []:
                    text = getattr(part, "text", None)
                    if text:
                        parts.append(text)
            content = "".join(parts)
        return AIResponse(
            content=content,
            model=model,
            provider=self.provider_name,
            raw_response=response,
            tokens_used=total_tokens,
            finish_reason=None,
        )
