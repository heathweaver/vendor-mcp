"""Main AI service that orchestrates multiple providers."""

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from .base import AIProvider, AIResponse, ProviderName
from .models import AnthropicProvider, DeepSeekProvider, GeminiProvider, OpenAIProvider, XAIProvider


load_dotenv(Path(__file__).resolve().parents[3] / ".env")


class AIService:
    """AI service with provider selection and image-analysis helpers."""

    def __init__(self, preferred_provider: Optional[ProviderName] = None):
        self.providers: dict[ProviderName, AIProvider] = {}
        self.preferred_provider = preferred_provider
        self._init_providers()

    def _init_providers(self) -> None:
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if openai_api_key:
            self.providers[ProviderName.OPENAI] = OpenAIProvider(api_key=openai_api_key)
        anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
        if anthropic_api_key:
            self.providers[ProviderName.ANTHROPIC] = AnthropicProvider(api_key=anthropic_api_key)
        google_api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
        if google_api_key:
            self.providers[ProviderName.GOOGLE] = GeminiProvider(api_key=google_api_key)
        xai_api_key = os.getenv("XAI_API_KEY")
        if xai_api_key:
            self.providers[ProviderName.XAI] = XAIProvider(api_key=xai_api_key)
        deepseek_api_key = os.getenv("DEEPSEEK_API_KEY")
        if deepseek_api_key:
            self.providers[ProviderName.DEEPSEEK] = DeepSeekProvider(api_key=deepseek_api_key)

    def get_provider(self, provider_name: Optional[ProviderName] = None, require_vision: bool = False) -> AIProvider:
        if provider_name and provider_name in self.providers:
            selected = self.providers[provider_name]
            if require_vision and not selected.supports_vision:
                raise ValueError(f"Provider {provider_name.value} is configured but does not support vision input.")
            return selected
        if self.preferred_provider and self.preferred_provider in self.providers:
            selected = self.providers[self.preferred_provider]
            if not require_vision or selected.supports_vision:
                return selected
        if self.providers:
            for selected in self.providers.values():
                if not require_vision or selected.supports_vision:
                    return selected
        raise ValueError("No AI providers configured. Set API keys in the root .env or environment.")

    def list_available_providers(self) -> list[str]:
        return [provider.value for provider in self.providers.keys()]

    def analyze_image(
        self,
        *,
        prompt: str,
        image_path: str,
        schema: dict,
        system_prompt: Optional[str] = None,
        model: Optional[str] = None,
        max_tokens: int = 3000,
        provider: Optional[ProviderName] = None,
    ) -> AIResponse:
        ai_provider = self.get_provider(provider, require_vision=True)
        return ai_provider.analyze_image(
            prompt=prompt,
            image_path=image_path,
            schema=schema,
            system_prompt=system_prompt,
            model=model,
            max_tokens=max_tokens,
        )

    def complete(
        self,
        *,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.2,
        max_tokens: int = 200,
        model: Optional[str] = None,
        provider: Optional[ProviderName] = None,
    ) -> AIResponse:
        ai_provider = self.get_provider(provider, require_vision=False)
        return ai_provider.complete(
            prompt=prompt,
            system_prompt=system_prompt,
            temperature=temperature,
            max_tokens=max_tokens,
            model=model,
        )

    def complete_json(
        self,
        *,
        prompt: str,
        schema: dict,
        system_prompt: Optional[str] = None,
        max_tokens: int = 500,
        model: Optional[str] = None,
        provider: Optional[ProviderName] = None,
    ) -> AIResponse:
        ai_provider = self.get_provider(provider, require_vision=False)
        return ai_provider.complete_json(
            prompt=prompt,
            schema=schema,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
            model=model,
        )
