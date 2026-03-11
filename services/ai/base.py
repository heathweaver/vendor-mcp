"""Base classes for AI providers."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


class ProviderName(str, Enum):
    """Supported AI providers."""

    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    XAI = "xai"
    GOOGLE = "google"
    DEEPSEEK = "deepseek"


@dataclass
class AIResponse:
    """Standardized response from any AI provider."""

    content: str
    model: str
    provider: ProviderName
    raw_response: Any | None = None
    tokens_used: Optional[int] = None
    finish_reason: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "content": self.content,
            "model": self.model,
            "provider": self.provider.value,
            "tokens_used": self.tokens_used,
            "finish_reason": self.finish_reason,
        }


class AIProvider(ABC):
    """Abstract base class for AI providers."""

    provider_name: ProviderName

    @abstractmethod
    def __init__(self, api_key: str, **kwargs: Any):
        """Initialize the provider with API key."""

    @abstractmethod
    def complete(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.2,
        max_tokens: int = 1000,
        model: Optional[str] = None,
    ) -> AIResponse:
        """Generate a text completion."""

    @abstractmethod
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
        """Generate structured JSON from an image input."""

    @abstractmethod
    def is_available(self) -> bool:
        """Check if the provider is properly configured and available."""

    @property
    @abstractmethod
    def supports_vision(self) -> bool:
        """Whether the provider supports image input for this integration."""

    def complete_json(
        self,
        *,
        prompt: str,
        schema: dict[str, Any],
        system_prompt: Optional[str] = None,
        max_tokens: int = 500,
        model: Optional[str] = None,
    ) -> AIResponse:
        raise NotImplementedError(f"{self.provider_name.value} does not support structured JSON text output in this integration.")
