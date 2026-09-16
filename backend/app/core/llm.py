import httpx
import json
import logging
from typing import AsyncGenerator, Any, Optional
from backend.app.config import settings

logger = logging.getLogger(__name__)

class OllamaClient:
    def __init__(self, base_url: str = settings.OLLAMA_BASE_URL, default_model: str = settings.OLLAMA_MODEL):
        self.base_url = base_url.rstrip("/")
        self.default_model = default_model

    async def check_health(self) -> dict[str, Any]:
        """Checks if Ollama is running and lists installed models."""
        try:
            async with httpx.AsyncClient(timeout=4.0) as client:
                res = await client.get(f"{self.base_url}/api/tags")
                if res.status_code == 200:
                    data = res.json()
                    models = [m.get("name") for m in data.get("models", [])]
                    return {
                        "status": "online",
                        "base_url": self.base_url,
                        "current_model": self.default_model,
                        "available_models": models,
                        "model_ready": any(self.default_model in m for m in models)
                    }
        except Exception as e:
            return {
                "status": "offline",
                "base_url": self.base_url,
                "current_model": self.default_model,
                "available_models": [],
                "model_ready": False,
                "error": str(e)
            }
        return {"status": "error", "model_ready": False}

    async def chat(self, messages: list[dict[str, str]], model: Optional[str] = None, stream: bool = False, temperature: float = 0.7) -> dict[str, Any]:
        """Sends chat request to Ollama."""
        target_model = model or self.default_model
        payload = {
            "model": target_model,
            "messages": messages,
            "stream": stream,
            "options": {
                "temperature": temperature
            }
        }
        
        try:
            async with httpx.AsyncClient(timeout=settings.OLLAMA_TIMEOUT) as client:
                res = await client.post(f"{self.base_url}/api/chat", json=payload)
                if res.status_code == 200:
                    data = res.json()
                    message = data.get("message", {})
                    return {
                        "role": "assistant",
                        "content": message.get("content", ""),
                        "model": target_model,
                        "done": True
                    }
                else:
                    return {
                        "role": "assistant",
                        "content": f"Ollama Error ({res.status_code}): {res.text}",
                        "model": target_model,
                        "done": True
                    }
        except Exception as e:
            logger.warning(f"Ollama connection error: {e}")
            return {
                "role": "assistant",
                "content": f"[JARVIS Core Offline Response]: Processing query locally. (Ollama connection: {str(e)})",
                "model": target_model,
                "done": True
            }

    async def chat_stream(self, messages: list[dict[str, str]], model: Optional[str] = None, temperature: float = 0.7) -> AsyncGenerator[str, None]:
        """Streams tokens from Ollama chat API."""
        target_model = model or self.default_model
        payload = {
            "model": target_model,
            "messages": messages,
            "stream": True,
            "options": {
                "temperature": temperature
            }
        }
        
        try:
            async with httpx.AsyncClient(timeout=settings.OLLAMA_TIMEOUT) as client:
                async with client.stream("POST", f"{self.base_url}/api/chat", json=payload) as response:
                    if response.status_code != 200:
                        yield f"Ollama connection status: {response.status_code}"
                        return
                        
                    async for line in response.aiter_lines():
                        if line:
                            try:
                                chunk = json.loads(line)
                                msg = chunk.get("message", {})
                                content = msg.get("content", "")
                                if content:
                                    yield content
                            except Exception:
                                pass
        except Exception as e:
            yield f" [LLM Stream Exception: {str(e)}]"

    async def get_embedding(self, text: str, model: Optional[str] = None) -> list[float]:
        """Generates embeddings via Ollama embeddings endpoint."""
        target_model = model or settings.OLLAMA_EMBED_MODEL
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                res = await client.post(f"{self.base_url}/api/embeddings", json={"model": target_model, "prompt": text})
                if res.status_code == 200:
                    return res.json().get("embedding", [])
        except Exception:
            pass
        return []

ollama_client = OllamaClient()
