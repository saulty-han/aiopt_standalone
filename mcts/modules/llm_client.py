"""
mcts.utils.llm_client - LLM API client with connection pooling and retry logic.

Wraps the OpenAI-compatible chat API. Supports multiple API endpoints with
cooldown-based failover. All calls are synchronous (threaded concurrency is
handled by the caller via ThreadPoolExecutor).
"""
from __future__ import annotations

import json
import random
import threading
import time
import uuid
from typing import Dict, List, Optional, Tuple

import requests

from mcts.types import LLMCompletion, LLMRequest, LLMStatus, MCTSConfig

from mcts import logger


# ---------------------------------------------------------------------------
# API Pool (thread-safe)
# ---------------------------------------------------------------------------

class _APIEndpoint:
    __slots__ = ("url", "key", "model")

    def __init__(self, url: str, key: str, model: str) -> None:
        self.url = url
        self.key = key
        self.model = model


class APIPool:
    """Manages multiple LLM API endpoints with cooldown-based rotation."""

    def __init__(
        self,
        endpoints: List[List[str]],
        cooldown_seconds: float = 30.0,
    ) -> None:
        if not endpoints:
            raise ValueError("At least one [url, key, model] endpoint is required")
        self._endpoints = [_APIEndpoint(e[0], e[1], e[2]) for e in endpoints]
        self._total = len(self._endpoints)
        self._cooldown_seconds = cooldown_seconds
        self._cooldown_until: Dict[int, float] = {}  # idx -> timestamp
        self._lock = threading.Lock()

    @property
    def total(self) -> int:
        return self._total

    def available_count(self) -> int:
        with self._lock:
            now = time.time()
            return sum(
                1
                for i in range(self._total)
                if self._cooldown_until.get(i, 0.0) <= now
            )

    def get_available(self) -> Optional[_APIEndpoint]:
        with self._lock:
            now = time.time()
            available = [
                (i, ep)
                for i, ep in enumerate(self._endpoints)
                if self._cooldown_until.get(i, 0.0) <= now
            ]
            if not available:
                return None
            _, ep = random.choice(available)
            return ep

    def mark_cooldown(self, endpoint: _APIEndpoint) -> None:
        with self._lock:
            for i, ep in enumerate(self._endpoints):
                if ep is endpoint:
                    self._cooldown_until[i] = time.time() + self._cooldown_seconds
                    break

    def all_in_cooldown(self) -> bool:
        return self.get_available() is None


# ---------------------------------------------------------------------------
# LLM Client
# ---------------------------------------------------------------------------

class LLMClient:
    """Synchronous LLM client for one completion at a time.

    Thread-safe: multiple threads may call ``complete()`` concurrently.
    """

    def __init__(self, config: MCTSConfig) -> None:
        self._pool = APIPool(
            config.llm_api_url_key,
            cooldown_seconds=config.api_cooldown_seconds,
        )
        self._cooldown_seconds = config.api_cooldown_seconds
        self._wsid = config.wsid
        self._temperature = config.temperature
        self._top_p = config.top_p
        self._top_k = config.top_k
        self._max_tokens = config.max_tokens
        self._repetition_penalty = config.repetition_penalty
        self._stop_tokens = list(config.stop_tokens) if config.stop_tokens else []
        self._chat_template_kwargs = dict(config.chat_template_kwargs or {})
        self._max_retries = config.tpm_rate_limit_max_retries
        self._all_failed_wait = config.api_all_failed_wait_seconds

    def complete(self, prompt: str) -> LLMCompletion:
        """Send a prompt and return a single completion.

        Handles retries and endpoint rotation internally.
        - LLMStatus.HTTP_ERROR: non-retriable, returned immediately.
        - LLMStatus.UNAVAILABLE: endpoint rotated into cooldown, retried.
        - LLMStatus.OK: success.
        """
        all_cooldown_count = 0
        total_eps = self._pool.total

        while all_cooldown_count <= self._max_retries:
            endpoint = self._pool.get_available()

            if endpoint is None:
                all_cooldown_count += 1
                if all_cooldown_count > self._max_retries:
                    logger.error(
                        f"[LLM] All {total_eps} API endpoints are rate-limited/unavailable; "
                        f"still failing after {self._max_retries} retry rounds, giving up this request"
                    )
                    return LLMCompletion(
                        text="Error: All API endpoints exhausted after max retries",
                        status=LLMStatus.RATE_LIMIT_EXCEEDED,
                        input_chars=len(prompt),
                    )
                logger.warning(
                    f"[LLM] All rate-limited: all {total_eps} API endpoints are in cooldown, "
                    f"retry round {all_cooldown_count}/{self._max_retries}, "
                    f"waiting {self._all_failed_wait}s before retrying all"
                )
                time.sleep(self._all_failed_wait)
                continue

            result = self._call_endpoint(endpoint, prompt)

            if result.status == LLMStatus.OK:
                return result

            # HTTP error → non-retriable, return immediately
            if result.status == LLMStatus.HTTP_ERROR:
                logger.error(
                    f"[LLM] API {endpoint.url} returned HTTP error (no retry): {result.text}"
                )
                return result

            # UNAVAILABLE → endpoint unavailable (network/rate-limit), rotate
            self._pool.mark_cooldown(endpoint)
            if self._pool.all_in_cooldown():
                all_cooldown_count += 1
                if all_cooldown_count > self._max_retries:
                    break
                logger.warning(
                    f"[LLM] All rate-limited: all {total_eps} API endpoints entered cooldown, "
                    f"retry round {all_cooldown_count}/{self._max_retries}, "
                    f"waiting {self._all_failed_wait}s before retrying all"
                )
                time.sleep(self._all_failed_wait)
            else:
                logger.warning(
                    f"[LLM] API {endpoint.url} timed out/rate-limited, cooling down {self._cooldown_seconds}s, "
                    f"switching to the next API to retry (available {self._pool.available_count()}/{total_eps})"
                )

        return LLMCompletion(
            text="Error: All API endpoints exhausted after max retries",
            status=LLMStatus.RATE_LIMIT_EXCEEDED,
            input_chars=len(prompt),
        )

    def _call_endpoint(self, endpoint: _APIEndpoint, prompt: str) -> LLMCompletion:
        """Attempt a single non-stream call.

        Always returns an LLMCompletion:
        - LLMStatus.OK on success
        - LLMStatus.HTTP_ERROR on HTTP 4xx/5xx (non-retriable)
        - LLMStatus.UNAVAILABLE on network/rate-limit errors (retriable)
        """
        json_data = {
            "query_id": f"query_{uuid.uuid4()}",
            "messages": [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt},
            ],
            "model": endpoint.model,
            "temperature": self._temperature,
            "top_p": self._top_p,
            "top_k": self._top_k,
            "repetition_penalty": self._repetition_penalty,
            "output_seq_len": self._max_tokens,
            "max_input_seq_len": 120 * 1024,
            "stream": False,
            "stop": self._stop_tokens if self._stop_tokens else None,
        }
        if self._chat_template_kwargs:
            json_data["chat_template_kwargs"] = self._chat_template_kwargs

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {endpoint.key}",
        }
        if self._wsid:
            headers["Wsid"] = self._wsid

        start = time.time()
        try:
            resp = requests.post(endpoint.url, headers=headers, json=json_data, timeout=600)

            # Check rate-limit before raise_for_status
            resp_text = resp.text
            if "limit exceeded" in resp_text.lower():
                duration = time.time() - start
                logger.warning(
                    f"[LLM] API rate-limited: {endpoint.url} returned limit exceeded "
                    f"(took {duration:.1f}s)"
                )
                return LLMCompletion(
                    text=f"Error: Rate limit from {endpoint.url}",
                    status=LLMStatus.RATE_LIMIT_EXCEEDED,
                    input_chars=len(prompt),
                    latency_seconds=duration,
                )

            resp.raise_for_status()
            data = resp.json()

            text = ""
            stop_reason = None

            if "choices" in data and data["choices"]:
                choice = data["choices"][0]
                message = choice.get("message", {})
                reasoning = message.get("reasoning_content", "") or ""
                content = message.get("content", "") or ""
                text = (reasoning + content).strip()
                stop_reason = choice.get("stop_reason")

                # Local stop-token detection (fallback)
                if stop_reason is None and self._stop_tokens:
                    for token in self._stop_tokens:
                        if token in text:
                            stop_reason = token
                            break

            duration = time.time() - start
            return LLMCompletion(
                text=text,
                status=LLMStatus.OK,
                stop_reason=stop_reason,
                input_chars=len(prompt),
                output_chars=len(text),
                latency_seconds=duration,
            )

        except requests.exceptions.HTTPError as e:
            duration = time.time() - start
            try:
                body = e.response.text[:200] if e.response is not None else ""
            except Exception:
                body = ""
            error_detail = f"HTTP {e.response.status_code if e.response is not None else '?'} {e}: {body}"
            logger.error(f"[LLM] HTTP error from {endpoint.url} after {duration:.1f}s: {error_detail}")
            return LLMCompletion(
                text=f"Error: HTTP error from {endpoint.url}: {error_detail}",
                status=LLMStatus.HTTP_ERROR,
                input_chars=len(prompt),
                latency_seconds=duration,
            )
        except TimeoutError as e:
            duration = time.time() - start
            logger.warning(
                f"[LLM] API timed out: {endpoint.url} timed out after {duration:.1f}s "
                f"({type(e).__name__})"
            )
            return LLMCompletion(
                text=f"Error: Timeout from {endpoint.url}: {e}",
                status=LLMStatus.UNAVAILABLE,
                input_chars=len(prompt),
                latency_seconds=duration,
            )
        except Exception as e:
            duration = time.time() - start
            logger.warning(
                f"[LLM] API network error: {endpoint.url} (took {duration:.1f}s) "
                f"{type(e).__name__}: {e}"
            )
            return LLMCompletion(
                text=f"Error: {type(e).__name__} from {endpoint.url}: {e}",
                status=LLMStatus.UNAVAILABLE,
                input_chars=len(prompt),
                latency_seconds=duration,
            )
