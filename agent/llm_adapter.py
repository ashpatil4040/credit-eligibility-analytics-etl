"""
llm_adapter.py — Local LLM abstraction with deterministic fallback.

LEARNING NOTE:
  This module wraps ChatOllama so the rest of the agent code never imports
  LangChain or Ollama directly. If you later switch to Azure OpenAI or AWS
  Bedrock, you change ONLY this file. Every other agent module stays the same.

  The fallback_response() function is critical: if Ollama is down or slow,
  the agent still produces a useful output from Python templates rather than
  crashing. This keeps your pipeline's health unaffected by LLM availability.
"""

import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

def get_llm():
    provider = os.getenv("LLM_PROVIDER", "ollama")

    if provider == "groq":
        from langchain_groq import ChatGroq
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            raise RuntimeError("GROQ_API_KEY not set in .env")
        return ChatGroq(
            model=os.getenv("GROQ_MODEL", "llama3-8b-8192"),
            temperature=float(os.getenv("LLM_TEMPERATURE", "0.0")),
            api_key=api_key,
        )

    # fallback to ollama
    from langchain_ollama import ChatOllama
    return ChatOllama(
        model=os.getenv("OLLAMA_MODEL", "mistral"),
        base_url=os.getenv("OLLAMA_BASE_URL", "http://ollama:11434"),
        temperature=float(os.getenv("LLM_TEMPERATURE", "0.0")),
        num_ctx=int(os.getenv("OLLAMA_NUM_CTX", "2048")),
        num_predict=int(os.getenv("OLLAMA_NUM_PREDICT", "512")),
    )


def invoke_llm(prompt: str, context: Optional[str] = None) -> str:
    """
    Send a prompt to the LLM and return the text response.
    Falls back to a deterministic template response if LLM is unavailable.

    Args:
        prompt:  The main instruction or question for the LLM.
        context: Optional additional context (logs, trace data, metrics).

    Returns:
        A string response — either from the LLM or from the fallback template.

    LEARNING NOTE — why always return a string?
      Agent tools expect string outputs. Returning None or raising an exception
      breaks the ReAct loop. The fallback ensures the loop always continues
      even when the LLM is unreachable.
    """
    llm = get_llm()

    full_prompt = prompt
    if context:
        full_prompt = f"{prompt}\n\nContext:\n{context}"

    if llm is not None:
        try:
            response = llm.invoke(full_prompt)
            # LangChain returns an AIMessage object; .content is the text
            return response.content
        except Exception as exc:
            logger.warning("LLM invocation failed (%s). Using fallback.", exc)

    return _fallback_response(prompt)


def _fallback_response(prompt: str) -> str:
    """
    Deterministic template-based response when LLM is unavailable.

    LEARNING NOTE:
      This is intentionally simple. The agent still works in degraded mode —
      it will use rule-based logic rather than LLM reasoning. This is better
      than failing silently or crashing the pipeline.
    """
    prompt_lower = prompt.lower()

    if "fail" in prompt_lower or "error" in prompt_lower:
        return (
            "[FALLBACK MODE — Ollama unavailable] "
            "A failure was detected. Manual investigation required. "
            "Check Airflow task logs and Jaeger traces for details."
        )
    if "summary" in prompt_lower or "status" in prompt_lower:
        return (
            "[FALLBACK MODE — Ollama unavailable] "
            "Pipeline run completed. See Airflow UI and Jaeger for full metrics."
        )
    return (
        "[FALLBACK MODE — Ollama unavailable] "
        "Agent received your request but the local LLM is unreachable. "
        "Please check that the local Ollama service is running on port 11434."
    )