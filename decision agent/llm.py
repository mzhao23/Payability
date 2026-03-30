from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()


MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

SKILL_PATH = Path(__file__).resolve().parent / "SKILL.md"
SKILL_PROMPT = SKILL_PATH.read_text(encoding="utf-8")
SYSTEM_PROMPT = (
    f"{SKILL_PROMPT}\n\n"
    "# Output Format\n"
    "Respond ONLY with a valid JSON object. No preamble, no markdown fences.\n"
    '{"final_score": <integer 5-10>, "reason": "<exactly 3 English sentences>"}'
)


class LLMValidationError(Exception):
    pass


def _count_sentences(reason: str) -> int:
    """Count sentences without splitting on decimal points (e.g. 8.33, 5.2%)."""
    text = reason.strip()
    if not text:
        return 0
    parts = re.split(r"(?<=[.!?])\s+", text)
    return len([p for p in parts if p.strip()])


def _extract_text(response_json: dict[str, Any]) -> str:
    try:
        return response_json["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception as exc:  # noqa: BLE001
        raise LLMValidationError(f"Unexpected Gemini response shape: {response_json}") from exc


def _validate_output(raw: str) -> dict[str, Any]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise LLMValidationError(f"Invalid JSON from model: {raw}") from exc

    final_score = parsed.get("final_score")
    reason = parsed.get("reason")

    if not isinstance(final_score, int) or not (5 <= final_score <= 10):
        raise LLMValidationError(f"final_score must be integer in [5,10]. Got: {final_score}")
    if not isinstance(reason, str) or not reason.strip():
        raise LLMValidationError("reason must be a non-empty string")

    sentence_count = _count_sentences(reason)
    if sentence_count != 3:
        raise LLMValidationError(f"reason must have exactly 3 sentences. Got {sentence_count}")

    return {"final_score": final_score, "reason": reason.strip()}


def _call_once(profile: dict[str, Any]) -> dict[str, Any]:
    if not GEMINI_API_KEY:
        raise ValueError("Missing GEMINI_API_KEY in env")

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL_NAME}:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "system_instruction": {"parts": [{"text": SYSTEM_PROMPT}]},
        "contents": [{"role": "user", "parts": [{"text": json.dumps(profile, ensure_ascii=False)}]}],
        "generationConfig": {
            "temperature": 0.1,
            "responseMimeType": "application/json",
        },
    }
    res = requests.post(url, json=payload, timeout=60)
    res.raise_for_status()
    raw = _extract_text(res.json())
    return _validate_output(raw)


def call_llm(profile: dict[str, Any]) -> dict[str, Any]:
    try:
        return _call_once(profile)
    except LLMValidationError:
        repair_profile = {
            **profile,
            "_validation_retry": (
                "Your previous JSON failed validation. "
                "The reason field must be exactly three English sentences. "
                "End each sentence with . or ! or ? followed by a space before the next sentence. "
                "Do not put sentence breaks inside numbers (e.g. write 8.33 as one number)."
            ),
        }
        return _call_once(repair_profile)
    except Exception:  # noqa: BLE001
        return _call_once(profile)
