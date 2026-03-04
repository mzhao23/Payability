# ============================================================
# gemini_client.py
# Handles all Gemini API calls via Vercel AI Gateway.
# ============================================================

import json
import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# Vercel AI Gateway config
GATEWAY_URL = "https://ai-gateway.vercel.sh/v1"
# MODEL = "google/gemini-2.0-flash"
MODEL = "google/gemini-2.5-flash"


def get_client() -> OpenAI:
    """
    Initialize and return an OpenAI-compatible client
    pointed at Vercel AI Gateway.
    """
    api_key = os.environ.get("AI_GATEWAY_API_KEY")
    if not api_key:
        raise ValueError("AI_GATEWAY_API_KEY environment variable is not set")

    return OpenAI(
        api_key=api_key,
        base_url=GATEWAY_URL
    )


def clean_response(raw: str) -> str:
    """Strip markdown code fences if Gemini adds them."""
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1]
        raw = raw.rsplit("```", 1)[0]
    return raw.strip()


def call_gemini(prompt: str, temperature: float = 0.1) -> str:
    """
    Send a prompt to Gemini via Vercel AI Gateway.
    Returns the raw text response.

    Low temperature (0.1) for more consistent, deterministic outputs.
    """
    client = get_client()

    response = client.chat.completions.create(
        model=MODEL,
        temperature=temperature,
        max_tokens=20000,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    return response.choices[0].message.content


def generate_sql_queries(prompt: str) -> list[dict]:
    """
    Call Gemini to generate SQL queries.
    Returns parsed list of query dicts.

    Each dict has keys: id, name, description, sql
    """
    print("Calling Gemini to generate SQL queries...")
    raw_response = call_gemini(prompt)
    cleaned = clean_response(raw_response)

    try:
        parsed = json.loads(cleaned)
        queries = parsed["queries"]
        print(f"  ✓ Gemini generated {len(queries)} SQL queries")
        return queries
    except (json.JSONDecodeError, KeyError) as e:
        print(f"  ✗ Failed to parse Gemini SQL response: {e}")
        print(f"  Raw response: {raw_response[:500]}")
        raise


def generate_risk_report(prompt: str) -> dict:
    """
    Call Gemini to analyze SQL results and generate risk report.
    Returns parsed report dict.
    """
    print("Calling Gemini to generate risk report...")
    raw_response = call_gemini(prompt)
    cleaned = clean_response(raw_response)

    try:
        parsed = json.loads(cleaned)
        print(f"  ✓ Risk report generated, overall risk: {parsed.get('overall_risk_level')}")
        return parsed
    except json.JSONDecodeError as e:
        print(f"  ✗ Failed to parse Gemini report response: {e}")
        print(f"  Raw response: {raw_response[:500]}")
        raise


# if __name__ == "__main__":
#     # Quick test to verify Gateway connection
#     print("Testing Vercel AI Gateway connection...")
#     response = call_gemini("Reply with exactly this JSON: {\"status\": \"ok\"}")
#     print(f"Response: {response}")

if __name__ == "__main__":
    print("Testing clean_response...")
    test = '```json\n{"status": "ok"}\n```'
    cleaned = clean_response(test)
    parsed = json.loads(cleaned)
    print(f"Parsed successfully: {parsed}")