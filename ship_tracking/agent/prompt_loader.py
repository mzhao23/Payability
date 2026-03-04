# ============================================================
# prompt_loader.py
# Loads and renders prompt templates and config files.
# Injects values from schema.yaml and risk_focus.yaml
# into prompt markdown files before sending to Gemini.
# ============================================================

import yaml
from pathlib import Path


# File paths
CONFIG_DIR = Path(__file__).parent / "config"
PROMPTS_DIR = Path(__file__).parent / "prompts"

RISK_FOCUS_PATH = CONFIG_DIR / "risk_focus.yaml"
SCHEMA_PATH = CONFIG_DIR / "schema.yaml"


def load_yaml(path: Path) -> dict:
    """Load a YAML file and return as dict."""
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_risk_focus() -> dict:
    """Load risk_focus.yaml."""
    return load_yaml(RISK_FOCUS_PATH)


def load_schema() -> dict:
    """Load schema.yaml."""
    return load_yaml(SCHEMA_PATH)


def load_prompt(filename: str) -> str:
    """Load a raw prompt markdown file."""
    path = PROMPTS_DIR / filename
    with open(path, "r") as f:
        return f.read()


def render_sql_generation_prompt(sql_results: dict | None = None) -> str:
    """
    Render the sql_generation.md prompt with values from
    schema.yaml and risk_focus.yaml.
    """
    risk_config = load_risk_focus()
    schema_config = load_schema()
    template = load_prompt("sql_generation.md")

    # Build schema columns text
    columns_text = ""
    for col in schema_config["columns"]:
        note = col.get("business_note", "").strip().replace("\n", " ")
        columns_text += f"- **{col['name']}** ({col['type']}): {note}\n"

    # Build risk focus text
    risk_focus_text = ""
    for i, item in enumerate(risk_config["risk_focus"], 1):
        desc = item["description"].strip().replace("\n", " ")
        risk_focus_text += (
            f"### {i}. {item['name']} (Priority: {item['priority']})\n"
            f"{desc}\n\n"
        )

    # Fill in all placeholders
    prompt = template
    prompt = prompt.replace("{{ table.full_path }}", schema_config["table"]["full_path"])
    prompt = prompt.replace("{{ columns_text }}", columns_text)
    prompt = prompt.replace("{{ thresholds.overdue_pickup_hours }}", str(risk_config["thresholds"]["overdue_pickup_hours"]))
    prompt = prompt.replace("{{ thresholds.high_value_order_usd }}", str(risk_config["thresholds"]["high_value_order_usd"]))
    prompt = prompt.replace("{{ thresholds.untracked_rate_alert * 100 }}", str(risk_config["thresholds"]["untracked_rate_alert"] * 100))
    prompt = prompt.replace("{{ thresholds.cost_increase_alert * 100 }}", str(risk_config["thresholds"]["cost_increase_alert"] * 100))
    prompt = prompt.replace("{{ thresholds.lookback_days }}", str(risk_config["thresholds"]["lookback_days"]))
    prompt = prompt.replace("{{ risk_focus_text }}", risk_focus_text)
    prompt = prompt.replace("{{ num_queries }}", str(len(risk_config["risk_focus"])))

    return prompt


def render_risk_analysis_prompt(
    sql_results: str,
    historical_context: str,
) -> str:
    """
    Render the risk_analysis.md prompt with actual query results
    and historical context.
    """
    risk_config = load_risk_focus()
    template = load_prompt("risk_analysis.md")

    # Build risk focus text
    risk_focus_text = ""
    for item in risk_config["risk_focus"]:
        desc = item["description"].strip().replace("\n", " ")
        risk_focus_text += (
            f"### {item['name']} (Priority: {item['priority']})\n"
            f"{desc}\n\n"
        )

    # Fill in all placeholders
    prompt = template
    prompt = prompt.replace("{{ sql_results }}", sql_results)
    prompt = prompt.replace("{{ historical_context }}", historical_context)
    prompt = prompt.replace("{{ risk_focus_text }}", risk_focus_text)
    prompt = prompt.replace("{{ thresholds.untracked_rate_alert * 100 }}", str(risk_config["thresholds"]["untracked_rate_alert"] * 100))
    prompt = prompt.replace("{{ thresholds.cost_increase_alert * 100 }}", str(risk_config["thresholds"]["cost_increase_alert"] * 100))
    prompt = prompt.replace("{{ thresholds.overdue_pickup_hours }}", str(risk_config["thresholds"]["overdue_pickup_hours"]))

    return prompt


if __name__ == "__main__":
    # Quick test to verify prompts render correctly
    print("=== SQL Generation Prompt ===")
    print(render_sql_generation_prompt())

    print("\n=== Risk Analysis Prompt (sample) ===")
    sample_results = '{"untracked_orders": [{"supplier_key": "SUP_001", "untracked_rate": 0.18}]}'
    sample_history = '{"untracked_orders": [{"supplier_key": "SUP_001", "avg_untracked_rate_30d": 0.03}]}'
    print(render_risk_analysis_prompt(sample_results, sample_history))