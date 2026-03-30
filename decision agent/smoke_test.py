from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

from db import get_flagged_suppliers_today
from llm import call_llm
from profile_builder import build_supplier_profile


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a smoke test with real Supabase data and write JSON output."
    )
    parser.add_argument(
        "--report-date",
        type=str,
        default=date.today().isoformat(),
        help="Report date in YYYY-MM-DD format (default: today).",
    )
    parser.add_argument(
        "--supplier-key",
        type=str,
        default=None,
        help="Optional supplier_key. If omitted, picks the first flagged supplier on report date.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="outputs",
        help="Directory to write output JSON files.",
    )
    return parser.parse_args()


def _resolve_supplier(report_date: date, supplier_key: str | None) -> dict[str, Any]:
    suppliers = get_flagged_suppliers_today(report_date)
    if not suppliers:
        raise RuntimeError(f"No flagged suppliers found on {report_date.isoformat()}.")

    if supplier_key is None:
        return suppliers[0]

    for supplier in suppliers:
        if supplier.get("supplier_key") == supplier_key:
            return supplier
    raise RuntimeError(
        f"supplier_key={supplier_key} not found in consolidated_flagged_supplier_list for {report_date.isoformat()}."
    )


def main() -> None:
    args = _parse_args()
    report_date = datetime.strptime(args.report_date, "%Y-%m-%d").date()
    supplier = _resolve_supplier(report_date, args.supplier_key)

    profile = build_supplier_profile(
        supplier_key=supplier["supplier_key"],
        supplier_name=supplier.get("supplier_name") or "",
        report_date=report_date,
    )
    llm_output = call_llm(profile)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    supplier_key = supplier["supplier_key"]

    profile_file = output_dir / f"{report_date.isoformat()}_{supplier_key}_{stamp}_profile.json"
    result_file = output_dir / f"{report_date.isoformat()}_{supplier_key}_{stamp}_llm_result.json"
    combined_file = output_dir / f"{report_date.isoformat()}_{supplier_key}_{stamp}_combined.json"

    profile_file.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")
    result_file.write_text(json.dumps(llm_output, ensure_ascii=False, indent=2), encoding="utf-8")
    combined_file.write_text(
        json.dumps(
            {
                "supplier_key": supplier_key,
                "supplier_name": supplier.get("supplier_name") or "",
                "report_date": report_date.isoformat(),
                "profile": profile,
                "llm_output": llm_output,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("Smoke test completed.")
    print(f"Supplier: {supplier_key}")
    print(f"Profile JSON: {profile_file}")
    print(f"LLM result JSON: {result_file}")
    print(f"Combined JSON: {combined_file}")


if __name__ == "__main__":
    main()
