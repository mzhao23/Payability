from __future__ import annotations

import argparse
from datetime import date, timedelta

from health_risk.bootstrap import build_default_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="HealthData production risk pipeline")
    parser.add_argument("--report-date", type=str, default="", help="YYYY-MM-DD")
    parser.add_argument("--days-back", type=int, default=0, help="Run latest date and previous N days")
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--chunk-size", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-export-json", action="store_true")
    parser.add_argument(
        "--no-llm-narrative",
        action="store_true",
        help="Skip LLM narrative for high-risk rows (rule scores still run)",
    )

    args = parser.parse_args()
    export_json = not args.no_export_json
    enable_llm = not args.no_llm_narrative

    pipeline = build_default_pipeline()

    if args.report_date:
        rd = date.fromisoformat(args.report_date)
        pipeline.run_for_date(
            rd,
            limit=args.limit,
            chunk_size=args.chunk_size,
            export_json=export_json,
            dry_run=args.dry_run,
            enable_llm_narrative=enable_llm,
        )
        return

    latest = pipeline.get_latest_report_date()
    for i in range(args.days_back + 1):
        rd = latest - timedelta(days=i)
        pipeline.run_for_date(
            rd,
            limit=args.limit,
            chunk_size=args.chunk_size,
            export_json=export_json,
            dry_run=args.dry_run,
            enable_llm_narrative=enable_llm,
        )


if __name__ == "__main__":
    main()
