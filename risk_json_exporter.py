import json
from datetime import date


def build_unified_json(row, report_date, table_name, status_cols):

    metrics = []

    for col in status_cols:
        value = row.get(col)

        metrics.append({
            "metric_id": col.upper(),
            "value": value,
            "unit": "status",
            "explanation": "Derived from risk monitoring metrics"
        })

    result = {
        "table_name": table_name,
        "supplier_key": row.get("mp_sup_key"),
        "report_date": report_date if isinstance(report_date, str) else report_date.isoformat(),
        "metrics": metrics,
        "overall_risk_score": row.get("risk_score")
    }

    return result


def export_json(rows, report_date, status_cols, output_file="risk_output.json"):

    output = []

    for r in rows:
        output.append(
            build_unified_json(
                r,
                report_date,
                "customer_health_metrics",
                status_cols
            )
        )

    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Exported {len(output)} records to {output_file}")