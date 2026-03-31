from __future__ import annotations

from datetime import date
from typing import Any, Dict, List

from google.cloud import bigquery

from health_risk.config import Settings
from health_risk.metrics_catalog import BQ_STATUS_COLS, METRIC_CONFIG
from health_risk.utils import normalize_key


class BigQueryRepository:
    """Data access for Amazon health metrics and Payability summary (BigQuery)."""

    def __init__(self, client: bigquery.Client, settings: Settings) -> None:
        self._bq = client
        self._settings = settings

    def get_latest_report_date(self) -> date:
        q = f"""
        SELECT MAX(DATE(snapshot_date)) AS report_date
        FROM {self._settings.bq_full_table}
        """
        rows = list(self._bq.query(q).result())
        if not rows or rows[0]["report_date"] is None:
            raise RuntimeError("No snapshot_date found in BigQuery table.")
        return rows[0]["report_date"]

    def fetch_latest_health_snapshot(
        self, report_date: date, limit: int = 5000
    ) -> List[Dict[str, Any]]:
        metric_cols = [m["source_column"] for m in METRIC_CONFIG]
        metric_cols += ["orders_count_60", "orders_count_30", "orders_count_90", "path_golden"]

        selected_cols = ["mp_sup_key", "snapshot_date"] + metric_cols + BQ_STATUS_COLS
        selected_cols = list(dict.fromkeys(selected_cols))
        select_sql = ",\n            ".join([f"h.`{c}`" for c in selected_cols])

        q = f"""
        WITH ranked AS (
          SELECT
            DATE(snapshot_date) AS report_date,
            {select_sql},
            ROW_NUMBER() OVER (
              PARTITION BY h.mp_sup_key, DATE(h.snapshot_date)
              ORDER BY h.snapshot_date DESC
            ) AS rn
          FROM {self._settings.bq_full_table} h
          WHERE DATE(h.snapshot_date) = @report_date
        ),
        order_channel AS (
          SELECT
            mp_sup_key,
            COUNT(DISTINCT CASE WHEN fulfillment_channel = 'Amazon' THEN amazon_order_id END) AS fba_orders_60,
            COUNT(DISTINCT CASE WHEN fulfillment_channel = 'Merchant' THEN amazon_order_id END) AS fbm_orders_60
          FROM `bigqueryexport-183608.amazon.customer_order_metrics`
          WHERE purchase_day >= DATE_SUB(@report_date, INTERVAL 60 DAY)
            AND purchase_day <= @report_date
            AND order_status != 'Cancelled'
          GROUP BY mp_sup_key
        )
        SELECT ranked.*, oc.fba_orders_60, oc.fbm_orders_60
        FROM ranked
        LEFT JOIN order_channel oc USING(mp_sup_key)
        WHERE rn = 1
        LIMIT @lim
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("report_date", "DATE", report_date),
                bigquery.ScalarQueryParameter("lim", "INT64", limit),
            ]
        )

        rows = self._bq.query(q, job_config=job_config).result()
        return [dict(r) for r in rows]

    def fetch_payability_status_map(self) -> Dict[str, Dict[str, Any]]:
        q = f"""
        SELECT
          supplier_key,
          supplier_name,
          payability_status
        FROM {self._settings.bq_payability_table}
        """
        rows = self._bq.query(q).result()

        out: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            supplier_key = normalize_key(r["supplier_key"])
            if supplier_key is None:
                continue
            out[supplier_key] = {
                "supplier_name": r.get("supplier_name"),
                "payability_status": r.get("payability_status"),
            }
        return out
