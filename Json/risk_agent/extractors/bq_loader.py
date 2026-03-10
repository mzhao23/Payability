"""extractors/bq_loader.py — pull rows from BigQuery.

Two queries are executed:
1. Main query: current window rows with all columns.
2. Prev-policy query: for each mp_sup_key in the current window, fetch the
   policy_compliance total from the immediately preceding record. This is a
   lightweight scalar query (no full data column) to keep BQ costs low.
"""

from __future__ import annotations

import json
from typing import Iterator

from google.cloud import bigquery
from google.oauth2 import service_account

from config import settings
from utils.logger import get_logger

log = get_logger("bq_loader")


def _build_client() -> bigquery.Client:
    """Build a BigQuery client from service-account credentials."""
    if settings.BQ_SERVICE_ACCOUNT_PATH:
        creds = service_account.Credentials.from_service_account_file(
            settings.BQ_SERVICE_ACCOUNT_PATH,
            scopes=[
                "https://www.googleapis.com/auth/bigquery",
                "https://www.googleapis.com/auth/cloud-platform",
            ],
        )
    elif settings.BQ_SERVICE_ACCOUNT_JSON:
        try:
            info = json.loads(settings.BQ_SERVICE_ACCOUNT_JSON)
        except json.JSONDecodeError as exc:
            raise EnvironmentError(
                "BQ_SERVICE_ACCOUNT_JSON is set but is not valid JSON. "
                "Set BQ_SERVICE_ACCOUNT_PATH instead."
            ) from exc
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=[
                "https://www.googleapis.com/auth/bigquery",
                "https://www.googleapis.com/auth/cloud-platform",
            ],
        )
    else:
        raise EnvironmentError(
            "BigQuery credentials not configured. "
            "Set BQ_SERVICE_ACCOUNT_PATH to your service account JSON file."
        )
    return bigquery.Client(project=settings.BQ_PROJECT_ID, credentials=creds)


def _build_main_query() -> str:
    full_table = f"`{settings.BQ_PROJECT_ID}.{settings.BQ_DATASET}.{settings.BQ_TABLE}`"

    time_filter = ""
    if settings.BQ_LOOKBACK_HOURS > 0:
        time_filter = (
            f"WHERE create_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), "
            f"INTERVAL {settings.BQ_LOOKBACK_HOURS} HOUR)"
        )

    limit_clause = f"LIMIT {settings.MAX_ROWS}" if settings.MAX_ROWS > 0 else ""

    return f"""
        SELECT
            marketplace_ext_data_key,
            created_date,
            mp_sup_key,
            data,
            create_ts,
            update_ts,
            last_login_id,
            order_defect_rate,
            order_defect_rate_not_applic,
            late_shipment_rate,
            late_shipment_rate_not_applic,
            cancellation_rate,
            cancellation_rate_not_applic,
            valid_tracking_rate_all_cat,
            valid_tracking_rate_all_cat_not_applic,
            account_status,
            late_responses,
            late_responses_cat_not_applic,
            return_dissatisfaction_rate,
            return_dissatisfaction_rate_not_applic,
            customer_service_dissatisfaction_rate_beta,
            customer_service_dissatisfaction_rate_beta_not_applic,
            order_defect_rate_short_term_value,
            late_shipment_rate_30_days,
            delivered_on_time,
            sales_30_days,
            sales_7_days,
            channel_sales_all,
            channel_sales_amazon,
            channel_sales_seller,
            cust_complaints_prod_authenticity,
            cust_complaints_prod_safety,
            cust_complaints_intelectual_prop,
            cust_complaints_policy_violation,
            eligibilities_status,
            inv_report_value,
            inv_report_amazon_fulfilled_value,
            cancellation_orders_short_term,
            cancellation_orders_long_term,
            cancellation_value_short_term,
            cancellation_value_long_term,
            order_defect_orders_short_term,
            order_defect_orders_long_term,
            order_defect_value_short_term,
            order_defect_value_long_term,
            late_shipment_orders_short_term,
            late_shipment_value_short_term,
            late_shipment_orders_long_term,
            late_shipment_value_long_term,
            chargeback_claims_value_short_term,
            negative_feedback_value_short_term,
            a_to_z_guarantee_claims_value_short_term,
            chargeback_claims_orders_short_term,
            negative_feedback_orders_short_term,
            a_to_z_guarantee_claims_orders_short_term,
            last_txid
        FROM {full_table}
        {time_filter}
        ORDER BY create_ts DESC
        {limit_clause}
    """


def _build_prev_policy_query(sup_keys: list[str]) -> str:
    """
    For each mp_sup_key, fetch the policy_compliance total from the record
    immediately before the current window. Only scalar JSON fields are
    extracted in BQ — no full data column transfer — keeping costs minimal.

    Returns one row per mp_sup_key with:
      mp_sup_key, prev_policy_total, prev_created_date
    """
    full_table = f"`{settings.BQ_PROJECT_ID}.{settings.BQ_DATASET}.{settings.BQ_TABLE}`"

    # Build a safe IN list
    keys_list = ", ".join(f"'{k}'" for k in sup_keys)

    time_filter = ""
    if settings.BQ_LOOKBACK_HOURS > 0:
        time_filter = (
            f"AND create_ts < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), "
            f"INTERVAL {settings.BQ_LOOKBACK_HOURS} HOUR)"
        )

    return f"""
        WITH ranked AS (
            SELECT
                mp_sup_key,
                created_date,
                -- Sum all numeric policy_compliance fields directly in BQ
                -- so we only transfer a single integer per row
                COALESCE(SAFE_CAST(JSON_VALUE(data, '$.policy_compliance.Other Policy Violations')                    AS INT64), 0)
              + COALESCE(SAFE_CAST(JSON_VALUE(data, '$.policy_compliance.Listing Policy Violations')                  AS INT64), 0)
              + COALESCE(SAFE_CAST(JSON_VALUE(data, '$.policy_compliance.Food and Product Safety Issues')             AS INT64), 0)
              + COALESCE(SAFE_CAST(JSON_VALUE(data, '$.policy_compliance.Restricted Product Policy Violations')       AS INT64), 0)
              + COALESCE(SAFE_CAST(JSON_VALUE(data, '$.policy_compliance.Product Condition Customer Complaints')      AS INT64), 0)
              + COALESCE(SAFE_CAST(JSON_VALUE(data, '$.policy_compliance.Product Authenticity Customer Complaints')   AS INT64), 0)
              + COALESCE(SAFE_CAST(JSON_VALUE(data, '$.policy_compliance.Received Intellectual Property Complaints')  AS INT64), 0)
              + COALESCE(SAFE_CAST(JSON_VALUE(data, '$.policy_compliance.Customer Product Reviews Policy Violations') AS INT64), 0)
              + COALESCE(SAFE_CAST(JSON_VALUE(data, '$.policy_compliance.Suspected Intellectual Property Violations') AS INT64), 0)
              + COALESCE(SAFE_CAST(JSON_VALUE(data, '$.policy_compliance.Regulatory Compliance')                      AS INT64), 0)
                AS prev_policy_total,
                ROW_NUMBER() OVER (
                    PARTITION BY mp_sup_key
                    ORDER BY create_ts DESC
                ) AS rn
            FROM {full_table}
            WHERE mp_sup_key IN ({keys_list})
            {time_filter}
        )
        SELECT mp_sup_key, prev_policy_total, created_date AS prev_created_date
        FROM ranked
        WHERE rn = 1
    """


def fetch_rows() -> Iterator[dict]:
    """
    Yield each current-window BigQuery row as a plain Python dict,
    enriched with `prev_policy_total` from the preceding record.
    """
    client = _build_client()

    # ── Query 1: current window ───────────────────────────────────────────────
    main_query = _build_main_query()
    log.info("Executing main BigQuery query …")
    rows = [dict(row) for row in client.query(main_query).result()]
    log.info("Fetched %d rows from BigQuery.", len(rows))

    if not rows:
        return

    # ── Query 2: prev policy totals (lightweight) ─────────────────────────────
    sup_keys = list({r["mp_sup_key"] for r in rows if r.get("mp_sup_key")})
    log.info("Fetching prev policy totals for %d suppliers …", len(sup_keys))
    try:
        prev_query = _build_prev_policy_query(sup_keys)
        prev_results = {
            row["mp_sup_key"]: row["prev_policy_total"]
            for row in client.query(prev_query).result()
            if row["prev_policy_total"] is not None
        }
        log.info("Prev policy totals fetched for %d suppliers.", len(prev_results))
    except Exception as exc:
        log.warning("Could not fetch prev policy totals: %s — skipping delta.", exc)
        prev_results = {}

    # ── Enrich and yield ──────────────────────────────────────────────────────
    for row in rows:
        row["prev_policy_total"] = prev_results.get(row.get("mp_sup_key"))
        yield row