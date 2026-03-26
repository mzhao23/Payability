from __future__ import annotations

from typing import Any, Dict, List, Tuple

from health_risk.metrics_catalog import METRIC_ID_TO_GROUP
from health_risk.scoring import subscores as S
from health_risk.utils import clamp, iso, pct_to_ratio, safe_float, utc_now_iso


class RiskScoreEngine:
    """
    Encapsulates risk_formula_v2: row extraction, subscores, aggregation, drivers.
    Swap or subclass to experiment with new formulas without touching I/O.
    """

    PIPELINE_VERSION = "risk_formula_v3_production"

    def score_supplier_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        metric_values: Dict[str, Any] = {
            "ORDER_DEFECT_RATE_60": pct_to_ratio(row.get("orderWithDefects_60_rate")),
            "CHARGEBACK_RATE_90": pct_to_ratio(row.get("chargebacks_90_rate")),
            "A_TO_Z_CLAIM_RATE_90": pct_to_ratio(row.get("a_z_claims_90_rate")),
            "NEGATIVE_FEEDBACK_RATE_90": pct_to_ratio(row.get("negativeFeedbacks_90_rate")),
            "LATE_SHIPMENT_RATE_30": pct_to_ratio(row.get("lateShipment_30_rate")),
            "PRE_FULFILL_CANCEL_RATE_30": pct_to_ratio(
                row.get("preFulfillmentCancellation_30_rate")
            ),
            "AVG_RESPONSE_HOURS_30": safe_float(row.get("averageResponseTimeInHours_30")),
            "NO_RESPONSE_OVER_24H_30": safe_float(
                row.get("noResponseForContactsOlderThan24Hours_30")
            ),
            "VALID_TRACKING_RATE_30": pct_to_ratio(row.get("validTracking_rate_30")),
            "ON_TIME_DELIVERY_RATE_30": pct_to_ratio(row.get("onTimeDelivery_rate_30")),
            "PRODUCT_SAFETY_STATUS": row.get("productSafetyStatus_status"),
            "PRODUCT_AUTHENTICITY_STATUS": row.get("productAuthenticityStatus_status"),
            "POLICY_VIOLATION_STATUS": row.get("policyViolation_status"),
            "LISTING_POLICY_STATUS": row.get("listingPolicyStatus_status"),
            "INTELLECTUAL_PROPERTY_STATUS": row.get("intellectualProperty_status"),
        }

        orders_count_60 = safe_float(row.get("orders_count_60"))

        outcome_subscores = {
            "ORDER_DEFECT_RATE_60": S.score_odr(metric_values["ORDER_DEFECT_RATE_60"]),
            "CHARGEBACK_RATE_90": S.score_chargeback(metric_values["CHARGEBACK_RATE_90"]),
            "A_TO_Z_CLAIM_RATE_90": S.score_a_to_z(metric_values["A_TO_Z_CLAIM_RATE_90"]),
            "NEGATIVE_FEEDBACK_RATE_90": S.score_negative_feedback(
                metric_values["NEGATIVE_FEEDBACK_RATE_90"]
            ),
        }

        operational_subscores = {
            "LATE_SHIPMENT_RATE_30": S.score_late_shipment(metric_values["LATE_SHIPMENT_RATE_30"]),
            "PRE_FULFILL_CANCEL_RATE_30": S.score_cancellation(
                metric_values["PRE_FULFILL_CANCEL_RATE_30"]
            ),
            "AVG_RESPONSE_HOURS_30": S.score_response_hours(metric_values["AVG_RESPONSE_HOURS_30"]),
            "NO_RESPONSE_OVER_24H_30": S.score_no_response(
                metric_values["NO_RESPONSE_OVER_24H_30"]
            ),
            "VALID_TRACKING_RATE_30": S.score_valid_tracking(
                metric_values["VALID_TRACKING_RATE_30"]
            ),
            "ON_TIME_DELIVERY_RATE_30": S.score_on_time_delivery(
                metric_values["ON_TIME_DELIVERY_RATE_30"]
            ),
        }

        compliance_subscores = {
            "PRODUCT_SAFETY_STATUS": S.score_status(metric_values["PRODUCT_SAFETY_STATUS"]),
            "PRODUCT_AUTHENTICITY_STATUS": S.score_status(
                metric_values["PRODUCT_AUTHENTICITY_STATUS"]
            ),
            "POLICY_VIOLATION_STATUS": S.score_status(metric_values["POLICY_VIOLATION_STATUS"]),
            "LISTING_POLICY_STATUS": S.score_status(metric_values["LISTING_POLICY_STATUS"]),
            "INTELLECTUAL_PROPERTY_STATUS": S.score_status(
                metric_values["INTELLECTUAL_PROPERTY_STATUS"]
            ),
        }

        outcome_score = round(
            0.55 * outcome_subscores["ORDER_DEFECT_RATE_60"]
            + 0.18 * outcome_subscores["CHARGEBACK_RATE_90"]
            + 0.15 * outcome_subscores["A_TO_Z_CLAIM_RATE_90"]
            + 0.12 * outcome_subscores["NEGATIVE_FEEDBACK_RATE_90"],
            2,
        )

        operational_score = round(
            0.25 * operational_subscores["LATE_SHIPMENT_RATE_30"]
            + 0.20 * operational_subscores["PRE_FULFILL_CANCEL_RATE_30"]
            + 0.08 * operational_subscores["AVG_RESPONSE_HOURS_30"]
            + 0.07 * operational_subscores["NO_RESPONSE_OVER_24H_30"]
            + 0.20 * operational_subscores["VALID_TRACKING_RATE_30"]
            + 0.20 * operational_subscores["ON_TIME_DELIVERY_RATE_30"],
            2,
        )

        comp_values = list(compliance_subscores.values())
        comp_max = max(comp_values) if comp_values else 0.0
        comp_avg = sum(comp_values) / len(comp_values) if comp_values else 0.0
        compliance_score = round(0.7 * comp_max + 0.3 * comp_avg, 2)

        act_gate = S.activity_gate(orders_count_60)
        inact_penalty = S.inactivity_penalty(orders_count_60)

        base_risk = round(
            0.55 * outcome_score
            + 0.35 * operational_score
            + 0.10 * compliance_score,
            2,
        )

        risk_score = round(
            act_gate * base_risk + (1.0 - act_gate) * inact_penalty,
            2,
        )
        risk_score = clamp(risk_score, 0.0, 10.0)
        risk_level = S.risk_level_from_score(risk_score)

        driver_contributions: Dict[str, float] = {
            "ORDER_DEFECT_RATE_60": 0.55 * 0.55 * outcome_subscores["ORDER_DEFECT_RATE_60"],
            "CHARGEBACK_RATE_90": 0.55 * 0.18 * outcome_subscores["CHARGEBACK_RATE_90"],
            "A_TO_Z_CLAIM_RATE_90": 0.55 * 0.15 * outcome_subscores["A_TO_Z_CLAIM_RATE_90"],
            "NEGATIVE_FEEDBACK_RATE_90": 0.55 * 0.12 * outcome_subscores["NEGATIVE_FEEDBACK_RATE_90"],
            "LATE_SHIPMENT_RATE_30": 0.35 * 0.25 * operational_subscores["LATE_SHIPMENT_RATE_30"],
            "PRE_FULFILL_CANCEL_RATE_30": 0.35
            * 0.20
            * operational_subscores["PRE_FULFILL_CANCEL_RATE_30"],
            "AVG_RESPONSE_HOURS_30": 0.35 * 0.08 * operational_subscores["AVG_RESPONSE_HOURS_30"],
            "NO_RESPONSE_OVER_24H_30": 0.35
            * 0.07
            * operational_subscores["NO_RESPONSE_OVER_24H_30"],
            "VALID_TRACKING_RATE_30": 0.35 * 0.20 * operational_subscores["VALID_TRACKING_RATE_30"],
            "ON_TIME_DELIVERY_RATE_30": 0.35
            * 0.20
            * operational_subscores["ON_TIME_DELIVERY_RATE_30"],
            "PRODUCT_SAFETY_STATUS": 0.10 * compliance_subscores["PRODUCT_SAFETY_STATUS"],
            "PRODUCT_AUTHENTICITY_STATUS": 0.10
            * compliance_subscores["PRODUCT_AUTHENTICITY_STATUS"],
            "POLICY_VIOLATION_STATUS": 0.10 * compliance_subscores["POLICY_VIOLATION_STATUS"],
            "LISTING_POLICY_STATUS": 0.10 * compliance_subscores["LISTING_POLICY_STATUS"],
            "INTELLECTUAL_PROPERTY_STATUS": 0.10
            * compliance_subscores["INTELLECTUAL_PROPERTY_STATUS"],
        }

        top_risk_drivers = [
            k
            for k, v in sorted(driver_contributions.items(), key=lambda x: x[1], reverse=True)
            if v > 0
        ][:5]

        driver_1 = top_risk_drivers[0] if len(top_risk_drivers) > 0 else None
        driver_2 = top_risk_drivers[1] if len(top_risk_drivers) > 1 else None
        driver_3 = top_risk_drivers[2] if len(top_risk_drivers) > 2 else None

        outcome_drivers = [d for d in top_risk_drivers if METRIC_ID_TO_GROUP.get(d) == "outcome"]
        operational_drivers = [
            d for d in top_risk_drivers if METRIC_ID_TO_GROUP.get(d) == "operational"
        ]
        compliance_drivers = [
            d for d in top_risk_drivers if METRIC_ID_TO_GROUP.get(d) == "compliance"
        ]

        reason_parts = []
        if outcome_drivers:
            reason_parts.append("outcome risk: " + ", ".join(outcome_drivers[:2]))
        if operational_drivers:
            reason_parts.append("operational risk: " + ", ".join(operational_drivers[:2]))
        if compliance_drivers:
            reason_parts.append("compliance risk: " + ", ".join(compliance_drivers[:2]))

        if reason_parts:
            risk_reason = "Top risk drivers include " + "; ".join(reason_parts)
        else:
            risk_reason = "No significant risk signals detected."

        def score_to_band(score: float) -> str:
            if score == 0:
                return "green"
            if score < 6:
                return "yellow"
            return "red"

        all_scores = {**outcome_subscores, **operational_subscores, **compliance_subscores}
        red_metric_count = sum(1 for v in all_scores.values() if score_to_band(v) == "red")
        yellow_metric_count = sum(1 for v in all_scores.values() if score_to_band(v) == "yellow")

        return {
            "report_date": iso(row.get("report_date")),
            "mp_sup_key": str(row["mp_sup_key"]),
            "supplier_key": row.get("supplier_key"),
            "supplier_name": row.get("supplier_name"),
            "payability_status": row.get("payability_status"),
            "snapshot_timestamp": iso(row.get("snapshot_date")),
            "pipeline_version": self.PIPELINE_VERSION,
            "risk_score": round(risk_score, 2),
            "risk_level": risk_level,
            "risk_reason": risk_reason,
            "top_risk_drivers": top_risk_drivers,
            "driver_1": driver_1,
            "driver_2": driver_2,
            "driver_3": driver_3,
            "red_metric_count": red_metric_count,
            "yellow_metric_count": yellow_metric_count,
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
            "_metric_values": metric_values,
            "_subscores": all_scores,
        }

    def build_payload(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        dedup: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for r in rows:
            if r.get("mp_sup_key") is None:
                continue
            item = self.score_supplier_row(r)
            dedup[(item["report_date"], item["mp_sup_key"])] = item
        return list(dedup.values())
