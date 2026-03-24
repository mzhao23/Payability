from typing import Any, Dict, List

METRIC_CONFIG: List[Dict[str, Any]] = [
    {
        "metric_id": "ORDER_DEFECT_RATE_60",
        "source_column": "orderWithDefects_60_rate",
        "direction": "higher_is_worse",
        "group": "outcome",
    },
    {
        "metric_id": "CHARGEBACK_RATE_90",
        "source_column": "chargebacks_90_rate",
        "direction": "higher_is_worse",
        "group": "outcome",
    },
    {
        "metric_id": "A_TO_Z_CLAIM_RATE_90",
        "source_column": "a_z_claims_90_rate",
        "direction": "higher_is_worse",
        "group": "outcome",
    },
    {
        "metric_id": "NEGATIVE_FEEDBACK_RATE_90",
        "source_column": "negativeFeedbacks_90_rate",
        "direction": "higher_is_worse",
        "group": "outcome",
    },
    {
        "metric_id": "LATE_SHIPMENT_RATE_30",
        "source_column": "lateShipment_30_rate",
        "direction": "higher_is_worse",
        "group": "operational",
    },
    {
        "metric_id": "PRE_FULFILL_CANCEL_RATE_30",
        "source_column": "preFulfillmentCancellation_30_rate",
        "direction": "higher_is_worse",
        "group": "operational",
    },
    {
        "metric_id": "AVG_RESPONSE_HOURS_30",
        "source_column": "averageResponseTimeInHours_30",
        "direction": "higher_is_worse",
        "group": "operational",
    },
    {
        "metric_id": "NO_RESPONSE_OVER_24H_30",
        "source_column": "noResponseForContactsOlderThan24Hours_30",
        "direction": "higher_is_worse",
        "group": "operational",
    },
    {
        "metric_id": "VALID_TRACKING_RATE_30",
        "source_column": "validTracking_rate_30",
        "direction": "lower_is_worse",
        "group": "operational",
    },
    {
        "metric_id": "ON_TIME_DELIVERY_RATE_30",
        "source_column": "onTimeDelivery_rate_30",
        "direction": "lower_is_worse",
        "group": "operational",
    },
    {
        "metric_id": "PRODUCT_SAFETY_STATUS",
        "source_column": "productSafetyStatus_status",
        "direction": "status",
        "group": "compliance",
    },
    {
        "metric_id": "PRODUCT_AUTHENTICITY_STATUS",
        "source_column": "productAuthenticityStatus_status",
        "direction": "status",
        "group": "compliance",
    },
    {
        "metric_id": "POLICY_VIOLATION_STATUS",
        "source_column": "policyViolation_status",
        "direction": "status",
        "group": "compliance",
    },
    {
        "metric_id": "LISTING_POLICY_STATUS",
        "source_column": "listingPolicyStatus_status",
        "direction": "status",
        "group": "compliance",
    },
    {
        "metric_id": "INTELLECTUAL_PROPERTY_STATUS",
        "source_column": "intellectualProperty_status",
        "direction": "status",
        "group": "compliance",
    },
]

METRIC_ID_TO_GROUP = {m["metric_id"]: m["group"] for m in METRIC_CONFIG}

BQ_STATUS_COLS = [
    "policyViolation_status",
    "listingPolicyStatus_status",
    "customerServiceDissatisfactionRate_status",
    "returnDissatisfactionRate_status",
    "contactResponseTime_status",
    "productSafetyStatus_status",
    "lateShipmentRate_status",
    "intellectualProperty_status",
    "orderCancellationRate_status",
    "orderDefectRate_status",
    "productAuthenticityStatus_status",
]
