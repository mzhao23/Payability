from typing import Any, Optional


def score_odr(odr: Optional[float]) -> float:
    if odr is None:
        return 0.0
    if odr == 0:
        return 0.0
    if odr < 0.0015:
        return 3.0
    if odr < 0.004:
        return 6.0
    if odr < 0.008:
        return 8.0
    if odr < 0.015:
        return 10.0
    return 10.0


def score_chargeback(rate: Optional[float]) -> float:
    if rate is None:
        return 0.0
    if rate == 0:
        return 0.0
    if rate < 0.0002:
        return 3.0
    if rate < 0.0007:
        return 6.0
    if rate < 0.0015:
        return 8.0
    return 10.0


def score_a_to_z(rate: Optional[float]) -> float:
    if rate is None:
        return 0.0
    if rate == 0:
        return 0.0
    if rate < 0.0002:
        return 3.0
    if rate < 0.001:
        return 6.0
    if rate < 0.003:
        return 8.0
    return 10.0


def score_negative_feedback(rate: Optional[float]) -> float:
    if rate is None:
        return 0.0
    if rate == 0:
        return 0.0
    if rate < 0.0015:
        return 3.0
    if rate < 0.004:
        return 6.0
    if rate < 0.012:
        return 8.0
    return 10.0


def score_late_shipment(rate: Optional[float]) -> float:
    if rate is None:
        return 0.0
    if rate < 0.008:
        return 0.0
    if rate < 0.02:
        return 6.0
    if rate < 0.04:
        return 8.0
    return 10.0


def score_cancellation(rate: Optional[float]) -> float:
    if rate is None:
        return 0.0
    if rate < 0.004:
        return 0.0
    if rate < 0.015:
        return 6.0
    if rate < 0.03:
        return 8.0
    return 10.0


def score_response_hours(hours: Optional[float]) -> float:
    if hours is None:
        return 0.0
    if hours < 5:
        return 0.0
    if hours < 12:
        return 6.0
    if hours < 24:
        return 8.0
    return 10.0


def score_no_response(count: Optional[float]) -> float:
    if count is None:
        return 0.0
    if count == 0:
        return 0.0
    if count < 2:
        return 6.0
    if count < 6:
        return 8.0
    return 10.0


def score_valid_tracking(rate: Optional[float]) -> float:
    if rate is None:
        return 0.0
    if rate >= 0.98:
        return 0.0
    if rate >= 0.97:
        return 6.0
    if rate >= 0.94:
        return 8.0
    return 10.0


def score_on_time_delivery(rate: Optional[float]) -> float:
    if rate is None:
        return 0.0
    if rate >= 0.97:
        return 0.0
    if rate >= 0.95:
        return 6.0
    if rate >= 0.91:
        return 8.0
    return 10.0


def score_status(value: Any) -> float:
    if value is None:
        return 0.0
    v = str(value).strip().lower()
    if v in {"good", "ok", "healthy"}:
        return 0.0
    if v in {"fair", "warning", "watch"}:
        return 7.0
    return 10.0


def activity_gate(order_count_60: Optional[float]) -> float:
    if order_count_60 is None:
        return 0.40
    if order_count_60 == 0:
        return 0.40
    if order_count_60 < 25:
        return 0.60
    if order_count_60 < 100:
        return 0.80
    if order_count_60 < 500:
        return 0.95
    return 1.0


def inactivity_penalty(order_count_60: Optional[float]) -> float:
    if order_count_60 is None:
        return 5.0
    if order_count_60 == 0:
        return 5.0
    if order_count_60 < 25:
        return 3.0
    return 0.0


def risk_level_from_score(score: float) -> str:
    if score < 2:
        return "Healthy"
    if score < 4:
        return "Watch"
    if score < 7:
        return "Risky"
    return "Critical"
