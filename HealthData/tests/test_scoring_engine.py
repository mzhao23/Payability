from datetime import date

from health_risk.scoring.engine import RiskScoreEngine


def _minimal_row(**overrides):
    base = {
        "report_date": date(2025, 3, 1),
        "mp_sup_key": "mp_test_1",
        "snapshot_date": None,
        "supplier_key": "sup_1",
        "supplier_name": "Name",
        "payability_status": "active",
        "orderWithDefects_60_rate": 0.0,
        "chargebacks_90_rate": 0.0,
        "a_z_claims_90_rate": 0.0,
        "negativeFeedbacks_90_rate": 0.0,
        "lateShipment_30_rate": 0.0,
        "preFulfillmentCancellation_30_rate": 0.0,
        "averageResponseTimeInHours_30": 0.0,
        "noResponseForContactsOlderThan24Hours_30": 0.0,
        "validTracking_rate_30": 100.0,
        "onTimeDelivery_rate_30": 100.0,
        "productSafetyStatus_status": "Good",
        "productAuthenticityStatus_status": "Good",
        "policyViolation_status": "Good",
        "listingPolicyStatus_status": "Good",
        "intellectualProperty_status": "Good",
        "orders_count_60": 1000.0,
    }
    base.update(overrides)
    return base


def test_score_supplier_row_returns_expected_keys():
    engine = RiskScoreEngine()
    out = engine.score_supplier_row(_minimal_row())
    assert out["pipeline_version"] == RiskScoreEngine.PIPELINE_VERSION
    assert "risk_score" in out
    assert "risk_level" in out
    assert "top_risk_drivers" in out
    assert out["mp_sup_key"] == "mp_test_1"


def test_build_payload_dedupes_by_report_date_and_mp_key():
    engine = RiskScoreEngine()
    r = _minimal_row()
    rows = [r, dict(r)]
    payload = engine.build_payload(rows)
    assert len(payload) == 1


def test_high_odr_increases_risk_score():
    engine = RiskScoreEngine()
    low = engine.score_supplier_row(_minimal_row(orderWithDefects_60_rate=0.0))["risk_score"]
    high = engine.score_supplier_row(_minimal_row(orderWithDefects_60_rate=5.0))["risk_score"]
    assert high > low
