from datetime import date

from health_risk.scoring.engine import RiskScoreEngine
from health_risk.scoring import subscores as S


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
        "fba_orders_60": 500.0,
        "fbm_orders_60": 500.0,
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
    assert "fba_orders_60" in out
    assert "fbm_orders_60" in out


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


def test_pure_fba_reduces_operational_impact():
    """A pure-FBA seller with bad operational metrics should score lower than
    an identical FBM seller, because operational metrics are Amazon's
    responsibility for FBA."""
    engine = RiskScoreEngine()
    bad_ops = dict(
        lateShipment_30_rate=8.0,
        preFulfillmentCancellation_30_rate=5.0,
        validTracking_rate_30=85.0,
        onTimeDelivery_rate_30=80.0,
    )
    fbm_score = engine.score_supplier_row(
        _minimal_row(fbm_orders_60=500.0, fba_orders_60=500.0, **bad_ops)
    )["risk_score"]
    fba_score = engine.score_supplier_row(
        _minimal_row(fbm_orders_60=0.0, fba_orders_60=1000.0, **bad_ops)
    )["risk_score"]
    assert fba_score < fbm_score, (
        f"Pure FBA ({fba_score}) should be lower than FBM ({fbm_score})"
    )


def test_low_fbm_ratio_reduces_operational_impact():
    """A seller with 10 FBM / 10000 total (0.1% FBM) should have a much lower
    operational impact than a seller with 500 FBM / 1000 total (50% FBM),
    even though the absolute FBM count (10) is not tiny."""
    engine = RiskScoreEngine()
    bad_ops = dict(
        lateShipment_30_rate=8.0,
        preFulfillmentCancellation_30_rate=5.0,
        validTracking_rate_30=85.0,
        onTimeDelivery_rate_30=80.0,
    )
    low_ratio = engine.score_supplier_row(
        _minimal_row(fbm_orders_60=10.0, fba_orders_60=9990.0, **bad_ops)
    )["risk_score"]
    high_ratio = engine.score_supplier_row(
        _minimal_row(fbm_orders_60=500.0, fba_orders_60=500.0, **bad_ops)
    )["risk_score"]
    assert low_ratio < high_ratio, (
        f"Low FBM ratio ({low_ratio}) should be lower than high ratio ({high_ratio})"
    )


def test_missing_order_data_falls_back_gracefully():
    """When fba_orders_60 / fbm_orders_60 are None (data missing), the engine
    should still produce a valid score using the conservative default gate."""
    engine = RiskScoreEngine()
    out = engine.score_supplier_row(
        _minimal_row(fba_orders_60=None, fbm_orders_60=None)
    )
    assert 0.0 <= out["risk_score"] <= 10.0


# ── subscores unit tests ──────────────────────────────────────────────────────

def test_activity_gate_operational_pure_fba():
    assert S.activity_gate_operational(0, 1000) == 0.10

def test_activity_gate_operational_heavy_fbm():
    assert S.activity_gate_operational(500, 600) == 1.0

def test_activity_gate_operational_none():
    assert S.activity_gate_operational(None, None) == 0.30

def test_activity_gate_operational_low_volume_cap():
    """Even 100% FBM ratio, if only 3 orders, cap at 0.40 (noisy data)."""
    assert S.activity_gate_operational(3, 3) == 0.40

def test_activity_gate_operational_mid_ratio():
    """20-50% FBM ratio with enough volume should give 0.70."""
    assert S.activity_gate_operational(100, 400) == 0.70
