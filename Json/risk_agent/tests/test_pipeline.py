"""tests/test_pipeline.py

Offline unit tests — no BigQuery or Supabase connection required.
Run with:  python -m pytest tests/ -v
"""

from __future__ import annotations

import json
import sys
import pathlib

# Add project root to path
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import pytest

from extractors.feature_extractor import extract_features, classify_error, FeatureSet
from scoring.rule_scorer import score as rule_score


# ── Sample data fixtures ───────────────────────────────────────────────────────

SAMPLE_DATA_HEALTHY = json.dumps({
    "Supplier Key": "test-key-001",
    "Supplier Name": "Healthy Seller LLC",
    "Store Name": "Healthy Store",
    "Account Status": "OK",
    "Two step verification": "Active",
    "Account Performance Info": {
        "Order Defect Rate": "0.13%",
        "Late Shipment Rate": "1.73%",
        "Cancellation Rate": "0.09%",
        "Valid Tracking Rate - All Categories": "99.49%",
        "Delivered on time": "93.3%",
    },
    "feedback": {
        "Summary": "4.8 stars",
        "Positive": {"30 days": "90 %(90)"},
        "Negative": {"30 days": "5 %(5)"},
        "Neutral": {"30 days": "5 %(5)"},
        "Count": {"30 days": "100"},
    },
    "policy_compliance": {
        "Regulatory Compliance": "1",
        "Other Policy Violations": "5",
        "Listing Policy Violations": "2",
        "Food and Product Safety Issues": "0",
        "Restricted Product Policy Violations": "0",
        "Product Authenticity Customer Complaints": "0",
        "Received Intellectual Property Complaints": "0",
        "Suspected Intellectual Property Violations": "0",
    },
    "StatementsSummary": {
        "Total Balance": {"All Accounts": "$10,000.00"},
        "Funds Available": {"All Accounts": "$10,000.00"},
    },
    "Loans": {},
    "Closed Loans": [],
    "Last Notification Titles": ["March 1, 2026: Your payout was processed"],
})

SAMPLE_DATA_HIGH_RISK = json.dumps({
    "Supplier Key": "test-key-002",
    "Supplier Name": "Risky Seller Co",
    "Account Status": "OK",
    "Two step verification": "Active",
    "Account Performance Info": {
        "Order Defect Rate": "2.5%",       # > 1% threshold
        "Late Shipment Rate": "6.0%",      # > 4% threshold
        "Cancellation Rate": "3.5%",       # > 2.5%
        "Delivered on time": "80%",        # < 90%
    },
    "feedback": {
        "Summary": "2.1 stars",
        "Positive": {"30 days": "20 %(20)"},
        "Negative": {"30 days": "65 %(65)"},
        "Neutral": {"30 days": "15 %(15)"},
        "Count": {"30 days": "100"},
    },
    "policy_compliance": {
        "Other Policy Violations": "2000",
        "Listing Policy Violations": "200",
        "Restricted Product Policy Violations": "15",
        "Received Intellectual Property Complaints": "3",
        "Suspected Intellectual Property Violations": "5",
        "Product Authenticity Customer Complaints": "2",
    },
    "Loans": {},
    "Closed Loans": [],
    "Last Notification Titles": [
        "Feb 24: Action Required: Listing Removed from Amazon",
        "Feb 20: Restricted Products removal - Resolve violations",
        "Feb 18: Account at risk of deactivation",
        "Feb 15: Trademark violation notice",
        "Feb 10: Listing deactivated",
        "Feb 5: Urgent: policy warning",
        "Feb 1: Restricted Products removal",
        "Jan 28: Account health warning",
        "Jan 25: Listing removed",
        "Jan 20: Restricted Products removal",
    ],
})

SAMPLE_DATA_LOGIN_ERROR = json.dumps({
    "Error": "Error: Something was wrong in login process",
    "Proxy": "1.2.3.4:65432",
    "Supplier Key": "test-key-003",
    "Supplier Name": "Login Error Supplier",
})

SAMPLE_DATA_PAST_DUE = json.dumps({
    "Supplier Key": "test-key-004",
    "Supplier Name": "Past Due LLC",
    "Account Status": "OK",
    "Two step verification": "Active",
    "Account Performance Info": {
        "Order Defect Rate": "0.5%",
    },
    "Loans": {
        "loan_001": {
            "outstandingLoanAmount": 50000.0,
            "pastDueAmount": 15000.0,
        }
    },
    "Closed Loans": [],
    "policy_compliance": {},
    "Last Notification Titles": [],
})


def _make_row(data_json: str, extra: dict | None = None) -> dict:
    """Build a minimal BQ-row-like dict."""
    row = {"mp_sup_key": "test-key", "created_date": "2026-03-02", "data": data_json}
    if extra:
        row.update(extra)
    return row


# ── Tests ──────────────────────────────────────────────────────────────────────

class TestClassifyError:
    def test_login_error(self):
        assert classify_error('{"Error": "Something was wrong in login process"}') == "login_error"

    def test_not_authorized(self):
        assert classify_error('{"Error": "Not Authorized: This email address isn\'t associated"}') == "not_authorized"

    def test_wrong_password(self):
        assert classify_error('{"Error": "Authorization error: / Your password is incorrect"}') == "wrong_password"

    def test_bank_page_error(self):
        assert classify_error('{"Error": "Internal Error in processing Bank Account page: The data has not been displayed"}') == "bank_page_error"

    def test_no_error(self):
        assert classify_error('{"Supplier Key": "abc", "Account Status": "OK"}') is None


class TestFeatureExtractor:
    def test_healthy_seller(self):
        row = _make_row(SAMPLE_DATA_HEALTHY)
        fs = extract_features(row)
        assert fs.supplier_key == "test-key-001"
        assert fs.data_quality_flag == "ok"
        assert fs.order_defect_rate is not None
        assert abs(fs.order_defect_rate - 0.13) < 0.01
        assert fs.late_shipment_rate is not None
        assert abs(fs.late_shipment_rate - 1.73) < 0.01
        assert fs.feedback_negative_30d == 5.0
        assert fs.two_step_verification == "Active"

    def test_login_error_row(self):
        row = _make_row(SAMPLE_DATA_LOGIN_ERROR)
        fs = extract_features(row)
        assert fs.data_quality_flag == "login_error"
        assert fs.supplier_key == "test-key-003"
        assert "login" in (fs.raw_error or "").lower()

    def test_past_due_loan(self):
        row = _make_row(SAMPLE_DATA_PAST_DUE)
        fs = extract_features(row)
        assert fs.past_due_amount == 15000.0
        assert fs.outstanding_loan_amount == 50000.0

    def test_high_risk_notifications(self):
        row = _make_row(SAMPLE_DATA_HIGH_RISK)
        fs = extract_features(row)
        assert fs.high_risk_notification_count >= 5


class TestRuleScorer:
    def test_healthy_seller_low_score(self):
        row = _make_row(SAMPLE_DATA_HEALTHY)
        fs = extract_features(row)
        result = rule_score(fs)
        assert result.preliminary_score <= 5, f"Expected <= 5, got {result.preliminary_score}"

    def test_high_risk_seller_high_score(self):
        row = _make_row(SAMPLE_DATA_HIGH_RISK)
        fs = extract_features(row)
        result = rule_score(fs)
        assert result.preliminary_score >= 7, f"Expected >= 7, got {result.preliminary_score}"
        # Should have triggered ODR, late shipment, cancellation rules
        combined = " ".join(result.triggered_rules)
        assert "ORDER_DEFECT" in combined or "LATE_SHIPMENT" in combined

    def test_past_due_loan_critical(self):
        row = _make_row(SAMPLE_DATA_PAST_DUE)
        fs = extract_features(row)
        result = rule_score(fs)
        assert result.preliminary_score >= 9, f"Past due loan should score >= 9, got {result.preliminary_score}"
        assert any("LOAN_PAST_DUE" in r for r in result.triggered_rules)

    def test_login_error_scores_correctly(self):
        row = _make_row(SAMPLE_DATA_LOGIN_ERROR)
        fs = extract_features(row)
        result = rule_score(fs)
        assert result.preliminary_score >= 5
        assert any("DATA_QUALITY" in r for r in result.triggered_rules)

    def test_score_bounds(self):
        """Score must always be between 1 and 10."""
        for data in [SAMPLE_DATA_HEALTHY, SAMPLE_DATA_HIGH_RISK, SAMPLE_DATA_LOGIN_ERROR]:
            row = _make_row(data)
            fs = extract_features(row)
            result = rule_score(fs)
            assert 1 <= result.preliminary_score <= 10


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
