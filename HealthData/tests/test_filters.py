from health_risk.filters import filter_active_population


def test_filter_active_population_excludes_suspended_and_pending():
    rows = [
        {"payability_status": "Suspended", "mp_sup_key": "a"},
        {"payability_status": "pending", "mp_sup_key": "b"},
        {"payability_status": "active", "mp_sup_key": "c"},
        {"payability_status": None, "mp_sup_key": "d"},
    ]
    out = filter_active_population(rows)
    keys = {r["mp_sup_key"] for r in out}
    assert keys == {"c", "d"}
