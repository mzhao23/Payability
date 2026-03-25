from __future__ import annotations

import os
from pathlib import Path

import pytest

HEALTHDATA_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def healthdata_root() -> Path:
    return HEALTHDATA_ROOT


@pytest.fixture(autouse=True)
def _supabase_env_for_unit_tests(monkeypatch: pytest.MonkeyPatch) -> None:
    if not os.environ.get("SUPABASE_URL"):
        monkeypatch.setenv("SUPABASE_URL", "http://test.local")
    if not os.environ.get("SUPABASE_SERVICE_ROLE_KEY"):
        monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-key")
