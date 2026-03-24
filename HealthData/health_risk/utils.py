from datetime import datetime, timezone
from decimal import Decimal
import time
from typing import Any, Optional


def iso(v: Any) -> Optional[str]:
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def pct_to_ratio(v: Any) -> Optional[float]:
    """
    BigQuery values appear to be percentage-style values.
    Examples:
      0.418  -> 0.00418
      99.74  -> 0.9974
    """
    x = safe_float(v)
    if x is None:
        return None
    return x / 100.0


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def backoff_sleep(attempt: int, base: float = 0.5, cap: float = 5.0) -> None:
    time.sleep(min(cap, base * (2**attempt)))


def normalize_key(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip().lower()
    return s if s else None
