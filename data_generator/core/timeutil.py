"""
Realistic timestamp helpers for Africa/Cairo.
- real timezone offset (Egypt DST is back since 2023) with fixed +02:00 fallback
- operating-day logic (SAT-THU, Friday closed) + holidays
- business-hours timestamps, shift patterns, and event->posted lag
"""
from __future__ import annotations
import random
from datetime import date, datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
    CAIRO = ZoneInfo("Africa/Cairo")
except Exception:  # tzdata missing (common on Windows)
    CAIRO = timezone(timedelta(hours=2))

_DAY = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}


def parse_operating_days(spec: str = "SAT-THU") -> set[int]:
    """'SAT-THU' -> {5,6,0,1,2,3} (wraps over the week end)."""
    a, b = (s.strip().upper() for s in spec.split("-"))
    i, out = _DAY[a], set()
    while True:
        out.add(i)
        if i == _DAY[b]:
            return out
        i = (i + 1) % 7


def is_operating_day(d: date, spec: str = "SAT-THU", holidays: set[date] | None = None) -> bool:
    return d.weekday() in parse_operating_days(spec) and d not in (holidays or set())


def next_operating_day(d: date, spec: str = "SAT-THU", holidays: set[date] | None = None) -> date:
    while not is_operating_day(d, spec, holidays):
        d += timedelta(days=1)
    return d


def add_operating_days(d: date, n: int, spec: str = "SAT-THU", holidays: set[date] | None = None) -> date:
    d = next_operating_day(d, spec, holidays)
    for _ in range(n):
        d = next_operating_day(d + timedelta(days=1), spec, holidays)
    return d


def holiday_dates(calendar_rows: list[dict]) -> set[date]:
    """Expand calendar events with closes_warehouse=True into a set of dates."""
    out = set()
    for r in calendar_rows:
        if r.get("closes_warehouse"):
            s = date.fromisoformat(str(r["start_date"]))
            e = date.fromisoformat(str(r["end_date"]))
            for i in range((e - s).days + 1):
                out.add(s + timedelta(days=i))
    return out


def business_ts(rng: random.Random, d: date, start_h: float = 8, end_h: float = 18,
                peak_h: float | None = None) -> datetime:
    """Random tz-aware timestamp inside [start_h, end_h); triangular around peak_h if given."""
    h = rng.triangular(start_h, end_h, peak_h) if peak_h is not None else rng.uniform(start_h, end_h)
    sec = min(int(h * 3600), int(end_h * 3600) - 1)
    return datetime(d.year, d.month, d.day, tzinfo=CAIRO) + timedelta(seconds=sec)


# (name, start_h, end_h, weight) - warehouse shifts
SHIFTS = [("MORNING", 6, 14, 0.5), ("EVENING", 14, 22, 0.35), ("NIGHT", 22, 30, 0.15)]


def shift_ts(rng: random.Random, d: date) -> datetime:
    """Warehouse event time; NIGHT shift spills into the next calendar day."""
    _, s, e, _ = rng.choices(SHIFTS, weights=[x[3] for x in SHIFTS])[0]
    base = datetime(d.year, d.month, d.day, tzinfo=CAIRO)
    return base + timedelta(seconds=rng.randint(s * 3600, e * 3600 - 1))


def posting_lag(rng: random.Random, event_ts: datetime) -> datetime:
    """posted_ts = event_ts + lag. 80% seconds-minutes, 15% hours (batch post), 5% next-day."""
    r = rng.random()
    if r < 0.80:
        lag = timedelta(seconds=rng.randint(5, 600))
    elif r < 0.95:
        lag = timedelta(minutes=rng.randint(30, 360))
    else:
        lag = timedelta(hours=rng.randint(12, 30))
    return event_ts + lag


def fmt(ts: datetime | None) -> str | None:
    """'YYYY-MM-DD HH:MM:SS+02:00' (matches the raw layer format, real offset)."""
    if ts is None:
        return None
    s = ts.strftime("%Y-%m-%d %H:%M:%S%z")
    return s[:-2] + ":" + s[-2:]
