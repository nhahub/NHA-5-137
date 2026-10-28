"""
Reference Data Generators:
- R01: product_categories (CSV)
- R20: calendar_events (CSV)
- R21: weather_daily (JSON)
"""

import math
from datetime import date, timedelta
import pandas as pd
from data_generator.core.registry import DefectSpec, messy_case, set_value

L1_CATEGORIES = [
    ("CAT-01", "Dairy & Eggs", "CHILLED", True),
    ("CAT-02", "Bakery & Grains", "AMBIENT", False),
    ("CAT-03", "Beverages & Drinks", "AMBIENT", False),
    ("CAT-04", "Frozen Foods", "FROZEN", True),
    ("CAT-05", "Confectionery & Snacks", "AMBIENT", False),
    ("CAT-06", "Cooking Essentials & Oils", "AMBIENT", False),
    ("CAT-07", "Personal Care & Hygiene", "AMBIENT", False),
    ("CAT-08", "Household & Laundry", "AMBIENT", False),
]

L2_SUBCATEGORIES = {
    "CAT-01": [
        ("CAT-0101", "Fresh Milk & Laban", "CHILLED", True),
        ("CAT-0102", "White Cheese & Feta", "CHILLED", True),
        ("CAT-0103", "Hard & Yellow Cheese", "CHILLED", True),
        ("CAT-0104", "Yogurt & Curd", "CHILLED", True),
        ("CAT-0105", "Table Eggs", "AMBIENT", True),
    ],
    "CAT-02": [
        ("CAT-0201", "Egyptian Baladi & Shami Bread", "AMBIENT", True),
        ("CAT-0202", "Egyptian Rice", "AMBIENT", False),
        ("CAT-0203", "Macaroni & Pasta", "AMBIENT", False),
        ("CAT-0204", "Flour & Semolina", "AMBIENT", False),
        ("CAT-0205", "Breakfast Cereals & Oats", "AMBIENT", False),
    ],
    "CAT-03": [
        ("CAT-0301", "Mineral & Bottled Water", "AMBIENT", False),
        ("CAT-0302", "Fruit Nectars & Fresh Juices", "CHILLED", True),
        ("CAT-0303", "Carbonated Soft Drinks", "AMBIENT", False),
        ("CAT-0304", "Black & Green Tea", "AMBIENT", False),
        ("CAT-0305", "Instant Coffee & Beans", "AMBIENT", False),
    ],
    "CAT-04": [
        ("CAT-0401", "Frozen Poultry & Chicken", "FROZEN", True),
        ("CAT-0402", "Frozen Beef & Minced Meat", "FROZEN", True),
        ("CAT-0403", "Frozen Vegetables (Molokhia/Okra)", "FROZEN", True),
        ("CAT-0404", "Frozen French Fries", "FROZEN", True),
        ("CAT-0405", "Ice Cream & Frozen Desserts", "FROZEN", True),
    ],
    "CAT-05": [
        ("CAT-0501", "Chocolates & Bars", "AMBIENT", False),
        ("CAT-0502", "Biscuits & Wafers", "AMBIENT", False),
        ("CAT-0503", "Potato Chips & Crackers", "AMBIENT", False),
        ("CAT-0504", "Oriental Sweets & Halawa", "AMBIENT", False),
        ("CAT-0505", "Nuts & Dried Seeds", "AMBIENT", False),
    ],
    "CAT-06": [
        ("CAT-0601", "Corn & Sunflower Cooking Oil", "AMBIENT", False),
        ("CAT-0602", "Refined Table Sugar", "AMBIENT", False),
        ("CAT-0603", "Tomato Paste & Sauce", "AMBIENT", False),
        ("CAT-0604", "Ghee & Vegetable Butter", "AMBIENT", False),
        ("CAT-0605", "Table Salt & Spices", "AMBIENT", False),
    ],
    "CAT-07": [
        ("CAT-0701", "Bar Soap & Body Wash", "AMBIENT", False),
        ("CAT-0702", "Hair Shampoo & Conditioners", "AMBIENT", False),
        ("CAT-0703", "Toothpaste & Oral Care", "AMBIENT", False),
        ("CAT-0704", "Deodorants & Fragrances", "AMBIENT", False),
        ("CAT-0705", "Facial Care & Skin Lotions", "AMBIENT", False),
    ],
    "CAT-08": [
        ("CAT-0801", "Liquid & Powder Laundry Detergent", "AMBIENT", False),
        ("CAT-0802", "Dishwashing Liquids", "AMBIENT", False),
        ("CAT-0803", "Disinfectants & Chlorine Bleach", "AMBIENT", False),
        ("CAT-0804", "Facial Tissues & Toilet Paper", "AMBIENT", False),
        ("CAT-0805", "Garbage Bags & Foil Rolls", "AMBIENT", False),
    ]
}


CLEAN_L2_NAME = {cid: name for subs in L2_SUBCATEGORIES.values() for cid, name, _, _ in subs}


def generate_product_categories(reg) -> pd.DataFrame:
    """R01 product_categories. Defects: messy name case/spaces (25%), orphan parent (1 row)."""
    rows, base_ts = [], "2024-01-01 00:00:00+02:00"
    for cat_id, name, storage, perishable in L1_CATEGORIES:
        rows.append({"category_id": cat_id, "parent_category_id": None, "category_name": name,
                     "category_level": 1, "default_storage_type": storage,
                     "is_perishable_default": perishable, "updated_ts": base_ts})
    for l1_id, subcats in L2_SUBCATEGORIES.items():
        for cat_id, name, storage, perishable in subcats:
            rows.append({"category_id": cat_id, "parent_category_id": l1_id, "category_name": name,
                         "category_level": 2, "default_storage_type": storage,
                         "is_perishable_default": perishable, "updated_ts": base_ts})
    df = pd.DataFrame(rows)

    key = ["category_id"]
    df = reg.mutate(df, DefectSpec("DEF-CAT-001", "product_categories", "category_name",
                                   "whitespace / mixed case in category_name", 0.25, "fix"),
                    key, messy_case)
    df = reg.mutate(df, DefectSpec("DEF-CAT-002", "product_categories", "parent_category_id",
                                   "orphan parent_category_id (CAT-99 does not exist)", 0.005, "fix"),
                    key, set_value("CAT-99"), mask=df["category_level"] == 2)
    return df


# ---------------------------------------------------------------------------
# Calendar: computed for every year (no hand-typed lists), incl. one forward year
# ---------------------------------------------------------------------------
def _hijri_to_greg(hy: int, hm: int, hd: int) -> date:
    try:
        from hijridate import Hijri
        return Hijri(hy, hm, hd).to_gregorian()
    except Exception:
        try:
            from hijri_converter import Hijri
            return Hijri(hy, hm, hd).to_gregorian()
        except Exception:
            # tabular (civil) Islamic calendar fallback, accurate to ~1-2 days
            jd = hd + math.ceil(29.5 * (hm - 1)) + (hy - 1) * 354 + (3 + 11 * hy) // 30 + 1948439.5 - 1
            return date.fromordinal(int(jd - 1721424.5 + 0.5))


def _coptic_easter(year: int) -> date:
    a, b, c = year % 4, year % 7, year % 19
    d = (19 * c + 15) % 30
    e = (2 * a + 4 * b - d + 34) % 7
    month, day = divmod(d + e + 114, 31)
    return date(year, month, day + 1) + timedelta(days=13)   # Julian -> Gregorian


def _last_friday_november(year: int) -> date:
    d = date(year, 11, 30)
    while d.weekday() != 4:
        d -= timedelta(days=1)
    return d


def _events_for_year(y: int) -> list[tuple]:
    """(name, type, start, end, closes_warehouse, demand_effect, source_note)"""
    O, A = "OFFICIAL_CALENDAR", "ASSUMPTION"
    ev = [
        ("Coptic Christmas Day", "PUBLIC_HOLIDAY", date(y, 1, 7), date(y, 1, 7), True, "DOWN", O),
        ("Revolution & Police Day", "PUBLIC_HOLIDAY", date(y, 1, 25), date(y, 1, 25), True, "DOWN", O),
        ("Sinai Liberation Day", "PUBLIC_HOLIDAY", date(y, 4, 25), date(y, 4, 25), True, "DOWN", O),
        ("Labor Day", "PUBLIC_HOLIDAY", date(y, 5, 1), date(y, 5, 1), True, "DOWN", O),
        ("June 30 Revolution Day", "PUBLIC_HOLIDAY", date(y, 6, 30), date(y, 6, 30), True, "DOWN", O),
        ("Revolution Day (July 23)", "PUBLIC_HOLIDAY", date(y, 7, 23), date(y, 7, 23), True, "DOWN", O),
        ("Armed Forces Day (6th of October)", "PUBLIC_HOLIDAY", date(y, 10, 6), date(y, 10, 6), True, "DOWN", O),
        ("Back to School Season", "SCHOOL_SEASON", date(y, 9, 15), date(y, 10, 5), False, "UP", A),
    ]
    sham = _coptic_easter(y) + timedelta(days=1)
    ev.append(("Sham El-Nessim", "PUBLIC_HOLIDAY", sham, sham, True, "DOWN", O))
    bf = _last_friday_november(y)
    ev.append(("White Friday / Black Friday Week", "RETAIL_EVENT", bf - timedelta(days=5),
               bf + timedelta(days=1), False, "UP", A))
    # lunar events: scan the Hijri years that overlap Gregorian year y
    for hy in range(int((y - 622) * 33 / 32) - 1, int((y - 622) * 33 / 32) + 3):
        r1 = _hijri_to_greg(hy, 9, 1)
        s1 = _hijri_to_greg(hy, 10, 1)
        arafat = _hijri_to_greg(hy, 12, 9)
        newyr = _hijri_to_greg(hy, 1, 1)
        mawlid = _hijri_to_greg(hy, 3, 12)
        lunar = [
            ("Holy Month of Ramadan", "RELIGIOUS_SEASON", r1, s1 - timedelta(days=1), False, "UP", A),
            ("Eid al-Fitr Holiday", "PUBLIC_HOLIDAY", s1, s1 + timedelta(days=3), True, "MIXED", A),
            ("Arafat Day & Eid al-Adha", "PUBLIC_HOLIDAY", arafat, arafat + timedelta(days=4), True, "MIXED", A),
            ("Islamic New Year", "PUBLIC_HOLIDAY", newyr, newyr, True, "DOWN", A),
            ("Prophet Birthday (Mawlid)", "PUBLIC_HOLIDAY", mawlid, mawlid, True, "UP", A),
        ]
        ev += [e for e in lunar if e[2].year == y]
    return ev


def generate_calendar_events(start_year: int, end_year: int, reg) -> pd.DataFrame:
    """R20 calendar_events for start_year..end_year+1 (one forward year for forecasting)."""
    rows, base_ts = [], "2024-01-01 00:00:00+02:00"
    for y in range(start_year, end_year + 2):
        for n, e in enumerate(sorted(_events_for_year(y), key=lambda x: x[2]), start=1):
            rows.append({"event_id": f"EVT-{y}-{n:02d}", "event_name": e[0], "event_type": e[1],
                         "start_date": e[2].isoformat(), "end_date": e[3].isoformat(),
                         "closes_warehouse": e[4], "expected_demand_effect": e[5],
                         "source_note": e[6], "updated_ts": base_ts})
    df = pd.DataFrame(rows)
    variants = ["  eid el fitr (est)  ", "EID AL FITR HOLIDAY - Moon Sighting TBD", "Eid Al-Fitr (tentative)"]
    df = reg.mutate(df, DefectSpec("DEF-CAL-001", "calendar_events", "event_name",
                                   "Eid event name variant / moon-sighting uncertainty", 0.04, "fix"),
                    ["event_id"], lambda v, row, rng: rng.choice(variants),
                    mask=df["event_name"].str.contains("Eid al-Fitr", regex=False))
    return df


# ---------------------------------------------------------------------------
# Weather: smooth seasonality, physical limits, defects injected afterwards
# ---------------------------------------------------------------------------
_LOCS = {  # annual mean max, seasonal amplitude, diurnal range, floor for min, rain prob (winter)
    "GIZA":       dict(mean=28.0, amp=9.0,  rng=(9, 15), floor=3.0, rain=0.06),
    "ALEXANDRIA": dict(mean=25.0, amp=6.5,  rng=(5, 9),  floor=5.0, rain=0.20),
    "ASSIUT":     dict(mean=31.0, amp=10.5, rng=(11, 17), floor=2.0, rain=0.03),
}


def generate_weather_daily(start_date: date, end_date: date, rng, reg) -> list:
    """R21 weather_daily. Defects: missing days (1%), Fahrenheit values (0.5%)."""
    rows, d = [], start_date
    while d <= end_date:
        doy = d.timetuple().tm_yday
        season = math.cos(2 * math.pi * (doy - 200) / 365)          # peak ~ 19 July
        winter = d.month in (11, 12, 1, 2, 3)
        for loc, c in _LOCS.items():
            tmax = round(c["mean"] + c["amp"] * season + rng.gauss(0, 2.0), 1)
            tmin = round(max(c["floor"], tmax - rng.uniform(*c["rng"])), 1)
            rain_p = c["rain"] if winter else c["rain"] * 0.02
            precip = round(rng.uniform(0.5, 12.0), 1) if rng.random() < rain_p else 0.0
            rows.append({"obs_date": d.isoformat(), "location_code": loc, "temp_max_c": tmax,
                         "temp_min_c": tmin, "precip_mm": precip, "source": "SYNTHETIC_MODEL"})
        d += timedelta(days=1)
    df = pd.DataFrame(rows)
    key = ["obs_date", "location_code"]
    df = reg.mutate_multi(df, DefectSpec("DEF-WTH-001", "weather_daily", "temp_max_c|temp_min_c",
                                         "temperature recorded in Fahrenheit", 0.005, "fix"), key,
                          lambda r, rng: {"temp_max_c": round(r["temp_max_c"] * 1.8 + 32, 1),
                                          "temp_min_c": round(r["temp_min_c"] * 1.8 + 32, 1)})
    df = reg.drop_rows(df, DefectSpec("DEF-WTH-002", "weather_daily", None,
                                      "missing observation day", 0.01, "flag"), key)
    return df.to_dict(orient="records")
