"""nyc-restroom-finder.py - NYC Public Restroom Finder backend (team 11).

One file, Starlette + uvicorn. Run it with:

    python nyc-restroom-finder.py

Then open http://127.0.0.1:8011/ (the server also hands out index.html from this
folder, so the map tiles and the API work without any extra setup).

On first start it downloads the NYC Open Data "Public Restrooms" dataset
(i7jb-7jku) and the borough boundaries, and stores them in restrooms.db beside
this file. Every later start reuses that database. To re-import, stop the server,
delete restrooms.db and start it again (or run: python nyc-restroom-finder.py --sync).

Third-party packages: starlette, uvicorn. Everything else is standard library.
Works on Python 3.9 and newer.
"""

import contextlib
import hashlib
import json
import math
import os
import sqlite3
import sys
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone

import uvicorn
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import FileResponse, JSONResponse, PlainTextResponse
from starlette.routing import Mount, Route

# --- Configuration -----------------------------------------------------------
# If you change ROUTE_PREFIX or PORT, change API_BASE at the top of index.html too.

ROUTE_PREFIX = os.getenv("TEAM11_API_PREFIX", "/api/team11").rstrip("/")
PORT = int(os.getenv("TEAM11_PORT", "8011"))

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.getenv("TEAM11_DB", os.path.join(HERE, "restrooms.db"))
INDEX_HTML = os.path.join(HERE, "index.html")

# ":*,*" also returns NYC's computed region columns, used as a borough fallback.
RESTROOMS_URL = ("https://data.cityofnewyork.us/resource/i7jb-7jku.json"
                 "?$select=:*,*&$limit=5000")
BOROUGHS_URL = "https://data.cityofnewyork.us/resource/gthc-hcne.json"
REGION_FIELD = ":@computed_region_yeji_bk3q"
BOROUGH_BY_REGION = {"1": "Staten Island", "2": "Brooklyn", "3": "Queens",
                     "4": "Manhattan", "5": "Bronx"}

# New York City bounding box.
NYC_LAT_MIN, NYC_LAT_MAX = 40.4774, 40.9176
NYC_LON_MIN, NYC_LON_MAX = -74.2591, -73.7004
SHORELINE_TOLERANCE_M = 500  # piers and beaches just outside a borough polygon

TEXT_FIELDS = [
    "facility_name", "location_type", "operator", "status", "open",
    "hours_of_operation", "accessibility", "restroom_type",
    "changing_stations", "menstrual_friendly", "additional_notes",
]
RESTROOM_FILTERS = [
    "borough", "accessibility", "location_type", "open",
    "changing_stations", "menstrual_friendly", "status",
]
STATS_FILTERS = ["borough", "location_type", "accessibility"]
UNKNOWN_FILTER_VALUE = "unknown"   # filter value that matches a missing field
UNKNOWN_LABEL = "Unknown"
ACCESSIBILITY_ORDER = [
    "Fully Accessible", "Partially Accessible", "Limited Accessibility",
    "Not Accessible", UNKNOWN_LABEL,
]

# Report categories (same keys as the report form). "closes" ones make a restroom
# show as "Reported closed" while the report is active.
REPORT_CATEGORIES = [
    "closed", "out_of_order", "wrong_hours", "supplies",
    "cleanliness", "access", "safety",
]
CLOSING_CATEGORIES = {"closed", "out_of_order"}
# Older category names from the first API version are still accepted.
CATEGORY_ALIASES = {
    "dirty": "cleanliness", "no_supplies": "supplies",
    "accessibility_problem": "access", "unsafe": "safety", "other": "cleanliness",
}
SEEN_CHOICES = (0, 180, 1440)       # "just now", "earlier today", "yesterday"
NOTE_MAX_LENGTH = 280

REPORT_ACTIVE_HOURS = 48   # a report affects status and counts as "active" this long
REPORT_KEEP_DAYS = 7       # older than this, a report is returned with "stale": true
RATE_LIMIT_MINUTES = 60    # one report per restroom per visitor in this window

SCHEMA = """
CREATE TABLE IF NOT EXISTS restrooms(
    id TEXT PRIMARY KEY, facility_name TEXT, location_type TEXT, operator TEXT,
    status TEXT, open TEXT, hours_of_operation TEXT, accessibility TEXT,
    restroom_type TEXT, changing_stations TEXT, menstrual_friendly TEXT,
    additional_notes TEXT, website TEXT, latitude REAL, longitude REAL,
    borough TEXT, synced_at TEXT);

CREATE TABLE IF NOT EXISTS reports(
    id INTEGER PRIMARY KEY, restroom_id TEXT NOT NULL REFERENCES restrooms(id),
    category TEXT NOT NULL, note TEXT,
    created_at TEXT NOT NULL,          -- when the server received it (UTC)
    observed_at TEXT NOT NULL,         -- when the visitor saw it (created_at - seen)
    client TEXT);                      -- salted hash of the sender's address

CREATE TABLE IF NOT EXISTS confirmations(
    id INTEGER PRIMARY KEY, report_id INTEGER NOT NULL REFERENCES reports(id),
    agrees INTEGER NOT NULL CHECK (agrees IN (0, 1)), created_at TEXT NOT NULL,
    client TEXT);

CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT);

CREATE INDEX IF NOT EXISTS reports_restroom_idx ON reports(restroom_id);
CREATE INDEX IF NOT EXISTS confirmations_report_idx ON confirmations(report_id);
"""


# --- Helpers -----------------------------------------------------------------

def iso(moment):
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_now():
    return iso(datetime.now(timezone.utc))


def hours_ago(hours):
    return iso(datetime.now(timezone.utc) - timedelta(hours=hours))


def active_cutoff():
    return hours_ago(REPORT_ACTIVE_HOURS)


def stale_cutoff():
    return hours_ago(REPORT_KEEP_DAYS * 24)


def connect():
    db = sqlite3.connect(DB_PATH, timeout=15)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    return db


def fetch_json(url):
    request = urllib.request.Request(url, headers={"User-Agent": "team11-import",
                                                   "Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.loads(response.read().decode("utf-8"))


def clean(value, unquote=False):
    """Trim; empty becomes None. unquote=True drops one pair of wrapping quotes."""
    if value is None:
        return None
    value = str(value).strip()
    if unquote and len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        value = value[1:-1].strip()
    return value or None


def flatten_website(value):
    if isinstance(value, dict):
        value = value.get("url")
    return clean(value, unquote=True)


def client_key(request):
    """A salted hash of the visitor's address, used only for the rate limit.
    Behind the course proxy the real address is the first X-Forwarded-For entry."""
    forwarded = request.headers.get("x-forwarded-for", "")
    address = forwarded.split(",")[0].strip() or (request.client.host if request.client else "")
    return hashlib.sha256(("team11|" + address).encode("utf-8")).hexdigest()[:16]


# --- Borough lookup (plain-Python point in polygon) --------------------------

def load_boroughs():
    boroughs = []
    for row in fetch_json(BOROUGHS_URL):
        geometry = row["the_geom"]
        polygons = geometry["coordinates"]
        if geometry["type"] == "Polygon":
            polygons = [polygons]
        shapes = []
        for rings in polygons:
            xs = [p[0] for p in rings[0]]
            ys = [p[1] for p in rings[0]]
            shapes.append({"bbox": (min(xs), min(ys), max(xs), max(ys)), "rings": rings})
        boroughs.append((row["boroname"], shapes))
    if len(boroughs) != 5:
        raise RuntimeError("expected 5 boroughs, got %d" % len(boroughs))
    return boroughs


def point_in_ring(lon, lat, ring):
    inside = False
    x1, y1 = ring[-1][0], ring[-1][1]
    for point in ring:
        x2, y2 = point[0], point[1]
        if (y1 > lat) != (y2 > lat):
            if lon < (x2 - x1) * (lat - y1) / (y2 - y1) + x1:
                inside = not inside
        x1, y1 = x2, y2
    return inside


def point_in_polygon(lon, lat, shape):
    west, south, east, north = shape["bbox"]
    if not (west <= lon <= east and south <= lat <= north):
        return False
    rings = shape["rings"]
    if not point_in_ring(lon, lat, rings[0]):
        return False
    return not any(point_in_ring(lon, lat, hole) for hole in rings[1:])


def distance_to_ring_m(lon, lat, ring):
    kx, ky = 84400.0, 111200.0  # metres per degree near 40.7 N
    best = float("inf")
    ax, ay = (ring[-1][0] - lon) * kx, (ring[-1][1] - lat) * ky
    for point in ring:
        bx, by = (point[0] - lon) * kx, (point[1] - lat) * ky
        dx, dy = bx - ax, by - ay
        length_sq = dx * dx + dy * dy
        t = 0.0 if length_sq == 0 else max(0.0, min(1.0, -(ax * dx + ay * dy) / length_sq))
        cx, cy = ax + t * dx, ay + t * dy
        best = min(best, cx * cx + cy * cy)
        ax, ay = bx, by
    return best ** 0.5


def find_borough(lon, lat, boroughs):
    """Return (borough or None, how): 'polygon', 'nearest' or 'none'."""
    for name, shapes in boroughs:
        if any(point_in_polygon(lon, lat, shape) for shape in shapes):
            return name, "polygon"
    best_name, best_distance = None, float("inf")
    for name, shapes in boroughs:
        for shape in shapes:
            distance = distance_to_ring_m(lon, lat, shape["rings"][0])
            if distance < best_distance:
                best_name, best_distance = name, distance
    if best_distance <= SHORELINE_TOLERANCE_M:
        return best_name, "nearest"
    return None, "none"


# --- Import ------------------------------------------------------------------

def init_db():
    with contextlib.closing(connect()) as db:
        db.executescript(SCHEMA)
        db.commit()


def import_rows(rows, boroughs):
    """Clean the raw dataset rows and upsert them. Returns a summary dict.
    boroughs may be None (boundaries unavailable): NYC's region code is used then."""
    synced_at = utc_now()
    records, seen_ids, skips = [], set(), []
    how_counts = Counter()

    for row in rows:
        name = clean(row.get("facility_name"), unquote=True)
        lat_text, lon_text = clean(row.get("latitude")), clean(row.get("longitude"))
        label = name or "(no name)"
        if name is None:
            skips.append(("missing facility name", label))
            continue
        if lat_text is None or lon_text is None:
            skips.append(("missing latitude or longitude", label))
            continue
        try:
            lat, lon = float(lat_text), float(lon_text)
        except ValueError:
            skips.append(("latitude or longitude is not a number", label))
            continue
        if not (NYC_LAT_MIN <= lat <= NYC_LAT_MAX and NYC_LON_MIN <= lon <= NYC_LON_MAX):
            skips.append(("outside the NYC bounding box", label))
            continue

        restroom_id = hashlib.sha1((name + lat_text + lon_text).encode("utf-8")).hexdigest()[:12]
        if restroom_id in seen_ids:
            skips.append(("exact duplicate (same name and coordinates)", label))
            continue
        seen_ids.add(restroom_id)

        borough, how = (None, "none")
        if boroughs:
            borough, how = find_borough(lon, lat, boroughs)
        if borough is None:
            region = BOROUGH_BY_REGION.get(str(row.get(REGION_FIELD) or ""))
            if region:
                borough, how = region, "region code"
        how_counts[how] += 1

        records.append(
            [restroom_id]
            + [clean(row.get(field), unquote=True) for field in TEXT_FIELDS]
            + [flatten_website(row.get("website")), lat, lon, borough, synced_at]
        )

    columns = ["id"] + TEXT_FIELDS + ["website", "latitude", "longitude", "borough", "synced_at"]
    sql = ("INSERT INTO restrooms (%s) VALUES (%s) ON CONFLICT(id) DO UPDATE SET %s" % (
        ",".join(columns), ",".join("?" * len(columns)),
        ",".join("%s=excluded.%s" % (c, c) for c in columns if c != "id")))
    with contextlib.closing(connect()) as db:
        db.executemany(sql, records)
        db.execute("INSERT INTO metadata(key, value) VALUES('synced_at', ?)"
                   " ON CONFLICT(key) DO UPDATE SET value = excluded.value", (synced_at,))
        db.commit()
    return {"fetched": len(rows), "imported": len(records), "skipped": len(skips),
            "skip_reasons": dict(Counter(reason for reason, _ in skips)),
            "borough_method": dict(how_counts), "synced_at": synced_at}


def import_restrooms():
    print("Import: fetching restrooms from NYC Open Data ...")
    rows = fetch_json(RESTROOMS_URL)
    if not isinstance(rows, list) or not rows:
        raise ValueError("NYC Open Data returned no rows")
    print("Import: fetching borough boundaries ...")
    try:
        boroughs = load_boroughs()
    except Exception as error:
        print("Import: borough boundaries unavailable (%r); using NYC's region code instead." % (error,))
        boroughs = None
    summary = import_rows(rows, boroughs)
    print("")
    print("=== Import summary ===")
    print(json.dumps(summary, indent=2))
    print("======================")
    print("")
    return summary


def startup():
    init_db()
    with contextlib.closing(connect()) as db:
        existing = db.execute("SELECT COUNT(*) FROM restrooms").fetchone()[0]
    if existing:
        print("Import: %d restrooms already in %s, skipping import." % (existing, os.path.basename(DB_PATH)))
        return
    try:
        import_restrooms()
    except Exception as error:  # keep the API up even if the import fails
        print("Import FAILED: %r. The restrooms table is empty; restart to retry." % (error,))


# --- Shared queries ----------------------------------------------------------

def meta(db):
    synced = db.execute("SELECT value FROM metadata WHERE key = 'synced_at'").fetchone()
    return {
        "source": "NYC Open Data (i7jb-7jku)",
        "official_synced_at": synced[0] if synced else None,
        "report_ttl_hours": REPORT_ACTIVE_HOURS,
        "report_keep_days": REPORT_KEEP_DAYS,
        "closing_categories": sorted(CLOSING_CATEGORIES),
        "categories": REPORT_CATEGORIES,
    }


REPORT_SELECT = (
    "SELECT p.id, p.restroom_id, p.category, p.note, p.created_at, p.observed_at,"
    " (SELECT COUNT(*) FROM confirmations c WHERE c.report_id = p.id AND c.agrees = 1) AS agree_count,"
    " (SELECT COUNT(*) FROM confirmations c WHERE c.report_id = p.id AND c.agrees = 0) AS disagree_count"
    " FROM reports p")


def report_dict(row, active_from, stale_from):
    report = dict(row)
    report["active"] = report["observed_at"] >= active_from
    report["stale"] = report["observed_at"] < stale_from
    return report


def filter_sql(params, columns):
    """Query parameters -> SQL conditions on restrooms (alias r). Column names come
    from the constant list given, never from the request; values are bound."""
    where, values = [], []
    for column in columns:
        if column in params and params[column].strip():
            value = params[column].strip()
            if value.lower() == UNKNOWN_FILTER_VALUE:
                where.append('r."%s" IS NULL' % column)
            else:
                where.append('r."%s" = ?' % column)
                values.append(value)
    return where, values


def distance_m(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin((p2 - p1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 2 * 6371000 * math.asin(min(1.0, math.sqrt(a)))


def parse_coordinate(params, name, low, high):
    try:
        value = float(params[name])
    except ValueError:
        value = None
    if value is None or not (low <= value <= high):  # also rejects NaN
        raise HTTPException(400, "%s must be a number between %d and %d" % (name, low, high))
    return value


# --- Routes ------------------------------------------------------------------

def health(request):
    with contextlib.closing(connect()) as db:
        counts = {table: db.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
                  for table in ("restrooms", "reports", "confirmations")}
        info = meta(db)
    return JSONResponse({"status": "ok", "counts": counts,
                         "last_import": info["official_synced_at"]})


def list_restrooms(request):
    """All restrooms (optionally filtered), plus every active report so the map can
    work out each restroom's status. With lat & lng: distance_m, nearest first."""
    params = request.query_params
    active_from, stale_from = active_cutoff(), stale_cutoff()

    where, values = filter_sql(params, RESTROOM_FILTERS)
    sql = ("SELECT r.*, (SELECT COUNT(*) FROM reports p WHERE p.restroom_id = r.id"
           " AND p.observed_at >= ?) AS active_report_count FROM restrooms r")
    values.insert(0, active_from)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY facility_name, id"

    origin = None
    if "lat" in params or "lng" in params:
        if not ("lat" in params and "lng" in params):
            raise HTTPException(400, "lat and lng must be given together")
        origin = (parse_coordinate(params, "lat", -90, 90),
                  parse_coordinate(params, "lng", -180, 180))

    with contextlib.closing(connect()) as db:
        restrooms = [dict(row) for row in db.execute(sql, values)]
        reports = [report_dict(row, active_from, stale_from) for row in db.execute(
            REPORT_SELECT + " WHERE p.observed_at >= ? ORDER BY p.observed_at DESC, p.id DESC",
            (active_from,))]
        info = meta(db)

    if origin:
        for restroom in restrooms:
            restroom["distance_m"] = round(distance_m(
                origin[0], origin[1], restroom["latitude"], restroom["longitude"]))
        restrooms.sort(key=lambda restroom: restroom["distance_m"])

    ids = {restroom["id"] for restroom in restrooms}
    return JSONResponse({"meta": info, "count": len(restrooms), "restrooms": restrooms,
                         "reports": [r for r in reports if r["restroom_id"] in ids]})


def get_restroom(request):
    active_from, stale_from = active_cutoff(), stale_cutoff()
    with contextlib.closing(connect()) as db:
        row = db.execute("SELECT * FROM restrooms WHERE id = ?",
                         (request.path_params["id"],)).fetchone()
        if row is None:
            raise HTTPException(404, "Restroom not found")
        restroom = dict(row)
        restroom["reports"] = [report_dict(r, active_from, stale_from) for r in db.execute(
            REPORT_SELECT + " WHERE p.restroom_id = ? ORDER BY p.observed_at DESC, p.id DESC",
            (restroom["id"],))]
        info = meta(db)
    restroom["active_report_count"] = sum(r["active"] for r in restroom["reports"])
    return JSONResponse({"meta": info, "restroom": restroom})


async def read_json_object(request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "Request body must be valid JSON")
    if not isinstance(body, dict):
        raise HTTPException(400, "Request body must be a JSON object")
    return body


async def create_report(request):
    body = await read_json_object(request)

    restroom_id = body.get("restroom_id")
    if not isinstance(restroom_id, str) or not restroom_id.strip():
        raise HTTPException(400, "restroom_id is required")
    restroom_id = restroom_id.strip()

    category = body.get("category")
    category = CATEGORY_ALIASES.get(category, category)
    if category not in REPORT_CATEGORIES:
        raise HTTPException(400, "category must be one of: " + ", ".join(REPORT_CATEGORIES))

    note = body.get("note")
    if note is not None and not isinstance(note, str):
        raise HTTPException(400, "note must be text")
    note = clean(note)
    if note is not None and len(note) > NOTE_MAX_LENGTH:
        raise HTTPException(400, "note must be %d characters or fewer (got %d)"
                            % (NOTE_MAX_LENGTH, len(note)))

    seen = body.get("seen_minutes_ago", 0)
    if isinstance(seen, bool) or not isinstance(seen, int) or seen not in SEEN_CHOICES:
        raise HTTPException(400, "seen_minutes_ago must be 0, 180 or 1440")

    now = datetime.now(timezone.utc)  # server time; client timestamps are ignored
    created_at, observed_at = iso(now), iso(now - timedelta(minutes=seen))
    client = client_key(request)
    with contextlib.closing(connect()) as db:
        if db.execute("SELECT 1 FROM restrooms WHERE id = ?", (restroom_id,)).fetchone() is None:
            raise HTTPException(400, "restroom_id does not match any restroom")
        recent = db.execute(
            "SELECT 1 FROM reports WHERE restroom_id = ? AND client = ? AND created_at >= ?",
            (restroom_id, client, hours_ago(RATE_LIMIT_MINUTES / 60))).fetchone()
        if recent:
            raise HTTPException(429, "You already reported this restroom in the last hour. "
                                     "Please try again later.")
        cursor = db.execute(
            "INSERT INTO reports(restroom_id, category, note, created_at, observed_at, client)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (restroom_id, category, note, created_at, observed_at, client))
        db.commit()
        row = db.execute(REPORT_SELECT + " WHERE p.id = ?", (cursor.lastrowid,)).fetchone()
        report = report_dict(row, active_cutoff(), stale_cutoff())
    return JSONResponse(report, status_code=201)


async def create_confirmation(request):
    report_id = request.path_params["id"]
    body = await read_json_object(request)
    agrees = body.get("agrees")
    if not isinstance(agrees, bool):
        raise HTTPException(400, "agrees must be true or false")
    client = client_key(request)
    with contextlib.closing(connect()) as db:
        if db.execute("SELECT 1 FROM reports WHERE id = ?", (report_id,)).fetchone() is None:
            raise HTTPException(404, "Report not found")
        if db.execute("SELECT 1 FROM confirmations WHERE report_id = ? AND client = ?",
                      (report_id, client)).fetchone():
            raise HTTPException(409, "You already answered this report.")
        created_at = utc_now()
        cursor = db.execute(
            "INSERT INTO confirmations(report_id, agrees, created_at, client) VALUES (?, ?, ?, ?)",
            (report_id, int(agrees), created_at, client))
        db.commit()
        agree_count, disagree_count = db.execute(
            "SELECT COALESCE(SUM(agrees = 1), 0), COALESCE(SUM(agrees = 0), 0)"
            " FROM confirmations WHERE report_id = ?", (report_id,)).fetchone()
    return JSONResponse({"id": cursor.lastrowid, "report_id": report_id, "agrees": agrees,
                         "created_at": created_at, "agree_count": agree_count,
                         "disagree_count": disagree_count}, status_code=201)


def stats(request):
    """Dashboard aggregates. Missing values are grouped as "Unknown"."""
    where, values = filter_sql(request.query_params, STATS_FILTERS)
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    from_restrooms = " FROM restrooms r" + where_sql
    from_reports = " FROM reports p JOIN restrooms r ON r.id = p.restroom_id" + where_sql
    active_from = active_cutoff()

    with contextlib.closing(connect()) as db:
        def grouped(column):
            rows = db.execute(
                'SELECT COALESCE(r."%s", ?) AS value, COUNT(*) AS n' % column
                + from_restrooms + " GROUP BY value ORDER BY n DESC, value",
                [UNKNOWN_LABEL] + values)
            return [{"value": row["value"], "count": row["n"]} for row in rows]

        totals_row = db.execute(
            "SELECT COUNT(*),"
            " COALESCE(SUM(r.status = 'Operational'), 0),"
            " COALESCE(SUM(r.accessibility = 'Fully Accessible'), 0),"
            " COALESCE(SUM(r.changing_stations LIKE 'Yes%'), 0),"
            " COALESCE(SUM(r.menstrual_friendly LIKE 'Yes%'), 0)" + from_restrooms,
            values).fetchone()
        reports_row = db.execute(
            "SELECT COUNT(*), COALESCE(SUM(p.observed_at >= ?), 0)" + from_reports,
            [active_from] + values).fetchone()

        by_accessibility = {item["value"]: item["count"] for item in grouped("accessibility")}
        accessibility_levels = ACCESSIBILITY_ORDER + sorted(
            level for level in by_accessibility if level not in ACCESSIBILITY_ORDER)

        by_borough = grouped("borough")
        within = {item["value"]: dict.fromkeys(accessibility_levels, 0) for item in by_borough}
        for row in db.execute(
                "SELECT COALESCE(r.borough, ?) AS borough, COALESCE(r.accessibility, ?) AS level,"
                " COUNT(*) AS n" + from_restrooms + " GROUP BY borough, level",
                [UNKNOWN_LABEL, UNKNOWN_LABEL] + values):
            within[row["borough"]][row["level"]] = row["n"]

        category_counts = dict(db.execute(
            "SELECT p.category, COUNT(*)" + from_reports + " GROUP BY p.category", values).fetchall())
        by_day = db.execute(
            "SELECT SUBSTR(p.created_at, 1, 10) AS day, COUNT(*) AS n" + from_reports
            + " GROUP BY day ORDER BY day", values).fetchall()
        top_reported = db.execute(
            "SELECT r.id, r.facility_name, r.borough, r.latitude, r.longitude,"
            " COUNT(*) AS report_count, SUM(p.observed_at >= ?) AS active_report_count,"
            " MAX(p.created_at) AS latest_report_at" + from_reports
            + " GROUP BY r.id ORDER BY report_count DESC, latest_report_at DESC, r.id LIMIT 10",
            [active_from] + values).fetchall()
        info = meta(db)
        by_location_type, by_operator, by_open = (
            grouped("location_type"), grouped("operator"), grouped("open"))

    return JSONResponse({
        "meta": info,
        "filters": {column: request.query_params[column].strip()
                    for column in STATS_FILTERS if request.query_params.get(column, "").strip()},
        "totals": {
            "restrooms": totals_row[0], "operational": totals_row[1],
            "fully_accessible": totals_row[2], "with_changing_stations": totals_row[3],
            "menstrual_friendly": totals_row[4],
            "reports": reports_row[0], "active_reports": reports_row[1],
        },
        "by_borough": by_borough,
        "by_location_type": by_location_type,
        "by_operator": by_operator,
        "by_accessibility": [{"value": level, "count": by_accessibility.get(level, 0)}
                             for level in accessibility_levels],
        "by_open": by_open,
        "accessibility_by_borough": [
            {"borough": item["value"], "total": item["count"], "counts": within[item["value"]]}
            for item in by_borough],
        "reports_by_category": [{"category": c, "count": category_counts.get(c, 0)}
                                for c in REPORT_CATEGORIES],
        "reports_by_day": [{"day": row["day"], "count": row["n"]} for row in by_day],
        "top_reported": [dict(row) for row in top_reported],
    })


def homepage(request):
    """Hand out index.html from this folder, so one command runs the whole app."""
    if os.path.exists(INDEX_HTML):
        return FileResponse(INDEX_HTML, media_type="text/html")
    return PlainTextResponse("API is running. Put index.html beside this file to serve the app.")


async def http_error(request, exc):
    return JSONResponse({"error": exc.detail}, status_code=exc.status_code)


async def server_error(request, exc):
    return JSONResponse({"error": "Internal server error"}, status_code=500)


@contextlib.asynccontextmanager
async def lifespan(app):
    startup()
    yield


api_routes = [
    Route("/health", health, methods=["GET"]),
    Route("/restrooms", list_restrooms, methods=["GET"]),
    Route("/restrooms/{id}", get_restroom, methods=["GET"]),
    Route("/stats", stats, methods=["GET"]),
    Route("/reports", create_report, methods=["POST"]),
    Route("/reports/{id:int}/confirmations", create_confirmation, methods=["POST"]),
]

app = Starlette(
    routes=[
        Mount(ROUTE_PREFIX, routes=api_routes),
        Route("/", homepage, methods=["GET"]),
        Route("/index.html", homepage, methods=["GET"]),
    ],
    # No cookies or logins, so any origin may call the API (also file:// pages).
    middleware=[Middleware(CORSMiddleware, allow_origins=["*"],
                           allow_methods=["GET", "POST"], allow_headers=["*"])],
    lifespan=lifespan,
    exception_handlers={HTTPException: http_error, Exception: server_error},
)


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    if "--sync" in sys.argv:          # re-download the official data, keep reports
        init_db()
        import_restrooms()
    else:
        print("Open http://127.0.0.1:%d/  (API at http://127.0.0.1:%d%s/health)"
              % (PORT, PORT, ROUTE_PREFIX))
        uvicorn.run(app, host="127.0.0.1", port=PORT)
