#!/web/urban-data/.env/bin/python
"""NYC Public Restroom Finder - backend (Team 11).

Run:    python api.py            then open http://127.0.0.1:8091/
Sync:   python api.py --sync     re-download the NYC data (keeps reports)
Export: python api.py --export   save every report to reports.csv

On first start the server downloads the NYC Open Data "Public Restrooms" dataset
and the borough boundaries into restrooms.db (SQLite) beside this file.
The server also hands out index.html, and pushes live updates over a WebSocket.
Needs: starlette, uvicorn, websockets
"""
import contextlib
import csv
import hashlib
import io
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
from starlette.responses import FileResponse, JSONResponse, PlainTextResponse, Response
from starlette.routing import Mount, Route, WebSocketRoute
from starlette.websockets import WebSocketDisconnect

# ---------------------------------------------------------------------------
# Settings (if you change PORT or API_PREFIX, change API_BASE in index.html too)
# ---------------------------------------------------------------------------
PORT = 8091
API_PREFIX = "/api/team11"
HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.getenv("TEAM11_DB", os.path.join(HERE, "restrooms.db"))
INDEX_HTML = os.path.join(HERE, "index.html")

# "$select=:*,*" also returns NYC's region code, used when a borough can't be found.
RESTROOMS_URL = "https://data.cityofnewyork.us/resource/i7jb-7jku.json?$select=:*,*&$limit=5000"
BOROUGHS_URL = "https://data.cityofnewyork.us/resource/gthc-hcne.json"
REGION_FIELD = ":@computed_region_yeji_bk3q"
BOROUGH_BY_REGION = {"1": "Staten Island", "2": "Brooklyn", "3": "Queens", "4": "Manhattan", "5": "Bronx"}

# New York City bounding box, and how far outside a borough a pier/beach may be.
LAT_MIN, LAT_MAX = 40.4774, 40.9176
LON_MIN, LON_MAX = -74.2591, -73.7004
SHORELINE_TOLERANCE_M = 500

TEXT_FIELDS = ["facility_name", "location_type", "operator", "status", "open", "hours_of_operation",
               "accessibility", "restroom_type", "changing_stations", "menstrual_friendly", "additional_notes"]
RESTROOM_FILTERS = ["borough", "accessibility", "location_type", "open",
                    "changing_stations", "menstrual_friendly", "status"]
STATS_FILTERS = ["borough", "location_type", "accessibility"]
UNKNOWN = "Unknown"   # label for missing values; the filter value "unknown" matches them
ACCESSIBILITY_ORDER = ["Fully Accessible", "Partially Accessible", "Limited Accessibility",
                       "Not Accessible", UNKNOWN]

# Community reports
REPORT_CATEGORIES = ["closed", "out_of_order", "wrong_hours", "supplies", "cleanliness", "access", "safety"]
CLOSING_CATEGORIES = ["closed", "out_of_order"]          # these show "Reported closed"
CATEGORY_ALIASES = {"dirty": "cleanliness", "no_supplies": "supplies", "accessibility_problem": "access",
                    "unsafe": "safety", "other": "cleanliness"}  # old names still accepted
SEEN_CHOICES = [0, 180, 1440]      # minutes ago: just now, earlier today, yesterday
NOTE_MAX_LENGTH = 280
REPORT_ACTIVE_HOURS = 48           # a report changes the status this long
REPORT_KEEP_DAYS = 7               # older reports are returned with "stale": true
RATE_LIMIT_HOURS = 1               # one report per restroom per visitor per hour

# Community-added restrooms
ADD_LOCATION_TYPES = ["Park", "Library", "Shop or café", "Transit station", "Other"]
ADD_ACCESSIBILITY = ["", "Fully Accessible", "Not Accessible"]
VERIFY_VOTES = 3                   # "Yes, it's here" votes needed to be verified
REMOVE_VOTES = 3                   # "Not here" votes (and more than yes) hide it
ADDS_PER_DAY = 5
COMMUNITY_OPERATOR = "Added by the community"

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
CREATE TABLE IF NOT EXISTS restroom_votes(
    id INTEGER PRIMARY KEY, restroom_id TEXT NOT NULL REFERENCES restrooms(id),
    here INTEGER NOT NULL CHECK (here IN (0, 1)), created_at TEXT NOT NULL, client TEXT);
CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT);
CREATE INDEX IF NOT EXISTS reports_restroom_idx ON reports(restroom_id);
CREATE INDEX IF NOT EXISTS confirmations_report_idx ON confirmations(report_id);
CREATE INDEX IF NOT EXISTS restroom_votes_idx ON restroom_votes(restroom_id);
"""


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def time_text(moment):
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")  # e.g. 2026-11-24T17:00:00Z (UTC)


def hours_ago(hours):
    return time_text(datetime.now(timezone.utc) - timedelta(hours=hours))


def connect():
    db = sqlite3.connect(DB_PATH, timeout=15)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    return db


def fetch_json(url):
    request = urllib.request.Request(url, headers={"User-Agent": "team11-import", "Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.loads(response.read().decode("utf-8"))


def clean(value, unquote=False):
    """Trim text; empty becomes None. unquote=True also drops one pair of wrapping
    quotes (some dataset values arrive as '"Yes, in women's restroom only"')."""
    if value is None:
        return None
    value = str(value).strip()
    if unquote and len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        value = value[1:-1].strip()
    return value or None


def client_key(request):
    """Salted hash of the visitor's address (only used for the rate limits).
    Behind the course proxy the real address is the first X-Forwarded-For entry."""
    forwarded = request.headers.get("x-forwarded-for", "")
    address = forwarded.split(",")[0].strip() or (request.client.host if request.client else "")
    return hashlib.sha256(("team11|" + address).encode("utf-8")).hexdigest()[:16]


async def read_json(request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "Request body must be valid JSON")
    if not isinstance(body, dict):
        raise HTTPException(400, "Request body must be a JSON object")
    return body


def text_field(body, name, max_length, required=False):
    value = body.get(name)
    if value is not None and not isinstance(value, str):
        raise HTTPException(400, "%s must be text" % name)
    value = clean(value)
    if required and (value is None or len(value) < 2):
        raise HTTPException(400, "%s is required" % name)
    if value is not None and len(value) > max_length:
        raise HTTPException(400, "%s must be %d characters or fewer" % (name, max_length))
    return value


# ---------------------------------------------------------------------------
# Borough lookup (point in polygon, plain Python)
# ---------------------------------------------------------------------------
def load_boroughs():
    """List of (borough name, shapes); each shape has a bounding box and rings."""
    boroughs = []
    for row in fetch_json(BOROUGHS_URL):
        polygons = row["the_geom"]["coordinates"]
        if row["the_geom"]["type"] == "Polygon":
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
    """Ray casting: count the edges a ray going east crosses."""
    inside = False
    x1, y1 = ring[-1][0], ring[-1][1]
    for x2, y2 in (point[:2] for point in ring):
        if (y1 > lat) != (y2 > lat) and lon < (x2 - x1) * (lat - y1) / (y2 - y1) + x1:
            inside = not inside
        x1, y1 = x2, y2
    return inside


def point_in_shape(lon, lat, shape):
    west, south, east, north = shape["bbox"]
    if not (west <= lon <= east and south <= lat <= north):
        return False
    outline, holes = shape["rings"][0], shape["rings"][1:]
    return point_in_ring(lon, lat, outline) and not any(point_in_ring(lon, lat, hole) for hole in holes)


def distance_to_ring_m(lon, lat, ring):
    """Shortest distance (metres) from a point to a ring's edges; flat-earth is fine at city scale."""
    kx, ky = 84400.0, 111200.0  # metres per degree of lon / lat near 40.7 N
    best = float("inf")
    ax, ay = (ring[-1][0] - lon) * kx, (ring[-1][1] - lat) * ky
    for point in ring:
        bx, by = (point[0] - lon) * kx, (point[1] - lat) * ky
        dx, dy = bx - ax, by - ay
        length_sq = dx * dx + dy * dy
        t = 0.0 if length_sq == 0 else max(0.0, min(1.0, -(ax * dx + ay * dy) / length_sq))
        best = min(best, (ax + t * dx) ** 2 + (ay + t * dy) ** 2)
        ax, ay = bx, by
    return best ** 0.5


def find_borough(lon, lat, boroughs):
    """Return (borough or None, how): 'polygon', 'nearest' (shoreline) or 'none'."""
    for name, shapes in boroughs:
        if any(point_in_shape(lon, lat, shape) for shape in shapes):
            return name, "polygon"
    distance, name = min((distance_to_ring_m(lon, lat, shape["rings"][0]), name)
                         for name, shapes in boroughs for shape in shapes)
    if distance <= SHORELINE_TOLERANCE_M:
        return name, "nearest"
    return None, "none"


def distance_m(lat1, lon1, lat2, lon2):
    """Great-circle (haversine) distance in metres."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin((p2 - p1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 2 * 6371000 * math.asin(min(1.0, math.sqrt(a)))


# ---------------------------------------------------------------------------
# Database setup and NYC Open Data import
# ---------------------------------------------------------------------------
def init_db():
    with connect() as db:
        db.executescript(SCHEMA)
        # Columns for community restrooms (older databases get them here).
        # source is NULL for NYC Open Data rows and 'community' for added ones.
        columns = [row[1] for row in db.execute("PRAGMA table_info(restrooms)")]
        for column in ["source", "added_at", "added_by"]:
            if column not in columns:
                db.execute("ALTER TABLE restrooms ADD COLUMN %s TEXT" % column)
    db.close()


def import_rows(rows, boroughs):
    """Clean the dataset rows and save them. boroughs may be None (then NYC's region code is used)."""
    synced_at = hours_ago(0)
    records, seen_ids, skipped, methods = [], set(), Counter(), Counter()
    for row in rows:
        name = clean(row.get("facility_name"), unquote=True)
        lat_text, lon_text = clean(row.get("latitude")), clean(row.get("longitude"))
        if name is None:
            skipped["missing facility name"] += 1
            continue
        if lat_text is None or lon_text is None:
            skipped["missing latitude or longitude"] += 1
            continue
        try:
            lat, lon = float(lat_text), float(lon_text)
        except ValueError:
            skipped["latitude or longitude is not a number"] += 1
            continue
        if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
            skipped["outside the NYC bounding box"] += 1
            continue
        # The dataset has no id, so make one from the name and coordinates.
        restroom_id = hashlib.sha1((name + lat_text + lon_text).encode("utf-8")).hexdigest()[:12]
        if restroom_id in seen_ids:
            skipped["exact duplicate (same name and coordinates)"] += 1
            continue
        seen_ids.add(restroom_id)

        borough, how = find_borough(lon, lat, boroughs) if boroughs else (None, "none")
        if borough is None and BOROUGH_BY_REGION.get(str(row.get(REGION_FIELD) or "")):
            borough, how = BOROUGH_BY_REGION[str(row.get(REGION_FIELD))], "region code"
        methods[how] += 1

        website = row.get("website")
        if isinstance(website, dict):
            website = website.get("url")
        records.append([restroom_id] + [clean(row.get(field), unquote=True) for field in TEXT_FIELDS]
                       + [clean(website, unquote=True), lat, lon, borough, synced_at])

    columns = ["id"] + TEXT_FIELDS + ["website", "latitude", "longitude", "borough", "synced_at"]
    updates = ",".join("%s=excluded.%s" % (column, column) for column in columns[1:])
    with connect() as db:
        db.executemany("INSERT INTO restrooms (%s) VALUES (%s) ON CONFLICT(id) DO UPDATE SET %s"
                       % (",".join(columns), ",".join("?" * len(columns)), updates), records)
        db.execute("INSERT INTO metadata(key, value) VALUES('synced_at', ?)"
                   " ON CONFLICT(key) DO UPDATE SET value = excluded.value", (synced_at,))
    db.close()
    return {"fetched": len(rows), "imported": len(records), "skipped": sum(skipped.values()),
            "skip_reasons": dict(skipped), "borough_method": dict(methods), "synced_at": synced_at}


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
    print("\n=== Import summary ===\n%s\n======================\n" % json.dumps(summary, indent=2))
    return summary


def startup():
    """Create the tables; download the data the first time (the API stays up if that fails)."""
    init_db()
    db = connect()
    existing = db.execute("SELECT COUNT(*) FROM restrooms").fetchone()[0]
    db.close()
    if existing:
        print("Import: %d restrooms already in %s, skipping import." % (existing, os.path.basename(DB_PATH)))
        return
    try:
        import_restrooms()
    except Exception as error:
        print("Import FAILED: %r. The restrooms table is empty; restart to retry." % (error,))


# ---------------------------------------------------------------------------
# Shared queries
# ---------------------------------------------------------------------------
RESTROOM_SELECT = (
    "SELECT r.*,"
    " (SELECT COUNT(*) FROM reports p WHERE p.restroom_id = r.id AND p.observed_at >= ?) AS active_report_count,"
    " (SELECT COUNT(*) FROM restroom_votes v WHERE v.restroom_id = r.id AND v.here = 1) AS here_count,"
    " (SELECT COUNT(*) FROM restroom_votes v WHERE v.restroom_id = r.id AND v.here = 0) AS not_here_count"
    " FROM restrooms r")

REPORT_SELECT = (
    "SELECT p.id, p.restroom_id, p.category, p.note, p.created_at, p.observed_at,"
    " (SELECT COUNT(*) FROM confirmations c WHERE c.report_id = p.id AND c.agrees = 1) AS agree_count,"
    " (SELECT COUNT(*) FROM confirmations c WHERE c.report_id = p.id AND c.agrees = 0) AS disagree_count"
    " FROM reports p")

RECENT_SELECT = (
    "SELECT p.id, p.created_at, p.observed_at, p.restroom_id, r.facility_name, r.borough, p.category, p.note,"
    " (SELECT COUNT(*) FROM confirmations c WHERE c.report_id = p.id AND c.agrees = 1) AS agree_count,"
    " (SELECT COUNT(*) FROM confirmations c WHERE c.report_id = p.id AND c.agrees = 0) AS disagree_count"
    " FROM reports p JOIN restrooms r ON r.id = p.restroom_id ORDER BY p.created_at DESC, p.id DESC")
CSV_COLUMNS = ["id", "created_at", "observed_at", "restroom_id", "facility_name", "borough",
               "category", "note", "agree_count", "disagree_count"]


def meta(db):
    synced = db.execute("SELECT value FROM metadata WHERE key = 'synced_at'").fetchone()
    return {"source": "NYC Open Data (i7jb-7jku)", "official_synced_at": synced[0] if synced else None,
            "report_ttl_hours": REPORT_ACTIVE_HOURS, "report_keep_days": REPORT_KEEP_DAYS,
            "closing_categories": CLOSING_CATEGORIES, "categories": REPORT_CATEGORIES,
            "verify_votes": VERIFY_VOTES}


def restroom_dict(row):
    """A restroom as JSON; community restrooms also get verified / removed flags."""
    restroom = dict(row)
    restroom.pop("added_by", None)  # never send out who added it
    here, gone = restroom["here_count"] or 0, restroom["not_here_count"] or 0
    restroom["community"] = restroom.get("source") == "community"
    restroom["verified"] = not restroom["community"] or here >= VERIFY_VOTES
    restroom["removed"] = restroom["community"] and gone >= REMOVE_VOTES and gone > here
    return restroom


def report_dict(row):
    report = dict(row)
    report["active"] = report["observed_at"] >= hours_ago(REPORT_ACTIVE_HOURS)
    report["stale"] = report["observed_at"] < hours_ago(REPORT_KEEP_DAYS * 24)
    return report


def get_one_restroom(db, restroom_id):
    row = db.execute(RESTROOM_SELECT + " WHERE r.id = ?", (hours_ago(REPORT_ACTIVE_HOURS), restroom_id)).fetchone()
    return restroom_dict(row) if row else None


def filter_sql(params, columns):
    """Query parameters -> SQL conditions. Column names come from our own lists, values are bound."""
    where, values = [], []
    for column in columns:
        value = params.get(column, "").strip()
        if value.lower() == "unknown":
            where.append('r."%s" IS NULL' % column)
        elif value:
            where.append('r."%s" = ?' % column)
            values.append(value)
    return where, values


def coordinate(params, name, low, high):
    try:
        value = float(params[name])
    except ValueError:
        value = None
    if value is None or not (low <= value <= high):  # also rejects NaN
        raise HTTPException(400, "%s must be a number between %d and %d" % (name, low, high))
    return value


# ---------------------------------------------------------------------------
# Live updates: every open page keeps a WebSocket; changes are pushed to all
# ---------------------------------------------------------------------------
live_clients = set()


async def broadcast(message):
    for websocket in list(live_clients):
        try:
            await websocket.send_json(message)
        except Exception:
            live_clients.discard(websocket)


async def live_socket(websocket):
    """WebSocket at /api/team11/ws. The server sends updates; a page may send "ping"."""
    await websocket.accept()
    live_clients.add(websocket)
    await broadcast({"type": "presence", "online": len(live_clients)})
    try:
        while True:
            if await websocket.receive_text() == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        pass
    finally:
        live_clients.discard(websocket)
        await broadcast({"type": "presence", "online": len(live_clients)})


# ---------------------------------------------------------------------------
# Routes: restrooms
# ---------------------------------------------------------------------------
def health(request):
    db = connect()
    counts = {table: db.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
              for table in ["restrooms", "reports", "confirmations"]}
    last_import = meta(db)["official_synced_at"]
    db.close()
    return JSONResponse({"status": "ok", "counts": counts, "last_import": last_import,
                         "live_clients": len(live_clients)})


def list_restrooms(request):
    """All restrooms (optionally filtered) plus every active report, so the map can work
    out each status. With lat & lng it adds distance_m and sorts nearest first."""
    params = request.query_params
    where, values = filter_sql(params, RESTROOM_FILTERS)
    sql = RESTROOM_SELECT + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY facility_name, id"
    origin = None
    if "lat" in params or "lng" in params:
        if not ("lat" in params and "lng" in params):
            raise HTTPException(400, "lat and lng must be given together")
        origin = (coordinate(params, "lat", -90, 90), coordinate(params, "lng", -180, 180))

    active_from = hours_ago(REPORT_ACTIVE_HOURS)
    db = connect()
    restrooms = [restroom_dict(row) for row in db.execute(sql, [active_from] + values)]
    restrooms = [restroom for restroom in restrooms if not restroom["removed"]]
    reports = [report_dict(row) for row in db.execute(
        REPORT_SELECT + " WHERE p.observed_at >= ? ORDER BY p.observed_at DESC, p.id DESC", (active_from,))]
    info = meta(db)
    db.close()

    if origin:
        for restroom in restrooms:
            restroom["distance_m"] = round(distance_m(origin[0], origin[1], restroom["latitude"], restroom["longitude"]))
        restrooms.sort(key=lambda restroom: restroom["distance_m"])
    ids = {restroom["id"] for restroom in restrooms}
    return JSONResponse({"meta": info, "count": len(restrooms), "restrooms": restrooms,
                         "reports": [report for report in reports if report["restroom_id"] in ids]})


def get_restroom(request):
    db = connect()
    restroom = get_one_restroom(db, request.path_params["id"])
    if restroom is None:
        db.close()
        raise HTTPException(404, "Restroom not found")
    restroom["reports"] = [report_dict(row) for row in db.execute(
        REPORT_SELECT + " WHERE p.restroom_id = ? ORDER BY p.observed_at DESC, p.id DESC", (restroom["id"],))]
    info = meta(db)
    db.close()
    restroom["active_report_count"] = sum(report["active"] for report in restroom["reports"])
    return JSONResponse({"meta": info, "restroom": restroom})


def nearest_borough(db, lat, lon):
    """Borough of the closest official restroom (an estimate for community additions)."""
    rows = db.execute("SELECT latitude, longitude, borough FROM restrooms WHERE borough IS NOT NULL AND source IS NULL")
    best = min(rows, key=lambda row: (row[0] - lat) ** 2 + ((row[1] - lon) * 0.76) ** 2, default=None)
    return best[2] if best else None


async def add_restroom(request):
    """A visitor adds a restroom that isn't in the NYC data. It shows as "not verified"
    until VERIFY_VOTES other visitors say it's there."""
    body = await read_json(request)
    name = text_field(body, "facility_name", 80, required=True)
    location_type = body.get("location_type")
    if location_type not in ADD_LOCATION_TYPES:
        raise HTTPException(400, "location_type must be one of: " + ", ".join(ADD_LOCATION_TYPES))
    hours = text_field(body, "hours_of_operation", 80)
    note = text_field(body, "additional_notes", NOTE_MAX_LENGTH)
    accessibility = body.get("accessibility") or ""
    if accessibility not in ADD_ACCESSIBILITY:
        raise HTTPException(400, "accessibility must be Fully Accessible, Not Accessible or empty")
    try:
        lat, lon = float(body.get("latitude")), float(body.get("longitude"))
    except (TypeError, ValueError):
        raise HTTPException(400, "latitude and longitude must be numbers")
    if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
        raise HTTPException(400, "That pin is outside New York City.")

    client = client_key(request)
    restroom_id = "c" + hashlib.sha1(("%s|%.6f|%.6f" % (name.lower(), lat, lon)).encode("utf-8")).hexdigest()[:11]
    db = connect()
    try:
        if db.execute("SELECT 1 FROM restrooms WHERE id = ?", (restroom_id,)).fetchone():
            raise HTTPException(409, "This restroom has already been added.")
        added_today = db.execute("SELECT COUNT(*) FROM restrooms WHERE source = 'community'"
                                 " AND added_by = ? AND added_at >= ?", (client, hours_ago(24))).fetchone()[0]
        if added_today >= ADDS_PER_DAY:
            raise HTTPException(429, "You've added %d restrooms today. Please try again tomorrow." % ADDS_PER_DAY)
        db.execute("INSERT INTO restrooms (id, facility_name, location_type, operator, hours_of_operation,"
                   " accessibility, additional_notes, latitude, longitude, borough, source, added_at, added_by)"
                   " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'community', ?, ?)",
                   (restroom_id, name, location_type, COMMUNITY_OPERATOR, hours, accessibility or None,
                    note, lat, lon, nearest_borough(db, lat, lon), hours_ago(0), client))
        db.commit()
        restroom = get_one_restroom(db, restroom_id)
    finally:
        db.close()
    await broadcast({"type": "restroom_added", "restroom": restroom})
    return JSONResponse(restroom, status_code=201)


async def vote_restroom(request):
    """"Yes, it's here" / "Not here" on a community-added restroom."""
    restroom_id = request.path_params["id"]
    body = await read_json(request)
    here = body.get("here")
    if not isinstance(here, bool):
        raise HTTPException(400, "here must be true or false")
    client = client_key(request)
    db = connect()
    try:
        row = db.execute("SELECT source, added_by FROM restrooms WHERE id = ?", (restroom_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "Restroom not found")
        if row["source"] != "community":
            raise HTTPException(400, "Only restrooms added by the community can be confirmed.")
        if row["added_by"] == client:
            raise HTTPException(403, "You added this restroom, so someone else needs to confirm it.")
        if db.execute("SELECT 1 FROM restroom_votes WHERE restroom_id = ? AND client = ?",
                      (restroom_id, client)).fetchone():
            raise HTTPException(409, "You already answered for this restroom.")
        db.execute("INSERT INTO restroom_votes(restroom_id, here, created_at, client) VALUES (?, ?, ?, ?)",
                   (restroom_id, int(here), hours_ago(0), client))
        db.commit()
        restroom = get_one_restroom(db, restroom_id)
    finally:
        db.close()
    result = {"restroom_id": restroom_id, "here": here,
              "here_count": restroom["here_count"], "not_here_count": restroom["not_here_count"],
              "verified": restroom["verified"], "removed": restroom["removed"]}
    await broadcast(dict(result, type="restroom_voted"))
    return JSONResponse(result, status_code=201)


# ---------------------------------------------------------------------------
# Routes: community reports
# ---------------------------------------------------------------------------
async def create_report(request):
    body = await read_json(request)
    restroom_id = body.get("restroom_id")
    if not isinstance(restroom_id, str) or not restroom_id.strip():
        raise HTTPException(400, "restroom_id is required")
    restroom_id = restroom_id.strip()
    category = CATEGORY_ALIASES.get(body.get("category"), body.get("category"))
    if category not in REPORT_CATEGORIES:
        raise HTTPException(400, "category must be one of: " + ", ".join(REPORT_CATEGORIES))
    note = body.get("note")
    if note is not None and not isinstance(note, str):
        raise HTTPException(400, "note must be text")
    note = clean(note)
    if note is not None and len(note) > NOTE_MAX_LENGTH:
        raise HTTPException(400, "note must be %d characters or fewer (got %d)" % (NOTE_MAX_LENGTH, len(note)))
    seen = body.get("seen_minutes_ago", 0)
    if isinstance(seen, bool) or not isinstance(seen, int) or seen not in SEEN_CHOICES:
        raise HTTPException(400, "seen_minutes_ago must be 0, 180 or 1440")

    now = datetime.now(timezone.utc)  # server time; created_at - seen = when the visitor saw it
    created_at, observed_at = time_text(now), time_text(now - timedelta(minutes=seen))
    client = client_key(request)
    db = connect()
    try:
        if db.execute("SELECT 1 FROM restrooms WHERE id = ?", (restroom_id,)).fetchone() is None:
            raise HTTPException(400, "restroom_id does not match any restroom")
        if db.execute("SELECT 1 FROM reports WHERE restroom_id = ? AND client = ? AND created_at >= ?",
                      (restroom_id, client, hours_ago(RATE_LIMIT_HOURS))).fetchone():
            raise HTTPException(429, "You already reported this restroom in the last hour. Please try again later.")
        cursor = db.execute("INSERT INTO reports(restroom_id, category, note, created_at, observed_at, client)"
                            " VALUES (?, ?, ?, ?, ?, ?)", (restroom_id, category, note, created_at, observed_at, client))
        db.commit()
        report = report_dict(db.execute(REPORT_SELECT + " WHERE p.id = ?", (cursor.lastrowid,)).fetchone())
    finally:
        db.close()
    await broadcast({"type": "report_added", "report": report})
    return JSONResponse(report, status_code=201)


async def create_confirmation(request):
    """"Still true" / "Not true" on a report."""
    report_id = request.path_params["id"]
    body = await read_json(request)
    agrees = body.get("agrees")
    if not isinstance(agrees, bool):
        raise HTTPException(400, "agrees must be true or false")
    client = client_key(request)
    db = connect()
    try:
        report = db.execute("SELECT restroom_id FROM reports WHERE id = ?", (report_id,)).fetchone()
        if report is None:
            raise HTTPException(404, "Report not found")
        if db.execute("SELECT 1 FROM confirmations WHERE report_id = ? AND client = ?", (report_id, client)).fetchone():
            raise HTTPException(409, "You already answered this report.")
        created_at = hours_ago(0)
        cursor = db.execute("INSERT INTO confirmations(report_id, agrees, created_at, client) VALUES (?, ?, ?, ?)",
                            (report_id, int(agrees), created_at, client))
        db.commit()
        agree_count, disagree_count = db.execute(
            "SELECT COALESCE(SUM(agrees = 1), 0), COALESCE(SUM(agrees = 0), 0) FROM confirmations WHERE report_id = ?",
            (report_id,)).fetchone()
    finally:
        db.close()
    await broadcast({"type": "report_confirmed", "report_id": report_id, "restroom_id": report["restroom_id"],
                     "agree_count": agree_count, "disagree_count": disagree_count})
    return JSONResponse({"id": cursor.lastrowid, "report_id": report_id, "agrees": agrees, "created_at": created_at,
                         "agree_count": agree_count, "disagree_count": disagree_count}, status_code=201)


def recent_reports(request):
    """Newest reports first, with the restroom's name. ?limit= 1-500 (default 50)."""
    try:
        limit = max(1, min(500, int(request.query_params.get("limit", "50"))))
    except ValueError:
        raise HTTPException(400, "limit must be a whole number")
    db = connect()
    total = db.execute("SELECT COUNT(*) FROM reports").fetchone()[0]
    reports = [dict(row) for row in db.execute(RECENT_SELECT + " LIMIT ?", (limit,))]
    db.close()
    for report in reports:
        report["active"] = report["observed_at"] >= hours_ago(REPORT_ACTIVE_HOURS)
    return JSONResponse({"total": total, "reports": reports})


def reports_csv_text():
    """Every report as CSV (opens in Excel, Google Sheets or a text editor)."""
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(CSV_COLUMNS)
    db = connect()
    for row in db.execute(RECENT_SELECT):
        writer.writerow(["" if row[column] is None else row[column] for column in CSV_COLUMNS])
    db.close()
    return out.getvalue()


def reports_csv(request):
    return Response(reports_csv_text(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": 'attachment; filename="reports.csv"'})


# ---------------------------------------------------------------------------
# Route: dashboard numbers
# ---------------------------------------------------------------------------
def stats(request):
    """Dashboard aggregates. Official charts count NYC rows only; missing values are "Unknown"."""
    where, values = filter_sql(request.query_params, STATS_FILTERS)
    official = " FROM restrooms r WHERE " + " AND ".join(where + ["r.source IS NULL"])
    community = " FROM restrooms r WHERE " + " AND ".join(where + ["r.source = 'community'"])
    reports = " FROM reports p JOIN restrooms r ON r.id = p.restroom_id" + (" WHERE " + " AND ".join(where) if where else "")
    active_from = hours_ago(REPORT_ACTIVE_HOURS)
    db = connect()

    def grouped(column):
        rows = db.execute('SELECT COALESCE(r."%s", ?) AS value, COUNT(*) AS n' % column + official
                          + " GROUP BY value ORDER BY n DESC, value", [UNKNOWN] + values)
        return [{"value": row["value"], "count": row["n"]} for row in rows]

    totals = db.execute("SELECT COUNT(*), COALESCE(SUM(r.status = 'Operational'), 0),"
                        " COALESCE(SUM(r.accessibility = 'Fully Accessible'), 0),"
                        " COALESCE(SUM(r.changing_stations LIKE 'Yes%'), 0),"
                        " COALESCE(SUM(r.menstrual_friendly LIKE 'Yes%'), 0)" + official, values).fetchone()
    added = [restroom_dict(row) for row in db.execute(RESTROOM_SELECT.replace(" FROM restrooms r", "") + community,
                                                      [active_from] + values)]
    added = [restroom for restroom in added if not restroom["removed"]]
    report_totals = db.execute("SELECT COUNT(*), COALESCE(SUM(p.observed_at >= ?), 0)" + reports,
                               [active_from] + values).fetchone()

    by_accessibility = {item["value"]: item["count"] for item in grouped("accessibility")}
    levels = ACCESSIBILITY_ORDER + sorted(level for level in by_accessibility if level not in ACCESSIBILITY_ORDER)
    by_borough = grouped("borough")
    within = {item["value"]: dict.fromkeys(levels, 0) for item in by_borough}
    for row in db.execute("SELECT COALESCE(r.borough, ?) AS borough, COALESCE(r.accessibility, ?) AS level,"
                          " COUNT(*) AS n" + official + " GROUP BY borough, level", [UNKNOWN, UNKNOWN] + values):
        within[row["borough"]][row["level"]] = row["n"]

    per_category = dict(db.execute("SELECT p.category, COUNT(*)" + reports + " GROUP BY p.category", values).fetchall())
    by_day = db.execute("SELECT SUBSTR(p.created_at, 1, 10) AS day, COUNT(*) AS n" + reports
                        + " GROUP BY day ORDER BY day", values).fetchall()
    top_reported = db.execute("SELECT r.id, r.facility_name, r.borough, r.latitude, r.longitude,"
                              " COUNT(*) AS report_count, SUM(p.observed_at >= ?) AS active_report_count,"
                              " MAX(p.created_at) AS latest_report_at" + reports
                              + " GROUP BY r.id ORDER BY report_count DESC, latest_report_at DESC, r.id LIMIT 10",
                              [active_from] + values).fetchall()
    result = {
        "meta": meta(db),
        "filters": {column: request.query_params[column].strip()
                    for column in STATS_FILTERS if request.query_params.get(column, "").strip()},
        "totals": {"restrooms": totals[0], "operational": totals[1], "fully_accessible": totals[2],
                   "with_changing_stations": totals[3], "menstrual_friendly": totals[4],
                   "reports": report_totals[0], "active_reports": report_totals[1],
                   "community_added": len(added),
                   "community_verified": sum(restroom["verified"] for restroom in added)},
        "by_borough": by_borough,
        "by_location_type": grouped("location_type"),
        "by_operator": grouped("operator"),
        "by_accessibility": [{"value": level, "count": by_accessibility.get(level, 0)} for level in levels],
        "by_open": grouped("open"),
        "accessibility_by_borough": [{"borough": item["value"], "total": item["count"], "counts": within[item["value"]]}
                                     for item in by_borough],
        "reports_by_category": [{"category": category, "count": per_category.get(category, 0)}
                                for category in REPORT_CATEGORIES],
        "reports_by_day": [{"day": row["day"], "count": row["n"]} for row in by_day],
        "top_reported": [dict(row) for row in top_reported],
    }
    db.close()
    return JSONResponse(result)


# ---------------------------------------------------------------------------
# Route: the website itself
# ---------------------------------------------------------------------------
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
    Route(path="/health", endpoint=health, methods=["GET"]),
    Route(path="/restrooms", endpoint=list_restrooms, methods=["GET"]),
    Route(path="/restrooms", endpoint=add_restroom, methods=["POST"]),
    Route(path="/restrooms/{id}", endpoint=get_restroom, methods=["GET"]),
    Route(path="/restrooms/{id}/votes", endpoint=vote_restroom, methods=["POST"]),
    Route(path="/stats", endpoint=stats, methods=["GET"]),
    Route(path="/reports", endpoint=create_report, methods=["POST"]),
    Route(path="/reports/recent", endpoint=recent_reports, methods=["GET"]),
    Route(path="/reports.csv", endpoint=reports_csv, methods=["GET"]),
    Route(path="/reports/{id:int}/confirmations", endpoint=create_confirmation, methods=["POST"]),
    WebSocketRoute(path="/ws", endpoint=live_socket),
]

routes = [
    Mount(API_PREFIX, routes=api_routes),
    Route(path="/", endpoint=homepage, methods=["GET"]),
    Route(path="/index.html", endpoint=homepage, methods=["GET"]),
]

app = Starlette(
    debug=False,
    routes=routes,
    # No cookies or logins, so any page may call the API (also pages opened as a file).
    middleware=[Middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET", "POST"], allow_headers=["*"])],
    exception_handlers={HTTPException: http_error, Exception: server_error},
    lifespan=lifespan,
)

if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")  # odd characters never crash a print
    if "--export" in sys.argv:
        init_db()
        path = os.path.join(HERE, "reports.csv")
        with open(path, "w", encoding="utf-8-sig", newline="") as handle:
            handle.write(reports_csv_text())
        print("Saved %s" % path)
    elif "--sync" in sys.argv:
        init_db()
        import_restrooms()
    else:
        print("Open http://127.0.0.1:%d/  (API at http://127.0.0.1:%d%s/health)" % (PORT, PORT, API_PREFIX))
        uvicorn.run(app="api:app", host="127.0.0.1", port=PORT, reload=False, workers=1)
