# Team 11: NYC Public Restroom Finder

| File | What it is |
|---|---|
| `index.html` | Frontend. One file with inline CSS, JS and the embedded Daydream heading font. Includes Find a restroom (map and list), Restroom detail, Report a problem, Add a restroom, Dashboard and About. Uses Leaflet 1.9.4 and OpenStreetMap tiles, and links out to Google Maps for directions. |
| `api.py` | Backend. One file, Starlette and uvicorn. Imports the NYC Open Data restroom dataset into SQLite (`restrooms.db`) and serves the JSON API. It also serves `index.html`. |
| `requirements.txt` | The Python packages the backend needs (starlette, uvicorn, websockets). |

## Run it

```
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python api.py
```

Then open **http://127.0.0.1:8091/**. The first start needs internet access to download the dataset and the borough boundaries, and it prints an import summary.

Opening `index.html` by double-clicking it also works while the backend runs. OpenStreetMap blocks `file://` pages, so in that case the map uses Esri gray tiles instead.

To save every report to `reports.csv` in this folder (opens in Excel, Google Sheets or a text editor), run `.venv/bin/python api.py --export`. The Dashboard also has a Recent reports table with a Download CSV button.

To refresh the official data later without losing reports, run `.venv/bin/python api.py --sync`.

## Pages (hash routes in index.html)

`#/map` · `#/restroom/:id` · `#/report?restroom=:id` · `#/add` · `#/dashboard` · `#/about`

## API (all under `/api/team11`, JSON, errors are `{"error": "..."}`)

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Row counts and the last import time |
| GET | `/restrooms` | All restrooms plus every active report. Optional filters: `borough`, `accessibility`, `location_type`, `open`, `changing_stations`, `menstrual_friendly`, `status` (use `unknown` for missing values). With `lat` and `lng`, adds `distance_m` and sorts nearest first. |
| POST | `/restrooms` | Add a restroom: `{facility_name, location_type, latitude, longitude, hours_of_operation?, accessibility?, additional_notes?}` |
| POST | `/restrooms/{id}/votes` | `{here: true / false}` on a community-added restroom |
| GET | `/restrooms/{id}` | One restroom with all its reports and their confirmation counts |
| POST | `/reports` | `{restroom_id, category, note?, seen_minutes_ago: 0 / 180 / 1440}` |
| POST | `/reports/{id}/confirmations` | `{agrees: true / false}` |
| GET | `/reports/recent` | Newest reports with restroom names (`?limit=` 1–500, default 50) |
| GET | `/reports.csv` | Every report as a CSV download |
| GET | `/stats` | Dashboard numbers. Optional filters: `borough`, `location_type`, `accessibility` |

Report categories: `closed`, `out_of_order`, `wrong_hours`, `supplies`, `cleanliness`, `access`, `safety`. The old names `dirty`, `no_supplies`, `accessibility_problem` and `unsafe` are still accepted.

## Rules (constants at the top of `api.py`)

- A report counts as **active for 48 hours** (`REPORT_ACTIVE_HOURS`). During that window a `closed` or `out_of_order` report makes the restroom show as "Reported closed". The detail page shows older reports up to 7 days (`REPORT_KEEP_DAYS`).
- Each visitor can send one report per restroom per hour (HTTP 429) and one answer per report (HTTP 409). Visitors are recognised by a salted hash of their IP address, and no raw addresses are stored.
- Added restrooms are saved in the same `restrooms` table with `source = 'community'`. They show as "Not verified yet" until 3 other visitors say "Yes, it's here" (`VERIFY_VOTES`), and are hidden if 3+ say "Not here" and those outnumber the yes votes. Each visitor can add 5 a day, can't confirm their own, and answers once per restroom. The Dashboard's official charts leave them out and count them in their own tile.
- Borough comes from the NYC borough boundaries. If those can't be downloaded, NYC's region code is used instead.

## Course server

Change `API_PREFIX` and `PORT` at the top of `api.py` to what the professor assigns. If the prefix changes, also update `API_BASE` at the top of the script in `index.html`. When the page is not on localhost, it calls the API on its own site.

## Fonts

Headings use **Daydream** by DoubleGum, embedded in `index.html`. The version used is the free DEMO, licensed for personal use only, so a commercial project would need the paid licence (doublegumm.itch.io/daydream-pixel-font).
