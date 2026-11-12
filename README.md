# Team 11: NYC Public Restroom Finder

| File | What it is |
|---|---|
| `index.html` | Frontend. One file with inline CSS and JS. Includes Find a restroom (map and list), Restroom detail, Report a problem, Dashboard and About. Uses Leaflet 1.9.4 and OpenStreetMap tiles. |
| `nyc-restroom-finder.py` | Backend. One file, Starlette and uvicorn. Imports the NYC Open Data restroom dataset into SQLite (`restrooms.db`) and serves the JSON API. It also serves `index.html`. |
| `requirements.txt` | The two Python packages the backend needs. |

## Run it

```
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python nyc-restroom-finder.py
```

Then open **http://127.0.0.1:8011/**. The first start needs internet access to download the dataset and the borough boundaries, and it prints an import summary.

Opening `index.html` by double-clicking it also works while the backend runs. OpenStreetMap blocks `file://` pages, so in that case the map uses Esri gray tiles instead.

To refresh the official data later without losing reports, run `python nyc-restroom-finder.py --sync`.

## Pages (hash routes in index.html)

`#/map` · `#/restroom/:id` · `#/report?restroom=:id` · `#/dashboard` · `#/about`

## API (all under `/api/team11`, JSON, errors are `{"error": "..."}`)

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Row counts and the last import time |
| GET | `/restrooms` | All restrooms plus every active report. Optional filters: `borough`, `accessibility`, `location_type`, `open`, `changing_stations`, `menstrual_friendly`, `status` (use `unknown` for missing values). With `lat` and `lng`, adds `distance_m` and sorts nearest first. |
| GET | `/restrooms/{id}` | One restroom with all its reports and their confirmation counts |
| POST | `/reports` | `{restroom_id, category, note?, seen_minutes_ago: 0 / 180 / 1440}` |
| POST | `/reports/{id}/confirmations` | `{agrees: true / false}` |
| GET | `/stats` | Dashboard numbers. Optional filters: `borough`, `location_type`, `accessibility` |

Report categories: `closed`, `out_of_order`, `wrong_hours`, `supplies`, `cleanliness`, `access`, `safety`. The old names `dirty`, `no_supplies`, `accessibility_problem` and `unsafe` are still accepted.

## Rules (constants at the top of the .py file)

- A report counts as **active for 48 hours** (`REPORT_ACTIVE_HOURS`). During that window a `closed` or `out_of_order` report makes the restroom show as "Reported closed". The detail page shows older reports up to 7 days (`REPORT_KEEP_DAYS`).
- Each visitor can send one report per restroom per hour (HTTP 429) and one answer per report (HTTP 409). Visitors are recognised by a salted hash of their IP address, and no raw addresses are stored.
- Borough comes from the NYC borough boundaries. If those can't be downloaded, NYC's region code is used instead.

## Course server

Change `ROUTE_PREFIX` and `PORT` at the top of the .py file to what the professor assigns. If the prefix changes, also update `API_BASE` at the top of the script in `index.html`. When the page is not on localhost, it calls the API on its own site.
