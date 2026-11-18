#!/web/urban-data/.env/bin/python
"""Team 11 (NYC Public Restroom Finder): simple test version for the course server.

Run:  python api.py   then open http://127.0.0.1:8091/
Needs: starlette, uvicorn
"""
import uvicorn
from starlette.applications import Starlette
from starlette.responses import HTMLResponse
from starlette.routing import Route

PORT = 8091  # our team's assigned port


async def Root(request) -> HTMLResponse:
    return HTMLResponse(
        content="<p>hello world from Team 11: NYC Public Restroom Finder</p>"
    )


routes: list[Route] = [
    Route(path="/", endpoint=Root, methods=["GET"]),
]

app = Starlette(debug=False, routes=routes)

if __name__ == "__main__":
    uvicorn.run(app="api:app", host="127.0.0.1", port=PORT, reload=False, workers=1)
