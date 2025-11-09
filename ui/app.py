"""
FastAPI backend for train movement monitoring with station filtering
"""
import asyncio
import csv
import json
import logging
import os
import sys
from dataclasses import dataclass
from typing import Dict, List, Set

import aiohttp
import nats
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("train-monitor")
app = FastAPI(title="Train Movement Monitor")

# CORS for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@dataclass
class Station:
    stanox: str
    stanme: str


# globals
stations: Dict[str, Station] = {}
active_connections: List[WebSocket] = []
user_filters: Dict[WebSocket, Set[str]] = {}  # ws -> STANOX codeset

# NATS connection
nc: nats.NATS | None = None


async def load_stations():
    url = "https://raw.githubusercontent.com/openraildata/reference-data/main/stanox-stanme.csv"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            content = await response.text()
            reader = csv.DictReader(
                content.splitlines(),
                fieldnames=["STANOX", "STANME"],
            )

            for row in reader:
                stanox = row.get('STANOX', '').strip()
                stanme = row.get('STANME', '').strip()

                if stanox and stanme:
                    stations[stanox] = Station(
                        stanox=stanox,
                        stanme=stanme,
                    )

    logger.info(f"Loaded {len(stations)} stations")


async def connect_nats():
    """Connect to NATS"""
    global nc
    nc = await nats.connect("nats://nats:4222")
    logger.info("Connected to NATS")


@app.on_event("startup")
async def startup():
    await load_stations()
    await connect_nats()
    # Start background task to consume NATS messages
    asyncio.create_task(consume_nats_messages())


@app.on_event("shutdown")
async def shutdown():
    if nc:
        await nc.close()


async def consume_nats_messages():
    """Subscribe to NATS and broadcast to WebSocket clients"""

    async def message_handler(msg):
        try:
            data = json.loads(msg.data.decode())

            # Extract STANOX codes for origin/destination/location
            # Adjust these fields based on your actual message structure
            loc_stanox = data.get('loc_stanox') or data.get('stanox')
            origin_stanox = data.get('origin_stanox') or data.get('from_stanox')
            dest_stanox = data.get('dest_stanox') or data.get('to_stanox')

            # Broadcast to clients interested in any of these stations
            stanox_list = [s for s in [loc_stanox, origin_stanox, dest_stanox] if s]

            for stanox in stanox_list:
                await broadcast_to_filtered_clients(stanox, data)

        except Exception as e:
            print(f"Error processing message: {e}")

    await nc.subscribe("train.mvt.>", cb=message_handler)
    print("Subscribed to train.mvt.>")


async def broadcast_to_filtered_clients(stanox: str, message: dict):
    """Send message to clients who have this station in their filter"""
    disconnected = []

    for ws in active_connections:
        try:
            # Check if this client wants this station
            if stanox in user_filters.get(ws, set()):
                await ws.send_json({
                    "type": "movement",
                    "stanox": stanox,
                    "station": stations.get(stanox, {}).stanme if stanox in stations else "Unknown",
                    "data": message
                })
        except Exception:
            disconnected.append(ws)

    # Clean up disconnected clients
    for ws in disconnected:
        if ws in active_connections:
            active_connections.remove(ws)
        if ws in user_filters:
            del user_filters[ws]


@app.get("/api/stations")
async def get_stations():
    """Get all available stations"""
    return {
        "stations": [
            {
                "stanox": s.stanox,
                "name": s.stanme,
            }
            for s in stations.values()
        ],
        "total": len(stations)
    }


@app.get("/api/stations/search")
async def search_stations(q: str = "", limit: int = 50):
    """Search stations by name"""
    q = q.lower()
    results = [
        {
            "stanox": s.stanox,
            "name": s.stanme,
        }
        for s in stations.values()
        if q in s.stanme.lower()
    ]
    return {"results": results[:limit]}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates"""
    await websocket.accept()
    active_connections.append(websocket)
    user_filters[websocket] = set()

    try:
        await websocket.send_json({"type": "connected", "message": "Connected to train monitor"})

        while True:
            data = await websocket.receive_json()

            if data.get("type") == "subscribe":
                # Client wants to monitor specific stations
                stanox_list = data.get("stanox", [])
                user_filters[websocket] = set(stanox_list)
                await websocket.send_json({
                    "type": "subscribed",
                    "count": len(stanox_list),
                    "stations": [stations[s].stanme for s in stanox_list if s in stations]
                })

            elif data.get("type") == "unsubscribe":
                user_filters[websocket] = set()
                await websocket.send_json({"type": "unsubscribed"})

    except WebSocketDisconnect:
        active_connections.remove(websocket)
        if websocket in user_filters:
            del user_filters[websocket]


# Serve static files (HTML frontend)
@app.get("/")
async def serve_frontend():
    return FileResponse("static/index.html")


# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
