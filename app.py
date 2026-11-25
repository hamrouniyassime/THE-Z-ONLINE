"""VICE CITY LIFE multiplayer WebSocket server.

Serves the single-file HTML client and relays player/vehicle state between
clients in the same room. This is a lightweight synchronization server, not
an authoritative physics simulation.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import time
import unicodedata
import uuid
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse

BASE_DIR = Path(__file__).resolve().parent
# Always serve the project entry page. This intentionally ignores any stale
# GAME_HTML environment variable left over from older deployments.
GAME_FILE = "index.html"
GAME_PATH = BASE_DIR / GAME_FILE
PORT = int(os.environ.get("PORT", "8000"))
MAX_PLAYERS_PER_ROOM = int(os.environ.get("MAX_PLAYERS_PER_ROOM", "32"))
MAX_ROOM_LENGTH = 24
MAX_NAME_LENGTH = 18
MIN_STATE_INTERVAL = 0.055  # Approximately 18 updates per second per player.

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger("vice-city-multiplayer")

app = FastAPI(title="VICE CITY LIFE Multiplayer", version="1.0.0")

# player_id -> {websocket, room, state}
clients: dict[str, dict[str, Any]] = {}
# room_name -> set[player_id]
rooms: dict[str, set[str]] = {}

ALLOWED_VEHICLES = {
    "sedan", "taxi", "sport", "police", "van", "pickup", "truck",
    "motorcycle", "gangBike", "bicycle", "jetski", "fighter",
    "helicopter", "airliner", "seaplane", "boat", "ship",
}
ALLOWED_ANIMATIONS = {
    "idle", "walk", "run", "jump", "jumpTuck", "jumpLong", "jumpSide",
    "land", "swim", "treadWater", "drive", "rideBike", "rideChopper",
    "rideBicycle", "fall", "fallBack", "getupBack", "hit", "stumble",
    "push", "punch", "punchL", "kick", "enter", "exit",
}


def clean_room(value: str | None) -> str:
    value = (value or "vice-city").strip().lower()
    value = re.sub(r"[^a-z0-9_-]", "", value)[:MAX_ROOM_LENGTH]
    return value or "vice-city"


def clean_name(value: str | None) -> str:
    value = (value or "Player").strip()
    # Keep Arabic and other ordinary letters, but remove control characters.
    value = "".join(ch for ch in value if unicodedata.category(ch) != "Cc")
    value = value[:MAX_NAME_LENGTH].strip()
    return value or "Player"


def safe_number(value: Any, default: float, low: float, high: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    if not math.isfinite(number):
        return default
    return max(low, min(high, number))


def public_state(state: dict[str, Any]) -> dict[str, Any]:
    """Return only fields intended to be shared with other players."""
    return {
        "id": state["id"],
        "name": state["name"],
        "gender": state["gender"],
        "x": state["x"],
        "y": state["y"],
        "z": state["z"],
        "heading": state["heading"],
        "anim": state["anim"],
        "inVehicle": state["inVehicle"],
        "vehicleType": state["vehicleType"],
        "vehicleColor": state["vehicleColor"],
    }


async def send_to_room(room: str, payload: dict[str, Any], exclude: str | None = None) -> None:
    player_ids = list(rooms.get(room, set()))
    sends = []
    for player_id in player_ids:
        if player_id == exclude:
            continue
        entry = clients.get(player_id)
        if entry is not None:
            sends.append(entry["websocket"].send_json(payload))
    if sends:
        results = await asyncio.gather(*sends, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                log.debug("A room broadcast could not reach one client: %s", result)


@app.get("/")
async def home():
    if not GAME_PATH.is_file():
        return JSONResponse(
            status_code=404,
            content={
                "error": f"Game file not found: {GAME_FILE}",
                "hint": "Make sure index.html is committed beside app.py in the deployed repository, then redeploy the latest commit.",
            },
        )
    return FileResponse(GAME_PATH, media_type="text/html")


@app.get("/healthz")
async def health():
    return {
        "ok": True,
        "players_online": len(clients),
        "rooms_active": len(rooms),
        "game_file_found": GAME_PATH.is_file(),
    }


@app.websocket("/ws")
async def multiplayer_socket(websocket: WebSocket):
    await websocket.accept()
    player_id = uuid.uuid4().hex
    room = clean_room(websocket.query_params.get("room"))
    name = clean_name(websocket.query_params.get("name"))
    gender = "f" if websocket.query_params.get("gender") == "f" else "m"

    room_members = rooms.setdefault(room, set())
    if len(room_members) >= MAX_PLAYERS_PER_ROOM:
        await websocket.send_json({"type": "server_full", "message": "This room is full."})
        await websocket.close(code=1013, reason="Room is full")
        return

    state: dict[str, Any] = {
        "id": player_id,
        "name": name,
        "gender": gender,
        "x": -1100.0,
        "y": 0.0,
        "z": 12.0,
        "heading": 0.0,
        "anim": "idle",
        "inVehicle": False,
        "vehicleType": "",
        "vehicleColor": "",
    }
    clients[player_id] = {"websocket": websocket, "room": room, "state": state}
    room_members.add(player_id)

    previous_players = [
        public_state(clients[other_id]["state"])
        for other_id in list(room_members)
        if other_id != player_id and other_id in clients
    ]

    try:
        await websocket.send_json({
            "type": "welcome",
            "id": player_id,
            "room": room,
            "players": previous_players,
            "count": len(room_members),
        })
        await send_to_room(room, {"type": "player_join", "player": public_state(state)}, exclude=player_id)

        last_update = 0.0
        while True:
            raw = await websocket.receive_text()
            if len(raw) > 4096:
                await websocket.close(code=1009, reason="Message too large")
                break
            try:
                packet = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(packet, dict) or packet.get("type") != "state":
                continue

            now = time.monotonic()
            if now - last_update < MIN_STATE_INTERVAL:
                continue
            last_update = now

            vehicle_type = str(packet.get("vehicleType", ""))
            in_vehicle = bool(packet.get("inVehicle")) and vehicle_type in ALLOWED_VEHICLES
            vehicle_color = str(packet.get("vehicleColor", ""))
            if not re.fullmatch(r"#[0-9a-fA-F]{6}", vehicle_color):
                vehicle_color = "#3c8fc4"

            state = {
                "id": player_id,
                "name": name,
                "gender": gender,
                "x": safe_number(packet.get("x"), -1100.0, -5000.0, 5000.0),
                "y": safe_number(packet.get("y"), 0.0, -80.0, 800.0),
                "z": safe_number(packet.get("z"), 12.0, -5000.0, 5000.0),
                "heading": safe_number(packet.get("heading"), 0.0, -100.0, 100.0),
                "anim": packet.get("anim") if isinstance(packet.get("anim"), str) and packet.get("anim") in ALLOWED_ANIMATIONS else "idle",
                "inVehicle": in_vehicle,
                "vehicleType": vehicle_type if in_vehicle else "",
                "vehicleColor": vehicle_color if in_vehicle else "",
            }
            clients[player_id]["state"] = state
            await send_to_room(room, {"type": "player_update", "player": public_state(state)}, exclude=player_id)

    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("WebSocket session ended unexpectedly for %s", player_id)
    finally:
        clients.pop(player_id, None)
        members = rooms.get(room)
        if members is not None:
            members.discard(player_id)
            if not members:
                rooms.pop(room, None)
        await send_to_room(room, {"type": "player_leave", "id": player_id})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=PORT, reload=False)
