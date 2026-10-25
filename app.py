import asyncio
import json
import math
import os
import random
import re
import secrets
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="THE Z — Luma Bay Roleplay")
INDEX_FILE = Path(__file__).with_name("index.html")

clients: dict[str, WebSocket] = {}
players: dict[str, dict[str, Any]] = {}
state_lock = asyncio.Lock()
COLORS = ["#ff6b6b", "#ffd166", "#06d6a0", "#4cc9f0", "#b197fc", "#ff8fab", "#f4a261", "#90be6d"]
VALID_MAPS = {"coins", "parkour", "city", "cars"}
VALID_CARS = {"compact", "sedan", "suv", "sport", "electric", "super"}
MAX_HOUSES = 50
used_house_slots: set[int] = set()


def finite_number(value: Any, fallback: float, low: float, high: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return fallback
    number = float(value)
    if not math.isfinite(number):
        return fallback
    return max(low, min(high, number))


def clean_name(value: Any, fallback: str) -> str:
    if not isinstance(value, str):
        return fallback
    name = " ".join(value.strip().split())
    # Strip control characters and avoid names made only from whitespace.
    name = re.sub(r"[\x00-\x1f\x7f]", "", name)[:22]
    return name or fallback


def allocate_house_slot() -> int:
    for slot in range(MAX_HOUSES):
        if slot not in used_house_slots:
            used_house_slots.add(slot)
            return slot
    # Fallback for very large rooms; this still assigns a plot, though the first
    # forty are the carefully built house plots on the main city map.
    slot = max(used_house_slots, default=-1) + 1
    used_house_slots.add(slot)
    return slot


async def send_safely(socket: WebSocket, message: dict[str, Any]) -> None:
    try:
        await socket.send_json(message)
    except Exception:
        pass


async def broadcast(message: dict[str, Any], exclude_id: str | None = None) -> None:
    async with state_lock:
        recipients = [(pid, socket) for pid, socket in clients.items() if pid != exclude_id]
    if recipients:
        await asyncio.gather(*(send_safely(socket, message) for _, socket in recipients))


@app.get("/", response_class=HTMLResponse)
async def home() -> HTMLResponse:
    if not INDEX_FILE.exists():
        return HTMLResponse("index.html is missing. Put it beside app.py.", status_code=500)
    return HTMLResponse(INDEX_FILE.read_text(encoding="utf-8"))


@app.get("/health")
async def health() -> JSONResponse:
    async with state_lock:
        by_map = {name: sum(1 for p in players.values() if p.get("map") == name) for name in sorted(VALID_MAPS)}
        count = len(clients)
    return JSONResponse({"ok": True, "players": count, "maps": by_map, "house_slots_in_use": len(used_house_slots)})


@app.websocket("/ws")
async def multiplayer_socket(websocket: WebSocket) -> None:
    await websocket.accept()
    player_id = secrets.token_hex(6)
    async with state_lock:
        home_slot = allocate_house_slot()
        player = {
            "id": player_id,
            "name": f"Player{random.randint(100, 999)}",
            "color": random.choice(COLORS),
            "x": 0.0,
            "y": 0.0,
            "z": 0.0,
            "yaw": 0.0,
            "map": "city",
            "home_slot": home_slot,
            "home_locked": False,
            "vehicle": None,
        }
        clients[player_id] = websocket
        players[player_id] = player
        initial_players = [dict(p) for p in players.values()]

    try:
        await websocket.send_json({"type": "init", "id": player_id, "players": initial_players})
        await broadcast({"type": "join", "player": dict(player)}, exclude_id=player_id)

        while True:
            raw_message = await websocket.receive_text()
            if len(raw_message) > 4096:
                continue
            try:
                data = json.loads(raw_message)
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(data, dict):
                continue

            message_type = data.get("type")
            if message_type == "identify":
                async with state_lock:
                    current = players.get(player_id)
                    if current is None:
                        break
                    current["name"] = clean_name(data.get("name"), current["name"])
                    updated_player = dict(current)
                await broadcast({"type": "state", "player": updated_player})
                continue

            if message_type != "state":
                continue

            map_name = data.get("map", "city")
            if not isinstance(map_name, str) or map_name not in VALID_MAPS:
                map_name = "city"

            vehicle = data.get("vehicle")
            clean_vehicle = None
            if isinstance(vehicle, dict) and vehicle.get("type") in VALID_CARS:
                color = vehicle.get("color", "#3b82f6")
                if not isinstance(color, str) or not re.fullmatch(r"#[0-9a-fA-F]{6}", color):
                    color = "#3b82f6"
                clean_vehicle = {
                    "type": vehicle["type"],
                    "color": color,
                    "x": finite_number(vehicle.get("x"), 0.0, -500.0, 500.0),
                    "z": finite_number(vehicle.get("z"), 0.0, -10_000_000.0, 10_000_000.0),
                    "yaw": finite_number(vehicle.get("yaw"), 0.0, -100000.0, 100000.0),
                    "driving": bool(vehicle.get("driving", False)),
                }

            async with state_lock:
                current = players.get(player_id)
                if current is None:
                    break
                current["x"] = finite_number(data.get("x"), current["x"], -500.0, 500.0)
                current["z"] = finite_number(data.get("z"), current["z"], -10_000_000.0, 10_000_000.0)
                current["y"] = finite_number(data.get("y"), current["y"], 0.0, 100.0)
                current["yaw"] = finite_number(data.get("yaw"), current["yaw"], -100000.0, 100000.0)
                current["map"] = map_name
                current["home_locked"] = bool(data.get("home_locked", current.get("home_locked", False)))
                current["vehicle"] = clean_vehicle
                state_message = {"type": "state", "player": dict(current)}
            await broadcast(state_message, exclude_id=player_id)

    except WebSocketDisconnect:
        pass
    except Exception:
        # A disconnected or malformed client must not interrupt the room.
        pass
    finally:
        async with state_lock:
            existed = player_id in players
            old_player = players.pop(player_id, None)
            clients.pop(player_id, None)
            if old_player is not None:
                used_house_slots.discard(int(old_player.get("home_slot", -1)))
        if existed:
            await broadcast({"type": "leave", "id": player_id})
