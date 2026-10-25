import asyncio
import json
import math
import os
import secrets
import random
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="THE Z Multiplayer Worlds")
INDEX_FILE = Path(__file__).with_name("index.html")

# One shared in-memory room. Players can switch between maps; clients only render
# people currently playing in the same map, while the server relays live state.
clients: dict[str, WebSocket] = {}
players: dict[str, dict[str, Any]] = {}
state_lock = asyncio.Lock()
COLORS = ["#ff6b6b", "#ffd166", "#06d6a0", "#4cc9f0", "#b197fc", "#ff8fab", "#f4a261", "#90be6d"]
VALID_MAPS = {"coins", "parkour"}


def finite_number(value: Any, fallback: float, low: float, high: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return fallback
    number = float(value)
    if not math.isfinite(number):
        return fallback
    return max(low, min(high, number))


async def send_safely(socket: WebSocket, message: dict[str, Any]) -> None:
    try:
        await socket.send_json(message)
    except Exception:
        # A failed send must not interrupt broadcasts to the other players.
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
        count = len(clients)
        by_map = {name: sum(1 for p in players.values() if p.get("map") == name) for name in VALID_MAPS}
    return JSONResponse({"ok": True, "players": count, "maps": by_map})


@app.websocket("/ws")
async def multiplayer_socket(websocket: WebSocket) -> None:
    await websocket.accept()
    player_id = secrets.token_hex(6)
    player = {
        "id": player_id,
        "name": f"Player{random.randint(100, 999)}",
        "color": random.choice(COLORS),
        "x": 0.0,
        "y": 0.0,
        "z": 0.0,
        "yaw": 0.0,
        "map": "coins",
    }

    try:
        async with state_lock:
            clients[player_id] = websocket
            players[player_id] = player
            initial_players = [dict(p) for p in players.values()]

        await websocket.send_json({"type": "init", "id": player_id, "players": initial_players})
        await broadcast({"type": "join", "player": dict(player)}, exclude_id=player_id)

        while True:
            raw_message = await websocket.receive_text()
            if len(raw_message) > 2048:
                continue
            try:
                data = json.loads(raw_message)
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(data, dict):
                continue

            message_type = data.get("type")
            if message_type == "identify":
                name = data.get("name", "")
                if not isinstance(name, str):
                    name = ""
                name = " ".join(name.strip().split())[:22] or f"Player{random.randint(100, 999)}"
                async with state_lock:
                    current = players.get(player_id)
                    if current is None:
                        break
                    current["name"] = name
                    updated_player = dict(current)
                # Broadcast the chosen display name to everyone, including its owner.
                await broadcast({"type": "state", "player": updated_player})
                continue

            if message_type != "state":
                continue

            map_name = data.get("map", "coins")
            if not isinstance(map_name, str) or map_name not in VALID_MAPS:
                map_name = "coins"

            async with state_lock:
                current = players.get(player_id)
                if current is None:
                    break
                # Large position bounds allow the endless parkour track to grow far
                # beyond a small plaza while still preventing non-finite coordinates.
                current["x"] = finite_number(data.get("x"), current["x"], -250.0, 250.0)
                current["z"] = finite_number(data.get("z"), current["z"], -10_000_000.0, 10_000_000.0)
                current["y"] = finite_number(data.get("y"), current["y"], 0.0, 100.0)
                current["yaw"] = finite_number(data.get("yaw"), current["yaw"], -100000.0, 100000.0)
                current["map"] = map_name
                state_message = {"type": "state", "player": dict(current)}

            await broadcast(state_message, exclude_id=player_id)

    except WebSocketDisconnect:
        pass
    except Exception:
        # A single lost connection must not take the room down.
        pass
    finally:
        async with state_lock:
            existed = player_id in players
            clients.pop(player_id, None)
            players.pop(player_id, None)
        if existed:
            await broadcast({"type": "leave", "id": player_id})
