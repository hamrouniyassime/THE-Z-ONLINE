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

app = FastAPI(title="THE Z Multiplayer 3D Game")
INDEX_FILE = Path(__file__).with_name("index.html")

# In-memory room: all connected players on this server instance see one another.
clients: dict[str, WebSocket] = {}
players: dict[str, dict[str, Any]] = {}
state_lock = asyncio.Lock()
COLORS = ["#ff6b6b", "#ffd166", "#06d6a0", "#4cc9f0", "#b197fc", "#ff8fab", "#f4a261", "#90be6d"]


def finite_number(value: Any, fallback: float, low: float, high: float) -> float:
    """Parse a finite number and clamp it to sensible world bounds."""
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
        # The normal disconnect handler removes dead sockets; failed broadcasts
        # should not interrupt delivery to the other players.
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
    return JSONResponse({"ok": True, "players": count})


@app.websocket("/ws")
async def multiplayer_socket(websocket: WebSocket) -> None:
    await websocket.accept()
    player_id = secrets.token_hex(6)
    player = {
        "id": player_id,
        "name": f"Player{random.randint(100, 999)}",
        "color": random.choice(COLORS),
        "x": round(random.uniform(-4.0, 4.0), 2),
        "y": 0.0,
        "z": round(random.uniform(-4.0, 4.0), 2),
        "yaw": 0.0,
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
            # State packets are intentionally tiny; ignore oversized/unexpected data.
            if len(raw_message) > 2048:
                continue
            try:
                data = json.loads(raw_message)
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(data, dict):
                continue
            if data.get("type") == "identify":
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
                await broadcast({"type": "state", "player": updated_player})
                continue
            if data.get("type") != "state":
                continue

            async with state_lock:
                current = players.get(player_id)
                if current is None:
                    break
                current["x"] = finite_number(data.get("x"), current["x"], -46.0, 46.0)
                current["z"] = finite_number(data.get("z"), current["z"], -46.0, 46.0)
                current["y"] = finite_number(data.get("y"), current["y"], 0.0, 5.0)
                current["yaw"] = finite_number(data.get("yaw"), current["yaw"], -100000.0, 100000.0)
                state_message = {"type": "state", "player": dict(current)}

            await broadcast(state_message, exclude_id=player_id)

    except WebSocketDisconnect:
        pass
    except Exception:
        # A single lost connection must never take down the whole room.
        pass
    finally:
        async with state_lock:
            existed = player_id in players
            clients.pop(player_id, None)
            players.pop(player_id, None)
        if existed:
            await broadcast({"type": "leave", "id": player_id})
