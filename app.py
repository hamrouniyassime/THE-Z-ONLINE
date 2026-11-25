"""THE Z ONLINE single-world multiplayer WebSocket server.

The server validates the initial immersive-mode attestation, places every
joined client in one fixed city, relays player state, and lets one elected
client publish deterministic NPC/traffic snapshots for the shared world.
This is a lightweight real-time relay, not an authoritative physics server.
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
GAME_PATH = BASE_DIR / "index.html"
IMAGE_PATH = BASE_DIR / "img.png"
PORT = int(os.environ.get("PORT", "8000"))
MAX_PLAYERS = max(2, min(128, int(os.environ.get("MAX_PLAYERS", "64"))))
MAX_NAME_LENGTH = 18
MAX_PACKET_LENGTH = 262_144
MIN_STATE_INTERVAL = 0.055
MIN_WORLD_INTERVAL = 0.45
WORLD_ROOM = "the-z-online"

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger("the-z-online")

app = FastAPI(title="THE Z ONLINE", version="2.0.0")

# player_id -> {websocket, room, state}
clients: dict[str, dict[str, Any]] = {}
rooms: dict[str, set[str]] = {WORLD_ROOM: set()}
world_host_id: str | None = None
latest_world_state: dict[str, Any] | None = None

ALLOWED_VEHICLES = {
    "sedan", "taxi", "sport", "police", "van", "pickup", "truck",
    "motorcycle", "gangBike", "bicycle", "jetski", "fighter",
    "helicopter", "airliner", "seaplane", "boat", "ship",
}
ALLOWED_WEAPONS = {"fist","smg","shotgun","deagle","rifle","sniper","knife","bat","crowbar","hammer","katana","axe"}
ALLOWED_ANIMATIONS = {
    "idle", "walk", "run", "wave", "dance", "sit", "jump", "jumpTuck", "jumpLong", "jumpSide",
    "land", "swim", "treadWater", "drive", "rideBike", "rideChopper",
    "rideBicycle", "fall", "fallBack", "getupBack", "hit", "stumble",
    "push", "punch", "punchL", "kick", "enter", "exit",
}
ALLOWED_HAIR = {"short", "long", "pony", "spiky", "buzz", "bald"}
CHARACTER_COLORS = {
    "hc", "skin", "eye", "shirt", "pants", "shoe",
}


def clean_name(value: Any) -> str:
    value = str(value or "Player").strip()
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


def clean_character(value: Any, gender: str) -> dict[str, Any]:
    source = value if isinstance(value, dict) else {}
    hair_value = source.get("hair")
    result: dict[str, Any] = {
        "hair": hair_value if isinstance(hair_value, str) and hair_value in ALLOWED_HAIR else ("long" if gender == "f" else "short"),
        "hc": "#211711",
        "skin": "#e8b890",
        "eye": "#3a2a1e",
        "shirt": "#e8e8ee",
        "pants": "#24395e",
        "shoe": "#272b38",
        "beard": False,
        "glasses": False,
        "skirt": False,
    }
    for key in CHARACTER_COLORS:
        color = source.get(key)
        if isinstance(color, str) and re.fullmatch(r"#[0-9a-fA-F]{6}", color):
            result[key] = color.lower()
    result["beard"] = bool(source.get("beard")) and gender == "m"
    result["glasses"] = bool(source.get("glasses"))
    result["skirt"] = bool(source.get("skirt")) and gender == "f"
    if gender == "f" and result["hair"] == "buzz":
        result["hair"] = "long"
    if gender == "m" and result["hair"] == "pony":
        result["hair"] = "short"
    return result


def public_state(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": state["id"],
        "name": state["name"],
        "gender": state["gender"],
        "character": state["character"],
        "x": state["x"],
        "y": state["y"],
        "z": state["z"],
        "heading": state["heading"],
        "anim": state["anim"],
        "ragdoll": state["ragdoll"],
        "ragdollRotation": state["ragdollRotation"],
        "ragdollPose": state["ragdollPose"],
        "inVehicle": state["inVehicle"],
        "vehicleId": state["vehicleId"],
        "vehicleType": state["vehicleType"],
        "vehicleColor": state["vehicleColor"],
        "vehicleSpeed": state["vehicleSpeed"],
        "weapon": state.get("weapon", "fist"),
        "health": state.get("health", 100),
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
                "error": "Game file not found: index.html",
                "hint": "Keep index.html beside app.py in the deployed repository.",
            },
        )
    return FileResponse(GAME_PATH, media_type="text/html")


@app.get("/img.png")
async def title_image():
    if not IMAGE_PATH.is_file():
        return JSONResponse(status_code=404, content={"error": "img.png is not installed beside app.py"})
    return FileResponse(IMAGE_PATH, media_type="image/png")


@app.get("/healthz")
async def health():
    return {
        "ok": True,
        "game": "THE Z ONLINE",
        "players_online": len(clients),
        "rooms_active": 1 if rooms.get(WORLD_ROOM) else 0,
        "world_room": WORLD_ROOM,
        "world_host_connected": world_host_id in clients if world_host_id else False,
        "game_file_found": GAME_PATH.is_file(),
        "title_image_found": IMAGE_PATH.is_file(),
    }


@app.websocket("/ws")
async def multiplayer_socket(websocket: WebSocket):
    """Verify the entry mode before admitting a player into the one shared city."""
    global world_host_id, latest_world_state
    await websocket.accept()

    # The browser can report its fullscreen/orientation state; the server cannot
    # independently inspect a remote device's physical screen orientation.
    fullscreen_claim = websocket.query_params.get("fullscreen") == "1"
    landscape_claim = websocket.query_params.get("landscape") == "1"
    if not (fullscreen_claim and landscape_claim):
        await websocket.send_json({
            "type": "immersive_rejected",
            "message": "Fullscreen and landscape mode are required before joining.",
        })
        await websocket.close(code=4403, reason="Immersive mode required")
        return

    await websocket.send_json({"type": "immersive_verified", "room": WORLD_ROOM})
    player_id: str | None = None
    last_update = 0.0
    last_world_update = 0.0

    try:
        # A socket is not counted as a player until the avatar lobby is submitted.
        while True:
            raw = await websocket.receive_text()
            if len(raw) > 8192:
                await websocket.close(code=1009, reason="Join packet too large")
                return
            try:
                join_packet = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(join_packet, dict) and join_packet.get("type") == "ping":
                await websocket.send_json({"type": "pong"})
                continue
            if isinstance(join_packet, dict) and join_packet.get("type") == "join":
                break
            await websocket.send_json({"type": "join_rejected", "message": "A character profile is required."})
            await websocket.close(code=4400, reason="Join profile required")
            return

        room_members = rooms.setdefault(WORLD_ROOM, set())
        if len(room_members) >= MAX_PLAYERS:
            await websocket.send_json({"type": "server_full", "message": "The city is currently full."})
            await websocket.close(code=1013, reason="City is full")
            return

        player_id = uuid.uuid4().hex
        name = clean_name(join_packet.get("name"))
        gender = "f" if join_packet.get("gender") == "f" else "m"
        character = clean_character(join_packet.get("character"), gender)
        # Spread arrivals around the same central street so players do not
        # spawn directly inside one another.
        spawn_slot = len(room_members)
        spawn_radius = 2.6 + math.sqrt(spawn_slot) * 2.0
        spawn_angle = spawn_slot * 2.399963229728653
        spawn_x = -1100.0 + math.cos(spawn_angle) * spawn_radius
        spawn_z = 12.0 + math.sin(spawn_angle) * spawn_radius
        state: dict[str, Any] = {
            "id": player_id,
            "name": name,
            "gender": gender,
            "character": character,
            "x": spawn_x,
            "y": 0.0,
            "z": spawn_z,
            "heading": 0.0,
            "anim": "idle",
            "ragdoll": False,
            "ragdollRotation": {"x": 0.0, "y": 0.0, "z": 0.0},
            "ragdollPose": [],
            "inVehicle": False,
            "vehicleId": "",
            "vehicleType": "",
            "vehicleColor": "",
            "vehicleSpeed": 0.0,
        }
        clients[player_id] = {"websocket": websocket, "room": WORLD_ROOM, "state": state}
        room_members.add(player_id)
        if world_host_id not in clients:
            world_host_id = player_id

        previous_players = [
            public_state(clients[other_id]["state"])
            for other_id in list(room_members)
            if other_id != player_id and other_id in clients
        ]
        await websocket.send_json({
            "type": "welcome",
            "id": player_id,
            "room": WORLD_ROOM,
            "players": previous_players,
            "count": len(room_members),
            "world_host": world_host_id,
            "is_world_host": player_id == world_host_id,
            "spawn": {"x": spawn_x, "y": 0.0, "z": spawn_z},
            "world_state": latest_world_state,
        })
        await send_to_room(WORLD_ROOM, {"type": "player_join", "player": public_state(state)}, exclude=player_id)
        await send_to_room(WORLD_ROOM, {"type": "world_host", "id": world_host_id})

        while True:
            raw = await websocket.receive_text()
            if len(raw) > MAX_PACKET_LENGTH:
                await websocket.close(code=1009, reason="Message too large")
                break
            try:
                packet = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(packet, dict):
                continue

            packet_type = packet.get("type")
            now = time.monotonic()
            if packet_type == "ping":
                await websocket.send_json({"type": "pong"})
                continue

            if packet_type == "state":
                if now - last_update < MIN_STATE_INTERVAL:
                    continue
                last_update = now
                vehicle_type = str(packet.get("vehicleType", ""))
                in_vehicle = bool(packet.get("inVehicle")) and vehicle_type in ALLOWED_VEHICLES
                vehicle_color = str(packet.get("vehicleColor", ""))
                if not re.fullmatch(r"#[0-9a-fA-F]{6}", vehicle_color):
                    vehicle_color = "#3c8fc4"
                vehicle_id = str(packet.get("vehicleId", ""))[:64]
                if not re.fullmatch(r"[A-Za-z0-9_-]*", vehicle_id):
                    vehicle_id = ""
                if in_vehicle and vehicle_id:
                    occupied = any(
                        other_id != player_id
                        and other.get("state", {}).get("inVehicle")
                        and other.get("state", {}).get("vehicleId") == vehicle_id
                        for other_id, other in clients.items()
                    )
                    if occupied:
                        await websocket.send_json({"type": "vehicle_claim_denied", "vehicleId": vehicle_id})
                        in_vehicle = False
                        vehicle_id = ""
                        vehicle_type = ""
                rotation = packet.get("ragdollRotation") if isinstance(packet.get("ragdollRotation"), dict) else {}
                pose_raw = packet.get("ragdollPose")
                if isinstance(pose_raw, list) and len(pose_raw) == 77:
                    pose = [safe_number(v, 0.0, -6000.0, 6000.0) if i % 7 < 3 else safe_number(v, 0.0, -1.1, 1.1) for i, v in enumerate(pose_raw)]
                else:
                    pose = []
                state = {
                    "id": player_id,
                    "name": name,
                    "gender": gender,
                    "character": character,
                    "x": safe_number(packet.get("x"), -1100.0, -5000.0, 5000.0),
                    "y": safe_number(packet.get("y"), 0.0, -80.0, 800.0),
                    "z": safe_number(packet.get("z"), 12.0, -5000.0, 5000.0),
                    "heading": safe_number(packet.get("heading"), 0.0, -100.0, 100.0),
                    "anim": packet.get("anim") if isinstance(packet.get("anim"), str) and packet.get("anim") in ALLOWED_ANIMATIONS else "idle",
                    "ragdoll": bool(packet.get("ragdoll")),
                    "ragdollRotation": {
                        "x": safe_number(rotation.get("x"), 0.0, -6.3, 6.3),
                        "y": safe_number(rotation.get("y"), 0.0, -6.3, 6.3),
                        "z": safe_number(rotation.get("z"), 0.0, -6.3, 6.3),
                    },
                    "ragdollPose": pose,
                    "inVehicle": in_vehicle,
                    "vehicleId": vehicle_id if in_vehicle else "",
                    "vehicleType": vehicle_type if in_vehicle else "",
                    "vehicleColor": vehicle_color if in_vehicle else "",
                    "vehicleSpeed": safe_number(packet.get("vehicleSpeed"), 0.0, -110.0, 110.0) if in_vehicle else 0.0,
                    "weapon": packet.get("weapon") if isinstance(packet.get("weapon"), str) and packet.get("weapon") in ALLOWED_WEAPONS else "fist",
                    "health": safe_number(packet.get("health"), 100.0, 0.0, 100.0),
                }
                clients[player_id]["state"] = state
                await send_to_room(WORLD_ROOM, {"type": "player_update", "player": public_state(state)}, exclude=player_id)
                continue

            if packet_type in ("combat", "weapon", "combat_sound"):
                # Relay combat / weapon / synced sound to everyone else - no trust in damage, clients apply locally.
                weapon = str(packet.get("weapon", "fist"))[:20]
                if weapon not in ALLOWED_WEAPONS:
                    weapon = "fist"
                payload = {
                    "type": packet_type,
                    "from": player_id,
                    "action": str(packet.get("action", "shot"))[:20] if packet_type == "combat" else None,
                    "weapon": weapon,
                    "x": safe_number(packet.get("x"), 0.0, -5000.0, 5000.0),
                    "y": safe_number(packet.get("y"), 0.0, -80.0, 800.0),
                    "z": safe_number(packet.get("z"), 0.0, -5000.0, 5000.0),
                    "heading": safe_number(packet.get("heading"), 0.0, -100.0, 100.0),
                    "damage": safe_number(packet.get("damage"), 0.0, 0.0, 200.0),
                }
                await send_to_room(WORLD_ROOM, payload, exclude=player_id)
                continue

            if packet_type == "world_state":
                # Only the elected world host may publish synchronized ambient AI.
                if player_id != world_host_id or now - last_world_update < MIN_WORLD_INTERVAL:
                    continue
                snapshot = packet.get("state")
                if not isinstance(snapshot, dict):
                    continue
                peds = snapshot.get("peds")
                cars = snapshot.get("cars")
                if not isinstance(peds, str) or not isinstance(cars, str):
                    continue
                if len(peds) > 100_000 or len(cars) > 100_000:
                    continue
                if not re.fullmatch(r"[A-Za-z0-9+/=]*", peds) or not re.fullmatch(r"[A-Za-z0-9+/=]*", cars):
                    continue
                ped_count = int(safe_number(snapshot.get("pedCount"), 0, 0, 3000))
                car_count = int(safe_number(snapshot.get("carCount"), 0, 0, 2000))
                if ped_count < 1 or car_count < 1:
                    continue
                latest_world_state = {
                    "peds": peds,
                    "cars": cars,
                    "pedCount": ped_count,
                    "carCount": car_count,
                    "at": int(time.time() * 1000),
                }
                last_world_update = now
                await send_to_room(WORLD_ROOM, {"type": "world_state", "state": latest_world_state}, exclude=player_id)

    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("WebSocket session ended unexpectedly for %s", player_id or "pending player")
    finally:
        if player_id:
            clients.pop(player_id, None)
            members = rooms.get(WORLD_ROOM)
            if members is not None:
                members.discard(player_id)
            await send_to_room(WORLD_ROOM, {"type": "player_leave", "id": player_id})
            if world_host_id == player_id:
                remaining = [pid for pid in rooms.get(WORLD_ROOM, set()) if pid in clients]
                world_host_id = remaining[0] if remaining else None
                await send_to_room(WORLD_ROOM, {"type": "world_host", "id": world_host_id})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=PORT, reload=False)
