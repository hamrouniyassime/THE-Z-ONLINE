"""THE Z ONLINE multiplayer relay/server.

Protocol v3 synchronizes player avatars, vehicle occupancy, player ragdolls,
shared pedestrian/traffic snapshots, and durable world events (damage,
explosions, object state). The simulation is still client-simulated: this
server validates and relays state; it is not an authoritative physics engine.
Run one server process/worker unless a shared Redis/pub-sub backend is added.
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
from collections import deque
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse

BASE_DIR = Path(__file__).resolve().parent
GAME_PATH = BASE_DIR / "index.html"
IMAGE_PATH = BASE_DIR / "img.png"
WORLD_MAP_PATH = BASE_DIR / "world-map.webp"
PORT = int(os.environ.get("PORT", "8000"))
MAX_PLAYERS = max(2, min(128, int(os.environ.get("MAX_PLAYERS", "64"))))
MAX_NAME_LENGTH = 18
MAX_PACKET_LENGTH = 524_288
MIN_STATE_INTERVAL = 0.045
MIN_WORLD_INTERVAL = 0.35
MIN_EVENT_INTERVAL = 0.025
MAX_EVENTS_PER_SECOND = 36
MAX_WORLD_STRING = 180_000
MAX_WORLD_META_BYTES = 120_000
MAX_STORED_ENTITIES = 1_500
WORLD_ROOM = "the-z-online"
PROTOCOL_VERSION = 3
WORLD_ID = "the-z-online-shared-city-v3"
MAP_VERSION = "the-z-city-map-v3"
MAP_SEED = 7

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger("the-z-online")
app = FastAPI(title="THE Z ONLINE", version="3.0.0")

# player_id -> {websocket, room, state, send_lock, event_times, last_*}
clients: dict[str, dict[str, Any]] = {}
rooms: dict[str, set[str]] = {WORLD_ROOM: set()}
world_host_id: str | None = None
latest_world_state: dict[str, Any] | None = None
world_revision = 0
world_sequence = 0
world_entities: dict[str, dict[str, Any]] = {}
world_event_history: deque[dict[str, Any]] = deque(maxlen=240)

ALLOWED_VEHICLES = {
    "sedan", "taxi", "sport", "police", "van", "pickup", "truck",
    "motorcycle", "gangBike", "bicycle", "jetski", "fighter",
    "helicopter", "airliner", "seaplane", "boat", "ship",
}
ALLOWED_ANIMATIONS = {
    "idle", "walk", "run", "wave", "dance", "sit", "jump", "jumpTuck",
    "jumpLong", "jumpSide", "land", "swim", "treadWater", "drive",
    "rideBike", "rideChopper", "rideBicycle", "fall", "fallBack",
    "getupBack", "hit", "stumble", "push", "punch", "punchL", "kick",
    "enter", "exit",
}
ALLOWED_HAIR = {"short", "long", "pony", "spiky", "buzz", "bald"}
CHARACTER_COLORS = {"hc", "skin", "eye", "shirt", "pants", "shoe"}
ALLOWED_WORLD_EVENTS = {
    "vehicle_state", "vehicle_damage", "vehicle_explosion", "vehicle_removed",
    "pedestrian_state", "pedestrian_hit", "pedestrian_ragdoll",
    "world_effect", "map_object_state", "world_interaction",
}
PERSISTENT_EVENT_KINDS = {
    "vehicle_state", "vehicle_damage", "vehicle_explosion", "vehicle_removed",
    "map_object_state",
}
OBJECT_ID_RE = re.compile(r"[A-Za-z0-9_:\-.]{1,96}")


def clean_name(value: Any) -> str:
    value = str(value or "Player").strip()
    value = "".join(ch for ch in value if unicodedata.category(ch) != "Cc")
    return value[:MAX_NAME_LENGTH].strip() or "Player"


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
    hair = source.get("hair")
    result: dict[str, Any] = {
        "hair": hair if isinstance(hair, str) and hair in ALLOWED_HAIR else ("long" if gender == "f" else "short"),
        "hc": "#211711", "skin": "#e8b890", "eye": "#3a2a1e",
        "shirt": "#e8e8ee", "pants": "#24395e", "shoe": "#272b38",
        "beard": False, "glasses": False, "skirt": False,
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


def clean_ragdoll_pose(value: Any) -> list[float]:
    if not isinstance(value, list) or len(value) != 77:
        return []
    return [
        safe_number(v, 0.0, -6000.0, 6000.0) if i % 7 < 3
        else safe_number(v, 0.0, -1.1, 1.1)
        for i, v in enumerate(value)
    ]


def public_state(state: dict[str, Any]) -> dict[str, Any]:
    # Keep a fresh object so callers cannot mutate the server's stored state.
    return {
        "id": state["id"], "name": state["name"],
        "gender": state["gender"], "character": dict(state["character"]),
        "x": state["x"], "y": state["y"], "z": state["z"],
        "heading": state["heading"], "anim": state["anim"],
        "ragdoll": state["ragdoll"],
        "ragdollRotation": dict(state["ragdollRotation"]),
        "ragdollPose": list(state["ragdollPose"]),
        "inVehicle": state["inVehicle"], "vehicleId": state["vehicleId"],
        "vehicleType": state["vehicleType"], "vehicleColor": state["vehicleColor"],
        "vehicleSpeed": state["vehicleSpeed"], "updatedAt": state.get("updatedAt", 0),
    }


def clean_json_value(value: Any, depth: int = 0) -> Any:
    """Bound and sanitize event data to JSON primitives only."""
    if depth > 3:
        return None
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return safe_number(value, 0.0, -100_000_000.0, 100_000_000.0)
    if isinstance(value, str):
        return "".join(ch for ch in value if unicodedata.category(ch) != "Cc")[:256]
    if isinstance(value, list):
        return [clean_json_value(v, depth + 1) for v in value[:100]]
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for key, item in list(value.items())[:40]:
            key = str(key)
            if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,39}", key):
                cleaned[key] = clean_json_value(item, depth + 1)
        return cleaned
    return None


def clean_ped_meta(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    output: list[dict[str, Any]] = []
    for item in value[:3000]:
        if not isinstance(item, dict):
            continue
        index = int(safe_number(item.get("i"), -1, -1, 2999))
        if index < 0:
            continue
        row: dict[str, Any] = {"i": index}
        pose = item.get("pose")
        if isinstance(pose, list) and len(pose) == 77:
            row["pose"] = clean_ragdoll_pose(pose)
        output.append(row)
    try:
        if len(json.dumps(output, separators=(",", ":"))) > MAX_WORLD_META_BYTES:
            return []
    except (TypeError, ValueError):
        return []
    return output


def clean_transient_peds(value: Any) -> list[dict[str, Any]]:
    """Validate temporary people (for example, drivers leaving vehicles)."""
    if not isinstance(value, list):
        return []
    output: list[dict[str, Any]] = []
    valid_reactions = {"ragdoll", "fall", "shove", "hitFace", "stumble"}
    valid_actions = {"yank", "fall", "shove", "flee", "exit"}
    for item in value[:180]:
        if not isinstance(item, dict):
            continue
        person_id = str(item.get("id", ""))[:96]
        if not OBJECT_ID_RE.fullmatch(person_id):
            continue
        gender = "f" if item.get("gender") == "f" else "m"
        row: dict[str, Any] = {
            "id": person_id,
            "gender": gender,
            "cfg": clean_character(item.get("cfg"), gender),
            "x": safe_number(item.get("x"), 0.0, -5300.0, 5300.0),
            "z": safe_number(item.get("z"), 0.0, -5300.0, 5300.0),
            "heading": safe_number(item.get("heading"), 0.0, -100.0, 100.0),
            "scale": safe_number(item.get("scale"), 0.84, 0.45, 1.4),
            "idx": int(safe_number(item.get("idx"), 0, 0, 1000)),
            "phase": safe_number(item.get("phase"), 0.0, -10000.0, 10000.0),
            "speed": safe_number(item.get("speed"), 1.0, 0.0, 12.0),
            "pause": safe_number(item.get("pause"), 0.0, 0.0, 120.0),
            "gesture": str(item.get("gesture", "walk"))[:24],
            "hitCooldown": safe_number(item.get("hitCooldown"), 0.0, 0.0, 10.0),
        }
        driver_exit = item.get("driverExit")
        if isinstance(driver_exit, dict):
            action = str(driver_exit.get("action", "exit"))
            row["driverExit"] = {
                "sx": safe_number(driver_exit.get("sx"), row["x"], -5300.0, 5300.0),
                "sz": safe_number(driver_exit.get("sz"), row["z"], -5300.0, 5300.0),
                "ex": safe_number(driver_exit.get("ex"), row["x"], -5300.0, 5300.0),
                "ez": safe_number(driver_exit.get("ez"), row["z"], -5300.0, 5300.0),
                "t": safe_number(driver_exit.get("t"), 0.0, 0.0, 30.0),
                "duration": safe_number(driver_exit.get("duration"), 1.0, 0.1, 30.0),
                "heading": safe_number(driver_exit.get("heading"), row["heading"], -100.0, 100.0),
                "action": action if action in valid_actions else "exit",
            }
        else:
            row["driverExit"] = None
        reaction = item.get("reaction")
        if isinstance(reaction, dict) and reaction.get("type") in valid_reactions:
            row["reaction"] = {
                "type": reaction["type"],
                "phase": "getup" if reaction.get("phase") == "getup" else "down",
                "t": safe_number(reaction.get("t"), 0.0, 0.0, 30.0),
                "vx": safe_number(reaction.get("vx"), 0.0, -30.0, 30.0),
                "vz": safe_number(reaction.get("vz"), 0.0, -30.0, 30.0),
                "back": bool(reaction.get("back")),
                "anger": bool(reaction.get("anger")),
            }
        else:
            row["reaction"] = None
        for key in ("flee", "angry"):
            status = item.get(key)
            row[key] = {
                "t": safe_number(status.get("t"), 0.0, 0.0, 30.0),
                "duration": safe_number(status.get("duration"), 0.0, 0.0, 30.0),
            } if isinstance(status, dict) else None
        pose = item.get("ragdollPose")
        row["ragdollPose"] = clean_ragdoll_pose(pose)
        output.append(row)
    try:
        if len(json.dumps(output, separators=(",", ":"))) > MAX_WORLD_META_BYTES:
            return []
    except (TypeError, ValueError):
        return []
    return output


def clean_dynamic_cars(value: Any) -> list[dict[str, Any]]:
    """Validate dynamically spawned traffic such as police response vehicles."""
    if not isinstance(value, list):
        return []
    output: list[dict[str, Any]] = []
    for item in value[:300]:
        if not isinstance(item, dict):
            continue
        car_id = str(item.get("id", ""))[:96]
        car_type = str(item.get("type", ""))
        color = str(item.get("color", "#3c8fc4"))
        if not OBJECT_ID_RE.fullmatch(car_id) or car_type not in ALLOWED_VEHICLES:
            continue
        if not re.fullmatch(r"#[0-9a-fA-F]{6}", color):
            color = "#3c8fc4"
        output.append({
            "id": car_id, "type": car_type, "color": color.lower(),
            "x": safe_number(item.get("x"), 0.0, -5300.0, 5300.0),
            "y": safe_number(item.get("y"), 0.0, -80.0, 800.0),
            "z": safe_number(item.get("z"), 0.0, -5300.0, 5300.0),
            "heading": safe_number(item.get("heading"), 0.0, -100.0, 100.0),
            "speed": safe_number(item.get("speed"), 0.0, -110.0, 110.0),
            "damage": safe_number(item.get("damage"), 0.0, 0.0, 7.0),
            "exploded": bool(item.get("exploded")),
            "disabled": bool(item.get("disabled")),
            "claimed": bool(item.get("claimed")),
            "retired": bool(item.get("retired")),
            "parked": bool(item.get("parked")),
            "policeChase": bool(item.get("policeChase")),
            "policeTargetId": str(item.get("policeTargetId", ""))[:64],
        })
    return output


async def send_client(player_id: str, payload: dict[str, Any]) -> bool:
    entry = clients.get(player_id)
    if entry is None:
        return False
    try:
        async with entry["send_lock"]:
            await entry["websocket"].send_json(payload)
        return True
    except Exception as exc:
        log.debug("Could not send to player %s: %s", player_id, exc)
        return False


async def send_to_room(room: str, payload: dict[str, Any], exclude: str | None = None) -> None:
    player_ids = [pid for pid in list(rooms.get(room, set())) if pid != exclude]
    if player_ids:
        await asyncio.gather(*(send_client(pid, payload) for pid in player_ids), return_exceptions=True)


def clean_initial_state(player_id: str, name: str, gender: str, character: dict[str, Any], x: float, z: float) -> dict[str, Any]:
    return {
        "id": player_id, "name": name, "gender": gender, "character": character,
        "x": x, "y": 0.0, "z": z, "heading": 0.0, "anim": "idle",
        "ragdoll": False,
        "ragdollRotation": {"x": 0.0, "y": 0.0, "z": 0.0},
        "ragdollPose": [], "inVehicle": False, "vehicleId": "",
        "vehicleType": "", "vehicleColor": "", "vehicleSpeed": 0.0,
        "updatedAt": int(time.time() * 1000),
    }


@app.get("/")
async def home():
    if not GAME_PATH.is_file():
        return JSONResponse(status_code=404, content={
            "error": "Game file not found: index.html",
            "hint": "Keep index.html beside app.py in the deployed repository.",
        })
    return FileResponse(GAME_PATH, media_type="text/html", headers={"Cache-Control": "no-store"})


@app.get("/img.png")
async def title_image():
    if not IMAGE_PATH.is_file():
        return JSONResponse(status_code=404, content={"error": "img.png is not installed beside app.py"})
    return FileResponse(IMAGE_PATH, media_type="image/png")


@app.get("/world-map.webp")
async def world_map_image():
    if not WORLD_MAP_PATH.is_file():
        return JSONResponse(status_code=404, content={"error": "world-map.webp is missing beside app.py"})
    return FileResponse(WORLD_MAP_PATH, media_type="image/webp", headers={"Cache-Control": "public, max-age=86400"})


@app.get("/healthz")
async def health():
    return {
        "ok": True, "game": "THE Z ONLINE", "protocol_version": PROTOCOL_VERSION,
        "players_online": len(clients), "max_players": MAX_PLAYERS,
        "rooms_active": 1 if rooms.get(WORLD_ROOM) else 0,
        "world_room": WORLD_ROOM,
        "world_host_connected": world_host_id in clients if world_host_id else False,
        "world_revision": world_revision, "world_events_cached": len(world_event_history),
        "world_entities_cached": len(world_entities),
        "game_file_found": GAME_PATH.is_file(),
        "title_image_found": IMAGE_PATH.is_file(),
        "world_map_found": WORLD_MAP_PATH.is_file(),
    }


@app.websocket("/ws")
async def multiplayer_socket(websocket: WebSocket):
    """Join the shared world and relay synchronized player/world state."""
    global world_host_id, latest_world_state, world_revision, world_sequence
    await websocket.accept()

    fullscreen_claim = websocket.query_params.get("fullscreen") == "1"
    landscape_claim = websocket.query_params.get("landscape") == "1"
    if not (fullscreen_claim and landscape_claim):
        await websocket.send_json({
            "type": "immersive_rejected",
            "message": "Fullscreen and landscape mode are required before joining.",
        })
        await websocket.close(code=4403, reason="Immersive mode required")
        return

    await websocket.send_json({"type": "immersive_verified", "room": WORLD_ROOM, "protocol": PROTOCOL_VERSION})
    player_id: str | None = None
    last_update = 0.0
    last_world_update = 0.0
    event_times: deque[float] = deque()

    try:
        # A socket is not counted until the player submits a character profile.
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
                await websocket.send_json({"type": "pong", "server_time": int(time.time() * 1000)})
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
        # Golden-angle spawn placement avoids stacking players in the arrival plaza.
        spawn_slot = len(room_members)
        spawn_radius = 4.0 + math.sqrt(spawn_slot) * 2.7
        spawn_angle = spawn_slot * 2.399963229728653
        spawn_x = 100.0 + math.cos(spawn_angle) * spawn_radius
        spawn_z = 1900.0 + math.sin(spawn_angle) * spawn_radius
        state = clean_initial_state(player_id, name, gender, character, spawn_x, spawn_z)
        clients[player_id] = {
            "websocket": websocket, "room": WORLD_ROOM, "state": state,
            "send_lock": asyncio.Lock(), "event_times": event_times,
        }
        room_members.add(player_id)
        if world_host_id not in clients:
            world_host_id = player_id

        previous_players = [
            public_state(clients[other_id]["state"])
            for other_id in list(room_members)
            if other_id != player_id and other_id in clients
        ]
        welcome = {
            "type": "welcome", "protocol": PROTOCOL_VERSION,
            "world_meta": {"id": WORLD_ID, "room": WORLD_ROOM, "map_version": MAP_VERSION, "seed": MAP_SEED},
            "id": player_id, "room": WORLD_ROOM, "players": previous_players,
            "count": len(room_members), "world_host": world_host_id,
            "is_world_host": player_id == world_host_id,
            "spawn": {"x": spawn_x, "y": 0.0, "z": spawn_z},
            "world_state": latest_world_state,
            "world_revision": world_revision,
            "world_entities": list(world_entities.values()),
            "world_events": list(world_event_history),
        }
        await send_client(player_id, welcome)
        await send_to_room(WORLD_ROOM, {"type": "player_join", "player": public_state(state)}, exclude=player_id)
        await send_to_room(WORLD_ROOM, {"type": "world_host", "id": world_host_id, "revision": world_revision})
        await send_to_room(WORLD_ROOM, {"type": "server_status", "players_online": len(room_members)}, exclude=player_id)

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
                await send_client(player_id, {"type": "pong", "server_time": int(time.time() * 1000)})
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
                vehicle_id = str(packet.get("vehicleId", ""))[:96]
                if not OBJECT_ID_RE.fullmatch(vehicle_id):
                    vehicle_id = ""
                if in_vehicle and not vehicle_id:
                    in_vehicle = False
                    vehicle_type = ""
                if in_vehicle and vehicle_id:
                    occupied = any(
                        other_id != player_id and other.get("state", {}).get("inVehicle")
                        and other.get("state", {}).get("vehicleId") == vehicle_id
                        for other_id, other in clients.items()
                    )
                    if occupied:
                        await send_client(player_id, {"type": "vehicle_claim_denied", "vehicleId": vehicle_id})
                        in_vehicle = False
                        vehicle_id = ""
                        vehicle_type = ""
                rotation = packet.get("ragdollRotation") if isinstance(packet.get("ragdollRotation"), dict) else {}
                pose = clean_ragdoll_pose(packet.get("ragdollPose"))
                # Profile identity remains anchored to the validated lobby profile.
                state = {
                    "id": player_id, "name": name, "gender": gender, "character": character,
                    "x": safe_number(packet.get("x"), spawn_x, -5300.0, 5300.0),
                    "y": safe_number(packet.get("y"), 0.0, -80.0, 800.0),
                    "z": safe_number(packet.get("z"), spawn_z, -5300.0, 5300.0),
                    "heading": safe_number(packet.get("heading"), 0.0, -100.0, 100.0),
                    "anim": packet.get("anim") if isinstance(packet.get("anim"), str) and packet.get("anim") in ALLOWED_ANIMATIONS else "idle",
                    "ragdoll": bool(packet.get("ragdoll")),
                    "ragdollRotation": {
                        "x": safe_number(rotation.get("x"), 0.0, -6.3, 6.3),
                        "y": safe_number(rotation.get("y"), 0.0, -6.3, 6.3),
                        "z": safe_number(rotation.get("z"), 0.0, -6.3, 6.3),
                    },
                    "ragdollPose": pose,
                    "inVehicle": in_vehicle, "vehicleId": vehicle_id if in_vehicle else "",
                    "vehicleType": vehicle_type if in_vehicle else "",
                    "vehicleColor": vehicle_color if in_vehicle else "",
                    "vehicleSpeed": safe_number(packet.get("vehicleSpeed"), 0.0, -110.0, 110.0) if in_vehicle else 0.0,
                    "updatedAt": int(time.time() * 1000),
                }
                clients[player_id]["state"] = state
                await send_to_room(WORLD_ROOM, {"type": "player_update", "player": public_state(state)}, exclude=player_id)
                continue

            if packet_type == "world_state":
                # One elected host publishes deterministic ambient NPC + traffic snapshots.
                if player_id != world_host_id or now - last_world_update < MIN_WORLD_INTERVAL:
                    continue
                snapshot = packet.get("state")
                if not isinstance(snapshot, dict):
                    continue
                peds = snapshot.get("peds")
                cars = snapshot.get("cars")
                if not isinstance(peds, str) or not isinstance(cars, str):
                    continue
                if len(peds) > MAX_WORLD_STRING or len(cars) > MAX_WORLD_STRING:
                    continue
                if not re.fullmatch(r"[A-Za-z0-9+/=]*", peds) or not re.fullmatch(r"[A-Za-z0-9+/=]*", cars):
                    continue
                ped_count = int(safe_number(snapshot.get("pedCount"), 0, 0, 3000))
                car_count = int(safe_number(snapshot.get("carCount"), 0, 0, 2000))
                ped_stride = int(safe_number(snapshot.get("pedStride"), 7, 7, 15))
                car_stride = int(safe_number(snapshot.get("carStride"), 9, 9, 13))
                if ped_count < 1 or car_count < 1 or ped_stride not in (7, 15) or car_stride not in (9, 13):
                    continue
                ped_meta = clean_ped_meta(snapshot.get("pedMeta"))
                transient_peds = clean_transient_peds(snapshot.get("transientPeds"))
                dynamic_cars = clean_dynamic_cars(snapshot.get("dynamicCars"))
                latest_world_state = {
                    "version": PROTOCOL_VERSION, "peds": peds, "cars": cars,
                    "pedCount": ped_count, "carCount": car_count,
                    "pedStride": ped_stride, "carStride": car_stride,
                    "pedMeta": ped_meta, "transientPeds": transient_peds,
                    "dynamicCars": dynamic_cars,
                    "worldTime": safe_number(snapshot.get("worldTime"), 0.0, 0.0, 1_000_000_000.0),
                    "at": int(time.time() * 1000),
                }
                world_revision += 1
                latest_world_state["revision"] = world_revision
                last_world_update = now
                await send_to_room(WORLD_ROOM, {"type": "world_state", "state": latest_world_state}, exclude=player_id)
                continue

            if packet_type == "world_event":
                # Short-lived effects and persistent object changes are relayed to everyone.
                while event_times and now - event_times[0] > 1.0:
                    event_times.popleft()
                if len(event_times) >= MAX_EVENTS_PER_SECOND or event_times and now - event_times[-1] < MIN_EVENT_INTERVAL:
                    continue
                event_times.append(now)
                kind = packet.get("kind")
                if not isinstance(kind, str) or kind not in ALLOWED_WORLD_EVENTS:
                    continue
                object_id = str(packet.get("objectId", ""))[:96]
                if not OBJECT_ID_RE.fullmatch(object_id):
                    object_id = ""
                data = clean_json_value(packet.get("data", {}))
                if not isinstance(data, dict):
                    data = {}
                if len(json.dumps(data, separators=(",", ":"))) > 6_000:
                    continue
                world_sequence += 1
                event = {
                    "type": "world_event", "seq": world_sequence,
                    "eventId": f"{world_sequence}-{uuid.uuid4().hex[:10]}",
                    "actor": player_id, "kind": kind, "objectId": object_id,
                    "data": data, "at": int(time.time() * 1000),
                }
                if kind in PERSISTENT_EVENT_KINDS and object_id:
                    entity_key = f"{kind}:{object_id}"
                    world_entities[entity_key] = {
                        "kind": kind, "objectId": object_id,
                        "data": data, "seq": world_sequence,
                        "at": event["at"],
                    }
                    if len(world_entities) > MAX_STORED_ENTITIES:
                        oldest_key = min(world_entities, key=lambda k: world_entities[k].get("seq", 0))
                        world_entities.pop(oldest_key, None)
                world_event_history.append(event)
                await send_to_room(WORLD_ROOM, event, exclude=player_id)
                continue

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
                await send_to_room(WORLD_ROOM, {
                    "type": "world_host", "id": world_host_id,
                    "revision": world_revision,
                })
            await send_to_room(WORLD_ROOM, {"type": "server_status", "players_online": len(rooms.get(WORLD_ROOM, set()))})


if __name__ == "__main__":
    import uvicorn

    # In-memory room state is shared inside one process only.
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, reload=False, workers=1)
