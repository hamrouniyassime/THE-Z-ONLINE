"""THE Z ONLINE multiplayer relay/server.

Protocol v6 synchronizes player avatars, vehicle occupancy, player ragdolls,
the visible city traffic/crowd layer, shared world snapshots, durable world events, account profiles, and SQLite-backed world persistence.
The elected host simulates ambient traffic and NPCs; this
server validates and relays state rather than running a physics engine.
Run one server process/worker unless a shared Redis/pub-sub backend is added.
"""
from __future__ import annotations

import array
import asyncio
import base64
import json
import logging
import hashlib
import hmac
import secrets
import sqlite3
import struct
import threading
import math
import os
import random
import re
import sys
import time
import unicodedata
import uuid
from collections import deque
from contextlib import closing
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
PROTOCOL_VERSION = 6
WORLD_ID = "the-z-online-shared-city-v7-northwest-military-summit"
MAP_VERSION = "the-z-city-map-v7-northwest-military-summit"
MAP_SEED = 7

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger("the-z-online")
app = FastAPI(title="THE Z ONLINE", version="6.0.0")

# player_id -> {websocket, room, state, send_lock, event_times, last_*}
clients: dict[str, dict[str, Any]] = {}
rooms: dict[str, set[str]] = {WORLD_ROOM: set()}
world_host_id: str | None = None
world_map_fingerprint: str | None = None
latest_world_state: dict[str, Any] | None = None
world_revision = 0
world_sequence = 0
world_entities: dict[str, dict[str, Any]] = {}
world_event_history: deque[dict[str, Any]] = deque(maxlen=240)
# Server-authoritative simulation of the street cars and walking crowd.
# Bootstrapped once from the first client's snapshot, then stepped on the server.
world_sim: dict[str, Any] | None = None
SIM_TICK = 0.1            # simulation step (seconds)
SIM_BROADCAST = 1.0       # snapshots are pushed to every player once per second
SIM_VX = (-450.0, 50.0, 300.0, 750.0)
SIM_HZ = (-330.0, 330.0)

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



def clean_life_ped_meta(value: Any) -> list[dict[str, Any]]:
    """Compact articulated ragdoll poses for the visible street crowd."""
    if not isinstance(value, list):
        return []
    output: list[dict[str, Any]] = []
    seen: set[int] = set()
    for item in value[:80]:
        if not isinstance(item, dict):
            continue
        index = int(safe_number(item.get("i"), -1, -1, 2999))
        pose = clean_ragdoll_pose(item.get("pose"))
        if index < 0 or index in seen or not pose:
            continue
        seen.add(index)
        output.append({"i": index, "pose": pose})
    try:
        if len(json.dumps(output, separators=(",", ":"))) > MAX_WORLD_META_BYTES:
            return []
    except (TypeError, ValueError):
        return []
    return output


def clean_game_data(value: Any) -> dict[str, Any]:
    """Whitelist the small user-owned game data persisted per account."""
    source = value if isinstance(value, dict) else {}
    def string_list(key: str, limit: int) -> list[str]:
        items = source.get(key)
        if not isinstance(items, list):
            return []
        return [str(v)[:80] for v in items[:limit] if isinstance(v, (str, int, float))]
    job = source.get("activeJob")
    clean_job = None
    if isinstance(job, dict):
        clean_job = {
            "name": str(job.get("name", ""))[:80],
            "targetId": str(job.get("targetId", ""))[:96],
            "reward": safe_number(job.get("reward"), 0, 0, 1_000_000),
            "x": safe_number(job.get("x"), 0, -5300, 5300),
            "z": safe_number(job.get("z"), 0, -5300, 5300),
        }
    return {
        "wallet": safe_number(source.get("wallet"), 2500, 0, 1_000_000_000),
        "bank": safe_number(source.get("bank"), 500, 0, 1_000_000_000),
        "owned": string_list("owned", 120),
        "inventory": string_list("inventory", 220),
        "phoneOwned": bool(source.get("phoneOwned")),
        "equippedUtility": str(source.get("equippedUtility", "كاميرا رقمية"))[:80],
        "activeJob": clean_job,
        "quality": str(source.get("quality", "simple"))[:16] if str(source.get("quality", "simple")) in {"simple", "medium", "high"} else "simple",
        "soundEnabled": bool(source.get("soundEnabled", True)),
        "soundVolume": safe_number(source.get("soundVolume"), 65, 0, 100),
        "poiVisible": bool(source.get("poiVisible", True)),
        "gpsTarget": {"x": safe_number(source.get("gpsTarget", {}).get("x"), 0, -32000, 32000), "z": safe_number(source.get("gpsTarget", {}).get("z"), 0, -32000, 32000)} if isinstance(source.get("gpsTarget"), dict) else None,
        "health": safe_number(source.get("health"), 100, 0, 100),
        "chestClaimed": bool(source.get("chestClaimed")),
        "summitReached": bool(source.get("summitReached")),
        "weaponSlot": int(safe_number(source.get("weaponSlot"), 0, 0, 2)),
        "summitSigns": [
            {"name": str(row.get("name", "Climber"))[:22], "message": str(row.get("message", "Reached the summit"))[:46], "flag": str(row.get("flag", "🏳️"))[:8]}
            for row in source.get("summitSigns", [])[-30:] if isinstance(row, dict)
        ] if isinstance(source.get("summitSigns"), list) else [],
    }


def clean_saved_player_state(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    in_vehicle = bool(value.get("inVehicle"))
    vehicle_id = str(value.get("vehicleId", ""))[:96]
    if not OBJECT_ID_RE.fullmatch(vehicle_id):
        vehicle_id = ""
        in_vehicle = False
    vehicle_type = str(value.get("vehicleType", ""))
    if vehicle_type not in ALLOWED_VEHICLES:
        vehicle_type = ""
        in_vehicle = False
    color = str(value.get("vehicleColor", ""))
    if not re.fullmatch(r"#[0-9a-fA-F]{6}", color):
        color = ""
    return {
        "x": safe_number(value.get("x"), 314, -32000, 32000),
        "y": safe_number(value.get("y"), 0, -80, 3500),
        "z": safe_number(value.get("z"), 60, -32000, 32000),
        "heading": safe_number(value.get("heading"), 0, -100, 100),
        "inVehicle": in_vehicle,
        "vehicleId": vehicle_id if in_vehicle else "",
        "vehicleType": vehicle_type if in_vehicle else "",
        "vehicleColor": color.lower() if in_vehicle else "",
        "vehicleSpeed": safe_number(value.get("vehicleSpeed"), 0, -110, 110) if in_vehicle else 0,
    }


# Hugging Face Space local disk is ephemeral. Persistent accounts/world state must
# live on a read-write attached Storage Bucket (normally mounted at /data).
_DB_EXPLICIT = os.environ.get("THEZ_DB_PATH", "").strip()
_DATA_MOUNT = Path(os.environ.get("THEZ_DATA_DIR", "/data"))
if _DB_EXPLICIT:
    DB_PATH = Path(_DB_EXPLICIT)
    PERSISTENT_STORAGE_ENABLED = not str(DB_PATH).startswith(("/tmp/", "/var/tmp/"))
elif _DATA_MOUNT.is_dir() and os.path.ismount(str(_DATA_MOUNT)) and os.access(_DATA_MOUNT, os.W_OK):
    DB_PATH = _DATA_MOUNT / "thez_online.sqlite3"
    PERSISTENT_STORAGE_ENABLED = True
else:
    DB_PATH = Path("/tmp/thez_online.sqlite3")
    PERSISTENT_STORAGE_ENABLED = False
_DB_LOCK = threading.RLock()
_WORLD_DB_SAVE_CLOCK = 0.0


def _db_connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=FULL")
    # DELETE journaling is more portable across mounted persistent volumes than WAL.
    conn.execute("PRAGMA journal_mode=DELETE")
    return conn


def _db_init() -> None:
    with _DB_LOCK, closing(_db_connect()) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS accounts (username TEXT PRIMARY KEY, salt BLOB NOT NULL, password_hash BLOB NOT NULL, profile_json TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)")
        conn.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute("CREATE TABLE IF NOT EXISTS entities (entity_key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.commit()


def _db_get(key: str) -> Any:
    with _DB_LOCK, closing(_db_connect()) as conn:
        row = conn.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        if not row:
            return None
        try:
            return json.loads(row["value"])
        except (TypeError, json.JSONDecodeError):
            return None


def _db_set(key: str, value: Any) -> None:
    payload = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    with _DB_LOCK, closing(_db_connect()) as conn:
        conn.execute("INSERT INTO kv(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, payload))
        conn.commit()


def _db_save_entity(key: str, value: dict[str, Any]) -> None:
    payload = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    with _DB_LOCK, closing(_db_connect()) as conn:
        conn.execute("INSERT INTO entities(entity_key,value) VALUES(?,?) ON CONFLICT(entity_key) DO UPDATE SET value=excluded.value", (key, payload))
        conn.commit()


def _db_delete_old_entities(keep: int = MAX_STORED_ENTITIES) -> None:
    with _DB_LOCK, closing(_db_connect()) as conn:
        conn.execute("DELETE FROM entities WHERE entity_key NOT IN (SELECT entity_key FROM entities ORDER BY rowid DESC LIMIT ?)", (keep,))
        conn.commit()


def _clean_public_profile(profile: Any) -> dict[str, Any]:
    src = profile if isinstance(profile, dict) else {}
    gender = "f" if src.get("gender") == "f" else "m"
    state = clean_saved_player_state(src.get("state"))
    return {
        "name": clean_name(src.get("name")),
        "gender": gender,
        "character": clean_character(src.get("character"), gender),
        "state": state,
        "gameData": clean_game_data(src.get("gameData")),
        "savedAt": int(safe_number(src.get("savedAt"), 0, 0, 9_999_999_999_999)),
    }


def authenticate_account(account: Any, initial_profile: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    src = account if isinstance(account, dict) else {}
    username = str(src.get("username", "")).strip().lower()
    password = str(src.get("password", ""))
    mode = str(src.get("mode", "login"))
    if not re.fullmatch(r"[a-z0-9.]{6,30}@gmail\.com", username):
        raise ValueError("أدخل عنوان Gmail صحيحاً (مثال: name@gmail.com).")
    if len(password) < 8 or len(password) > 128:
        raise ValueError("كلمة المرور يجب أن تحتوي على 8 أحرف على الأقل.")
    if mode not in {"login", "register"}:
        mode = "login"
    now_ms = int(time.time() * 1000)
    with _DB_LOCK, closing(_db_connect()) as conn:
        row = conn.execute("SELECT salt,password_hash,profile_json FROM accounts WHERE username=?", (username,)).fetchone()
        if row:
            salt = bytes(row["salt"])
            expected = bytes(row["password_hash"])
            candidate = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 310_000, dklen=32)
            if not hmac.compare_digest(candidate, expected):
                raise ValueError("كلمة المرور غير صحيحة لهذا الحساب.")
            try:
                profile = _clean_public_profile(json.loads(row["profile_json"]))
            except (TypeError, json.JSONDecodeError):
                profile = _clean_public_profile(initial_profile)
            return username, profile
        if mode != "register":
            raise ValueError("الحساب غير موجود. اختر إنشاء حساب جديد أولاً.")
        salt = secrets.token_bytes(16)
        password_hash = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 310_000, dklen=32)
        profile = _clean_public_profile(initial_profile)
        profile["savedAt"] = now_ms
        conn.execute("INSERT INTO accounts(username,salt,password_hash,profile_json,created_at,updated_at) VALUES(?,?,?,?,?,?)", (username, salt, password_hash, json.dumps(profile, separators=(",", ":"), ensure_ascii=False), now_ms, now_ms))
        conn.commit()
        return username, profile


def save_account_profile(username: str, profile: dict[str, Any]) -> None:
    clean = _clean_public_profile(profile)
    clean["savedAt"] = int(time.time() * 1000)
    payload = json.dumps(clean, separators=(",", ":"), ensure_ascii=False)
    with _DB_LOCK, closing(_db_connect()) as conn:
        conn.execute("UPDATE accounts SET profile_json=?,updated_at=? WHERE username=?", (payload, clean["savedAt"], username))
        conn.commit()


def persist_player_from_client(client: dict[str, Any], force: bool = False) -> None:
    username = client.get("account_username")
    if not username:
        return
    now = time.monotonic()
    if not force and now - float(client.get("last_profile_save", 0.0)) < 3.0:
        return
    state = client.get("state", {})
    profile = dict(client.get("profile", {}))
    profile.update({
        "name": state.get("name", profile.get("name", "Player")),
        "gender": state.get("gender", profile.get("gender", "m")),
        "character": state.get("character", profile.get("character", {})),
        "state": {
            "x": state.get("x", 314), "y": state.get("y", 0), "z": state.get("z", 60),
            "heading": state.get("heading", 0), "inVehicle": state.get("inVehicle", False),
            "vehicleId": state.get("vehicleId", ""), "vehicleType": state.get("vehicleType", ""),
            "vehicleColor": state.get("vehicleColor", ""), "vehicleSpeed": state.get("vehicleSpeed", 0),
        },
        "gameData": client.get("game_data", profile.get("gameData", {})),
        "savedAt": int(time.time() * 1000),
    })
    save_account_profile(username, profile)
    client["profile"] = _clean_public_profile(profile)
    client["last_profile_save"] = now


def persist_world_bundle(force: bool = False) -> None:
    global _WORLD_DB_SAVE_CLOCK
    now = time.monotonic()
    if not force and now - _WORLD_DB_SAVE_CLOCK < 8.0:
        return
    payload = {
        "map_fingerprint": world_map_fingerprint,
        "world_revision": world_revision,
        "world_sequence": world_sequence,
        "latest_world_state": latest_world_state,
    }
    _db_set("world_bundle_v6", payload)
    _WORLD_DB_SAVE_CLOCK = now


def release_saved_vehicle_in_world(state: dict[str, Any] | None) -> bool:
    """Park the last occupied vehicle in the persisted authoritative world snapshot."""
    global latest_world_state, world_revision
    if not isinstance(state, dict) or not state.get("inVehicle") or not isinstance(latest_world_state, dict):
        return False
    vehicle_id = str(state.get("vehicleId", ""))[:96]
    snapshot = dict(latest_world_state)
    x = safe_number(state.get("x"), 0.0, -5300.0, 5300.0)
    z = safe_number(state.get("z"), 0.0, -5300.0, 5300.0)
    heading = safe_number(state.get("heading"), 0.0, -100.0, 100.0)
    try:
        life_match = re.fullmatch(r"life_car_(\d{1,5})", vehicle_id)
        if life_match:
            index = int(life_match.group(1))
            count = int(safe_number(snapshot.get("lifeCarCount"), 0, 0, 1200))
            stride = int(safe_number(snapshot.get("lifeCarStride"), 0, 0, 32))
            encoded = snapshot.get("lifeCars")
            if index >= count or stride != 14 or not isinstance(encoded, str):
                return False
            raw = bytearray(base64.b64decode(encoded, validate=True))
            offset = index * stride * 4
            if len(raw) < offset + stride * 4:
                return False
            values = [x, z, heading, 0.0, 0.0, 0.0, 2.0, 1.0]
            for field, value in ((0, x), (1, z), (2, heading), (4, 0.0), (10, 2.0), (11, 1.0)):
                struct.pack_into("<f", raw, offset + field * 4, value)
            snapshot["lifeCars"] = base64.b64encode(raw).decode("ascii")
        else:
            # First try dynamic vehicles, then the fixed traffic manifest.
            dynamic = snapshot.get("dynamicCars")
            if isinstance(dynamic, list):
                for row in dynamic:
                    if isinstance(row, dict) and str(row.get("id", "")) == vehicle_id:
                        row.update({"x": x, "z": z, "heading": heading, "speed": 0.0, "claimed": False, "retired": False, "parked": True})
                        break
                else:
                    dynamic = None
            else:
                dynamic = None
            if dynamic is None:
                manifest = snapshot.get("carManifest")
                if not isinstance(manifest, list):
                    return False
                index = next((i for i, row in enumerate(manifest) if isinstance(row, dict) and str(row.get("id", "")) == vehicle_id), -1)
                count = int(safe_number(snapshot.get("carCount"), 0, 0, 2000))
                stride = int(safe_number(snapshot.get("carStride"), 0, 0, 32))
                encoded = snapshot.get("cars")
                if index < 0 or index >= count or stride != 13 or not isinstance(encoded, str):
                    return False
                raw = bytearray(base64.b64decode(encoded, validate=True))
                offset = index * stride * 4
                if len(raw) < offset + stride * 4:
                    return False
                for field, value in ((0, x), (1, z), (2, 0.0), (3, heading), (4, 0.0), (6, 0.0), (7, 0.0), (8, 0.0), (11, 1.0)):
                    struct.pack_into("<f", raw, offset + field * 4, value)
                snapshot["cars"] = base64.b64encode(raw).decode("ascii")
                manifest[index] = dict(manifest[index], axis="parked")
        world_revision += 1
        snapshot["revision"] = world_revision
        snapshot["at"] = int(time.time() * 1000)
        latest_world_state = snapshot
        return True
    except (ValueError, TypeError, struct.error, base64.binascii.Error):
        log.exception("Could not release saved vehicle %s", vehicle_id)
        return False


def restore_persistent_world() -> None:
    global world_map_fingerprint, world_revision, world_sequence, latest_world_state, world_entities
    bundle = _db_get("world_bundle_v6")
    if isinstance(bundle, dict):
        fingerprint = str(bundle.get("map_fingerprint") or "")[:80]
        if re.fullmatch(r"[0-9a-fA-F]{16}-\d{1,8}-\d{1,5}-\d{1,5}", fingerprint):
            world_map_fingerprint = fingerprint.lower()
        snapshot = bundle.get("latest_world_state")
        if isinstance(snapshot, dict) and snapshot.get("mapVersion") == MAP_VERSION and snapshot.get("version") == PROTOCOL_VERSION:
            latest_world_state = snapshot
            world_revision = int(safe_number(bundle.get("world_revision"), 0, 0, 2_000_000_000))
            world_sequence = int(safe_number(bundle.get("world_sequence"), 0, 0, 2_000_000_000))
    with _DB_LOCK, closing(_db_connect()) as conn:
        rows = conn.execute("SELECT entity_key,value FROM entities").fetchall()
    restored: dict[str, dict[str, Any]] = {}
    for row in rows:
        try:
            entity = json.loads(row["value"])
            if isinstance(entity, dict) and entity.get("kind") in PERSISTENT_EVENT_KINDS:
                restored[str(row["entity_key"])] = entity
        except (TypeError, json.JSONDecodeError):
            continue
    world_entities = restored


try:
    _db_init()
    restore_persistent_world()
except Exception:
    log.exception("Configured persistent storage could not be opened; falling back to temporary storage")
    DB_PATH = Path("/tmp/thez_online_fallback.sqlite3")
    PERSISTENT_STORAGE_ENABLED = False
    _db_init()


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
            "x": safe_number(item.get("x"), 0.0, -32000.0, 32000.0),
            "z": safe_number(item.get("z"), 0.0, -32000.0, 32000.0),
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


def clean_car_manifest(value: Any, expected_count: int) -> list[dict[str, Any]] | None:
    """Validate the host's ordered identity/route manifest for the shared base fleet."""
    if not isinstance(value, list) or len(value) != expected_count or len(value) > 2000:
        return None
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    valid_axes = {"x", "z", "parked", "special"}
    for item in value:
        if not isinstance(item, dict):
            return None
        car_id = str(item.get("id", ""))[:96]
        car_type = str(item.get("type", ""))
        color = str(item.get("color", "#667788"))
        axis = str(item.get("axis", "special"))
        if not OBJECT_ID_RE.fullmatch(car_id) or car_id in seen or car_type not in ALLOWED_VEHICLES:
            return None
        if not re.fullmatch(r"#[0-9a-fA-F]{6}", color) or axis not in valid_axes:
            return None
        seen.add(car_id)
        output.append({
            "id": car_id, "type": car_type, "color": color.lower(),
            "variant": int(safe_number(item.get("variant"), 0, 0, 64)),
            "axis": axis,
            "lane": safe_number(item.get("lane"), 0.0, -5300.0, 5300.0),
            "t": safe_number(item.get("t"), 0.0, -5300.0, 5300.0),
            "dir": safe_number(item.get("dir"), 1.0, -1.0, 1.0),
            "cx": safe_number(item.get("cx"), 0.0, -5300.0, 5300.0),
            "baseZ": safe_number(item.get("baseZ"), 0.0, -5300.0, 5300.0),
            "span": safe_number(item.get("span"), 0.0, 0.0, 10000.0),
            "routeCity": safe_number(item.get("routeCity"), 0.0, -5300.0, 5300.0),
        })
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
            "x": safe_number(item.get("x"), 0.0, -32000.0, 32000.0),
            "y": safe_number(item.get("y"), 0.0, -80.0, 800.0),
            "z": safe_number(item.get("z"), 0.0, -32000.0, 32000.0),
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



# ----------------------------------------------------------------------------
# Server-side city simulation (cars + pedestrians)
# ----------------------------------------------------------------------------
def _sim_green(clock: float, axis: str) -> bool:
    t = clock % 16.0
    return t < 7.0 if axis == "x" else (8.0 <= t < 15.0)


def _sim_cross(road: list[float | str], ) -> tuple[float, ...]:
    axis, center, a0, a1 = road[0], float(road[1]), float(road[2]), float(road[3])
    if axis == "x":
        return tuple(x for x in SIM_VX if a0 < x < a1 and (x != 750.0 or abs(center) <= 330.0))
    return SIM_HZ


def _sim_angle(value: float) -> float:
    return math.atan2(math.sin(value), math.cos(value))


def clean_life_sim(value: Any) -> dict[str, Any] | None:
    """Validate the road/pedestrian description uploaded once by the first client."""
    if not isinstance(value, dict):
        return None
    roads_in, peds_in = value.get("roads"), value.get("peds")
    if not isinstance(roads_in, list) or not isinstance(peds_in, list):
        return None
    if not (1 <= len(roads_in) <= 400) or not (1 <= len(peds_in) <= 3000):
        return None
    roads: list[list[Any]] = []
    for row in roads_in:
        if not isinstance(row, list) or len(row) != 5 or row[0] not in ("x", "z"):
            return None
        roads.append([row[0], safe_number(row[1], 0, -6000, 6000), safe_number(row[2], 0, -6000, 6000),
                      safe_number(row[3], 0, -6000, 6000), safe_number(row[4], 24, 4, 120)])
    peds: list[dict[str, Any]] = []
    for row in peds_in:
        if not isinstance(row, dict):
            return None
        item: dict[str, Any] = {"v": safe_number(row.get("v"), 1.5, 0.0, 12.0)}
        path = row.get("o")
        if isinstance(path, list) and path:
            item["o"] = [[safe_number(pt[0], 0, -6000, 6000), safe_number(pt[1], 0, -6000, 6000)]
                         for pt in path[:64] if isinstance(pt, list) and len(pt) == 2]
            item["i"] = int(safe_number(row.get("i"), 0, 0, 63))
            if not item["o"]:
                return None
        else:
            ai = int(safe_number(row.get("a"), -1, -1, len(roads) - 1))
            if ai < 0:
                item["sit"] = True
                item["a"] = -1
            else:
                item.update({
                    "a": ai, "s": 1 if safe_number(row.get("s"), 1, -1, 1) >= 0 else -1,
                    "u": safe_number(row.get("u"), 0, -6000, 6000),
                    "d": 1 if safe_number(row.get("d"), 1, -1, 1) >= 0 else -1,
                    "sit": bool(row.get("t")),
                })
        peds.append(item)
    return {"roads": roads, "peds": peds}


def build_world_sim(snapshot: dict[str, Any], life_sim: dict[str, Any]) -> dict[str, Any] | None:
    try:
        raw_cars = array.array("f")
        raw_cars.frombytes(base64.b64decode(snapshot["lifeCars"], validate=True))
        raw_peds = array.array("f")
        raw_peds.frombytes(base64.b64decode(snapshot["lifePeds"], validate=True))
    except (KeyError, ValueError, base64.binascii.Error):
        return None
    car_count, ped_count = int(snapshot["lifeCarCount"]), int(snapshot["lifePedCount"])
    if len(raw_cars) != car_count * 14 or len(raw_peds) != ped_count * 9 or len(life_sim["peds"]) != ped_count:
        return None
    roads = life_sim["roads"]
    cars: list[dict[str, Any]] = []
    for i in range(car_count):
        o = i * 14
        road = int(raw_cars[o + 7])
        state = int(round(raw_cars[o + 11]))
        moving = state in (0, 2) and 0 <= road < len(roads)
        cars.append({
            "x": raw_cars[o], "z": raw_cars[o + 1], "ry": raw_cars[o + 2], "u": raw_cars[o + 3],
            "v": raw_cars[o + 4], "dir": 1 if raw_cars[o + 5] >= 0 else -1, "vmax": max(0.0, raw_cars[o + 6]),
            "r": road if moving else (road if 0 <= road < len(roads) else -1),
            "pshift": raw_cars[o + 8], "dmg": raw_cars[o + 9], "pstate": int(round(raw_cars[o + 10])),
            "state": 0 if moving and state == 0 else (2 if moving else (4 if state == 4 else 1)),
            "stop": raw_cars[o + 12], "stun": max(0.0, raw_cars[o + 13]),
            "bx": 0.0, "bz": 0.0, "bry": 0.0, "bt": 1.0, "driver": None,
        })
    peds: list[dict[str, Any]] = []
    for i, info in enumerate(life_sim["peds"]):
        o = i * 9
        ped: dict[str, Any] = {"x": raw_peds[o], "z": raw_peds[o + 1], "yaw": raw_peds[o + 2], "ph": raw_peds[o + 3],
                               "anim": 0, "cross": False, "t": 0.0, "st": "walk", "v": info["v"], "side": 1, "dir": 1, "u": 0.0, "a": -1}
        if info.get("o"):
            ped.update({"path": info["o"], "idx": info.get("i", 0), "st": "off"})
        elif info.get("sit") or info["a"] < 0:
            ped["st"] = "sit"
            ped["anim"] = 4
        else:
            ped.update({"a": info["a"], "side": info["s"], "u": info["u"], "dir": info["d"]})
        peds.append(ped)
    return {"roads": roads, "cars": cars, "peds": peds, "clock": 0.0,
            "worldTime": safe_number(snapshot.get("worldTime"), 0.0, 0.0, 1_000_000_000.0)}


def sim_encode(sim: dict[str, Any]) -> tuple[str, str]:
    car_buf = array.array("f")
    for c in sim["cars"]:
        car_buf.extend((c["x"], c["z"], c["ry"], c["u"], c["v"], float(c["dir"]), c["vmax"], float(c["r"]),
                        c["pshift"], c["dmg"], float(c["pstate"]), float(c["state"]), c["stop"], c["stun"]))
    ped_buf = array.array("f")
    for p in sim["peds"]:
        ped_buf.extend((p["x"], p["z"], p["yaw"], p["ph"], 0.0, 0.0, 0.0, float(p["anim"]), 2.0 if p["cross"] else 0.0))
    if sys.byteorder != "little":
        car_buf.byteswap()
        ped_buf.byteswap()
    return base64.b64encode(car_buf.tobytes()).decode("ascii"), base64.b64encode(ped_buf.tobytes()).decode("ascii")


def sim_apply_players(sim: dict[str, Any]) -> list[tuple[float, float, float]]:
    """Mirror real players into the simulation: occupied street cars follow their driver."""
    obstacles: list[tuple[float, float, float]] = []
    driven: set[int] = set()
    for client in clients.values():
        st = client.get("state") or {}
        px, pz = float(st.get("x", 0.0)), float(st.get("z", 0.0))
        match = re.fullmatch(r"life_car_(\d{1,5})", str(st.get("vehicleId", ""))) if st.get("inVehicle") else None
        if match and int(match.group(1)) < len(sim["cars"]):
            idx = int(match.group(1))
            car = sim["cars"][idx]
            if car["state"] != 4:
                driven.add(idx)
                car.update({"x": px, "z": pz, "ry": float(st.get("heading", 0.0)), "v": abs(float(st.get("vehicleSpeed", 0.0))),
                            "state": 3, "driver": True})
            obstacles.append((px, pz, 2.2))
        else:
            obstacles.append((px, pz, 0.9))
    for idx, car in enumerate(sim["cars"]):
        if car["state"] == 3 and idx not in driven:
            # Driver left: park the vehicle where it was abandoned.
            car.update({"state": 1, "r": -1, "v": 0.0, "pstate": 2, "pshift": 0.0, "driver": None})
    return obstacles


def sim_step(sim: dict[str, Any], dt: float) -> None:
    roads = sim["roads"]
    clock = sim["clock"] = sim["clock"] + dt
    sim["worldTime"] += dt
    obstacles = sim_apply_players(sim)
    rnd = random.random
    for ped in sim["peds"]:
        if ped["cross"]:
            obstacles.append((ped["x"], ped["z"], 0.8))
    # --- cars -----------------------------------------------------------------
    lanes: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for car in sim["cars"]:
        if car["state"] in (0, 2) and car["r"] >= 0:
            lanes.setdefault((car["r"], car["dir"]), []).append(car)
    for lane in lanes.values():
        lane.sort(key=lambda c: c["u"])
    for (ri, direction), lane in lanes.items():
        road = roads[ri]
        crossings = _sim_cross(road)
        axis = road[0]
        for pos, car in enumerate(lane):
            if car["state"] != 0:
                if car["stun"] > 0:
                    car["stun"] = max(0.0, car["stun"] - dt)
                continue
            neighbour = lane[pos + 1] if direction > 0 and pos + 1 < len(lane) else (lane[pos - 1] if direction < 0 and pos > 0 else None)
            gap = abs(neighbour["u"] - car["u"]) if neighbour is not None else 999.0
            target = car["vmax"]
            if gap < 26:
                target = min(target, max(0.0, (gap - 8) * 1.2))
            for X in crossings:
                d = (X - car["u"]) * direction
                if 8 < d < 36 and not _sim_green(clock, axis):
                    target = min(target, max(0.0, (d - 17) * 0.9))
            fx, fz = (float(direction), 0.0) if axis == "x" else (0.0, float(direction))
            for ox, oz, orad in obstacles:
                dx, dz = ox - car["x"], oz - car["z"]
                ahead = dx * fx + dz * fz
                if 0 < ahead < 26 and abs(-dx * fz + dz * fx) < 2.4 + orad:
                    target = min(target, max(0.0, (ahead - 6 - orad) * 1.1))
            if car["stun"] > 0:
                target = 0.0
                car["stun"] -= dt
            rate = (6.0 if target < 1 else 3.5) if target < car["v"] else 1.3
            car["v"] += (target - car["v"]) * min(1.0, dt * rate)
            prev_u = car["u"]
            car["u"] += car["v"] * direction * dt
            if car["v"] > 0:
                for X in crossings:
                    if (X - prev_u) * (X - car["u"]) <= 0:
                        if rnd() < 0.4:
                            for qi, q in enumerate(roads):
                                if q[0] != axis and abs(q[1] - X) < 1 and q[2] <= road[1] <= q[3]:
                                    car.update({"bx": car["x"], "bz": car["z"], "bry": car["ry"], "bt": 0.0,
                                                "r": qi, "u": road[1], "dir": 1 if rnd() < 0.5 else -1})
                                    car["v"] *= 0.6
                                    break
                        break
    for car in sim["cars"]:
        if car["state"] != 0 or car["r"] < 0:
            continue
        q = roads[car["r"]]
        if car["u"] > q[3]:
            car["u"] = q[2]
        elif car["u"] < q[2]:
            car["u"] = q[3]
        off = car["dir"] * q[4] / 4.0
        if q[0] == "x":
            x, z, ry = car["u"], q[1] + off, (0.0 if car["dir"] > 0 else math.pi)
        else:
            x, z, ry = q[1] - off, car["u"], (-math.pi / 2 if car["dir"] > 0 else math.pi / 2)
        if car["bt"] < 1.0:
            car["bt"] = min(1.0, car["bt"] + dt * 0.7)
            k = car["bt"] * car["bt"] * (3 - 2 * car["bt"])
            x = car["bx"] + (x - car["bx"]) * k
            z = car["bz"] + (z - car["bz"]) * k
            ry = car["bry"] + _sim_angle(ry - car["bry"]) * k
        car["x"], car["z"], car["ry"] = x, z, ry
    # --- pedestrians ------------------------------------------------------------
    scale = dt * 60.0
    p_turn = 1.0 - (1.0 - 0.01) ** scale
    p_stop = 1.0 - (1.0 - 0.0012) ** scale
    for ped in sim["peds"]:
        st = ped["st"]
        if st == "sit":
            continue
        if st == "off":
            path = ped["path"]
            tx, tz = path[ped["idx"] % len(path)]
            vx, vz = tx - ped["x"], tz - ped["z"]
            dist = math.hypot(vx, vz)
            if dist < 1.35:
                ped["idx"] = (ped["idx"] + 1) % len(path)
                tx, tz = path[ped["idx"]]
                vx, vz = tx - ped["x"], tz - ped["z"]
                dist = math.hypot(vx, vz)
            length = dist or 1.0
            ped["x"] += vx / length * ped["v"] * dt
            ped["z"] += vz / length * ped["v"] * dt
            ped["yaw"] = math.atan2(vx, vz)
            ped["anim"] = 1
            ped["ph"] += dt * 3.8
            continue
        road = roads[ped["a"]]
        axis = road[0]
        ped["t"] -= dt
        move = ped["v"]
        if st == "stop":
            move = 0.0
            if ped["t"] <= 0:
                ped["st"] = st = "walk"
        crossings = _sim_cross(road)
        on_crossing = False
        for X in crossings:
            d = (X - ped["u"]) * ped["dir"]
            if 14 < d < 17 and _sim_green(clock, "z" if axis == "x" else "x"):
                move = 0.0
            if abs(X - ped["u"]) < 12.5:
                on_crossing = True
                move = max(move, 2.4 if st == "walk" else 0.0)
            if -1 < d < 1 and rnd() < p_turn:
                for qi, q in enumerate(roads):
                    if q[0] != axis and abs(q[1] - X) < 1 and q[2] <= road[1] <= q[3]:
                        ped["u"] = road[1] + ped["side"] * (road[4] / 2.0 + 2.5)
                        ped["a"] = qi
                        ped["side"] = 1 if rnd() < 0.5 else -1
                        ped["dir"] = 1 if rnd() < 0.5 else -1
                        break
        ped["cross"] = on_crossing
        if move > 0 and st == "walk" and rnd() < p_stop:
            ped["st"] = "stop"
            ped["t"] = random.uniform(2.0, 6.0)
        road = roads[ped["a"]]
        ped["u"] += move * ped["dir"] * dt
        if ped["u"] > road[3] - 20 or ped["u"] < road[2] + 20:
            ped["dir"] = -ped["dir"]
        ped["ph"] += move * dt * 3.7
        lateral = road[1] + ped["side"] * (road[4] / 2.0 + 2.5)
        ped["anim"] = 1 if move > 0.05 else 0
        if road[0] == "x":
            ped["x"], ped["z"], ped["yaw"] = ped["u"], lateral, (math.pi / 2 if ped["dir"] > 0 else -math.pi / 2)
        else:
            ped["x"], ped["z"], ped["yaw"] = lateral, ped["u"], (0.0 if ped["dir"] > 0 else math.pi)


async def sim_loop() -> None:
    """Advance the shared city every 100 ms and broadcast it to everyone once per second."""
    global latest_world_state, world_revision
    last = time.monotonic()
    next_broadcast = last + SIM_BROADCAST
    next_save = last + 15.0
    while True:
        await asyncio.sleep(SIM_TICK)
        now = time.monotonic()
        dt = min(0.35, now - last)
        last = now
        sim = world_sim
        if sim is None or not clients or latest_world_state is None:
            next_broadcast = now + SIM_BROADCAST
            continue
        try:
            sim_step(sim, dt)
            if now >= next_broadcast:
                next_broadcast = now + SIM_BROADCAST
                cars_b64, peds_b64 = sim_encode(sim)
                latest_world_state.update({
                    "lifeCars": cars_b64, "lifePeds": peds_b64, "lifePedMeta": [],
                    "worldTime": sim["worldTime"], "simClock": sim["clock"], "at": int(time.time() * 1000),
                })
                world_revision += 1
                latest_world_state["revision"] = world_revision
                await send_to_room(WORLD_ROOM, {"type": "world_state", "state": latest_world_state})
            if now >= next_save:
                next_save = now + 15.0
                persist_sim()
        except Exception:
            log.exception("Simulation step failed")


def persist_sim() -> None:
    if world_sim is not None:
        _db_set("world_sim_v1", world_sim)


def restore_sim() -> None:
    global world_sim
    data = _db_get("world_sim_v1")
    if isinstance(data, dict) and isinstance(data.get("cars"), list) and isinstance(data.get("peds"), list) and isinstance(data.get("roads"), list) and latest_world_state:
        world_sim = data


@app.on_event("startup")
async def _start_simulation() -> None:
    try:
        restore_sim()
        if world_sim is not None:
            log.info("Restored persisted server-side city simulation")
    except Exception:
        log.exception("Could not restore the persisted city simulation")
    asyncio.create_task(sim_loop())


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
        "ok": True, "game": "THE Z ONLINE", "protocol_version": PROTOCOL_VERSION, "map_version": MAP_VERSION,
        "players_online": len(clients), "max_players": MAX_PLAYERS,
        "rooms_active": 1 if rooms.get(WORLD_ROOM) else 0,
        "world_room": WORLD_ROOM,
        "world_host_connected": world_host_id in clients if world_host_id else False,
        "world_revision": world_revision, "world_events_cached": len(world_event_history),
        "world_entities_cached": len(world_entities),
        "accounts_storage": "persistent" if PERSISTENT_STORAGE_ENABLED else "ephemeral_fallback",
        "database_path": str(DB_PATH),
        "game_file_found": GAME_PATH.is_file(),
        "title_image_found": IMAGE_PATH.is_file(),
        "world_map_found": WORLD_MAP_PATH.is_file(),
    }


@app.websocket("/ws")
async def multiplayer_socket(websocket: WebSocket):
    """Join the shared world and relay synchronized player/world state."""
    global world_host_id, world_map_fingerprint, latest_world_state, world_revision, world_sequence, world_sim
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
                if join_packet.get("map_version") != MAP_VERSION:
                    await websocket.send_json({"type": "map_mismatch", "expected_map_version": MAP_VERSION, "message": "نسخة الخريطة غير متوافقة. ثبّت index.html و app.py من الحزمة نفسها على جميع الأجهزة."})
                    await websocket.close(code=4409, reason="Map version mismatch")
                    return
                fingerprint = str(join_packet.get("map_fingerprint") or "")[:80]
                if not re.fullmatch(r"[0-9a-fA-F]{16}-\d{1,8}-\d{1,5}-\d{1,5}", fingerprint):
                    await websocket.send_json({"type": "map_mismatch", "expected_map_version": MAP_VERSION, "message": "تعذّر التحقق من بصمة الخريطة. حدّث index.html من حزمة V7 وافتح اللعبة من رابط السيرفر."})
                    await websocket.close(code=4409, reason="Invalid map fingerprint")
                    return
                if world_map_fingerprint is None:
                    world_map_fingerprint = fingerprint.lower()
                elif fingerprint.lower() != world_map_fingerprint and not any(pid in clients for pid in rooms.get(WORLD_ROOM, set())):
                    # Nobody is online, so the saved fingerprint is stale (index.html changed since the last session).
                    # Adopt the new build and discard the old world snapshot instead of rejecting every player forever.
                    log.warning("Map fingerprint changed (%s -> %s); resetting stale persisted world", world_map_fingerprint, fingerprint.lower())
                    world_map_fingerprint = fingerprint.lower()
                    latest_world_state = None
                    world_sim = None
                    world_revision = 0
                    world_entities.clear()
                    world_event_history.clear()
                    try:
                        with _DB_LOCK, closing(_db_connect()) as conn:
                            conn.execute("DELETE FROM entities")
                            conn.commit()
                        _db_set("world_sim_v1", None)
                        persist_world_bundle(force=True)
                    except Exception:
                        log.exception("Could not reset stale persisted world")
                elif fingerprint.lower() != world_map_fingerprint:
                    await websocket.send_json({"type": "map_mismatch", "expected_map_version": MAP_VERSION, "message": "هذا الجهاز يستخدم خريطة مختلفة عن بقية اللاعبين. استبدل index.html و app.py بملفي V7 نفسيهما على جميع الأجهزة ثم أعد تشغيل السيرفر."})
                    await websocket.close(code=4409, reason="Different city build")
                    return
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
        # Account passwords are hashed with PBKDF2 and are never stored in plain text.
        initial_profile = {"name": name, "gender": gender, "character": character, "state": None, "gameData": {}}
        try:
            account_username, saved_profile = authenticate_account(join_packet.get("account"), initial_profile)
        except ValueError as auth_error:
            await websocket.send_json({"type": "auth_error", "message": str(auth_error)})
            await websocket.close(code=4401, reason="Account authentication failed")
            return
        name = clean_name(saved_profile.get("name", name))
        gender = "f" if saved_profile.get("gender") == "f" else "m"
        character = clean_character(saved_profile.get("character"), gender)
        saved_state = clean_saved_player_state(saved_profile.get("state"))
        # Golden-angle spawn placement avoids stacking brand-new accounts in the arrival plaza.
        spawn_slot = len(room_members)
        spawn_radius = 4.0 + math.sqrt(spawn_slot) * 2.7
        spawn_angle = spawn_slot * 2.399963229728653
        spawn_x = 100.0 + math.cos(spawn_angle) * spawn_radius
        spawn_z = 1900.0 + math.sin(spawn_angle) * spawn_radius
        if saved_state:
            spawn_x, spawn_z = saved_state["x"], saved_state["z"]
        state = clean_initial_state(player_id, name, gender, character, spawn_x, spawn_z)
        if saved_state:
            state.update({"x": saved_state["x"], "y": saved_state["y"], "z": saved_state["z"], "heading": saved_state["heading"]})
            # A previous vehicle is restored by the client only after it has loaded the shared street-car manifest.
        clients[player_id] = {
            "websocket": websocket, "room": WORLD_ROOM, "state": state,
            "send_lock": asyncio.Lock(), "event_times": event_times,
            "account_username": account_username, "profile": saved_profile,
            "game_data": clean_game_data(saved_profile.get("gameData")),
            "last_profile_save": 0.0,
        }
        room_members.add(player_id)
        if world_sim is not None:
            world_host_id = None  # the server owns the simulation; no client is ever the host
        elif world_host_id not in clients:
            world_host_id = player_id
        if world_sim is not None and latest_world_state is not None:
            cars_b64, peds_b64 = sim_encode(world_sim)
            latest_world_state.update({"lifeCars": cars_b64, "lifePeds": peds_b64, "lifePedMeta": [],
                                       "worldTime": world_sim["worldTime"], "simClock": world_sim["clock"]})

        previous_players = [
            public_state(clients[other_id]["state"])
            for other_id in list(room_members)
            if other_id != player_id and other_id in clients
        ]
        welcome = {
            "type": "welcome", "protocol": PROTOCOL_VERSION,
            "world_meta": {"id": WORLD_ID, "room": WORLD_ROOM, "map_version": MAP_VERSION, "fingerprint": world_map_fingerprint or "", "seed": MAP_SEED, "protocol": PROTOCOL_VERSION},
            "id": player_id, "room": WORLD_ROOM, "players": previous_players,
            "count": len(room_members), "world_host": world_host_id,
            "is_world_host": world_sim is None and player_id == world_host_id,
            "server_sim": world_sim is not None,
            "spawn": {"x": spawn_x, "y": state["y"], "z": spawn_z, "heading": state["heading"]},
            "profile": saved_profile,
            "storage_persistent": PERSISTENT_STORAGE_ENABLED,
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
                # Keep the account profile current when the player changes their name or outfit in-game.
                name = clean_name(packet.get("name", name))
                gender = "f" if packet.get("gender") == "f" else "m"
                character = clean_character(packet.get("character", character), gender)
                state = {
                    "id": player_id, "name": name, "gender": gender, "character": character,
                    "x": safe_number(packet.get("x"), spawn_x, -32000.0, 32000.0),
                    "y": safe_number(packet.get("y"), 0.0, -80.0, 3500.0),
                    "z": safe_number(packet.get("z"), spawn_z, -32000.0, 32000.0),
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
                clients[player_id]["game_data"] = clean_game_data(packet.get("gameData", clients[player_id].get("game_data", {})))
                persist_player_from_client(clients[player_id])
                await send_to_room(WORLD_ROOM, {"type": "player_update", "player": public_state(state)}, exclude=player_id)
                continue

            if packet_type == "world_state":
                # One elected host publishes deterministic ambient NPC + traffic snapshots.
                if world_sim is not None or player_id != world_host_id or now - last_world_update < MIN_WORLD_INTERVAL:
                    continue
                snapshot = packet.get("state")
                if not isinstance(snapshot, dict) or snapshot.get("mapVersion") != MAP_VERSION or int(safe_number(snapshot.get("version"), 0, 0, 99)) != PROTOCOL_VERSION:
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
                car_manifest = clean_car_manifest(snapshot.get("carManifest"), car_count)
                if car_manifest is None:
                    continue
                life_cars = snapshot.get("lifeCars")
                life_peds = snapshot.get("lifePeds")
                life_car_count = int(safe_number(snapshot.get("lifeCarCount"), 0, 0, 1200))
                life_ped_count = int(safe_number(snapshot.get("lifePedCount"), 0, 0, 3000))
                life_car_stride = int(safe_number(snapshot.get("lifeCarStride"), 14, 14, 14))
                life_ped_stride = int(safe_number(snapshot.get("lifePedStride"), 9, 9, 9))
                if not isinstance(life_cars, str) or not isinstance(life_peds, str):
                    continue
                if len(life_cars) > MAX_WORLD_STRING or len(life_peds) > MAX_WORLD_STRING:
                    continue
                if not re.fullmatch(r"[A-Za-z0-9+/=]*", life_cars) or not re.fullmatch(r"[A-Za-z0-9+/=]*", life_peds):
                    continue
                if life_car_count < 1 or life_ped_count < 1:
                    continue
                try:
                    if len(base64.b64decode(life_cars, validate=True)) != life_car_count * life_car_stride * 4:
                        continue
                    if len(base64.b64decode(life_peds, validate=True)) != life_ped_count * life_ped_stride * 4:
                        continue
                except (ValueError, base64.binascii.Error):
                    continue
                ped_meta = clean_ped_meta(snapshot.get("pedMeta"))
                life_ped_meta = clean_life_ped_meta(snapshot.get("lifePedMeta"))
                transient_peds = clean_transient_peds(snapshot.get("transientPeds"))
                dynamic_cars = clean_dynamic_cars(snapshot.get("dynamicCars"))
                latest_world_state = {
                    "version": PROTOCOL_VERSION, "mapVersion": MAP_VERSION, "peds": peds, "cars": cars,
                    "pedCount": ped_count, "carCount": car_count, "carManifest": car_manifest,
                    "pedStride": ped_stride, "carStride": car_stride,
                    "lifeCars": life_cars, "lifePeds": life_peds,
                    "lifeCarCount": life_car_count, "lifePedCount": life_ped_count,
                    "lifeCarStride": life_car_stride, "lifePedStride": life_ped_stride,
                    "pedMeta": ped_meta, "lifePedMeta": life_ped_meta,
                    "transientPeds": transient_peds, "dynamicCars": dynamic_cars,
                    "worldTime": safe_number(snapshot.get("worldTime"), 0.0, 0.0, 1_000_000_000.0),
                    "at": int(time.time() * 1000),
                }
                world_revision += 1
                latest_world_state["revision"] = world_revision
                last_world_update = now
                life_sim = clean_life_sim(snapshot.get("lifeSim"))
                if life_sim is not None and world_sim is None:
                    built = build_world_sim(latest_world_state, life_sim)
                    if built is not None:
                        world_sim = built
                        world_host_id = None
                        latest_world_state["dynamicCars"] = []
                        latest_world_state["simClock"] = 0.0
                        persist_sim()
                        log.info("City simulation is now running on the server (%d cars, %d people)", len(built["cars"]), len(built["peds"]))
                        await send_to_room(WORLD_ROOM, {"type": "server_sim", "ready": True})
                        await send_to_room(WORLD_ROOM, {"type": "world_host", "id": None, "revision": world_revision})
                if world_revision == 1 or now - _WORLD_DB_SAVE_CLOCK >= 8.0:
                    persist_world_bundle(force=world_revision == 1)
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
                if world_sim is not None and kind == "vehicle_explosion":
                    wreck = re.fullmatch(r"life_car_(\d{1,5})", object_id)
                    if wreck and int(wreck.group(1)) < len(world_sim["cars"]):
                        world_sim["cars"][int(wreck.group(1))].update({"state": 4, "v": 0.0})
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
                    _db_save_entity(entity_key, world_entities[entity_key])
                    if len(world_entities) > MAX_STORED_ENTITIES:
                        oldest_key = min(world_entities, key=lambda k: world_entities[k].get("seq", 0))
                        world_entities.pop(oldest_key, None)
                        with _DB_LOCK, closing(_db_connect()) as conn:
                            conn.execute("DELETE FROM entities WHERE entity_key=?", (oldest_key,))
                            conn.commit()
                    _db_delete_old_entities()
                world_event_history.append(event)
                await send_to_room(WORLD_ROOM, event, exclude=player_id)
                continue

    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("WebSocket session ended unexpectedly for %s", player_id or "pending player")
    finally:
        if player_id:
            leaving_client = clients.get(player_id)
            if leaving_client:
                try:
                    persist_player_from_client(leaving_client, force=True)
                except Exception:
                    log.exception("Could not persist profile for player %s", player_id)
            if world_sim is not None and leaving_client:
                try:
                    leave_state = leaving_client.get("state") or {}
                    match = re.fullmatch(r"life_car_(\d{1,5})", str(leave_state.get("vehicleId", ""))) if leave_state.get("inVehicle") else None
                    if match and int(match.group(1)) < len(world_sim["cars"]):
                        car = world_sim["cars"][int(match.group(1))]
                        if car["state"] != 4:
                            car.update({"x": float(leave_state.get("x", car["x"])), "z": float(leave_state.get("z", car["z"])),
                                        "ry": float(leave_state.get("heading", car["ry"])), "state": 1, "r": -1,
                                        "v": 0.0, "pstate": 2, "pshift": 0.0})
                except Exception:
                    log.exception("Could not park the abandoned vehicle")
            clients.pop(player_id, None)
            members = rooms.get(WORLD_ROOM, set())
            if members is not None:
                members.discard(player_id)
            await send_to_room(WORLD_ROOM, {"type": "player_leave", "id": player_id})
            if world_host_id == player_id:
                remaining = [pid for pid in rooms.get(WORLD_ROOM, set()) if pid in clients]
                world_host_id = None if world_sim is not None else (remaining[0] if remaining else None)
                await send_to_room(WORLD_ROOM, {
                    "type": "world_host", "id": world_host_id,
                    "revision": world_revision,
                })
            remaining_members = rooms.get(WORLD_ROOM, set())
            await send_to_room(WORLD_ROOM, {"type": "server_status", "players_online": len(remaining_members)})
            # Keep the last authoritative world snapshot and persistent entities for
            # the next session. Only transient event history is discarded.
            if not remaining_members:
                try:
                    if world_sim is not None and latest_world_state is not None:
                        cars_b64, peds_b64 = sim_encode(world_sim)
                        latest_world_state.update({"lifeCars": cars_b64, "lifePeds": peds_b64, "lifePedMeta": [],
                                                   "worldTime": world_sim["worldTime"], "simClock": world_sim["clock"]})
                        persist_sim()
                    elif leaving_client and release_saved_vehicle_in_world(leaving_client.get("state")):
                        log.info("Released last occupied vehicle to the shared world")
                    persist_world_bundle(force=True)
                except Exception:
                    log.exception("Could not persist final world snapshot")
                world_event_history.clear()


if __name__ == "__main__":
    import uvicorn

    # Keep one worker: live WebSocket room state is process-local; profiles/world snapshots use SQLite.
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, reload=False, workers=1)
