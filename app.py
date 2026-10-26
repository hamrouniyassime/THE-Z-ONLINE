from __future__ import annotations

import asyncio
import json
import math
import os
import random
import re
import secrets
import sqlite3
import time
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

APP_DIR = Path(__file__).resolve().parent
INDEX_FILE = APP_DIR / "index.html"
DB_PATH = Path(os.environ.get("THEZ_DB_PATH", str(APP_DIR / "thez_game.sqlite3")))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="THE Z — CAR SALING MAP", version="2.0.0")
clients: dict[str, WebSocket] = {}
players: dict[str, dict[str, Any]] = {}
state_lock = asyncio.Lock()
COLORS = ["#ff6b6b", "#ffd166", "#06d6a0", "#4cc9f0", "#b197fc", "#ff8fab", "#f4a261", "#90be6d"]
VALID_MAPS = {"city"}  # The game now exposes one world only.
CAR_CATALOG = {
    "compact": {"name": "Luma Mini", "price": 1800},
    "sedan": {"name": "Coast Sedan", "price": 4200},
    "suv": {"name": "Ridge SUV", "price": 7200},
    "sport": {"name": "Vortex GT", "price": 12500},
    "electric": {"name": "Luma E", "price": 16500},
    "super": {"name": "Aurora X", "price": 26000},
}
MAX_HOUSES = 50
MAX_SHOPS = 30


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=15, isolation_level="DEFERRED")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    with connect_db() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS profiles (
            client_key TEXT PRIMARY KEY,
            home_slot INTEGER NOT NULL,
            shop_slot INTEGER NOT NULL,
            profile_json TEXT NOT NULL,
            name TEXT NOT NULL DEFAULT 'Player',
            updated REAL NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS market_listings (
            listing_id TEXT PRIMARY KEY,
            seller_key TEXT NOT NULL,
            seller_name TEXT NOT NULL,
            car_type TEXT NOT NULL,
            color TEXT NOT NULL,
            price INTEGER NOT NULL,
            max_price INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'listed',
            created REAL NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS market_offers (
            offer_id TEXT PRIMARY KEY,
            listing_id TEXT NOT NULL,
            buyer_key TEXT NOT NULL,
            buyer_name TEXT NOT NULL,
            offer INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created REAL NOT NULL,
            FOREIGN KEY(listing_id) REFERENCES market_listings(listing_id)
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_market_status ON market_listings(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_offers_listing ON market_offers(listing_id, status)")


def default_profile(name: str = "Player") -> dict[str, Any]:
    return {
        "name": name,
        "wallet": 25000,
        "garageCars": [],
        "primaryCarId": None,
        "ownedCars": [],
        "homeLocked": False,
        "tutorialStep": 0,
        "homePurchased": False,
        "hasSlept": False,
        "shopBuilt": False,
        "constructionStartedAt": 0,
        "stockCars": [],
        "pendingDelivery": None,
    }


def clean_name(value: Any, fallback: str = "Player") -> str:
    if not isinstance(value, str):
        return fallback
    name = " ".join(value.strip().split())
    name = re.sub(r"[\x00-\x1f\x7f]", "", name)[:22]
    return name or fallback


def clean_key(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    # Client keys are random device identifiers, not authentication credentials.
    value = re.sub(r"[^a-zA-Z0-9._:-]", "", value)[:100]
    return value if len(value) >= 8 else ""


def finite_number(value: Any, fallback: float, low: float, high: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return fallback
    number = float(value)
    if not math.isfinite(number):
        return fallback
    return max(low, min(high, number))


def clean_profile(raw: Any, fallback: dict[str, Any] | None = None) -> dict[str, Any]:
    base = default_profile()
    if fallback:
        base.update(fallback)
    if not isinstance(raw, dict):
        return base
    name = clean_name(raw.get("name"), str(base.get("name", "Player")))
    base["name"] = name
    base["wallet"] = int(finite_number(raw.get("wallet"), float(base.get("wallet", 25000)), 0, 2_000_000_000))
    base["homeLocked"] = bool(raw.get("homeLocked", False))
    base["tutorialStep"] = int(finite_number(raw.get("tutorialStep"), 0, 0, 10))
    for flag in ("homePurchased", "hasSlept", "shopBuilt"):
        base[flag] = bool(raw.get(flag, False))
    base["constructionStartedAt"] = int(finite_number(raw.get("constructionStartedAt"), 0, 0, 10**13))
    # Keep distinct vehicle instances: a player may own several cars of the same model.
    legacy_owned = raw.get("ownedCars", [])
    incoming_garage = raw.get("garageCars")
    safe_garage: list[dict[str, Any]] = []
    source_garage = incoming_garage if isinstance(incoming_garage, list) else []
    if not isinstance(incoming_garage, list) and isinstance(legacy_owned, list):
        source_garage = [{"id": f"legacy-{kind}", "type": kind, "delivered": True, "price": CAR_CATALOG.get(kind, {}).get("price", 1)}
                         for kind in dict.fromkeys(v for v in legacy_owned if isinstance(v, str) and v in CAR_CATALOG)]
    seen_ids: set[str] = set()
    for item in source_garage[:50]:
        if not isinstance(item, dict) or item.get("type") not in CAR_CATALOG:
            continue
        kind = item["type"]
        car_id = re.sub(r"[^a-zA-Z0-9._:-]", "", str(item.get("id") or ""))[:100]
        if not car_id or car_id in seen_ids:
            car_id = secrets.token_hex(8)
        seen_ids.add(car_id)
        safe_garage.append({
            "id": car_id,
            "type": kind,
            "color": item.get("color") if isinstance(item.get("color"), str) and re.fullmatch(r"#[0-9a-fA-F]{6}", item.get("color")) else "#43c6e8",
            "price": int(finite_number(item.get("price"), CAR_CATALOG[kind]["price"], 1, CAR_CATALOG[kind]["price"])),
            "delivered": bool(item.get("delivered", True)),
            "primary": bool(item.get("primary", False)),
            "yaw": finite_number(item.get("yaw"), 0.0, -100000, 100000),
            "x": finite_number(item.get("x"), 0.0, -10000, 10000) if item.get("x") is not None else None,
            "z": finite_number(item.get("z"), 0.0, -10000, 10000) if item.get("z") is not None else None,
        })
    base["garageCars"] = safe_garage
    requested_primary = re.sub(r"[^a-zA-Z0-9._:-]", "", str(raw.get("primaryCarId") or ""))[:100]
    base["primaryCarId"] = requested_primary if any(c["id"] == requested_primary for c in safe_garage) else next((c["id"] for c in safe_garage if c["primary"]), safe_garage[0]["id"] if safe_garage else None)
    for item in safe_garage:
        item["primary"] = item["id"] == base["primaryCarId"]
    base["ownedCars"] = list(dict.fromkeys(c["type"] for c in safe_garage))
    stock = raw.get("stockCars", [])
    safe_stock = []
    if isinstance(stock, list):
        for car in stock[:50]:
            if not isinstance(car, dict) or car.get("type") not in CAR_CATALOG:
                continue
            car_id = re.sub(r"[^a-zA-Z0-9._:-]", "", str(car.get("id", "")))[:100]
            if not car_id:
                continue
            record = {
                "id": car_id,
                "type": car["type"],
                "color": car.get("color") if isinstance(car.get("color"), str) and re.fullmatch(r"#[0-9a-fA-F]{6}", car.get("color")) else "#43c6e8",
                "cost": int(finite_number(car.get("cost"), 0, 0, 2_000_000_000)),
                "delivered": bool(car.get("delivered")),
                "sold": bool(car.get("sold")),
                "listed": bool(car.get("listed")),
                "displayed": bool(car.get("displayed")),
                "price": int(finite_number(car.get("price"), 0, 0, 2_000_000_000)),
                "x": finite_number(car.get("x"), 0, -10000, 10000),
                "z": finite_number(car.get("z"), 0, -10000, 10000),
            }
            # Retail prices are capped at the catalog price for each model.
            record["price"] = min(record["price"], CAR_CATALOG[record["type"]]["price"])
            safe_stock.append(record)
    base["stockCars"] = safe_stock
    # Durable location and current car state are saved on the server as well as locally.
    pos = raw.get("playerPosition")
    if isinstance(pos, dict):
        base["playerPosition"] = {
            "x": finite_number(pos.get("x"), 0, -10000, 10000),
            "y": finite_number(pos.get("y"), 0, 0, 100),
            "z": finite_number(pos.get("z"), 0, -10000, 10000),
            "yaw": finite_number(pos.get("yaw"), 0, -100000, 100000),
            "insideShop": bool(pos.get("insideShop", False)),
            "insideHome": bool(pos.get("insideHome", False)),
            "insideHomeSlot": int(finite_number(pos.get("insideHomeSlot"), -1, -1, 10000)),
        }
    vehicle = raw.get("activeVehicle")
    if isinstance(vehicle, dict) and vehicle.get("type") in CAR_CATALOG:
        base["activeVehicle"] = {
            "type": vehicle["type"],
            "color": vehicle.get("color") if isinstance(vehicle.get("color"), str) and re.fullmatch(r"#[0-9a-fA-F]{6}", vehicle.get("color")) else "#43c6e8",
            "x": finite_number(vehicle.get("x"), 0, -10000, 10000),
            "z": finite_number(vehicle.get("z"), 0, -10000, 10000),
            "yaw": finite_number(vehicle.get("yaw"), 0, -100000, 100000),
            "kind": "stock" if vehicle.get("kind") == "stock" else "personal",
            "stockId": re.sub(r"[^a-zA-Z0-9._:-]", "", str(vehicle.get("stockId") or ""))[:100] or None,
            "personalId": re.sub(r"[^a-zA-Z0-9._:-]", "", str(vehicle.get("personalId") or ""))[:100] or None,
            "driving": bool(vehicle.get("driving", False)),
        }
    else:
        base["activeVehicle"] = None
    delivery = raw.get("pendingDelivery")
    if isinstance(delivery, dict) and delivery.get("type") in CAR_CATALOG:
        base["pendingDelivery"] = {
            "id": re.sub(r"[^a-zA-Z0-9._:-]", "", str(delivery.get("id", "")))[:100],
            "type": delivery["type"],
            "color": delivery.get("color") if isinstance(delivery.get("color"), str) and re.fullmatch(r"#[0-9a-fA-F]{6}", delivery.get("color")) else "#43c6e8",
            "cost": int(finite_number(delivery.get("cost"), 0, 0, 2_000_000_000)),
            "createdAt": int(finite_number(delivery.get("createdAt"), 0, 0, 10**13)),
        }
    else:
        base["pendingDelivery"] = None
    # The key and slots are server-controlled; the browser cannot overwrite them in profile JSON.
    return base


def get_or_create_profile(client_key: str, name: str, initial_profile: Any = None) -> tuple[dict[str, Any], int, int]:
    now = time.time()
    with connect_db() as conn:
        row = conn.execute("SELECT home_slot, shop_slot, profile_json FROM profiles WHERE client_key=?", (client_key,)).fetchone()
        if row:
            try:
                profile = clean_profile(json.loads(row["profile_json"]))
            except (ValueError, TypeError):
                profile = default_profile(name)
            profile["name"] = clean_name(profile.get("name"), name)
            conn.execute("UPDATE profiles SET name=?, updated=? WHERE client_key=?", (profile["name"], now, client_key))
            return profile, int(row["home_slot"]), int(row["shop_slot"])
        used_homes = {int(r[0]) for r in conn.execute("SELECT home_slot FROM profiles").fetchall()}
        home_slot = next((i for i in range(MAX_HOUSES) if i not in used_homes), max(used_homes, default=-1) + 1)
        used_shops: dict[int, int] = {}
        for r in conn.execute("SELECT shop_slot, COUNT(*) as n FROM profiles GROUP BY shop_slot").fetchall():
            used_shops[int(r["shop_slot"])] = int(r["n"])
        shop_slot = next((i for i in range(MAX_SHOPS) if i not in used_shops), min(range(MAX_SHOPS), key=lambda i: used_shops.get(i, 0)))
        profile = clean_profile(initial_profile) if isinstance(initial_profile, dict) else default_profile(name)
        profile["name"] = clean_name(name, profile.get("name", name))
        profile["shopSlot"] = shop_slot
        conn.execute("INSERT INTO profiles(client_key, home_slot, shop_slot, profile_json, name, updated) VALUES(?,?,?,?,?,?)",
                     (client_key, home_slot, shop_slot, json.dumps(profile, separators=(",", ":")), name, now))
        return profile, home_slot, shop_slot


def persist_profile(client_key: str, raw: Any, display_name: str | None = None) -> dict[str, Any]:
    with connect_db() as conn:
        row = conn.execute("SELECT home_slot, shop_slot, profile_json, name FROM profiles WHERE client_key=?", (client_key,)).fetchone()
        if not row:
            raise KeyError("Profile not found")
        try:
            existing = json.loads(row["profile_json"])
        except (ValueError, TypeError):
            existing = default_profile(row["name"])
        clean = clean_profile(raw, existing)
        clean["name"] = clean_name(display_name, clean["name"]) if display_name else clean_name(clean.get("name"), row["name"])
        clean["shopSlot"] = int(row["shop_slot"])
        conn.execute("UPDATE profiles SET profile_json=?, name=?, updated=? WHERE client_key=?",
                     (json.dumps(clean, separators=(",", ":")), clean["name"], time.time(), client_key))
        return clean


def public_player(p: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in p.items() if k != "client_key"}


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


@app.on_event("startup")
async def startup_event() -> None:
    init_db()


@app.get("/", response_class=HTMLResponse)
async def home() -> HTMLResponse:
    if not INDEX_FILE.exists():
        return HTMLResponse("index.html is missing. Put it beside app.py.", status_code=500)
    return HTMLResponse(INDEX_FILE.read_text(encoding="utf-8"))


@app.get("/health")
async def health() -> JSONResponse:
    async with state_lock:
        count = len(clients)
        by_map = {"city": count}
    with connect_db() as conn:
        saved_profiles = conn.execute("SELECT COUNT(*) FROM profiles").fetchone()[0]
        active_listings = conn.execute("SELECT COUNT(*) FROM market_listings WHERE status='listed'").fetchone()[0]
    return JSONResponse({"ok": True, "players_online": count, "worlds": by_map, "saved_profiles": saved_profiles,
                         "market_listings": active_listings, "database": str(DB_PATH.name)})


@app.get("/api/profile")
async def load_profile(client_key: str) -> JSONResponse:
    key = clean_key(client_key)
    if not key:
        raise HTTPException(400, "Invalid client key")
    with connect_db() as conn:
        row = conn.execute("SELECT home_slot, shop_slot, profile_json FROM profiles WHERE client_key=?", (key,)).fetchone()
    if not row:
        return JSONResponse({"ok": False, "profile": None})
    try:
        profile = clean_profile(json.loads(row["profile_json"]))
    except (ValueError, TypeError):
        profile = default_profile()
    profile["shopSlot"] = int(row["shop_slot"])
    return JSONResponse({"ok": True, "profile": profile, "home_slot": row["home_slot"], "shop_slot": row["shop_slot"]})


@app.websocket("/ws")
async def multiplayer_socket(websocket: WebSocket) -> None:
    await websocket.accept()
    session_id = secrets.token_hex(8)
    player: dict[str, Any] = {
        "id": session_id, "name": f"Player{random.randint(100, 999)}", "color": random.choice(COLORS),
        "x": 0.0, "y": 0.0, "z": 0.0, "yaw": 0.0, "map": "city", "home_slot": -1,
        "home_locked": False, "house_owned": False, "shop_slot": -1, "shop_built": False, "vehicle": None,
        "client_key": None,
    }
    async with state_lock:
        clients[session_id] = websocket
        players[session_id] = player
        initial = [public_player(p) for p in players.values()]
    identified_key = ""
    try:
        await websocket.send_json({"type": "init", "id": session_id, "players": initial})
        await broadcast({"type": "join", "player": public_player(player)}, exclude_id=session_id)
        while True:
            raw = await websocket.receive_text()
            if len(raw) > 200_000:
                continue
            try:
                data = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(data, dict):
                continue
            kind = data.get("type")
            if kind == "identify":
                name = clean_name(data.get("name"), player["name"])
                key = clean_key(data.get("client_key"))
                # Save data comes from this browser's localStorage only. The server
                # allocates temporary property lots from currently connected players.
                raw_profile = data.get("profile") if isinstance(data.get("profile"), dict) else {}
                profile = dict(raw_profile)
                async with state_lock:
                    current = players.get(session_id)
                    if current is None:
                        break
                    used_homes = {int(p.get("home_slot", -1)) for pid, p in players.items() if pid != session_id and int(p.get("home_slot", -1)) >= 0}
                    used_shops = {int(p.get("shop_slot", -1)) for pid, p in players.items() if pid != session_id and int(p.get("shop_slot", -1)) >= 0}
                    home_slot = next((i for i in range(50) if i not in used_homes), -1)
                    shop_slot = next((i for i in range(30) if i not in used_shops), -1)
                    current.update({"name": name, "client_key": key or None, "home_slot": home_slot,
                                    "shop_slot": shop_slot, "home_locked": bool(profile.get("homeLocked", False)),
                                    "house_owned": bool(profile.get("homePurchased", False)),
                                    "shop_built": bool(profile.get("shopBuilt", False))})
                    updated = public_player(current)
                identified_key = key or "local-only"
                profile["homeSlot"] = home_slot
                profile["shopSlot"] = shop_slot
                await websocket.send_json({"type": "profile", "home_slot": home_slot, "shop_slot": shop_slot, "profile": profile})
                await broadcast({"type": "state", "player": updated})
                continue
            if kind == "save":
                # Intentionally ignore profile saves: money, inventory, house/shop
                # progress and player position are persisted only in browser localStorage.
                continue
            if kind != "state":
                continue
            map_name = data.get("map", "city")
            if map_name not in VALID_MAPS:
                map_name = "city"
            vehicle = data.get("vehicle")
            clean_vehicle = None
            if isinstance(vehicle, dict) and vehicle.get("type") in CAR_CATALOG:
                color = vehicle.get("color", "#3b82f6")
                if not isinstance(color, str) or not re.fullmatch(r"#[0-9a-fA-F]{6}", color):
                    color = "#3b82f6"
                clean_vehicle = {
                    "type": vehicle["type"], "color": color,
                    "x": finite_number(vehicle.get("x"), 0.0, -10000.0, 10000.0),
                    "z": finite_number(vehicle.get("z"), 0.0, -10000.0, 10000.0),
                    "yaw": finite_number(vehicle.get("yaw"), 0.0, -100000.0, 100000.0),
                    "driving": bool(vehicle.get("driving", False)),
                }
            async with state_lock:
                current = players.get(session_id)
                if current is None:
                    break
                current["x"] = finite_number(data.get("x"), current["x"], -500.0, 500.0)
                current["z"] = finite_number(data.get("z"), current["z"], -10000.0, 10000.0)
                current["y"] = finite_number(data.get("y"), current["y"], 0.0, 100.0)
                current["yaw"] = finite_number(data.get("yaw"), current["yaw"], -100000.0, 100000.0)
                current["map"] = "city"
                current["home_locked"] = bool(data.get("home_locked", current.get("home_locked", False)))
                current["house_owned"] = bool(data.get("house_owned", current.get("house_owned", False)))
                current["shop_slot"] = int(finite_number(data.get("shop_slot"), current.get("shop_slot", -1), -1, 10000))
                current["shop_built"] = bool(data.get("shop_built", current.get("shop_built", False)))
                current["vehicle"] = clean_vehicle
                state_message = {"type": "state", "player": public_player(current)}
            await broadcast(state_message, exclude_id=session_id)
    except WebSocketDisconnect:
        pass
    except Exception:
        # A malformed or disconnected client should not terminate the shared room.
        pass
    finally:
        async with state_lock:
            existed = session_id in players
            players.pop(session_id, None)
            clients.pop(session_id, None)
        if existed:
            await broadcast({"type": "leave", "id": session_id})


class MarketListRequest(BaseModel):
    client_key: str
    listing_id: str
    seller_name: str = "Player"
    type: str
    color: str = "#43c6e8"
    price: int = Field(ge=1, le=2_000_000_000)
    max_price: int = Field(ge=1, le=2_000_000_000)


class MarketUnlistRequest(BaseModel):
    client_key: str
    listing_id: str


class MarketOfferRequest(BaseModel):
    buyer_key: str
    listing_id: str
    offer: int = Field(ge=1, le=2_000_000_000)


class MarketRespondRequest(BaseModel):
    client_key: str
    offer_id: str
    accept: bool


@app.get("/api/market")
async def market_listings() -> JSONResponse:
    with connect_db() as conn:
        rows = conn.execute("SELECT listing_id,seller_key,seller_name,car_type,color,price,max_price,created FROM market_listings WHERE status='listed' ORDER BY created DESC LIMIT 100").fetchall()
    items = [{"listing_id": r["listing_id"], "seller_key": r["seller_key"], "seller_name": r["seller_name"],
              "type": r["car_type"], "car_name": CAR_CATALOG[r["car_type"]]["name"], "color": r["color"],
              "price": r["price"], "max_price": r["max_price"], "created": r["created"]} for r in rows]
    return JSONResponse(items)


@app.post("/api/market/list")
async def market_list(req: MarketListRequest) -> JSONResponse:
    key = clean_key(req.client_key)
    listing_id = re.sub(r"[^a-zA-Z0-9._:-]", "", req.listing_id)[:100]
    if not key or not listing_id or req.type not in CAR_CATALOG:
        raise HTTPException(400, "Invalid listing")
    limit = CAR_CATALOG[req.type]["price"]
    price = int(req.price)
    max_price = min(int(req.max_price), limit)
    if price > max_price:
        raise HTTPException(400, f"Price cannot exceed ${max_price}")
    color = req.color if re.fullmatch(r"#[0-9a-fA-F]{6}", req.color or "") else "#43c6e8"
    with connect_db() as conn:
        profile = conn.execute("SELECT name FROM profiles WHERE client_key=?", (key,)).fetchone()
        if not profile:
            raise HTTPException(403, "Identify your player before listing a car")
        conn.execute("""INSERT INTO market_listings(listing_id,seller_key,seller_name,car_type,color,price,max_price,status,created)
            VALUES(?,?,?,?,?,?,?,'listed',?) ON CONFLICT(listing_id) DO UPDATE SET
            seller_key=excluded.seller_key,seller_name=excluded.seller_name,car_type=excluded.car_type,color=excluded.color,
            price=excluded.price,max_price=excluded.max_price,status='listed',created=excluded.created""",
            (listing_id, key, clean_name(req.seller_name, profile["name"]), req.type, color, price, max_price, time.time()))
    return JSONResponse({"ok": True, "message": "تم نشر السيارة في سوق اللاعبين."})


@app.post("/api/market/unlist")
async def market_unlist(req: MarketUnlistRequest) -> JSONResponse:
    key = clean_key(req.client_key)
    listing_id = re.sub(r"[^a-zA-Z0-9._:-]", "", req.listing_id)[:100]
    with connect_db() as conn:
        conn.execute("UPDATE market_listings SET status='unlisted' WHERE listing_id=? AND seller_key=? AND status='listed'", (listing_id, key))
    return JSONResponse({"ok": True})


@app.post("/api/market/offer")
async def market_offer(req: MarketOfferRequest) -> JSONResponse:
    buyer_key = clean_key(req.buyer_key)
    listing_id = re.sub(r"[^a-zA-Z0-9._:-]", "", req.listing_id)[:100]
    with connect_db() as conn:
        listing = conn.execute("SELECT * FROM market_listings WHERE listing_id=? AND status='listed'", (listing_id,)).fetchone()
        buyer = conn.execute("SELECT name,profile_json FROM profiles WHERE client_key=?", (buyer_key,)).fetchone()
        if not listing or not buyer:
            raise HTTPException(404, "Listing or buyer profile not found")
        if listing["seller_key"] == buyer_key:
            raise HTTPException(400, "You cannot buy your own listing")
        if req.offer > int(listing["max_price"]):
            raise HTTPException(400, "Offer exceeds the permitted price")
        try:
            buyer_profile = clean_profile(json.loads(buyer["profile_json"]))
        except (TypeError, ValueError):
            buyer_profile = default_profile(buyer["name"])
        if int(buyer_profile["wallet"]) < req.offer:
            raise HTTPException(400, "Not enough cash for that offer")
        offer_id = secrets.token_hex(10)
        conn.execute("INSERT INTO market_offers(offer_id,listing_id,buyer_key,buyer_name,offer,status,created) VALUES(?,?,?,?,?,'pending',?)",
                     (offer_id, listing_id, buyer_key, buyer["name"], int(req.offer), time.time()))
    return JSONResponse({"ok": True, "offer_id": offer_id, "message": "أُرسل عرضك إلى البائع. انتظر موافقته أو رفضه."})


@app.get("/api/market/offers")
async def market_offers(client_key: str) -> JSONResponse:
    key = clean_key(client_key)
    if not key:
        raise HTTPException(400, "Invalid client key")
    with connect_db() as conn:
        rows = conn.execute("""SELECT o.offer_id,o.listing_id,o.buyer_name,o.offer,o.status,o.created,l.car_type,l.price,l.max_price
            FROM market_offers o JOIN market_listings l ON l.listing_id=o.listing_id
            WHERE l.seller_key=? ORDER BY o.created DESC LIMIT 50""", (key,)).fetchall()
    return JSONResponse([dict(r) for r in rows])


@app.get("/api/market/buying")
async def market_buying(client_key: str) -> JSONResponse:
    key = clean_key(client_key)
    if not key:
        raise HTTPException(400, "Invalid client key")
    with connect_db() as conn:
        rows = conn.execute("""SELECT o.offer_id,o.listing_id,o.offer,o.status,o.created,l.car_type,l.seller_name
            FROM market_offers o JOIN market_listings l ON l.listing_id=o.listing_id
            WHERE o.buyer_key=? ORDER BY o.created DESC LIMIT 100""", (key,)).fetchall()
    return JSONResponse([{"offer_id": r["offer_id"], "listing_id": r["listing_id"], "offer": r["offer"],
                          "status": r["status"], "created": r["created"], "type": r["car_type"],
                          "car_name": CAR_CATALOG[r["car_type"]]["name"], "seller_name": r["seller_name"]} for r in rows])


@app.post("/api/market/respond")
async def market_respond(req: MarketRespondRequest) -> JSONResponse:
    seller_key = clean_key(req.client_key)
    offer_id = re.sub(r"[^a-zA-Z0-9._:-]", "", req.offer_id)[:100]
    with connect_db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        offer = conn.execute("SELECT * FROM market_offers WHERE offer_id=? AND status='pending'", (offer_id,)).fetchone()
        if not offer:
            raise HTTPException(404, "Offer is no longer pending")
        listing = conn.execute("SELECT * FROM market_listings WHERE listing_id=?", (offer["listing_id"],)).fetchone()
        if not listing or listing["seller_key"] != seller_key or listing["status"] != "listed":
            raise HTTPException(403, "You do not own this active listing")
        if not req.accept:
            conn.execute("UPDATE market_offers SET status='declined' WHERE offer_id=?", (offer_id,))
            return JSONResponse({"ok": True, "accepted": False, "message": "تم رفض العرض."})
        buyer_row = conn.execute("SELECT profile_json,name FROM profiles WHERE client_key=?", (offer["buyer_key"],)).fetchone()
        seller_row = conn.execute("SELECT profile_json,name FROM profiles WHERE client_key=?", (seller_key,)).fetchone()
        if not buyer_row or not seller_row:
            raise HTTPException(404, "Profile not found")
        try:
            buyer_profile = clean_profile(json.loads(buyer_row["profile_json"]))
            seller_profile = clean_profile(json.loads(seller_row["profile_json"]))
        except (TypeError, ValueError):
            raise HTTPException(500, "Saved profile data is invalid")
        amount = int(offer["offer"])
        if buyer_profile["wallet"] < amount:
            conn.execute("UPDATE market_offers SET status='declined' WHERE offer_id=?", (offer_id,))
            return JSONResponse({"ok": False, "accepted": False, "message": "لم يعد رصيد المشتري كافياً."})
        buyer_profile["wallet"] -= amount
        seller_profile["wallet"] += amount
        # Transfer a distinct car instance to the buyer; it must be picked up in-world.
        ctype = listing["car_type"]
        personal_id = f"playerbuy-{listing['listing_id']}"
        buyer_profile["garageCars"] = [c for c in buyer_profile.get("garageCars", []) if c.get("id") != personal_id]
        buyer_profile["garageCars"].append({"id": personal_id, "type": ctype, "color": listing["color"],
                                             "price": min(amount, CAR_CATALOG[ctype]["price"]), "delivered": False,
                                             "primary": False, "x": None, "z": None})
        if not buyer_profile.get("primaryCarId"):
            buyer_profile["primaryCarId"] = personal_id
        buyer_profile = clean_profile(buyer_profile)
        for car in seller_profile.get("stockCars", []):
            if car.get("id") == listing["listing_id"]:
                car["sold"] = True
                car["listed"] = False
        conn.execute("UPDATE profiles SET profile_json=?,updated=? WHERE client_key=?",
                     (json.dumps(buyer_profile, separators=(",", ":")), time.time(), offer["buyer_key"]))
        conn.execute("UPDATE profiles SET profile_json=?,updated=? WHERE client_key=?",
                     (json.dumps(seller_profile, separators=(",", ":")), time.time(), seller_key))
        conn.execute("UPDATE market_listings SET status='sold' WHERE listing_id=?", (listing["listing_id"],))
        conn.execute("UPDATE market_offers SET status='accepted' WHERE offer_id=?", (offer_id,))
        return JSONResponse({"ok": True, "accepted": True, "message": f"تمت الصفقة مقابل ${amount:,}.", "profile": seller_profile,
                             "buyer_key": offer["buyer_key"]})
