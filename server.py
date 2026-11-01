#!/usr/bin/env python3
"""Радар опасности v2 — Leaflet/OSM, регионы, точки ПВО и дронов."""
from __future__ import annotations

import asyncio, os, re, time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response

POLL = int(os.getenv("POLL_SEC", "10"))
ALERT_TTL = int(os.getenv("ALERT_TTL_MIN", "240")) * 60
CLEAR_TTL = int(os.getenv("CLEAR_TTL_MIN", "30")) * 60

def parse_channels(s: str) -> dict:
    out = {}
    for part in s.split(","):
        part = part.strip().lstrip("@")
        if not part:
            continue
        ch, _, reg = part.partition("=")
        out[ch.strip()] = reg.strip() or None
    return out

CHANNELS = parse_channels(os.getenv("CHANNELS", ""))

_R_RAW = [
    ("Белгородская область", r"белгород"),
    ("Курская область", r"курск"),
    ("Брянская область", r"брянск"),
    ("Воронежская область", r"воронеж"),
    ("Ростовская область", r"ростовск|ростов-на-дону|\bрнду\b"),
    ("Краснодарский край", r"краснодар|кубан"),
    ("Республика Крым", r"\bкрым"),
    ("Севастополь", r"севастопол"),
    ("Москва", r"\bмоскв"),
    ("Московская область", r"(?<!ново)московск|подмосков"),
    ("Санкт-Петербург", r"петербург|\bспб\b"),
    ("Ленинградская область", r"ленинградск"),
    ("Тульская область", r"тульск|\bтула\b"),
    ("Орловская область", r"орловск|\bор[её]л\b"),
    ("Липецкая область", r"липецк"),
    ("Тамбовская область", r"тамбов"),
    ("Рязанская область", r"рязан"),
    ("Калужская область", r"калуж"),
    ("Смоленская область", r"смоленск"),
    ("Псковская область", r"псковск|\bпсков"),
    ("Новгородская область", r"новгородск"),
    ("Тверская область", r"тверск|\bтвер"),
    ("Ярославская область", r"ярослав"),
    ("Владимирская область", r"владимирск"),
    ("Ивановская область", r"ивановск|\bиваново\b"),
    ("Костромская область", r"костром"),
    ("Нижегородская область", r"нижегород|нижегородск"),
    ("Вологодская область", r"вологод"),
    ("Архангельская область", r"архангельск"),
    ("Мурманская область", r"мурманск"),
    ("Республика Карелия", r"карели|петрозаводск"),
    ("Республика Коми", r"\bкоми\b|сыктывкар"),
    ("Калининградская область", r"калининград"),
    ("Свердловская область", r"екатеринбург|свердловск(?:ая|ой|ую)\s*обл"),
    ("Челябинская область", r"челябинск"),
    ("Тюменская область", r"тюмен"),
    ("Омская область", r"\bомск"),
    ("Новосибирская область", r"новосибирск"),
    ("Красноярский край", r"красноярск"),
    ("Иркутская область", r"иркутск"),
    ("Приморский край", r"приморск|владивосток"),
    ("Хабаровский край", r"хабаровск"),
    ("Сахалинская область", r"сахалин"),
    ("Камчатский край", r"камчат"),
    ("Чукотский АО", r"чукот"),
    ("Республика Татарстан", r"татарстан|казан"),
    ("Башкортостан", r"башкир|башкорт|\bуф(а|ы|е)\b"),
    ("Пермский край", r"пермск|\bпермь\b"),
    ("Самарская область", r"самарск|\bсамар(а|е|ы)\b"),
    ("Саратовская область", r"саратов"),
    ("Волгоградская область", r"волгоград"),
    ("Астраханская область", r"астрахан"),
    ("Ставропольский край", r"ставропол"),
    ("Республика Дагестан", r"дагестан|махачкал"),
    ("Чечня", r"чечн|грозн"),
    ("Ингушетия", r"ингуш"),
    ("Северная Осетия", r"осети|владикавказ"),
    ("Кабардино-Балкария", r"кабардин|балкари|нальчик"),
    ("Адыгея", r"адыге|майкоп"),
    ("Донецкая область", r"\bднр\b|\bлднр\b|л\s*/\s*днр|донецк(ая|ой|ую)?\s*обл"),
    ("Луганская область", r"\bлнр\b|\bлднр\b|л\s*/\s*днр|луганск(ая|ой|ую)?\s*обл|должанск|\bсвердловск(?:е|а|у|ом)?\b"),
    ("Херсонская область", r"херсон(а|у|е|ом|ская|ской|скую)?(?!\w)"),
    ("Запорожская область", r"запорож(ье|ья|ью|ская|ской|скую)?"),
]
_seen = {}
for n, p in _R_RAW:
    _seen.setdefault(n, []).append(p)
REG = [(n, re.compile("|".join("(?:%s)" % p for p in ps), re.I)) for n, ps in _seen.items()]
NAMES = [n for n, _ in REG]

DRONE = re.compile(r"беспилотн|бпла|\bдроны?\b|\buav\b", re.I)
MISSILE = re.compile(r"ракетн", re.I)
AIR = re.compile(r"воздушн\w*\s*(тревог|опасност)?|воздушн\w*\s+угроз", re.I)
AVIA = re.compile(
    r"авиационн[\w-]*ракетн[\w-]*бомбов|авиационн\w*\s+ракетн\w*\s+бомбов|"
    r"авиационно[\w-]*ракетно[\w-]*бомбов|арб\s*опасност|авиационн\w*\s+опасност", re.I)
ALERTW = re.compile(r"опасност|тревог|угроз", re.I)
CLEAR = re.compile(r"отбо[йяе]", re.I)
CLEAR_ALL = re.compile(
    r"отбо[йяе].{0,60}по\s+всем|по\s+всем.{0,40}(ранее\s+)?объявлен|"
    r"отбо[йяе].{0,40}всех\s+(ранее\s+)?объявлен|отбо[йяе].{0,30}по\s+всей\s+страны|"
    r"отбо[йяе]\s+всем|глобальн\w*\s+отбо|отбо[йяе].{0,20}везде", re.I)
LEVEL_Y = re.compile(r"ж[её]лт\w*\s*уровен|уровен\w*\s*ж[её]лт|ж[её]лт(ая|ый)?\s*(опасност|тревог)?", re.I)
LEVEL_R = re.compile(r"красн\w*\s*уровен|уровен\w*\s*красн|красн(ая|ый)?\s*(опасност|тревог)?", re.I)
SPECIFIC = {
    "h": re.compile(r"хорнет|hornet", re.I),
    "t": re.compile(r"дартс|darts", re.I),
    "v": re.compile(r"мвш", re.I),
    "f": re.compile(r"фламинго|flamingo|фп-?5|fp-?5", re.I),
    "s": re.compile(r"storm\s*shadow|шторм(?:а|у|ом|е|ы)?(?:\s*ш[эе]доу)?|scalp|скальп", re.I),
}
PVO_RX = re.compile(
    r"пво\s+(работа|отработ|сби|открыл|вед[её]т)|работа\s+пво|сбит[ыао]?\s+(бпла|беспилот|дрон)|"
    r"зенит|отраж[её]н\w*\s+атак", re.I)
FIX_RX = re.compile(
    r"фиксац\w*\s+(бпла|беспилот|дрон)|зафиксирован\w*\s+(бпла|беспилот|дрон)|"
    r"(бпла|дрон)\w*\s+зафиксир|обнаружен\w*\s+(бпла|беспилот|дрон)|борт\w*\s+в\s+район", re.I)

ALL = ["d", "dy", "dr", "av", "m", "h", "t", "v", "f", "s"]
FAMILY = {"d": ["dy", "dr", "h", "t", "v"], "m": ["f", "s"]}

PLACES = {
    "белгород": (50.60, 36.59), "шебекино": (50.41, 36.90), "валуйки": (50.21, 38.10),
    "старый оскол": (51.30, 37.84), "губкин": (51.28, 37.55), "алексеевка": (50.63, 38.69),
    "новый оскол": (50.76, 37.87), "короча": (50.81, 37.19), "грайворон": (50.49, 35.67),
    "борисовка": (50.60, 36.02), "строитель": (50.78, 36.44),
    "курск": (51.73, 36.19), "суджа": (51.20, 35.27), "рыльск": (51.57, 34.68),
    "льгов": (51.66, 35.27), "обоянь": (51.21, 36.26), "фатеж": (52.09, 35.86),
    "щигры": (51.88, 36.91), "курчатов": (51.66, 35.65), "глушково": (51.34, 34.65),
    "коренево": (51.41, 34.90),
    "брянск": (53.25, 34.37), "клинцы": (52.76, 32.24), "новозыбков": (52.54, 31.94),
    "стародуб": (52.58, 32.76), "унеча": (52.84, 32.67), "почеп": (52.89, 33.45),
    "трубчевск": (52.58, 33.76), "севск": (52.15, 34.49),
    "воронеж": (51.67, 39.18), "борисоглебск": (51.37, 42.08), "россошь": (50.20, 39.57),
    "лиски": (50.98, 39.50), "острогожск": (50.87, 39.07),
    "ростов": (47.23, 39.72), "таганрог": (47.24, 38.90), "шахты": (47.71, 40.21),
    "новочеркасск": (47.42, 40.09), "волгодонск": (47.51, 42.15), "азов": (47.11, 39.42),
    "миллерово": (48.92, 40.40), "каменск": (48.32, 40.27),
    "краснодар": (45.04, 38.98), "сочи": (43.60, 39.73), "новороссийск": (44.72, 37.76),
    "анапа": (44.89, 37.32), "ейск": (46.71, 38.27),
    "симферополь": (44.95, 34.10), "керчь": (45.36, 36.47), "ялта": (44.50, 34.17),
    "евпатория": (45.20, 33.37), "севастополь": (44.62, 33.53), "джанкой": (45.71, 34.39),
    "донецк": (48.02, 37.80), "макеевка": (48.06, 37.93), "мариуполь": (47.10, 37.55),
    "горловка": (48.31, 38.05), "луганск": (48.57, 39.31), "алчевск": (48.47, 38.80),
    "херсон": (46.64, 32.62), "запорожье": (47.84, 35.14), "мелитополь": (46.85, 35.37),
    "бердянск": (46.76, 36.80), "энергодар": (47.50, 34.66),
    "москва": (55.75, 37.62), "тула": (54.19, 37.62), "орёл": (52.97, 36.07), "орел": (52.97, 36.07),
    "липецк": (52.61, 39.59), "тамбов": (52.72, 41.45),
}
REGION_CENTER = {
    "Белгородская область": (50.6, 36.6), "Курская область": (51.7, 36.2),
    "Брянская область": (53.2, 34.4), "Воронежская область": (51.7, 39.2),
    "Ростовская область": (47.2, 39.7), "Краснодарский край": (45.0, 39.0),
    "Республика Крым": (45.0, 34.5), "Севастополь": (44.6, 33.5),
    "Донецкая область": (48.0, 37.8), "Луганская область": (48.6, 39.3),
    "Херсонская область": (46.6, 32.6), "Запорожская область": (47.5, 35.5),
    "Москва": (55.75, 37.62), "Московская область": (55.7, 37.3),
}

STATE = {n: {k: [0, 0] for k in ALL} for n in NAMES}
POINTS = deque(maxlen=200)
EVENTS = deque(maxlen=80)
SEEN = set()
HEALTH = {}
POINT_SEEN = set()


def _kinds(low: str) -> set:
    kinds = set()
    if DRONE.search(low): kinds.add("d")
    if MISSILE.search(low): kinds.add("m")
    if AVIA.search(low): kinds.add("av")
    for k, rx in SPECIFIC.items():
        if rx.search(low): kinds.add(k)
    yl, rl = bool(LEVEL_Y.search(low)), bool(LEVEL_R.search(low))
    if ("d" in kinds or kinds & {"h", "t", "v"}) and (yl or rl):
        if rl: kinds.add("dr")
        if yl: kinds.add("dy")
        kinds.discard("d")
    if kinds & {"h", "t", "v"}: kinds.discard("d")
    if kinds & {"f", "s"}: kinds.discard("m")
    return kinds


def parse(text, default=None, has_media=False):
    out, carry, pending = [], None, []
    for seg in re.split(r"[\n.;!]+", text or ""):
        low = seg.lower().replace("ё", "е").strip()
        if not low:
            continue
        kinds = _kinds(low)
        clear = bool(CLEAR.search(low))
        regs = [n for n, rx in REG if rx.search(low)]
        is_alert = bool(kinds) and (
            bool(ALERTW.search(low))
            or bool(kinds & {"h", "t", "v", "f", "s", "av", "dy", "dr", "d", "m"})
        )
        if clear or is_alert:
            carry = (kinds or {"d", "m"}, 2 if clear else 1)
            if not regs:
                regs = pending or ([default] if default else [])
            pending = []
        elif regs and not carry:
            pending += [r for r in regs if r not in pending]
            continue
        elif not regs:
            carry, pending = None, []
        if regs and carry:
            ks, v = carry
            out += [(r, k, v) for r in regs for k in sorted(ks)]
    if has_media and not out:
        low_all = (text or "").lower().replace("ё", "е")
        if not CLEAR.search(low_all):
            regs = [n for n, rx in REG if rx.search(low_all)] or ([default] if default else [])
            kinds = _kinds(low_all) or {"d"}
            out += [(r, k, 1) for r in regs for k in sorted(kinds)]
    return list(dict.fromkeys(out))


def find_place(text: str):
    low = (text or "").lower().replace("ё", "е")
    for name in sorted(PLACES.keys(), key=len, reverse=True):
        if re.search(r"(?<![а-яa-z])%s(?![а-яa-z])" % re.escape(name), low):
            return name, PLACES[name]
    return None, None


def parse_points(text, post_id, ts):
    low = (text or "").lower().replace("ё", "е")
    kind = None
    if PVO_RX.search(low):
        kind = "pvo"
    elif FIX_RX.search(low):
        kind = "fix"
    if not kind:
        return
    place, coords = find_place(text)
    if not coords:
        for n, rx in REG:
            if rx.search(low) and n in REGION_CENTER:
                coords = REGION_CENTER[n]
                place = n
                break
    if not coords:
        return
    pid = "%s:%s:%s" % (post_id, kind, place)
    if pid in POINT_SEEN:
        return
    POINT_SEEN.add(pid)
    if len(POINT_SEEN) > 2000:
        POINT_SEEN.clear()
    lat, lon = coords
    POINTS.appendleft({
        "id": pid, "t": ts, "kind": kind, "lat": lat, "lon": lon,
        "label": place or "?", "region": place or "",
    })


def apply(post_id, ts, text, default=None, has_media=False):
    if post_id in SEEN:
        return
    SEEN.add(post_id)
    if time.time() - ts > ALERT_TTL:
        return
    low_all = (text or "").lower().replace("ё", "е")
    if CLEAR_ALL.search(low_all):
        for region, st in STATE.items():
            for kk, (vv, t0) in list(st.items()):
                if vv == 1 and ts >= t0:
                    STATE[region][kk] = [2, ts]
                    EVENTS.appendleft({"t": ts, "r": region, "k": kk, "v": 2})
        return
    for region, k, v in parse(text, default, has_media):
        if region not in STATE:
            continue
        targets = [k]
        if v == 2 and k in FAMILY:
            targets += [x for x in FAMILY[k] if STATE[region][x][0] == 1]
        for kk in targets:
            if ts >= STATE[region][kk][1]:
                STATE[region][kk] = [v, ts]
                EVENTS.appendleft({"t": ts, "r": region, "k": kk, "v": v})
    parse_points(text, post_id, ts)


def _media_urls(w):
    urls = []
    for a in w.select("a.tgme_widget_message_photo_wrap"):
        st = a.get("style") or ""
        m = re.search(r"url\(['\"]?(https?://[^'\")\s]+)", st)
        if m:
            urls.append(m.group(1))
    seen, out = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u); out.append(u)
    return out[:2]


async def fetch(client, ch):
    r = await client.get("https://t.me/s/%s" % ch)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    posts = []
    for w in soup.select("div.tgme_widget_message"):
        tm = w.select_one("a.tgme_widget_message_date time")
        if not (w.get("data-post") and tm and tm.get("datetime")):
            continue
        body = w.select_one("div.tgme_widget_message_text")
        text = ""
        if body:
            for br in body.select("br"):
                br.replace_with("\n")
            text = body.get_text() or ""
        media = _media_urls(w)
        has_media = bool(media) or bool(w.select_one("video"))
        if not text.strip() and not has_media:
            continue
        posts.append((w["data-post"], datetime.fromisoformat(tm["datetime"]).timestamp(), text, has_media))
    return sorted(posts, key=lambda p: p[1])


async def poller():
    headers = {"User-Agent": "Mozilla/5.0 (compatible; Radar/2.0)"}
    async with httpx.AsyncClient(timeout=20, headers=headers, follow_redirects=True) as c:
        while True:
            for ch, default in CHANNELS.items():
                try:
                    posts = await fetch(c, ch)
                    for pid, ts, text, has_media in posts:
                        apply(pid, ts, text, default, has_media)
                    HEALTH[ch] = {"ok": True, "posts": len(posts)}
                except Exception as e:
                    HEALTH[ch] = {"ok": False, "err": str(e)[:120]}
                await asyncio.sleep(1.5)
            await asyncio.sleep(POLL)


@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(poller())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=800)


def view_state():
    now = time.time()
    out = {}
    for n, st in STATE.items():
        row = {}
        for k in ALL:
            v, t0 = st[k]
            if v and now - t0 > (ALERT_TTL if v == 1 else CLEAR_TTL):
                v = 0
            row[k] = [v, int(now - t0) if v else 0]
        out[n] = row
    return out


@app.get("/api/state")
def api_state():
    now = time.time()
    pts = [p for p in POINTS if now - p["t"] < 6 * 3600]
    return JSONResponse({
        "regions": view_state(),
        "points": pts,
        "events": list(EVENTS)[:40],
        "channels": HEALTH,
        "configured": len(CHANNELS),
        "ts": int(now),
    }, headers={"Cache-Control": "no-store"})

HTML = """<!DOCTYPE html>
<html lang="ru" data-theme="dark">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>Радар опасности</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<style>
:root,:root[data-theme=dark]{--bg:#0b1117;--fg:#e6edf3;--mut:#8b98a5;--tile:#151c24;--bd:#2a3542;--panel:rgba(15,22,30,.92);color-scheme:dark}
:root[data-theme=light]{--bg:#f0f2f5;--fg:#1a1f26;--mut:#5c6b7a;--tile:#fff;--bd:#c9d2dc;--panel:rgba(255,255,255,.94);color-scheme:light}
*{box-sizing:border-box}
html,body{margin:0;height:100%;font:400 14px/1.35 system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:var(--bg);color:var(--fg)}
body{display:flex;flex-direction:column;overflow:hidden}
header{flex:0 0 auto;padding:10px 12px 8px;border-bottom:1px solid var(--bd);background:var(--panel);backdrop-filter:blur(10px);z-index:500}
h1{margin:0 0 8px;font-size:18px;font-weight:600}
.stats{display:flex;gap:8px;margin-bottom:8px}
.stat{flex:1;background:var(--tile);border:1px solid var(--bd);border-radius:12px;padding:10px;text-align:center}
.stat b{display:block;font-size:22px}.stat span{font-size:11px;color:var(--mut)}
.stat b.a{color:#ef4444}.stat b.c{color:#22c55e}
.chips{display:flex;gap:6px;overflow-x:auto;padding-bottom:4px}
.chip{flex:0 0 auto;border:1px solid var(--bd);background:var(--tile);color:var(--fg);border-radius:999px;padding:6px 10px;font-size:12px;cursor:pointer}
.chip.on{outline:2px solid #38bdf8}
.chip i{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:4px}
.tools{display:flex;gap:6px;margin-top:8px;flex-wrap:wrap}
.tools button{border:1px solid var(--bd);background:var(--tile);color:var(--fg);border-radius:10px;padding:8px 10px;font-size:12px;cursor:pointer}
#map{flex:1;min-height:0;z-index:1}
#panel{position:absolute;left:10px;bottom:10px;z-index:450;max-width:min(320px,92vw);max-height:40%;overflow:auto;background:var(--panel);border:1px solid var(--bd);border-radius:14px;padding:10px 12px;backdrop-filter:blur(10px)}
#panel h2{margin:0 0 6px;font-size:14px}
#panel .line{font-size:12px;margin:3px 0}
#log{position:absolute;right:10px;top:10px;z-index:450;font-size:11px;color:var(--mut);background:var(--panel);padding:6px 8px;border-radius:8px;border:1px solid var(--bd)}
.leaflet-container{background:#0d1520;font:inherit}
.mk-pvo,.mk-fix{font-size:14px;line-height:1;width:16px!important;height:16px!important;filter:drop-shadow(0 1px 1px rgba(0,0,0,.55));pointer-events:auto}
.legend-dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px}
</style>
</head>
<body>
<header>
  <h1>Радар опасности</h1>
  <div class="stats">
    <div class="stat"><b class="a" id="cA">0</b><span>опасность · регионов</span></div>
    <div class="stat"><b class="c" id="cC">0</b><span>отбой · регионов</span></div>
  </div>
  <div class="chips" id="chips"></div>
  <div class="tools">
    <button type="button" id="thm">тема</button>
    <button type="button" id="fit">вся карта</button>
    <button type="button" id="shot">скрин</button>
  </div>
</header>
<div id="map"></div>
<div id="panel" hidden>
  <h2 id="pname">—</h2>
  <div id="plines"></div>
</div>
<div id="log">загрузка…</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const CATS=[
  {k:"d",n:"БПЛА",a:"#f59e0b",chip:1},{k:"dy",n:"БПЛА · жёлтый",a:"#eab308"},{k:"dr",n:"БПЛА · красный",a:"#ef4444"},
  {k:"av",n:"Авиация",a:"#0ea5e9",chip:1},{k:"m",n:"Ракеты",a:"#ef4444",chip:1},
  {k:"h",n:"Hornet",a:"#a855f7",chip:1},{k:"t",n:"Darts",a:"#14b8a6",chip:1},{k:"v",n:"МВШ",a:"#6366f1",chip:1},
  {k:"f",n:"Фламинго",a:"#ec4899",chip:1},{k:"s",n:"Storm Shadow",a:"#eab308",chip:1}
];
let flt="", S={}, geoLayer=null, markersLayer=null, map, selected=null;

function vis(k){
  if(!flt)return true;
  if(flt===k)return true;
  if(flt==="d"&&["d","dy","dr","h","t","v"].includes(k))return true;
  if(flt==="m"&&["m","f","s"].includes(k))return true;
  return false;
}
function regionCols(n){
  const st=S[n]||{}, cols=[];
  CATS.forEach(c=>{const v=(st[c.k]||[0])[0]; if(v===1&&vis(c.k)) cols.push(c.a); else if(v===2&&vis(c.k)) cols.push("#22c55e");});
  return cols;
}
function styleFeature(f){
  const n=f.properties.name;
  const cols=regionCols(n);
  return {
    color: selected===n ? "#fff" : "#64748b",
    weight: selected===n ? 2.2 : 1.1,
    fillColor: cols[0] || "#1a2330",
    fillOpacity: cols.length ? 0.48 : 0.18
  };
}
function buildChips(){
  const el=document.getElementById("chips"); el.innerHTML="";
  function add(k,label,col){
    const b=document.createElement("button"); b.type="button"; b.className="chip"+(flt===k?" on":"");
    b.innerHTML=(col?('<i style="background:'+col+'"></i>'):"")+label;
    b.onclick=()=>{flt=flt===k?"":k; buildChips(); if(geoLayer) geoLayer.setStyle(styleFeature);};
    el.appendChild(b);
  }
  add("","Все",null);
  CATS.filter(c=>c.chip).forEach(c=>add(c.k,c.n,c.a));
}
function showRegion(n){
  selected=n; if(geoLayer) geoLayer.setStyle(styleFeature);
  const panel=document.getElementById("panel"); panel.hidden=false;
  document.getElementById("pname").textContent=n;
  const st=S[n]||{}, lines=[];
  CATS.forEach(c=>{
    const v=(st[c.k]||[0])[0], age=(st[c.k]||[0,0])[1];
    if(!v)return;
    lines.push('<div class="line"><span class="legend-dot" style="background:'+(v===1?c.a:"#22c55e")+'"></span>'+(v===1?"Опасность: ":"Отбой: ")+c.n+(age?(" · "+Math.round(age/60)+" мин"):"")+"</div>");
  });
  document.getElementById("plines").innerHTML=lines.join("")||'<div class="line">Сейчас спокойно</div>';
}
function clearSelect(){ selected=null; if(geoLayer) geoLayer.setStyle(styleFeature); document.getElementById("panel").hidden=true; }
function countStats(){
  let A=0,C=0;
  Object.keys(S).forEach(n=>{
    let a=false,c=false;
    CATS.forEach(cat=>{const v=(S[n][cat.k]||[0])[0]; if(v===1&&vis(cat.k))a=true; if(v===2&&vis(cat.k))c=true;});
    if(a)A++; else if(c)C++;
  });
  document.getElementById("cA").textContent=A; document.getElementById("cC").textContent=C;
}
function renderPoints(pts){
  if(!markersLayer) markersLayer=L.layerGroup().addTo(map); else markersLayer.clearLayers();
  (pts||[]).forEach(p=>{
    const emoji=p.kind==="pvo"?"💥":"🚁";
    const icon=L.divIcon({className:p.kind==="pvo"?"mk-pvo":"mk-fix", html:emoji, iconSize:[16,16], iconAnchor:[8,8]});
    const m=L.marker([p.lat,p.lon],{icon});
    const when=new Date(p.t*1000).toLocaleTimeString("ru",{hour:"2-digit",minute:"2-digit"});
    m.bindPopup("<b>"+(p.kind==="pvo"?"ПВО":"Фиксация дрона")+"</b><br>"+p.label+"<br>"+when);
    markersLayer.addLayer(m);
  });
}
async function loadState(){
  try{
    const j=await (await fetch("/api/state",{cache:"no-store"})).json();
    S=j.regions||{}; countStats();
    if(geoLayer) geoLayer.setStyle(styleFeature);
    renderPoints(j.points||[]);
    const ok=Object.values(j.channels||{}).filter(x=>x&&x.ok).length;
    document.getElementById("log").textContent="обновлено "+new Date().toLocaleTimeString()+" · каналов: "+ok+"/"+(j.configured||0);
  }catch(e){ document.getElementById("log").textContent="ошибка загрузки"; }
}
function matchRegionName(geoName){
  if(!geoName) return geoName;
  if(S[geoName]) return geoName;
  const low=geoName.toLowerCase();
  for(const n of Object.keys(S)){ if(n.toLowerCase()===low) return n; }
  for(const n of Object.keys(S)){ if(low.includes(n.toLowerCase())||n.toLowerCase().includes(low)) return n; }
  return geoName;
}
async function loadGeo(){
  const urls=[
    "https://raw.githubusercontent.com/codeforgermany/click_that_hood/main/public/data/russia.geojson",
    "https://cdn.jsdelivr.net/gh/codeforgermany/click_that_hood@master/public/data/russia.geojson"
  ];
  let data=null;
  for(const u of urls){
    try{ const r=await fetch(u); if(r.ok){ data=await r.json(); break; } }catch(e){}
  }
  if(!data){ document.getElementById("log").textContent="не удалось загрузить границы"; return; }
  const skip=/чукот|chukot|чукчи/i;
  data.features=data.features.filter(f=>{
    const p=f.properties||{};
    const nm=(p.name||p.NAME||p.NAME_RU||p.region||"")+"";
    return !skip.test(nm);
  });
  data.features.forEach(f=>{
    const p=f.properties||{};
    f.properties.name=matchRegionName(p.name||p.NAME||p.NAME_RU||p.region||"?");
  });
  geoLayer=L.geoJSON(data,{
    style: styleFeature,
    onEachFeature:(f,layer)=>{
      layer.on("click", e=>{ L.DomEvent.stopPropagation(e); showRegion(f.properties.name); });
    }
  }).addTo(map);
  // старт около ЦФО, не на всю страну
  map.setView([54.5,37.5],6);
}
function initMap(){
  map=L.map("map",{zoomControl:true}).setView([54.5,37.5],6);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",{
    attribution:"&copy; OpenStreetMap", maxZoom:18, subdomains:"abc",
    className: "osm-tiles"
  }).addTo(map);
  // тёмный фильтр поверх OSM
  var st=document.createElement("style");
  st.textContent=".osm-tiles{filter:invert(1) hue-rotate(180deg) brightness(0.85) contrast(0.95);}";
  document.head.appendChild(st);
  map.on("click", clearSelect);
}
document.getElementById("thm").onclick=()=>{
  const cur=document.documentElement.getAttribute("data-theme")==="light"?"light":"dark";
  const next=cur==="dark"?"light":"dark";
  document.documentElement.setAttribute("data-theme",next);
  localStorage.setItem("theme",next);
};
document.getElementById("fit").onclick=()=>{ map.setView([54.5,37.5],6); };
(function(){ const t=localStorage.getItem("theme"); if(t==="light"||t==="dark") document.documentElement.setAttribute("data-theme",t); })();

document.getElementById("shot").onclick=async function(){
  try{
    const mapEl=document.getElementById("map");
    const origin=mapEl.getBoundingClientRect();
    const w=Math.floor(origin.width), h=Math.floor(origin.height);
    const c=document.createElement("canvas");
    const dpr=Math.min(2,window.devicePixelRatio||1);
    c.width=w*dpr; c.height=h*dpr;
    const ctx=c.getContext("2d");
    ctx.scale(dpr,dpr);
    ctx.fillStyle=getComputedStyle(document.documentElement).getPropertyValue("--bg").trim()||"#0b1117";
    ctx.fillRect(0,0,w,h);
    mapEl.querySelectorAll("img.leaflet-tile").forEach(img=>{
      if(!img.complete||!img.naturalWidth)return;
      const r=img.getBoundingClientRect();
      try{ ctx.drawImage(img, r.left-origin.left, r.top-origin.top, r.width, r.height); }catch(e){}
    });
    const svg=mapEl.querySelector("svg.leaflet-zoom-animated");
    if(svg){
      const xml=new XMLSerializer().serializeToString(svg);
      const url=URL.createObjectURL(new Blob([xml],{type:"image/svg+xml;charset=utf-8"}));
      await new Promise(res=>{
        const im=new Image();
        im.onload=()=>{ const sr=svg.getBoundingClientRect(); ctx.drawImage(im, sr.left-origin.left, sr.top-origin.top, sr.width, sr.height); URL.revokeObjectURL(url); res(); };
        im.onerror=()=>{URL.revokeObjectURL(url);res();};
        im.src=url;
      });
    }
    mapEl.querySelectorAll(".mk-pvo,.mk-fix").forEach(el=>{
      const r=el.getBoundingClientRect();
      ctx.font="14px serif"; ctx.textAlign="center"; ctx.textBaseline="middle";
      ctx.fillText(el.textContent||"", r.left-origin.left+r.width/2, r.top-origin.top+r.height/2);
    });
    ctx.fillStyle="rgba(255,255,255,.85)";
    ctx.font="600 12px system-ui,sans-serif";
    ctx.textAlign="right";
    ctx.fillText("t.me/peace_ptica", w-10, h-12);
    c.toBlob(blob=>{
      const a=document.createElement("a");
      a.href=URL.createObjectURL(blob);
      a.download="radar-"+new Date().toISOString().slice(0,19).replace(/[:T]/g,"-")+".png";
      a.click();
      setTimeout(()=>URL.revokeObjectURL(a.href),2000);
    },"image/png");
  }catch(e){ alert("Не удалось сделать скрин"); console.warn(e); }
};

buildChips(); initMap(); loadGeo().then(loadState); setInterval(loadState,10000);
</script>
</body>
</html>
"""

@app.get("/")
def index():
    return HTMLResponse(HTML)

@app.get("/manifest.json")
def manifest():
    return {"name": "Радар опасности", "short_name": "Радар", "start_url": "/", "display": "standalone",
            "background_color": "#0b1117", "theme_color": "#0b1117"}

@app.get("/icon.svg")
def icon():
    svg = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><rect width="100" height="100" rx="20" fill="#0b1117"/><text x="50" y="64" text-anchor="middle" font-size="40">💥</text></svg>'
    return Response(svg, media_type="image/svg+xml")
