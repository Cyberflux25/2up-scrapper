#!/usr/bin/env python3
"""
Cleaned & patched 2UP API scraper.

Usage:
    python scrape_2up_api_only.py --out out.json --max 20
        [--hours 48] [--page-size 50] [--start-page 1]
        [--cookies "name=value; ..."] [--sign SIGN] [--ts TIMESTAMP]
        [--verbose]

Environment fallbacks:
    TWOUP_COOKIES, TWOUP_SIGN, TWOUP_TS

Notes:
    - This script calls POST https://2up.io/api/sportProtal/web/event/date/list?eventDateList
    - Provide cookies + X-Request-Sign + X-Request-Timestamp if the server requires them.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter, Retry

DEFAULT_OUTPUT_DIR = Path(os.environ.get("BANGERSURE_OUTPUT_DIR", r"C:\xampp\htdocs\bangersure.com\bangersure-App\data"))

API_URL = "https://2up.io/api/sportProtal/web/event/date/list?eventDateList"
REFERER_URL = "https://2up.io/pt/sports/home?section=upcoming&sport=soccer"
BOOKMAKER_KEY = "2up"

# env / hardcoded defaults
HARDCODE_COOKIES = os.getenv("TWOUP_COOKIES", "")
HARDCODE_SIGN = os.getenv("TWOUP_SIGN", "")
HARDCODE_TS = os.getenv("TWOUP_TS", "")

# emojis & small helpers
EMO_OK = "✅"
EMO_RUN = "🚀"
EMO_WARN = "⚠️"
EMO_ERR = "❌"
EMO_PAGE = "📄"
EMO_MATCH = "🏟️"
EMO_SAVE = "💾"


def info(msg: str) -> None:
    print(f"{EMO_RUN} {msg}")


def ok(msg: str) -> None:
    print(f"{EMO_OK} {msg}")


def warn(msg: str) -> None:
    print(f"{EMO_WARN} {msg}")


def err(msg: str) -> None:
    print(f"{EMO_ERR} {msg}")


def format_decimal_str(value: Any) -> Optional[str]:
    """Normalize numeric/decimal strings to a simple 3-decimal string or None."""
    try:
        if value is None:
            return None
        if isinstance(value, str):
            value = value.replace(",", ".")
        val = float(value)
        return f"{val:.3f}"
    except Exception:
        return None


def compute_match_id(home: str, away: str, date_iso: Optional[str]) -> int:
    key = f"{home}__{away}__{date_iso or ''}"
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return int(digest[:10], 16)


def build_item_skeleton_from_api(event_node: Dict[str, Any]) -> Dict[str, Any]:
    home = str(event_node.get("homeTeamName") or "")
    away = str(event_node.get("awayTeamName") or "")
    event_time = event_node.get("eventTime")
    date_iso = None
    try:
        if isinstance(event_time, str) and event_time.isdigit():
            event_time = int(event_time)
        if isinstance(event_time, (int, float)):
            dt = datetime.fromtimestamp(int(event_time) / 1000.0, tz=timezone.utc)
            date_iso = dt.isoformat().replace("+00:00", "Z")
    except Exception:
        date_iso = None

    league = str(event_node.get("leagueName") or "")
    sport_slug = str(event_node.get("sportUrl") or "soccer")
    region = str(event_node.get("regionUrl") or "")
    league_url = str(event_node.get("leagueUrl") or "")
    event_url = str(event_node.get("eventUrl") or "")
    pretty_url = (
        f"https://2up.io/pt/sports/{sport_slug}/{region}/{league_url}/{event_url}"
        if event_url
        else ""
    )
    event_id_str = str(event_node.get("eventId") or "")
    if event_id_str:
        match_id = int(hashlib.sha256(event_id_str.encode("utf-8")).hexdigest()[:10], 16)
    else:
        match_id = compute_match_id(home, away, date_iso)
    return {
        "id": match_id,
        "home": home,
        "away": away,
        "date": date_iso or "",
        "sport": {"name": "Football", "slug": "soccer"},
        "league": {"name": league, "slug": league.lower().replace(" ", "-") if league else ""},
        "urls": {BOOKMAKER_KEY: pretty_url},
        "bookmakers": {BOOKMAKER_KEY: []},
        "status": "pending",
    }


def extract_markets_from_api(event_node: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract ML (moneyline), Totals, and Handicap (Asian Handicap) markets into a normalized structure.
    Keep the same core logic you provided, but a bit cleaned up.
    """
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    markets_out: List[Dict[str, Any]] = []
    all_markets = event_node.get("markets") or []
    ml_done = False
    totals_lines: Dict[float, Dict[str, Any]] = {}
    handicap_lines: Dict[float, Dict[str, Any]] = {}

    # Optional: quick event label for logs
    event_name = event_node.get("eventName") or f"{event_node.get('homeTeamName','')} vs {event_node.get('awayTeamName','')}"

    for m in all_markets:
        name = str(m.get("name") or "").lower()
        mtid = str(m.get("marketTypeId") or "").lower()
        selections = m.get("selections") or []

        # FT 1X2 (Moneyline) - APENAS mercado principal, não Double Chance
        # PRIORIDADE: nome exato "ft 1x2" ou mtid "ml0" (mais confiável)
        is_1x2_main = False
        is_main_confirmed = False
        
        # Verifica se é mercado 1X2 principal confirmado
        if name == "ft 1x2" or mtid == "ml0":
            if "double" not in name and "chance" not in name:
                is_1x2_main = True
                # Nome exato "ft 1x2" ou mtid "ml0" = mercado principal confirmado
                is_main_confirmed = True
        
        if not ml_done and is_1x2_main:
            home_p = draw_p = away_p = None
            for s in selections:
                outcome = str(s.get("outcomeType") or "").lower()
                dec = s.get("trueOdds") or (s.get("displayOdds") or {}).get("Decimal")
                price = format_decimal_str(dec)
                if not price:
                    continue
                if outcome == "home":
                    home_p = price
                elif outcome in ("tie", "draw"):
                    draw_p = price
                elif outcome == "away":
                    away_p = price
            # Só adiciona se tiver todas as 3 odds (casa, empate, fora)
            # E se for mercado principal confirmado (prioridade)
            if home_p and draw_p and away_p:
                # Se já existe ML e este não é principal confirmado, não sobrescreve
                # Se é principal confirmado ou não existe ML, adiciona
                existing_ml = next((m for m in markets_out if m.get("name") == "ML"), None)
                if is_main_confirmed or not existing_ml:
                    if existing_ml:
                        # Remove o anterior se este é principal confirmado
                        markets_out = [m for m in markets_out if m.get("name") != "ML"]
                    markets_out.append({"name": "ML", "updatedAt": now_iso, "odds": [{"home": home_p, "draw": draw_p, "away": away_p}]})
                    ml_done = True
            continue

        # FT O/U (Totals)
        if (name == "ft o/u" or mtid == "ou0"):
            for s in selections:
                ou = str(s.get("outcomeType") or s.get("name") or "").lower()
                points = s.get("points")
                try:
                    if isinstance(points, str):
                        points = float(points.replace(",", "."))
                    elif isinstance(points, (int, float)):
                        points = float(points)
                    else:
                        continue
                except Exception:
                    continue
                dec = s.get("trueOdds") or (s.get("displayOdds") or {}).get("Decimal")
                price = format_decimal_str(dec)
                if not price:
                    continue
                rec = totals_lines.setdefault(points, {"hdp": points, "over": None, "under": None})
                if "over" in ou:
                    rec["over"] = price
                elif "under" in ou:
                    rec["under"] = price
            continue

        # FT Asian Handicap
        if (name == "ft asian handicap" or mtid == "hc0"):
            home_pts = None
            home_price = None
            away_pts = None
            away_price = None
            for s in selections:
                outcome = str(s.get("outcomeType") or "").lower()
                points = s.get("points")
                try:
                    if isinstance(points, str):
                        # normalize unicode minus and commas
                        pts_str = points.replace(",", ".").replace("−", "-")
                        points = float(pts_str)
                    elif isinstance(points, (int, float)):
                        points = float(points)
                    else:
                        continue
                except Exception:
                    continue
                dec = s.get("trueOdds") or (s.get("displayOdds") or {}).get("Decimal")
                price = format_decimal_str(dec)
                if not price:
                    continue
                if outcome == "home":
                    home_pts = points
                    home_price = price
                elif outcome == "away":
                    away_pts = points
                    away_price = price
            if home_pts is not None and away_pts is not None and home_price and away_price:
                handicap_lines[float(home_pts)] = {"hdp": float(home_pts), "home": home_price, "away": away_price}
            continue

    # Emit collected markets
    totals_out = [v for v in totals_lines.values() if v.get("over") and v.get("under")]
    if totals_out:
        totals_out.sort(key=lambda x: x["hdp"])
        markets_out.append({"name": "Totals", "updatedAt": now_iso, "odds": totals_out})
    if handicap_lines:
        lines = list(handicap_lines.values())
        lines.sort(key=lambda x: x["hdp"])
        markets_out.append({"name": "Handicap", "updatedAt": now_iso, "odds": lines})

    # Log a small per-match summary
    try:
        ml_present = any(m.get("name") == "ML" for m in markets_out)
        totals_cnt = next((len(m.get("odds", [])) for m in markets_out if m.get("name") == "Totals"), 0)
        handicap_cnt = next((len(m.get("odds", [])) for m in markets_out if m.get("name") == "Handicap"), 0)
        ok(f"{EMO_MATCH} {event_name}: ML={'yes' if ml_present else 'no'} | Totals={totals_cnt} | Handicap={handicap_cnt}")
    except Exception:
        pass

    return markets_out


def make_session_with_retries(retries: int = 3, backoff: float = 0.3) -> requests.Session:
    s = requests.Session()
    retries_cfg = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
    )
    s.mount("https://", HTTPAdapter(max_retries=retries_cfg))
    s.mount("http://", HTTPAdapter(max_retries=retries_cfg))
    return s


def build_headers(cookies: str, x_sign: str, x_ts: str) -> Dict[str, str]:
    # Apply overrides, falling back to hardcoded env values
    if not cookies and HARDCODE_COOKIES:
        cookies = HARDCODE_COOKIES
    if not x_sign and HARDCODE_SIGN:
        x_sign = HARDCODE_SIGN
    if not x_ts and HARDCODE_TS:
        x_ts = HARDCODE_TS

    headers: Dict[str, str] = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Content-Type": "application/json",
        # Some custom headers observed in the browser traffic:
        "Lang": "pt",
        "Odds": "Decimal",
        "Origin": "https://2up.io",
        "Priority": "u=1, i",
        "Referer": REFERER_URL,
        "Sec-Ch-Ua": "\"Chromium\";v=\"142\", \"Google Chrome\";v=\"142\", \"Not A Brand\";v=\"99\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Socketclientid": "a1a51a18-d1de-44d1-8a36-ff739020987d",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "Zoneoffset": "0",
    }
    # Only set sensitive headers if values are provided
    if cookies:
        headers["Cookie"] = cookies
    if x_sign:
        headers["X-Request-Sign"] = x_sign
    if x_ts:
        headers["X-Request-Timestamp"] = x_ts
    return headers


def scrape_api_only(
    output_path: str,
    max_matches: Optional[int] = None,  # None = sem limite
    hours_ahead: int = 48,
    page_size: int = 50,
    page_num_start: int = 1,
    cookies: str = "",
    x_sign: str = "",
    x_ts: str = "",
    verbose: bool = False,
    exhaust: bool = True,  # Sempre True para coletar tudo
) -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    window_ms = int(hours_ahead * 3600 * 1000)
    start_ms = int(now.timestamp() * 1000)
    end_ms = start_ms + window_ms

    results: List[Dict[str, Any]] = []
    seen_event_ids: set[str] = set()

    session = make_session_with_retries()
    headers = build_headers(cookies, x_sign, x_ts)
    if verbose:
        info(f"{EMO_PAGE} Headers preview:\n{json.dumps(headers, ensure_ascii=False, indent=2)}")
    session.headers.update(headers)

    if headers.get("Cookie"):
        info("Using cookies header 🍪")
    if headers.get("X-Request-Sign") and headers.get("X-Request-Timestamp"):
        info("Using X-Request-Sign and X-Request-Timestamp 🔐")

    window_idx = 0
    # Iterate windows until empty (sem limite de jogos)
    while True:
        window_idx += 1
        human_start = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        human_end = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        info(f"Window #{window_idx} {human_start} → {human_end}")

        page = page_num_start
        got_any_in_window = False
        while True:
            # Sem limite - coleta TODOS os jogos disponíveis
            payload = {
                "isLive": 0,
                "pageType": 5,
                "sportUrl": "soccer",
                "regionUrl": "",
                "leagueUrl": "",
                "startTime": start_ms,
                "endTime": end_ms,
                "pageSize": page_size,  # Sem limite no pageSize
                "pageNum": page,
            }
            info(f"Fetching page {page} … {EMO_PAGE}")
            if verbose:
                print(f"{EMO_PAGE} Payload: {json.dumps(payload)}")

            try:
                resp = session.post(API_URL, json=payload, timeout=30)
            except Exception as e:
                err(f"Request error: {e}")
                raise

            print(f"{('✅' if resp.status_code == 200 else '❌')} HTTP {resp.status_code}")
            if resp.status_code != 200:
                err(f"API status {resp.status_code}: {resp.text[:400]}")
                warn("Tip: Pass your current browser cookies (and X-Request-Sign/Timestamp if present) via --cookies --sign --ts")
                raise RuntimeError(f"API status {resp.status_code}")

            try:
                data = resp.json()
            except Exception as e:
                err(f"Failed to parse JSON response: {e}")
                raise

            if not isinstance(data, dict) or str(data.get("code")) != "200":
                err(f"API envelope error: {str(data)[:400]}")
                warn("Server returned code != 200. This endpoint may require signed headers.")
                warn('Try: python scrape_2up_api_only.py --out out.json --cookies="<paste cookies>" --sign="<X-Request-Sign>" --ts="<X-Request-Timestamp>"')
                raise RuntimeError("API envelope error")

            items = (((data.get("data") or {}).get("items")) or [])
            ok(f"Received {len(items)} items on page {page}")

            if not items:
                break

            # Collect unique events (sem limite)
            for ev in items:
                ev_id = str(ev.get("eventId") or "")
                if ev_id and ev_id in seen_event_ids:
                    continue
                # Sem verificação de limite - adiciona todos
                item = build_item_skeleton_from_api(ev)
                markets = extract_markets_from_api(ev)
                if markets:
                    item["bookmakers"][BOOKMAKER_KEY] = markets
                results.append(item)
                if ev_id:
                    seen_event_ids.add(ev_id)
                got_any_in_window = True

            total_pages = int((data.get("data") or {}).get("totalPages") or 1)
            page_now = int((data.get("data") or {}).get("page") or page)
            if page_now >= total_pages:
                break
            page += 1

        # Stop if nothing returned in this window (sem limite de jogos)
        if not got_any_in_window:
            break
        # Advance window
        start_ms = end_ms + 1
        end_ms = start_ms + window_ms

    # Sem truncamento - retorna TODOS os resultados
    
    # Usa o diretório padrão se output_path não for absoluto
    if not os.path.isabs(output_path):
        DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = str(DEFAULT_OUTPUT_DIR / "2up_output_data.json")
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
    ok(f"{EMO_SAVE} Wrote {len(results)} matches to {output_path}")
    return results


def parse_args():
    p = argparse.ArgumentParser(description="Scrape upcoming soccer events from 2up.io (API-only).")
    p.add_argument("--out", "-o", default=None, help="Output JSON file path (default: 2up_output_data.json in BANGERSURE_OUTPUT_DIR)")
    p.add_argument("--max", "-m", type=int, default=None, help="Maximum number of matches to fetch (default: None = sem limite, coleta TODOS)")
    p.add_argument("--hours", type=int, default=48, help="Hours ahead window to fetch")
    p.add_argument("--page-size", type=int, default=50, help="Page size for API requests")
    p.add_argument("--start-page", type=int, default=1, help="Start page number")
    p.add_argument("--cookies", type=str, default="", help='Cookies string (e.g. "name=value; name2=value2")')
    p.add_argument("--sign", type=str, default="", help="X-Request-Sign header value")
    p.add_argument("--ts", type=str, default="", help="X-Request-Timestamp header value")
    p.add_argument("--verbose", action="store_true", help="Verbose output")
    p.add_argument("--exhaust", action="store_true", help="Keep advancing time windows until no items are returned")
    return p.parse_args()


def main():
    args = parse_args()
    # Se não foi especificado output, usa o padrão
    output_path = args.out
    if output_path is None:
        DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = str(DEFAULT_OUTPUT_DIR / "2up_output_data.json")
    
    # Loop infinito
    loop_count = 0
    while True:
        loop_count += 1
        print(f"\n{'='*60}")
        print(f"🔄 2UP SCRAPER - Execução #{loop_count}")
        print(f"{'='*60}\n")
        
        try:
            max_matches = args.max if args.max is not None else None
            info(f"Starting API-only scrape → out={output_path}, max={'TODOS (sem limite)' if max_matches is None else max_matches}")
            scrape_api_only(
                output_path=output_path,
                max_matches=max_matches,
                hours_ahead=args.hours,
                page_size=args.page_size,
                page_num_start=args.start_page,
                cookies=args.cookies,
                x_sign=args.sign,
                x_ts=args.ts,
                verbose=args.verbose,
                exhaust=True,  # Sempre True para coletar TODOS os dados
            )
            print(f"\n✅ Execução #{loop_count} concluída com sucesso!")
        except KeyboardInterrupt:
            print(f"\n\n⚠️  Interrompido pelo usuário. Encerrando...")
            sys.exit(0)
        except Exception as e:
            err(f"Erro na execução #{loop_count}: {e}")
            print(f"⏳ Aguardando 30 segundos antes de tentar novamente...")
            time.sleep(30)
        
        # Aguarda antes de reiniciar
        wait_time = 60  # 60 segundos entre execuções
        print(f"\n⏸️  Aguardando {wait_time} segundos antes da próxima execução...")
        print(f"   (Pressione Ctrl+C para encerrar)\n")
        time.sleep(wait_time)


if __name__ == "__main__":
    main()
