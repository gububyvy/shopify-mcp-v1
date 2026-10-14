"""Lokale aanvalstest voor de secret-path-beveiliging van shopify-mcp-v1.

Start server.py drie keer (geldig geheim, geen geheim, zwak geheim) en probeert binnen te komen.
Niets praat met Shopify: initialize en tools/list raken de Shopify-API niet.
"""
import json
import os
import secrets
import subprocess
import sys
import time

import httpx

REPO = "C:/Users/Naoufal/Shopify/shopify-mcp-v1"
INIT = {"jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}}
H = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
fouten = []


def check(naam, ok):
    print(("  OK   " if ok else "  FOUT ") + naam)
    if not ok:
        fouten.append(naam)


def start(port, secret):
    env = dict(os.environ, SHOPIFY_STORE="teststore", PORT=str(port), PYTHONIOENCODING="utf-8")
    env.pop("MCP_PATH_SECRET", None)
    if secret is not None:
        env["MCP_PATH_SECRET"] = secret
    log = open(f"server_{port}.log", "w", encoding="utf-8")
    p = subprocess.Popen([sys.executable, "server.py"], cwd=REPO, env=env, stdout=log, stderr=subprocess.STDOUT)
    for _ in range(60):
        try:
            httpx.get(f"http://127.0.0.1:{port}/", timeout=1)
            return p
        except Exception:
            time.sleep(0.5)
    p.kill()
    raise SystemExit("server kwam niet op")


def post(port, path, body=INIT):
    return httpx.post(f"http://127.0.0.1:{port}{path}", headers=H, json=body, timeout=10)


def get(port, path):
    return httpx.get(f"http://127.0.0.1:{port}{path}", timeout=5)


GEHEIM = secrets.token_urlsafe(32)  # 43 tekens

# 1. Geldig geheim
print("== stand 1: geldig geheim")
p = start(8761, GEHEIM)
try:
    r = post(8761, f"/mcp/{GEHEIM}")
    check("juist geheim: initialize 200", r.status_code == 200 and "serverInfo" in r.text)
    sid = r.headers.get("mcp-session-id")
    h2 = dict(H, **({"mcp-session-id": sid} if sid else {}))
    httpx.post(f"http://127.0.0.1:8761/mcp/{GEHEIM}", headers=h2, json={"jsonrpc": "2.0", "method": "notifications/initialized"})
    r = httpx.post(f"http://127.0.0.1:8761/mcp/{GEHEIM}", headers=h2, json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    n = len(r.json()["result"]["tools"]) if r.status_code == 200 else 0
    check(f"juist geheim: tools/list geeft tools ({n})", n > 50)
    for pad in [f"/mcp/{GEHEIM}/", f"/mcp/{GEHEIM}%2F", "/mcp", "/mcp/", f"/mcp/{GEHEIM[:-1]}", f"/mcp/{GEHEIM}x", f"/mcp/{GEHEIM.upper()}",
                "/MCP/" + GEHEIM, "/", "/sse", "/messages/", f"/mcp/{GEHEIM}/../", "/mcp/%2e%2e",
                "/.well-known/oauth-authorization-server", "/.well-known/oauth-protected-resource", "/register",
                "/authorize", "/token"]:
        r = post(8761, pad)
        check(f"POST {pad[:40]:40s} -> {r.status_code}, geen 401/WWW-Authenticate",
              r.status_code in (404, 405) and "location" not in {k.lower() for k in r.headers} and "www-authenticate" not in {k.lower() for k in r.headers})
        if r.status_code == 307:
            loc = r.headers.get("location", "")
            check(f"  redirect van {pad[:30]} lekt geen geheim", GEHEIM not in loc)
    for pad in ["/.well-known/oauth-authorization-server", "/.well-known/oauth-protected-resource", "/sse", "/"]:
        r = get(8761, pad)
        check(f"GET {pad} -> {r.status_code}", r.status_code == 404)
    log = open("server_8761.log", encoding="utf-8").read()
    check("geheim staat niet in de serverlog", GEHEIM not in log)
    check("log meldt 'MCP route: on'", "MCP route: on" in log)
finally:
    p.kill(); p.wait()

# 2. Geen geheim: alles dicht
print("== stand 2: geen geheim")
p = start(8762, None)
try:
    for pad in ["/mcp", "/mcp/", "/mcp/anything-goes-here-1234567890123456789", "/sse", "/"]:
        r = post(8762, pad)
        check(f"POST {pad[:40]:40s} -> {r.status_code}", r.status_code in (404, 405, 307) and "serverInfo" not in r.text)
    log = open("server_8762.log", encoding="utf-8").read()
    check("log meldt OFF + fout over MCP_PATH_SECRET", "MCP route: OFF" in log and "MCP_PATH_SECRET is missing" in log)
finally:
    p.kill(); p.wait()

# 3. Zwak geheim (te kort / verboden tekens): alles dicht
for port, zwak in [(8763, "kort123"), (8764, "a" * 31), (8765, "spaties en /slash" + "x" * 32)]:
    print(f"== stand 3: zwak geheim ({len(zwak)} tekens)")
    p = start(port, zwak)
    try:
        r = post(port, f"/mcp/{zwak}")
        check(f"zwak geheim wordt niet geaccepteerd -> {r.status_code}", "serverInfo" not in r.text)
        r = post(port, "/mcp")
        check(f"/mcp blijft dicht -> {r.status_code}", "serverInfo" not in r.text)
    finally:
        p.kill(); p.wait()

print("\nRESULTAAT:", "ALLES OK" if not fouten else f"{len(fouten)} FOUT(EN): {fouten}")
