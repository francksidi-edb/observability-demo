#!/usr/bin/env python3
"""
Observability Console
=====================
Go simulator -> Kafka -> ClickHouse -> this dashboard.

  python app.py [--host 127.0.0.1] [--port 8080]

Every API response carries `stats` (server time, rows read, bytes read) so the page can show
how much ClickHouse scanned for each panel. Set CHDB_PATH to run against an embedded ClickHouse
(chdb) for tests without a server.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

APP_DIR = Path(__file__).resolve().parent
SQL_DIR = APP_DIR.parent / "sql"
CFG = json.loads((APP_DIR / "config.json").read_text())
CHDB_PATH = os.getenv("CHDB_PATH")


class SafeJSON(JSONResponse):
    def render(self, content) -> bytes:
        def clean(o):
            if isinstance(o, float):
                return o if math.isfinite(o) else None
            if isinstance(o, dict):
                return {k: clean(v) for k, v in o.items()}
            if isinstance(o, (list, tuple)):
                return [clean(v) for v in o]
            return o
        return json.dumps(clean(content), default=str, allow_nan=False).encode()


app = FastAPI(title="Observability Console", default_response_class=SafeJSON)

# ---------------------------------------------------------------------------- database access
_clients: dict = {}
_lock = threading.Lock()
_chdb = None


def _client(user=None, password=None):
    import clickhouse_connect
    key = user or "_admin"
    with _lock:
        c = _clients.get(key)
        if c is None:
            ch = CFG["clickhouse"]
            c = clickhouse_connect.get_client(autogenerate_session_id=False, host=ch["host"], port=ch["http_port"],
                                              username=user or ch["user"], password=password if user else ch["password"],
                                              connect_timeout=5, send_receive_timeout=60, compress=True)
            _clients[key] = c
        return c


def Q(sql: str, params: dict | None = None, user: str | None = None):
    """Run one query. Returns {cols, rows, stats}. stats = server-side rows/bytes read and wall time."""
    t0 = time.perf_counter()
    if CHDB_PATH:
        global _chdb
        from chdb import session
        with _lock:
            if _chdb is None:
                _chdb = session.Session(CHDB_PATH)
            res = _chdb.query(sql, "JSON", params=params) if params else _chdb.query(sql, "JSON")
        doc = json.loads(res.bytes() or b"{}")
        cols = [m["name"] for m in doc.get("meta", [])]
        rows = [[r[c] for c in cols] for r in doc.get("data", [])]
        st = doc.get("statistics", {})
        stats = {"read_rows": st.get("rows_read", 0), "read_bytes": st.get("bytes_read", 0)}
    else:
        pw = CFG["govern_users"].get(user, {}).get("password") if user else None
        r = _client(user, pw).query(sql, parameters=params or {})
        cols, rows = list(r.column_names), [list(x) for x in r.result_rows]
        s = r.summary or {}
        stats = {"read_rows": int(s.get("read_rows", 0)), "read_bytes": int(s.get("read_bytes", 0))}
    stats["ms"] = round((time.perf_counter() - t0) * 1000, 1)
    return {"cols": cols, "rows": rows, "stats": stats}


def cmd(sql: str, params: dict | None = None):
    if CHDB_PATH:
        Q(sql, params)
    else:
        _client().command(sql, parameters=params or {})


def objs(res):
    return [dict(zip(res["cols"], r)) for r in res["rows"]]


# ---------------------------------------------------------------------------- helpers
def bucket_seconds(minutes: int, points: int = 120) -> int:
    for b in (60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 21600, 43200, 86400):
        if minutes * 60 / b <= points:
            return b
    return 86400


def time_clause(t_from: int, t_to: int, minutes: int) -> tuple[str, dict]:
    """Either a focus window (unix seconds) or the last `minutes` minutes."""
    if t_from and t_to:
        return "ts >= toDateTime({tf:UInt32}) AND ts < toDateTime({tt:UInt32})", {"tf": t_from, "tt": t_to}
    return "ts >= now() - INTERVAL {m:UInt32} MINUTE", {"m": minutes}


SERVICES = ["checkout", "payments", "auth", "search", "inventory", "notifications", "postgres"]


def svc_clause(service: str | None) -> tuple[str, dict]:
    if service and service != "all":
        return "AND service = {svc:String}", {"svc": service}
    return "", {}


# ---------------------------------------------------------------------------- health and ingest

# ---- tiering (added by patch_demo.py) -------------------------------------------------------------
def _merge_logs(tables: dict) -> dict:
    """obs.logs is a view once tiering is set up; the real rows are in logs_hist + logs_hot (+ Iceberg, counted elsewhere)."""
    parts = [tables[n] for n in ("logs", "logs_hist", "logs_hot") if n in tables]
    if not parts:
        return {}
    return {k: sum(int(p.get(k) or 0) for p in parts) for k in ("rows", "comp", "unc", "parts", "partitions")}


def _cold_rows() -> int:
    try:
        import tiering
        return tiering.cold_rows_cached()
    except Exception:  # noqa: BLE001
        return 0


@app.get("/api/health")
def health():
    try:
        v = Q("SELECT version(), toString(timezone())")
        parts = Q("""SELECT table, sum(rows) AS rows, sum(data_compressed_bytes) AS comp, sum(data_uncompressed_bytes) AS unc,
                            count() AS parts, uniqExact(partition) AS partitions
                     FROM system.parts WHERE database = 'obs' AND active GROUP BY table""")
        last = Q("SELECT toUnixTimestamp(max(ts)) FROM obs.logs WHERE ts >= now() - INTERVAL 10 MINUTE")
        tables = {r["table"]: r for r in objs(parts)}
        lg = _merge_logs(tables)
        tables["logs"] = {**tables.get("logs", {}), **lg}
        cold_n = _cold_rows()
        mx = last["rows"][0][0] if last["rows"] and last["rows"][0][0] else None
        return {"ok": True, "version": v["rows"][0][0], "timezone": v["rows"][0][1], "tables": tables,
                "log_rows": lg.get("rows", 0) + cold_n, "local_rows": lg.get("rows", 0), "cold_rows": cold_n, "compressed": lg.get("comp", 0), "uncompressed": lg.get("unc", 0),
                "bytes_per_row": round(lg["comp"] / lg["rows"], 1) if lg.get("rows") else None,
                "ratio": round(lg["unc"] / lg["comp"], 1) if lg.get("comp") else None,
                "latest_ts": mx, "lag_s": round(time.time() - mx, 1) if mx else None, "now": time.time(),
                "stats": last["stats"]}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e)[:300]}


@app.get("/api/counters")
def counters():
    """Cheap numbers polled every second by the Pipeline tab."""
    r = Q("SELECT sumIf(total_rows, name IN ('logs', 'logs_hist', 'logs_hot')), sumIf(total_rows, name = 'metrics'), sumIf(total_bytes, name IN ('logs', 'logs_hist', 'logs_hot')) "
          "FROM system.tables WHERE database = 'obs'")
    a = r["rows"][0]
    return {"logs": int(a[0] or 0) + _cold_rows(), "metrics": a[1], "bytes": a[2], "t": time.time()}


# ---------------------------------------------------------------------------- dashboard panels
@app.get("/api/overview")
def overview(minutes: int = 15, service: str = "all"):
    sc, p = svc_clause(service)
    p = {**p, "m": minutes}
    cur = Q(f"""SELECT sum(cnt) AS n, sum(errs) AS e, quantilesTDigestMerge(0.5, 0.99)(lat) AS q
                FROM obs.svc_1m WHERE minute >= now() - INTERVAL {{m:UInt32}} MINUTE {sc}""", p)
    prev = Q(f"""SELECT sum(cnt) AS n, sum(errs) AS e, quantilesTDigestMerge(0.5, 0.99)(lat) AS q
                 FROM obs.svc_1m WHERE minute >= now() - INTERVAL {{m:UInt32}} * 2 MINUTE
                   AND minute < now() - INTERVAL {{m:UInt32}} MINUTE {sc}""", p)
    hosts = Q("SELECT uniqExact(host) FROM obs.metrics WHERE ts >= now() - INTERVAL 5 MINUTE AND metric IN ('cpu_pct', 'pg_tps')")
    def pack(r):
        n, e, q = r["rows"][0]
        n, e = int(n or 0), int(e or 0)
        q = [x if x == x else 0 for x in (q or [0, 0])]
        return {"events": n, "errors": e, "err_pct": round(100 * e / n, 2) if n else 0,
                "p50": round(q[0]) if q else 0, "p99": round(q[1]) if len(q) > 1 else 0}
    stats = {"ms": cur["stats"]["ms"] + prev["stats"]["ms"] + hosts["stats"]["ms"],
             "read_rows": cur["stats"]["read_rows"] + prev["stats"]["read_rows"] + hosts["stats"]["read_rows"],
             "read_bytes": cur["stats"]["read_bytes"] + prev["stats"]["read_bytes"] + hosts["stats"]["read_bytes"]}
    return {"cur": pack(cur), "prev": pack(prev), "hosts": hosts["rows"][0][0], "minutes": minutes, "stats": stats}


@app.get("/api/timeseries")
def timeseries(minutes: int = 180, service: str = "all", source: str = "rollup"):
    b = bucket_seconds(minutes)
    sc, p = svc_clause(service)
    p = {**p, "m": minutes, "b": b}
    if source == "raw":
        res = Q(f"""SELECT toUnixTimestamp(toStartOfInterval(ts, INTERVAL {{b:UInt32}} SECOND)) AS t, count() AS n,
                           countIf(severity IN ('ERROR', 'FATAL')) AS e,
                           quantileTDigestIf(0.99)(duration_ms, duration_ms > 0) AS p99
                    FROM obs.logs WHERE ts >= now() - INTERVAL {{m:UInt32}} MINUTE {sc} GROUP BY t ORDER BY t""", p)
    else:
        res = Q(f"""SELECT toUnixTimestamp(toStartOfInterval(minute, INTERVAL {{b:UInt32}} SECOND)) AS t, sum(cnt) AS n, sum(errs) AS e,
                           quantilesTDigestMerge(0.5, 0.99)(lat)[2] AS p99
                    FROM obs.svc_1m WHERE minute >= now() - INTERVAL {{m:UInt32}} MINUTE {sc} GROUP BY t ORDER BY t""", p)
    pts = [{"t": r[0], "n": int(r[1]), "e": int(r[2]), "err_pct": round(100 * r[2] / r[1], 2) if r[1] else 0, "p99": round(r[3] or 0)}
           for r in res["rows"]]
    return {"bucket": b, "source": source, "points": pts, "stats": res["stats"]}


@app.get("/api/services_p99")
def services_p99(minutes: int = 180):
    b = bucket_seconds(minutes)
    res = Q("""SELECT toUnixTimestamp(toStartOfInterval(minute, INTERVAL {b:UInt32} SECOND)) AS t, service,
                      quantilesTDigestMerge(0.5, 0.99)(lat)[2] AS p99
               FROM obs.svc_1m WHERE minute >= now() - INTERVAL {m:UInt32} MINUTE
               GROUP BY t, service ORDER BY t""", {"m": minutes, "b": b})
    series: dict[str, list] = {}
    for t, s, v in res["rows"]:
        series.setdefault(s, []).append([t, round(v or 0)])
    return {"series": series, "stats": res["stats"]}


@app.get("/api/heatmap")
def heatmap(minutes: int = 180):
    b = bucket_seconds(minutes, 48)
    res = Q("""SELECT toUnixTimestamp(toStartOfInterval(minute, INTERVAL {b:UInt32} SECOND)) AS t, service,
                      sum(errs) / sum(cnt) * 100 AS pct, sum(cnt) AS n
               FROM obs.svc_1m WHERE minute >= now() - INTERVAL {m:UInt32} MINUTE
               GROUP BY t, service ORDER BY t""", {"m": minutes, "b": b})
    return {"bucket": b, "services": SERVICES, "cells": [[r[0], r[1], round(r[2] or 0, 2), int(r[3])] for r in res["rows"]],
            "stats": res["stats"]}


@app.get("/api/top_errors")
def top_errors(minutes: int = 15, service: str = "all", t_from: int = 0, t_to: int = 0):
    sc, p = svc_clause(service)
    tc, tp = time_clause(t_from, t_to, minutes)
    res = Q(f"""SELECT replaceRegexpAll(message, '[0-9]+', 'N') AS msg, count() AS n, uniqExact(host) AS hosts, any(service) AS svc
                FROM obs.logs WHERE severity IN ('ERROR', 'FATAL') AND {tc} {sc}
                GROUP BY msg ORDER BY n DESC LIMIT 8""", {**p, **tp})
    return {"rows": objs(res), "stats": res["stats"]}


@app.get("/api/versions")
def versions(minutes: int = 15, service: str = "payments", t_from: int = 0, t_to: int = 0):
    tc, tp = time_clause(t_from, t_to, minutes)
    res = Q(f"""SELECT attrs['version'] AS version, count() AS n, countIf(severity IN ('ERROR', 'FATAL')) AS errors,
                      round(100 * errors / n, 2) AS err_pct, round(quantileTDigestIf(0.99)(duration_ms, duration_ms > 0)) AS p99
               FROM obs.logs WHERE service = {{svc:String}} AND {tc}
               GROUP BY version HAVING version != '' ORDER BY version""", {"svc": service, **tp})
    return {"rows": objs(res), "stats": res["stats"]}


@app.get("/api/pg")
def pg(minutes: int = 60):
    b = bucket_seconds(minutes, 90)
    res = Q("""SELECT toUnixTimestamp(toStartOfInterval(minute, INTERVAL {b:UInt32} SECOND)) AS t,
                      extract(host, 'pg-([a-z]+)-') AS cluster, avgMerge(avg_v) AS v
               FROM obs.metrics_1m WHERE metric = 'pg_lock_waits' AND minute >= now() - INTERVAL {m:UInt32} MINUTE
               GROUP BY t, cluster ORDER BY t""", {"m": minutes, "b": b})
    series: dict[str, list] = {}
    for t, c, v in res["rows"]:
        series.setdefault(c, []).append([t, round(v, 2)])
    now = Q("""SELECT extract(host, 'pg-([a-z]+)-') AS cluster, round(avgIf(value, metric = 'pg_connections')) AS conns,
                      round(avgIf(value, metric = 'pg_tps')) AS tps, round(maxIf(value, metric = 'pg_lock_waits'), 1) AS lock_waits,
                      round(avgIf(value, metric = 'pg_repl_lag_ms')) AS lag_ms
               FROM obs.metrics WHERE startsWith(metric, 'pg_') AND ts >= now() - INTERVAL 5 MINUTE
               GROUP BY cluster ORDER BY cluster""")
    return {"series": series, "clusters": objs(now),
            "stats": {k: res["stats"][k] + now["stats"][k] for k in res["stats"]}}


@app.get("/api/tail")
def tail(n: int = 14, service: str = "all"):
    sc, p = svc_clause(service)
    res = Q(f"""SELECT formatDateTime(ts, '%H:%i:%S', 'UTC') AS t, service, host, severity, message
                FROM obs.logs WHERE ts >= now() - INTERVAL {{w:UInt32}} SECOND {sc} ORDER BY ts DESC LIMIT {{n:UInt32}}""", {**p, "n": n, "w": int(os.getenv("TAIL_WINDOW") or CFG.get("tail_window_seconds", 30))})
    return {"rows": objs(res), "stats": res["stats"]}


# ---------------------------------------------------------------------------- explore
UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")



SEARCH_COLS = ["time", "service", "host", "severity", "duration_ms", "trace_id", "message"]
SEARCH_SQL = """SELECT formatDateTime(ts, '%Y-%m-%d %H:%i:%S', 'UTC') AS time, service, host, severity, duration_ms, toString(trace_id) AS trace_id, message
                FROM obs.logs WHERE {w} ORDER BY ts DESC LIMIT {{lim:UInt32}}"""


def _newest_first(where, tc, p, trace, t_from, t_to, minutes):
    """Newest rows first: query 1 min, then 4, 16, ... slices of the window and stop once `lim` rows are found."""
    if trace:  # a trace id needs the whole window; the bloom filter keeps that cheap
        return Q(SEARCH_SQL.format(w=" AND ".join(where)), p)
    lim = int(p["lim"])
    focus = bool(t_from and t_to)
    now = int(time.time())
    hi = int(t_to) if focus else now + 5
    lo = int(t_from) if focus else now - int(minutes) * 60
    base = {k: v for k, v in p.items() if k not in ("tf", "tt", "m")}
    rest = [w for w in where if w != tc]
    rows, stats, size = [], {"ms": 0.0, "read_rows": 0, "read_bytes": 0}, 60
    while hi > lo and len(rows) < lim:
        a = max(lo, hi - size)
        r = Q(SEARCH_SQL.format(w=" AND ".join(rest + ["ts >= toDateTime({ca:UInt32}) AND ts < toDateTime({cb:UInt32})"])),
              {**base, "ca": a, "cb": hi})
        rows += r["rows"][: lim - len(rows)]
        for k in stats:
            stats[k] += r["stats"].get(k, 0)
        hi, size = a, size * 4
    stats["ms"] = round(stats["ms"], 1)
    return {"cols": SEARCH_COLS, "rows": rows, "stats": stats}

@app.get("/api/search")
def search(service: str = "all", severity: str = "all", q: str = "", minutes: int = 15, limit: int = 50, t_from: int = 0, t_to: int = 0):
    tc, tp = time_clause(t_from, t_to, minutes)
    where, p = [tc], {**tp, "lim": min(limit, 200)}
    q = q.strip()
    if UUID_RE.match(q):
        where = ["trace_id = toUUID({trace:String})", tc]
        p = {**tp, "trace": q, "lim": 50}
    elif q:
        if re.fullmatch(r"[A-Za-z0-9_]+", q):
            where.append("hasToken(message, {q:String})")
        else:
            where.append("position(message, {q:String}) > 0")
        p["q"] = q
    if service != "all":
        where.append("service = {svc:String}")
        p["svc"] = service
    if severity != "all":
        where.append("severity = {sev:String}")
        p["sev"] = severity
    res = _newest_first(where, tc, p, bool(UUID_RE.match(q)), t_from, t_to, minutes)
    total = int(Q("SELECT sum(total_rows) FROM system.tables WHERE database = 'obs' AND name IN ('logs', 'logs_hist', 'logs_hot')")["rows"][0][0] or 0) + _cold_rows()
    return {"rows": objs(res), "stats": res["stats"], "total_rows": int(total or 0)}


# ---------------------------------------------------------------------------- simulator proxy
def sim(path: str, method="GET"):
    try:
        req = urllib.request.Request(CFG["simulator"] + path, method=method)
        with urllib.request.urlopen(req, timeout=4) as r:
            return json.loads(r.read())
    except (urllib.error.URLError, OSError) as e:
        return {"error": f"simulator not reachable: {e}"}


@app.get("/api/sim/status")
def sim_status():
    return sim("/status")


@app.post("/api/sim/start")
def sim_start(rate: int = 0):
    return sim("/start" + (f"?rate={rate}" if rate else ""), "POST")


@app.post("/api/sim/stop")
def sim_stop():
    return sim("/stop", "POST")


@app.post("/api/sim/rate")
def sim_rate(eps: int):
    return sim(f"/rate?eps={int(eps)}", "POST")


@app.post("/api/sim/incident")
def sim_incident(on: int = 1, minutes: int = 0):
    return sim(f"/incident?on={int(on)}&service=payments&minutes={int(minutes)}", "POST")


# ---------------------------------------------------------------------------- backfill job
JOB = {"state": "idle", "done": 0, "total": 0, "slice": 0, "slices": 0, "started": None, "elapsed": 0, "error": None, "days": 0}


def _strip(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql).strip().rstrip(";")


def _backfill(total: int, days: int):
    try:
        JOB.update(state="running", done=0, total=total, started=time.time(), error=None, days=days)
        size = int(CFG.get("backfill_slice_rows", 100_000_000))
        size = min(size, total)
        JOB["slices"] = math.ceil(total / size)
        sql = _strip((SQL_DIR / "backfill_logs.sql").read_text())
        off, i = 0, 0
        while off < total and JOB["state"] == "running":
            n = min(size, total - off)
            JOB["slice"] = i + 1
            cmd(sql, {"total": total, "off": off, "n": n, "days": days})
            off += n
            i += 1
            JOB["done"] = off
            JOB["elapsed"] = round(time.time() - JOB["started"], 1)
        for f in ("fleet.sql",):
            for st in [x.strip() for x in _strip((SQL_DIR / f).read_text()).split(";") if x.strip()]:
                cmd(st)
        cmd(_strip((SQL_DIR / "backfill_metrics.sql").read_text()), {"days": min(days, 14)})
        JOB.update(state="done" if JOB["state"] == "running" else "stopped", elapsed=round(time.time() - JOB["started"], 1))
    except Exception as e:  # noqa: BLE001
        JOB.update(state="error", error=str(e)[:400])


@app.post("/api/backfill/start")
def backfill_start(rows_millions: int = Body(100, embed=True), days: int = Body(7, embed=True)):
    if JOB["state"] == "running":
        raise HTTPException(409, "a backfill is already running")
    threading.Thread(target=_backfill, args=(rows_millions * 1_000_000, days), daemon=True).start()
    time.sleep(0.2)
    return JOB


@app.post("/api/backfill/stop")
def backfill_stop():
    if JOB["state"] == "running":
        JOB["state"] = "stopping"
    return JOB


@app.get("/api/backfill")
def backfill_status():
    if JOB["started"] and JOB["state"] == "running":
        JOB["elapsed"] = round(time.time() - JOB["started"], 1)
    return JOB


# ---------------------------------------------------------------------------- governance
GOV_SQL = """SELECT service, count() AS events, countIf(severity IN ('ERROR','FATAL')) AS errors
FROM obs.logs WHERE ts >= now() - INTERVAL 60 MINUTE GROUP BY service ORDER BY service"""


@app.get("/api/govern")
def govern():
    out = {"users": []}
    for u, meta in CFG["govern_users"].items():
        try:
            if CHDB_PATH:  # test mode: emulate the row policy
                sql = GOV_SQL.replace("WHERE ts", "WHERE " + ("service = 'payments' AND " if u == "payments_user" else "") + "ts")
                r = Q(sql)
            else:
                r = Q(GOV_SQL, user=u)
            out["users"].append({"user": u, "label": meta["label"], "rows": objs(r), "stats": r["stats"]})
        except Exception as e:  # noqa: BLE001
            out["users"].append({"user": u, "label": meta["label"], "error": str(e)[:200], "rows": []})
    out["audit"], note = audit_rows()
    if note:
        out["audit_note"] = note
    return out


# ---------------------------------------------------------------------------- governance guardrails
GOV_USERS = ("sre_user", "payments_user")
ERR_NAMES = {158: "TOO_MANY_ROWS", 159: "TIMEOUT_EXCEEDED", 201: "QUOTA_EXCEEDED", 241: "MEMORY_LIMIT_EXCEEDED", 497: "ACCESS_DENIED"}
# heavy: no time filter, so it reads the whole table for sre_user and every payments row for payments_user.
# attrs: touches a column payments_user is not granted.  light: cheap query for the quota burst.
GOV_TRY_SQL = {
    "heavy": "SELECT count() AS events, countIf(severity IN ('ERROR', 'FATAL')) AS errors FROM obs.logs",
    "attrs": ("SELECT attrs['version'] AS version, count() AS n FROM obs.logs "
              "WHERE service = 'payments' AND ts >= now() - INTERVAL 5 MINUTE GROUP BY version"),
    "light": "SELECT count() AS n FROM obs.logs WHERE service = 'payments' AND ts >= now() - INTERVAL 1 MINUTE",
}
BURST_N = 30


def parse_error(e: Exception) -> dict:
    """ClickHouse errors arrive as 'Code: 497. DB::Exception: ...' or 'code: 497, server response: ...'."""
    text = re.sub(r"\s+", " ", str(e))
    m = re.search(r"[Cc]ode:? ?(\d+)", text)
    code = int(m.group(1)) if m else None
    dm = re.search(r"DB::Exception: (.*)", text)
    return {"code": code, "name": ERR_NAMES.get(code, "ERROR"), "message": (dm.group(1) if dm else text)[:240]}


def audit_rows() -> tuple[list, str | None]:
    """Last queries by the two demo users, rejected ones included. Returns (rows, note)."""
    try:
        try:
            cmd("SYSTEM FLUSH LOGS")  # query_log is written in batches; flush so the audit is current
        except Exception:  # noqa: BLE001
            pass
        res = Q("""SELECT formatDateTime(event_time, '%H:%i:%S', 'UTC') AS at, user, query_duration_ms AS ms, read_rows,
                          formatReadableSize(read_bytes) AS read,
                          multiIf(type = 'QueryFinish', 'ok', exception_code = 158, 'TOO_MANY_ROWS',
                                  exception_code = 159, 'TIMEOUT', exception_code = 201, 'QUOTA',
                                  exception_code = 497, 'DENIED', concat('error ', toString(exception_code))) AS status,
                          substring(replaceRegexpAll(query, '\\\\s+', ' '), 1, 70) AS query
                   FROM system.query_log
                   WHERE type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing')
                     AND user IN ('sre_user', 'payments_user') AND event_time > now() - INTERVAL 15 MINUTE
                   ORDER BY event_time DESC LIMIT 10""")
        return objs(res), None
    except Exception as e:  # noqa: BLE001
        return [], str(e)[:160]


def _run_as(user: str, sql: str) -> dict:
    t0 = time.perf_counter()
    try:
        res = Q(sql, user=user)
        s = res["stats"]
        return {"ok": True, "ms": s["ms"], "read_rows": s["read_rows"], "read_bytes": s["read_bytes"], "rows": objs(res)[:5]}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "ms": round((time.perf_counter() - t0) * 1000, 1), "error": parse_error(e)}


@app.post("/api/govern/try")
def govern_try(user: str, kind: str):
    if user not in GOV_USERS:
        raise HTTPException(400, "unknown user")
    if kind == "burst":
        ran, t0 = 0, time.perf_counter()
        for _ in range(BURST_N):
            out = _run_as(user, GOV_TRY_SQL["light"])
            if not out["ok"]:
                out.update(kind=kind, user=user, ran=ran, planned=BURST_N, ms=round((time.perf_counter() - t0) * 1000, 1))
                return out
            ran += 1
        return {"ok": True, "kind": kind, "user": user, "ran": ran, "planned": BURST_N,
                "ms": round((time.perf_counter() - t0) * 1000, 1)}
    if kind not in ("heavy", "attrs"):
        raise HTTPException(400, "unknown kind")
    out = _run_as(user, GOV_TRY_SQL[kind])
    out.update(kind=kind, user=user)
    return out


@app.get("/api/govern/controls")
def govern_controls():
    def safe(sql: str) -> list:
        try:
            return objs(Q(sql))
        except Exception as e:  # noqa: BLE001
            return [{"error": str(e)[:120]}]
    return {
        "grants": safe("""SELECT user_name, access_type, database, `table`,
                                 arrayStringConcat(arrayFilter(x -> x != '', groupArray(ifNull(`column`, ''))), ', ') AS columns,
                                 is_partial_revoke
                          FROM system.grants WHERE user_name IN ('sre_user', 'payments_user')
                          GROUP BY user_name, access_type, database, `table`, is_partial_revoke
                          ORDER BY user_name, `table` LIMIT 20"""),
        "policies": safe("""SELECT short_name AS policy, concat(database, '.', `table`) AS on_table, select_filter AS filter
                            FROM system.row_policies ORDER BY short_name LIMIT 10"""),
        "quota": safe("""SELECT quota_name, duration, max_queries, max_read_rows
                         FROM system.quota_limits WHERE quota_name = 'q_payments'"""),
        "profile": safe("""SELECT profile_name, setting_name, value FROM system.settings_profile_elements
                           WHERE profile_name = 'p_payments' AND isNotNull(setting_name) ORDER BY index"""),
    }


@app.get("/api/govern/audit")
def govern_audit():
    rows, note = audit_rows()
    return {"rows": rows, "note": note}


CODE_FILES = {"schema": "schema.sql", "backfill": "backfill_logs.sql", "governance": "governance.sql", "metrics": "backfill_metrics.sql"}


@app.get("/api/file")
def file(name: str):
    if name not in CODE_FILES:
        raise HTTPException(404, "unknown file")
    return {"name": CODE_FILES[name], "text": (SQL_DIR / CODE_FILES[name]).read_text()}


@app.get("/")
def index():
    return FileResponse(APP_DIR / "static" / "index.html")


try:
    import tiering
    tiering.mount(app, Q, cmd, objs, CFG)
except Exception as _e:  # noqa: BLE001
    print("tiering disabled:", _e)

app.mount("/static", StaticFiles(directory=APP_DIR / "static"), name="static")

if __name__ == "__main__":
    import uvicorn
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8080)
    a = ap.parse_args()
    uvicorn.run(app, host=a.host, port=a.port, log_level="warning")

