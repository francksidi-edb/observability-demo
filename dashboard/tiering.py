"""
tiering.py - API for the Tiering tab. Mounted by app.py with:   tiering.mount(app, Q, cmd, objs, CFG)

  GET  /api/tier/state     hot partitions, Iceberg row count, watermark, policy, last job, recent runs
  POST /api/tier/policy    {age_min, auto}        change the "TTL" and switch the background job on or off
  POST /api/tier/run       {age_min?}             tier now (runs in a thread, poll /api/tier/state)
  POST /api/tier/write     {n}                    write sample events straight into Iceberg
  GET  /api/tier/compare   same query over hot / cold / both / history

Setup itself is never started from the browser: run `python tier_job.py setup` once.
"""
from __future__ import annotations

import threading
import time
from typing import Optional

from fastapi import Body, HTTPException

from tier_job import Tier, TierError

_ctx: dict = {}
_cold_cache = {"t": 0.0, "v": 0}
POLICY = {"age_min": 60.0, "auto": False, "interval_s": 30}
JOB = {"state": "idle", "steps": [], "started": None, "finished": None, "error": None, "tiered": 0}
_job_lock = threading.Lock()


def _tier() -> Tier:
    t = _ctx.get("tier")
    if t is None:
        t = _ctx["tier"] = Tier(_ctx["Q"], _ctx["cmd"], _ctx["CFG"].get("tiering", {}), log=lambda *_: None)
    return t


def cold_rows_cached() -> int:
    """Used by app.py counters(); never raises, refreshes at most every 20 s."""
    if "Q" not in _ctx:
        return 0
    if time.time() - _cold_cache["t"] > 20:
        _cold_cache["t"] = time.time()
        try:
            _cold_cache["v"] = _tier().cold_rows() or 0 if _tier().is_setup() else 0
        except Exception:  # noqa: BLE001
            pass
    return _cold_cache["v"]


def _step(name: str, status: str, detail: str = ""):
    steps = JOB["steps"]
    if steps and steps[-1]["name"] == name and steps[-1]["status"] == "run":
        steps[-1].update(status=status, detail=detail or steps[-1]["detail"])
    else:
        steps.append({"name": name, "status": status, "detail": detail, "t": round(time.time() - JOB["started"], 1)})


def _run_job(age: float):
    if not _job_lock.acquire(blocking=False):
        return
    try:
        JOB.update(state="running", steps=[], started=time.time(), finished=None, error=None, tiered=0)
        r = _tier().run(age, 6, _step)
        JOB.update(state="error" if r["error"] else "done", error=r["error"], tiered=len(r["done"]), finished=time.time())
        _cold_cache["t"] = 0
    except Exception as e:  # noqa: BLE001
        JOB.update(state="error", error=str(e)[:300], finished=time.time())
    finally:
        _job_lock.release()


def _auto_loop():
    last = 0.0
    while True:
        time.sleep(2)
        if POLICY["auto"] and JOB["state"] != "running" and time.time() - last >= POLICY["interval_s"]:
            last = time.time()
            try:
                if _tier().is_setup() and any(p["eligible"] for p in _tier().partitions(POLICY["age_min"])):
                    threading.Thread(target=_run_job, args=(POLICY["age_min"],), daemon=True).start()
            except Exception:  # noqa: BLE001
                pass


def mount(app, Q, cmd, objs, CFG):
    _ctx.update(Q=Q, cmd=cmd, objs=objs, CFG=CFG)
    threading.Thread(target=_auto_loop, daemon=True).start()

    @app.get("/api/tier/state")
    def tier_state():
        t = _tier()
        try:
            if not t.is_setup():
                return {"setup": False, "policy": POLICY, "job": JOB, "mode": t.cfg["mode"]}
            parts = t.partitions(POLICY["age_min"])
            wm = t.meta_get("wm")
            return {"setup": True, "policy": POLICY, "job": JOB, "mode": t.cfg["mode"], "step_min": t.cfg["step_min"],
                    "partitions": parts, "hot_rows": sum(int(p["rows"]) for p in parts), "hot_bytes": sum(int(p["bytes"]) for p in parts),
                    "hist_rows": int(t.one("SELECT total_rows FROM system.tables WHERE database = 'obs' AND name = 'logs_hist'") or 0), "cold_rows": t.cold_rows(), "cold_exists": t.cold_exists(), "watermark": wm, "history_boundary": t.meta_get("H"),
                    "runs": t.runs(), "direct": t.direct_count() if wm and t.cold_exists() else 0,
                    "server_now": t.one("SELECT toString(now('UTC'))")}
        except Exception as e:  # noqa: BLE001
            return {"setup": False, "error": str(e)[:300], "policy": POLICY, "job": JOB}

    @app.post("/api/tier/policy")
    def tier_policy(age_min: float = Body(60, embed=True), auto: bool = Body(False, embed=True)):
        POLICY.update(age_min=max(0.0, min(float(age_min), 1440.0)), auto=bool(auto))
        return POLICY

    @app.post("/api/tier/run")
    def tier_run(age_min: Optional[float] = Body(None, embed=True)):
        if JOB["state"] == "running":
            raise HTTPException(409, "a tiering run is already in progress")
        threading.Thread(target=_run_job, args=(POLICY["age_min"] if age_min is None else float(age_min),), daemon=True).start()
        time.sleep(0.2)
        return JOB

    @app.post("/api/tier/write")
    def tier_write(n: int = Body(1000, embed=True)):
        try:
            r = _tier().direct_write(n)
            _cold_cache["t"] = 0
            return {**r, "visible": _tier().direct_count()}
        except TierError as e:
            raise HTTPException(409, str(e))
        except Exception as e:  # noqa: BLE001
            raise HTTPException(500, str(e)[:400])

    @app.get("/api/tier/compare")
    def tier_compare():
        try:
            return {"rows": _tier().compare()}
        except TierError as e:
            raise HTTPException(409, str(e))
        except Exception as e:  # noqa: BLE001
            raise HTTPException(500, str(e)[:400])

