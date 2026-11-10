#!/usr/bin/env python3
"""
tier_job.py - hot/cold tiering of obs.logs into Iceberg on Altinity Antalya.

There is no released "TTL ... MOVE TO ICEBERG" yet, so the policy is a small job that does what such a TTL would do,
in an order that never loses or double-counts a row:

    copy partition to Iceberg -> verify counts -> move the Hybrid watermark -> drop the hot partition

Layout after `setup`:

    obs.logs_hot            MergeTree, new rows only, one partition per STEP minutes (Kafka MV writes here)
    ice."obs.logs_cold"     Iceberg table on MinIO/S3 (older rows)
    obs.logs_tier           Hybrid( hot WHERE ts >= W , cold WHERE ts < W )       W = the watermark
    obs.logs_hist           the history you copied over (old obs.logs, daily partitions, never written again)
    obs.logs                VIEW = logs_hist(ts < H)  UNION ALL  logs_tier(ts >= H)    <- the app keeps querying this name

Usage (run next to config.json, same file the dashboard uses):

    python tier_job.py probe                 # test every moving part on throwaway tables first
    python tier_job.py setup --dry-run       # print the SQL
    python tier_job.py setup                 # do it (stops Kafka MVs for a few seconds, no data is lost)
    python tier_job.py run --age 60          # tier every partition older than 60 minutes (once)
    python tier_job.py run --age 60 --loop 60
    python tier_job.py status
    python tier_job.py rollback              # put everything back into a plain obs.logs

Experimental features used: Hybrid engine, INSERT into Iceberg (EXPORT PART is not needed). Check `probe` on your build.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent

DEFAULTS = {
    "hot": "obs.logs_hot", "hist": "obs.logs_hist", "tier": "obs.logs_tier", "view": "obs.logs",
    "catalog_db": "ice", "cold_name": "obs.logs_cold", "cluster": "demo",
    "step_min": 10,                    # hot partition width in minutes
    "mode": "insert",                  # insert: INSERT INTO iceberg SELECT ...   export: write Parquet to MinIO, then ice insert --no-copy
    "s3_endpoint": "http://127.0.0.1:9100", "bucket": "warehouse", "staging": "staging/logs_cold",
    "access_key": "iceberg", "secret_key": "iceberg123!",
    "ice_cmd": ["ice"],                # e.g. ["ice", "--config", "/opt/ice/.ice.yaml"]
}
SERVICES = ["checkout", "payments", "auth", "search", "inventory", "notifications", "postgres"]


COLN = "ts, service, host, severity, status, duration_ms, trace_id, message, attrs, ingested_at"
COLS = "ts, service, host, severity, status, duration_ms, toString(trace_id) AS trace_id, message, attrs, ingested_at"


class TierError(Exception):
    pass


def lit(ts: str) -> str:
    return f"toDateTime64('{ts}', 3, 'UTC')"


def ident(db: str, tbl: str) -> str:
    return f"`{db}`.`{tbl}`"


class Tier:
    def __init__(self, Q, cmd, cfg: dict | None = None, log=print):
        self.Q, self.cmd, self.log = Q, cmd, log
        self.cfg = {**DEFAULTS, **(cfg or {})}
        for k in ("TIER_MODE", "TIER_STEP_MIN"):
            if os.getenv(k):
                self.cfg[k[5:].lower()] = type(DEFAULTS[k[5:].lower()])(os.environ[k])
        self.dry = False
        self.hot_db, self.hot_tbl = self.cfg["hot"].split(".")

    # ------------------------------------------------------------------ small helpers
    @property
    def cold(self) -> str:
        return ident(self.cfg["catalog_db"], self.cfg["cold_name"])

    def sql(self, text: str, params=None):
        if self.dry:
            print(text.strip() + ";\n")
            return None
        self.cmd(text, params)

    def one(self, text: str, params=None):
        r = self.Q(text, params)
        return r["rows"][0][0] if r["rows"] else None

    def rows(self, text: str, params=None) -> list[dict]:
        r = self.Q(text, params)
        return [dict(zip(r["cols"], x)) for x in r["rows"]]

    def table_engine(self, db: str, name: str) -> str | None:
        return self.one("SELECT engine FROM system.tables WHERE database = {d:String} AND name = {n:String}", {"d": db, "n": name})

    def cold_exists(self) -> bool:
        """system.tables does not always list tables of a DataLakeCatalog database, so fall back to asking for the table itself."""
        try:
            if self.table_engine(self.cfg["catalog_db"], self.cfg["cold_name"]) is not None:
                return True
        except Exception:  # noqa: BLE001
            pass
        try:
            self.cmd(f"DESCRIBE TABLE {self.cold}")
            return True
        except Exception:  # noqa: BLE001
            return False

    def meta_get(self, k: str):
        try:
            return self.one("SELECT argMax(v, at) FROM obs.tier_meta WHERE k = {k:String}", {"k": k}) or None
        except Exception:  # noqa: BLE001
            return None

    def meta_set(self, k: str, v: str):
        self.sql("INSERT INTO obs.tier_meta (k, v) SELECT {k:String}, {v:String}", {"k": k, "v": v})

    # ------------------------------------------------------------------ DDL builders
    def view_sql(self, source: str) -> str:
        H, c = self.meta_get("H"), self.cfg
        return (f"CREATE OR REPLACE VIEW {c['view']} AS "
                f"SELECT * FROM {c['hist']} WHERE ts < {lit(H)} UNION ALL SELECT * FROM {source} WHERE ts >= {lit(H)}")

    def hybrid_sql(self, wm: str) -> str:
        c = self.cfg
        return (f"CREATE OR REPLACE TABLE {c['tier']} AS {c['hot']} ENGINE = Hybrid("
                f"cluster('{c['cluster']}', {c['hot']}), ts >= {lit(wm)}, {self.cold}, ts < {lit(wm)})")

    # ------------------------------------------------------------------ access control
    def sync_access(self, table_sql: str, db: str, tbl: str):
        """Copy row policies and SELECT grants from the old obs.logs name to a new table so governance still holds."""
        v_db, v_tbl = self.cfg["view"].split(".")
        try:
            pol = self.rows("""SELECT short_name, select_filter, is_restrictive, apply_to_all, apply_to_list, apply_to_except
                               FROM system.row_policies WHERE database = {d:String} AND `table` = {t:String}""", {"d": v_db, "t": v_tbl})
            for p in pol:
                to = "ALL" if p["apply_to_all"] else ", ".join(p["apply_to_list"]) or "NONE"
                if p["apply_to_all"] and p["apply_to_except"]:
                    to += " EXCEPT " + ", ".join(p["apply_to_except"])
                kind = "AS RESTRICTIVE " if p["is_restrictive"] else ""
                self.sql(f"CREATE ROW POLICY OR REPLACE `{p['short_name']}` ON {table_sql} {kind}USING {p['select_filter']} TO {to}")
            gr = self.rows("""SELECT user_name, role_name, `column` FROM system.grants
                              WHERE database = {d:String} AND `table` = {t:String} AND access_type = 'SELECT'""", {"d": v_db, "t": v_tbl})
            by: dict[str, list] = {}
            for g in gr:
                who = f"`{g['user_name'] or g['role_name']}`"
                by.setdefault(who, []).append(g["column"])
            for who, cols in by.items():
                cl = [x for x in cols if x]
                self.sql(f"GRANT SELECT{'(' + ', '.join(cl) + ')' if cl else ''} ON {table_sql} TO {who}")
        except Exception as e:  # noqa: BLE001
            self.log(f"  access sync for {table_sql} failed: {str(e)[:160]}")

    # ------------------------------------------------------------------ setup / rollback
    def setup(self, dry: bool = False):
        self.dry = dry
        c = self.cfg
        v_db, v_tbl = c["view"].split(".")
        if not dry and self.table_engine(v_db, v_tbl) != "MergeTree":
            raise TierError(f"{c['view']} is not a MergeTree table (engine={self.table_engine(v_db, v_tbl)}); already set up?")
        self.log("1/6 pausing Kafka -> logs materialized views (Kafka keeps the offsets, nothing is lost)")
        self.sql(f"DROP VIEW IF EXISTS obs.logs_kafka_mv")
        self.sql(f"DROP VIEW IF EXISTS obs.svc_1m_mv")
        self.log("2/6 finding the history boundary H")
        if dry:
            H = "<max(ts)+1ms>"
        else:
            day = self.one(f"SELECT max(partition) FROM system.parts WHERE database = '{v_db}' AND table = '{v_tbl}' AND active")
            H = self.one(f"SELECT toString(max(ts) + INTERVAL 1 MILLISECOND) FROM {c['view']} WHERE toYYYYMMDD(ts) = {int(day)}")
        self.log(f"    H = {H}")
        self.log("3/6 renaming the old table to logs_hist, creating logs_hot")
        self.sql(f"RENAME TABLE {c['view']} TO {c['hist']}")
        self.sql(f"""CREATE TABLE IF NOT EXISTS {c['hot']} AS {c['hist']} ENGINE = MergeTree
                     PARTITION BY toStartOfInterval(toDateTime(ts, 'UTC'), INTERVAL {int(c['step_min'])} MINUTE)
                     ORDER BY (service, severity, toStartOfMinute(ts), host) SETTINGS index_granularity = 8192""")
        self.sql("CREATE TABLE IF NOT EXISTS obs.tier_meta (k String, v String, at DateTime64(3) DEFAULT now64(3)) "
                 "ENGINE = MergeTree ORDER BY (k, at)")
        self.sql("""CREATE TABLE IF NOT EXISTS obs.tier_runs (at DateTime DEFAULT now(), partition String, rows UInt64, mode String,
                    ms UInt32, status String, detail String, wm String) ENGINE = MergeTree ORDER BY at""")
        if not dry:
            self.meta_set("H", H)
        else:
            self.meta_get = lambda k: "<H>"  # noqa: E731
        self.log("4/6 creating the obs.logs view (history + hot)")
        self.sql(self.view_sql(c["hot"]))
        self.log("5/6 re-creating the materialized views on the new tables")
        self.sql("""CREATE MATERIALIZED VIEW obs.logs_kafka_mv TO obs.logs_hot AS
                    SELECT parseDateTime64BestEffort(ts, 3, 'UTC') AS ts, service, host, severity, status, duration_ms,
                           toUUIDOrZero(trace_id) AS trace_id, message, attrs FROM obs.logs_kafka""")
        self.sql("""CREATE MATERIALIZED VIEW obs.svc_1m_mv TO obs.svc_1m AS
                    SELECT toStartOfMinute(ts) AS minute, service, count() AS cnt,
                           countIf(severity IN ('ERROR', 'FATAL')) AS errs,
                           quantilesTDigestIfState(0.5, 0.99)(duration_ms, duration_ms > 0) AS lat
                    FROM obs.logs_hot GROUP BY minute, service""")
        self.log("6/6 copying row policies and grants to the new tables")
        self.sync_access(c["hist"], *c["hist"].split("."))
        self.sync_access(c["hot"], *c["hot"].split("."))
        self.dry = False
        self.log("done. Next: python tier_job.py run --age 0   (tiers every closed partition) or let the app's Tiering tab do it.")

    def rollback(self):
        c = self.cfg
        self.log("moving hot and cold rows back into a plain obs.logs")
        self.cmd("DROP VIEW IF EXISTS obs.logs_kafka_mv")
        self.cmd("DROP VIEW IF EXISTS obs.svc_1m_mv")
        self.cmd(f"DROP VIEW IF EXISTS {c['view']}")
        self.cmd(f"DROP TABLE IF EXISTS {c['tier']}")
        self.cmd(f"RENAME TABLE {c['hist']} TO {c['view']}")
        self.cmd(f"INSERT INTO {c['view']} SELECT * FROM {c['hot']}")
        if self.cold_exists():
            self.cmd(f"INSERT INTO {c['view']} SELECT * FROM {self.cold}")
        self.cmd(f"DROP TABLE {c['hot']}")
        self.cmd("""CREATE MATERIALIZED VIEW obs.logs_kafka_mv TO obs.logs AS
                    SELECT parseDateTime64BestEffort(ts, 3, 'UTC') AS ts, service, host, severity, status, duration_ms,
                           toUUIDOrZero(trace_id) AS trace_id, message, attrs FROM obs.logs_kafka""")
        self.cmd("""CREATE MATERIALIZED VIEW obs.svc_1m_mv TO obs.svc_1m AS
                    SELECT toStartOfMinute(ts) AS minute, service, count() AS cnt,
                           countIf(severity IN ('ERROR', 'FATAL')) AS errs,
                           quantilesTDigestIfState(0.5, 0.99)(duration_ms, duration_ms > 0) AS lat
                    FROM obs.logs GROUP BY minute, service""")
        self.log("rolled back. The Iceberg table and its files are left in place; drop them by hand if you want.")

    # ------------------------------------------------------------------ state
    def is_setup(self) -> bool:
        v_db, v_tbl = self.cfg["view"].split(".")
        return self.table_engine(v_db, v_tbl) == "View" and self.table_engine(self.hot_db, self.hot_tbl) is not None

    def partitions(self, age_min: float) -> list[dict]:
        s = int(self.cfg["step_min"])
        out = self.rows(f"""
            SELECT partition, any(partition_id) AS pid, sum(rows) AS rows, sum(bytes_on_disk) AS bytes, count() AS parts,
                   toString(toDateTime(partition, 'UTC') + INTERVAL {s} MINUTE) AS ends,
                   (toUnixTimestamp(now()) - toUnixTimestamp(toDateTime(partition, 'UTC') + INTERVAL {s} MINUTE)) / 60 AS age_min
            FROM system.parts WHERE database = '{self.hot_db}' AND table = '{self.hot_tbl}' AND active
            GROUP BY partition ORDER BY partition""")
        for p in out:
            p["age_min"] = round(float(p["age_min"]), 1)
            p["eligible"] = p["age_min"] >= age_min
            p["ready_in_min"] = max(0, round(age_min - p["age_min"], 1))
        return out

    def cold_rows(self) -> int | None:
        if not self.cold_exists():
            return None
        try:
            return int(self.one(f"SELECT count() FROM {self.cold}") or 0)
        except Exception:  # noqa: BLE001
            return None

    def runs(self, n: int = 12) -> list[dict]:
        try:
            return self.rows(f"SELECT toString(at) AS at, partition, rows, mode, ms, status, detail FROM obs.tier_runs ORDER BY at DESC LIMIT {int(n)}")
        except Exception:  # noqa: BLE001
            return []

    # ------------------------------------------------------------------ moving one partition
    def _count(self, table: str, s: str, e: str) -> int:
        return int(self.one(f"SELECT count() FROM {table} WHERE ts >= {lit(s)} AND ts < {lit(e)}") or 0)

    def _export_register(self, part: dict, step):
        """Write the partition as one Parquet file with a plain INSERT INTO FUNCTION s3, then register it with ice."""
        c = self.cfg
        label = f"{part['pid']}-{uuid.uuid4().hex[:6]}"
        ns, tbl = c['cold_name'].split('.', 1)
        key = f"{ns}/{tbl}/data/{label}.parquet"
        s, e = part["partition"], part["ends"]
        step("export", "run", f"Parquet -> s3://{c['bucket']}/{key}")
        self.cmd(f"INSERT INTO FUNCTION s3('{c['s3_endpoint']}/{c['bucket']}/{key}', '{c['access_key']}', '{c['secret_key']}', 'Parquet') "
                 f"SELECT {COLS} FROM {c['hot']} WHERE ts >= {lit(s)} AND ts < {lit(e)}")
        step("export", "ok", "file written")
        step("register", "run", "ice insert --no-copy")
        r = subprocess.run([*c["ice_cmd"], "insert", "-p", c["cold_name"], "--no-copy", f"s3://{c['bucket']}/{key}"],
                           capture_output=True, text=True, timeout=1800)
        if r.returncode:
            raise TierError("ice insert failed: " + (r.stderr or r.stdout)[-300:])
        step("register", "ok", f"registered in {c['cold_name']}")

    def tier_partition(self, part: dict, step=lambda *a: None) -> dict:
        c, t0 = self.cfg, time.time()
        s, e = part["partition"], part["ends"]
        hot_rows = self._count(c["hot"], s, e)
        step("count", "ok", f"{hot_rows:,} rows in hot")
        cold_have = self._count(self.cold, s, e) if self.cold_exists() else 0
        first = not self.cold_exists()
        if cold_have == hot_rows and hot_rows:
            step("copy", "ok", "already in Iceberg (resumed)")
        elif cold_have:
            raise TierError(f"{cold_have:,} rows of this window are already in Iceberg but hot has {hot_rows:,}; fix by hand")
        elif c["mode"] == "export" or first:
            self._export_register(part, step)
        else:
            step("copy", "run", "INSERT INTO Iceberg SELECT ...")
            self.cmd(f"INSERT INTO {self.cold} ({COLN}) SELECT {COLS} FROM {c['hot']} WHERE ts >= {lit(s)} AND ts < {lit(e)} "
                     "SETTINGS allow_experimental_insert_into_iceberg = 1")
            step("copy", "ok", f"{hot_rows:,} rows written")
        cold_n = self._count(self.cold, s, e)
        if cold_n != hot_rows:
            raise TierError(f"verify failed: hot={hot_rows:,} cold={cold_n:,}; hot partition kept")
        again = self._count(c["hot"], s, e)
        if again != hot_rows:
            raise TierError(f"late rows arrived in {s} during the copy ({hot_rows:,} -> {again:,}); will retry next run")
        step("verify", "ok", f"{cold_n:,} = {hot_rows:,}")
        cur = self.meta_get("wm")
        if not cur or e > cur:
            self.cmd(self.hybrid_sql(e))
            if not cur:
                self.sync_access(c["tier"], *c["tier"].split("."))
                self.sync_access(self.cold, c["catalog_db"], c["cold_name"])
                self.cmd(self.view_sql(c["tier"]))
            self.meta_set("wm", e)
        step("watermark", "ok", f"moved to {e}")
        self.cmd(f"ALTER TABLE {c['hot']} DROP PARTITION ID '{part['pid']}'")
        step("drop", "ok", f"hot partition {s} dropped")
        res = {"partition": s, "rows": hot_rows, "ms": int((time.time() - t0) * 1000)}
        self.cmd("INSERT INTO obs.tier_runs (partition, rows, mode, ms, status, detail, wm) SELECT {p:String}, {r:UInt64}, {m:String}, {ms:UInt32}, 'done', '', {w:String}",
                 {"p": s, "r": hot_rows, "m": "export" if first or c["mode"] == "export" else "insert", "ms": res["ms"], "w": e})
        return res

    def run(self, age_min: float = 60, max_parts: int = 6, step=lambda *a: None) -> dict:
        todo = [p for p in self.partitions(age_min) if p["eligible"]][:max_parts]
        done = []
        for p in todo:
            step("partition", "run", p["partition"])
            try:
                done.append(self.tier_partition(p, step))
            except Exception as ex:  # noqa: BLE001
                self.cmd("INSERT INTO obs.tier_runs (partition, rows, mode, ms, status, detail, wm) SELECT {p:String}, 0, '', 0, 'failed', {d:String}, ''",
                         {"p": p["partition"], "d": str(ex)[:400]})
                step("partition", "fail", str(ex)[:300])
                return {"done": done, "error": str(ex)[:300]}
        return {"done": done, "error": None}

    # ------------------------------------------------------------------ direct write to Iceberg
    def direct_write(self, n: int = 1000) -> dict:
        wm = self.meta_get("wm")
        if not wm or not self.cold_exists():
            raise TierError("nothing has been tiered yet, so there is no Iceberg table to write to. Run tiering once first.")
        n = max(1, min(int(n), 100000))
        svc = str(SERVICES)
        t0 = time.time()
        self.cmd(f"""INSERT INTO {self.cold} (ts, service, host, severity, status, duration_ms, trace_id, message, attrs, ingested_at)
            SELECT {lit(wm)} - toIntervalMillisecond(1 + rand() % 3600000) AS ts, svc, concat(svc, '-', toString(rand() % 5)),
                   ['INFO', 'INFO', 'WARN', 'ERROR'][1 + rand() % 4], [200, 200, 201, 404, 500][1 + rand() % 5],
                   rand() % 800, toString(generateUUIDv4()), concat('direct write demo event ', toString(number)),
                   map('source', 'direct-write', 'version', 'v1'), now()
            FROM (SELECT number, {svc}[1 + rand() % 7] AS svc FROM numbers({n}))
            SETTINGS allow_experimental_insert_into_iceberg = 1""")
        return {"written": n, "ms": int((time.time() - t0) * 1000), "wm": wm}

    def direct_count(self) -> int:
        wm = self.meta_get("wm")
        if not wm:
            return 0
        return int(self.one(f"SELECT count() FROM {self.cfg['view']} WHERE ts >= {lit(wm)} - INTERVAL 61 MINUTE AND ts < {lit(wm)} "
                            "AND attrs['source'] = 'direct-write'") or 0)

    # ------------------------------------------------------------------ one query over the three tiers
    def compare(self) -> list[dict]:
        wm, H, v = self.meta_get("wm"), self.meta_get("H"), self.cfg["view"]
        if not wm:
            raise TierError("nothing has been tiered yet")
        def run(label, where, window):
            r = self.Q(f"SELECT count() AS n, countIf(severity IN ('ERROR', 'FATAL')) AS errs, round(quantileTDigest(0.99)(duration_ms)) AS p99 FROM {v} WHERE {where}")
            n, e, p = r["rows"][0]
            return {"tier": label, "window": window, "events": int(n), "errors": int(e), "p99": p, **r["stats"]}
        return [
            run("Hot (MergeTree)", f"ts >= now() - INTERVAL 15 MINUTE", "last 15 min"),
            run("Cold (Iceberg)", f"ts >= {lit(wm)} - INTERVAL 60 MINUTE AND ts < {lit(wm)}", "60 min before the watermark"),
            run("Both (Hybrid)", f"ts >= {lit(wm)} - INTERVAL 60 MINUTE", "60 min before the watermark until now"),
            run("History (MergeTree, daily)", f"ts >= {lit(H)} - INTERVAL 6 HOUR AND ts < {lit(H)}", "6 h before the demo switch"),
        ]

    # ------------------------------------------------------------------ probe: test every moving part on tiny throwaway data
    def probe(self) -> list[tuple[str, bool, str]]:
        c, out = self.cfg, []
        src, hyb = "obs._probe_src", "obs._probe_hybrid"
        cold_name = "obs._probe_" + uuid.uuid4().hex[:6]
        cold = ident(c["catalog_db"], cold_name)

        def check(name, fn):
            try:
                msg = fn()
                out.append((name, True, str(msg or "")[:200]))
                self.log(f"  ok    {name} {msg or ''}")
                return True
            except Exception as e:  # noqa: BLE001
                out.append((name, False, str(e)[:300]))
                self.log(f"  FAIL  {name}: {str(e)[:300]}")
                return False

        def settings():
            need = ["allow_experimental_hybrid_table", "allow_experimental_insert_into_iceberg"]
            have = {r[0]: r[1] for r in self.Q("SELECT name, value FROM system.settings WHERE name IN ({n:Array(String)})", {"n": need})["rows"]}
            miss = [x for x in need if x not in have]
            if miss:
                raise TierError("settings missing on this build: " + ", ".join(miss))
            return "all three settings exist; version " + str(self.one("SELECT version()"))

        def catalog():
            return "tables in catalog: " + ", ".join(r[0] for r in self.Q(f"SELECT name FROM system.tables WHERE database = '{c['catalog_db']}' LIMIT 8")["rows"])

        def mk():
            self.cmd(f"DROP TABLE IF EXISTS {src}")
            tpl = c['hist'] if self.table_engine('obs', 'logs_hist') else c['view']
            self.cmd(f"CREATE TABLE {src} AS {tpl} ENGINE = MergeTree ORDER BY ts")
            self.cmd(f"""INSERT INTO {src} (ts, service, host, severity, status, duration_ms, trace_id, message, attrs)
                         SELECT now64(3) - number, 'checkout', 'h1', 'INFO', 200, 5, generateUUIDv4(), 'probe', map('k', 'v') FROM numbers(3)""")
            return "3 rows, same columns as logs"

        def export():
            key = f"obs/{cold_name.split('.', 1)[1]}/data/probe.parquet"
            self.cmd(f"INSERT INTO FUNCTION s3('{c['s3_endpoint']}/{c['bucket']}/{key}', '{c['access_key']}', '{c['secret_key']}', 'Parquet') "
                     f"SELECT {COLS} FROM {src} SETTINGS s3_truncate_on_insert = 1")
            self._probe_files = [f"{c['bucket']}/{key}"]
            return f"wrote {self._probe_files[0]}"

        def register():
            r = subprocess.run([*c["ice_cmd"], "insert", "-p", cold_name, "--no-copy", *[f"s3://{f}" for f in self._probe_files]],
                               capture_output=True, text=True, timeout=300)
            if r.returncode:
                raise TierError((r.stderr or r.stdout)[-300:])
            n = int(self.one(f"SELECT count() FROM {cold}"))
            if n != 3:
                raise TierError(f"read back {n} rows, expected 3")
            return "registered, 3 rows read back through the catalog"

        def types():
            self.Q(f"SELECT ts, service, status, trace_id, attrs['k'] FROM {cold} LIMIT 1")
            a = self.Q(f"SELECT toTypeName(trace_id), toTypeName(attrs), toTypeName(status), toTypeName(ts) FROM {cold} LIMIT 1")["rows"][0]
            return "types in Iceberg: " + " | ".join(a)

        def direct():
            self.cmd(f"INSERT INTO {cold} ({COLN}) SELECT {COLS} FROM {src} SETTINGS allow_experimental_insert_into_iceberg = 1")
            n = int(self.one(f"SELECT count() FROM {cold}"))
            if n != 6:
                raise TierError(f"{n} rows after INSERT, expected 6")
            return "INSERT INTO Iceberg works (6 rows now)"

        def hybrid():
            self.cmd(f"DROP TABLE IF EXISTS {hyb}")
            self.cmd(f"CREATE TABLE {hyb} AS {src} ENGINE = Hybrid(cluster('{c['cluster']}', {src}), ts >= now() + INTERVAL 1 DAY, {cold}, ts < now() + INTERVAL 1 DAY)")
            n = int(self.one(f"SELECT count() FROM {hyb}"))
            if n != 6:
                raise TierError(f"hybrid returned {n}, expected 6 (all rows are in the cold segment)")
            self.Q(f"SELECT trace_id, attrs FROM {hyb} LIMIT 2")
            self.Q(f"SELECT count() FROM {hyb} WHERE trace_id = (SELECT any(trace_id) FROM {hyb})")
            return "2-segment Hybrid over MergeTree + Iceberg works, columns cast cleanly"

        def clean():
            for t in (hyb, src):
                self.cmd(f"DROP TABLE IF EXISTS {t}")
            try:
                self.cmd(f"DROP TABLE IF EXISTS {cold}")
                return "dropped probe tables (parquet under staging/_probe is left)"
            except Exception as e:  # noqa: BLE001
                return f"probe Iceberg table left behind ({str(e)[:80]})"

        self.log("probe: nothing here touches obs.logs")
        check("settings", settings)
        check("catalog", catalog)
        if check("create throwaway source", mk):
            if check("write Parquet to MinIO", export):
                if check("ice insert --no-copy", register):
                    check("Iceberg column types", types)
                    check("INSERT INTO Iceberg", direct)
                    check("Hybrid over MergeTree + Iceberg", hybrid)
        check("cleanup", clean)
        return out


# ---------------------------------------------------------------------- CLI
def _cli_client():
    import clickhouse_connect
    cfg = json.loads((HERE / "config.json").read_text())
    ch = cfg["clickhouse"]
    cl = clickhouse_connect.get_client(host=ch["host"], port=ch["http_port"], username=ch["user"], password=ch["password"],
                                       autogenerate_session_id=False, send_receive_timeout=3600)

    def Q(sql, params=None):
        r = cl.query(sql, parameters=params or {})
        return {"cols": list(r.column_names), "rows": [list(x) for x in r.result_rows], "stats": {}}

    def cmd(sql, params=None):
        cl.command(sql, parameters=params or {})
    return Q, cmd, cfg.get("tiering", {})


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["probe", "setup", "run", "status", "rollback"])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--age", type=float, default=60, help="minutes after a partition closes before it is tiered (0 = every closed partition)")
    ap.add_argument("--max", type=int, default=6, help="partitions per run")
    ap.add_argument("--loop", type=int, default=0, help="repeat every N seconds")
    a = ap.parse_args(argv)
    Q, cmd, tcfg = _cli_client()
    t = Tier(Q, cmd, tcfg)
    if a.action == "probe":
        res = t.probe()
        return 0 if all(ok for _, ok, _ in res) else 1
    if a.action == "setup":
        t.setup(a.dry_run)
    elif a.action == "rollback":
        t.rollback()
    elif a.action == "status":
        print(json.dumps({"setup": t.is_setup(), "watermark": t.meta_get("wm"), "history_boundary": t.meta_get("H"),
                          "hot_partitions": t.partitions(a.age) if t.is_setup() else [], "cold_rows": t.cold_rows(), "runs": t.runs()},
                         indent=2, default=str))
    else:
        while True:
            r = t.run(a.age, a.max, lambda n, s, d="": print(f"  [{s}] {n} {d}"))
            print(time.strftime("%H:%M:%S"), f"tiered {len(r['done'])} partition(s)", r["error"] or "")
            if not a.loop:
                return 1 if r["error"] else 0
            time.sleep(a.loop)
    return 0


if __name__ == "__main__":
    sys.exit(main())

