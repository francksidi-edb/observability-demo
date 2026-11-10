/* Tiering tab: adds itself to the nav, so index.html only needs <script src="/static/tiering.js"></script>.
   Relies on the page globals $, $$, api, post, esc, nf, human, bytes, st, show. */
(function () {
  "use strict";
  const css = `
  .tier-flow { display:grid; grid-template-columns:1fr 28px 1fr 28px 1fr; align-items:stretch; gap:6px; margin-bottom:22px; }
  .tier-flow .box { border:1px solid var(--mist); border-radius:8px; padding:12px 14px; }
  .tier-flow .box.hot { border-left:5px solid var(--coral); } .tier-flow .box.cold { border-left:5px solid var(--slate); } .tier-flow .box.hist { border-left:5px solid var(--gold); }
  .tier-flow .arrow { align-self:center; text-align:center; color:var(--slate); font-size:1.4rem; }
  .tier-flow .box b { font:700 1.7rem var(--head); display:block; font-variant-numeric:tabular-nums; }
  .tier-flow .box span { font-size:.8rem; color:var(--slate); }
  @media (max-width:860px) { .tier-flow { grid-template-columns:1fr; } .tier-flow .arrow { transform:rotate(90deg); } }
  .tbrow { display:grid; grid-template-columns:130px minmax(0,1fr) 270px; gap:12px; align-items:center; font-size:.85rem; padding:3px 0; }
  .tbrow .track i { background:var(--slate); } .tbrow .track i.ready { background:var(--coral); }
  .tsteps { display:grid; gap:6px; font-size:.88rem; }
  .tsteps div { display:grid; grid-template-columns:70px 100px 1fr; gap:10px; align-items:center; }
  .tier-note { font-size:.85rem; color:var(--slate); margin:6px 0 0; }
  `;
  const style = document.createElement("style"); style.textContent = css; document.head.appendChild(style);

  const nav = $("#nav"), codeBtn = nav.querySelector('[data-tab="code"]');
  const btn = document.createElement("button");
  btn.setAttribute("role", "tab"); btn.setAttribute("aria-selected", "false"); btn.dataset.tab = "tiering"; btn.textContent = "Tiering";
  nav.insertBefore(btn, codeBtn); btn.onclick = () => show("tiering");

  const sec = document.createElement("section");
  sec.className = "tab"; sec.id = "tab-tiering";
  sec.innerHTML = `
  <div class="mast"><div class="eyebrow">Lifecycle</div><h1>Hot for an hour, then cheap and still queryable</h1>
    <p class="sub">New rows land in MergeTree. After the retention window a job moves each partition to Iceberg on object storage. The dashboards keep querying the same table name.</p></div>
  <div id="t-off" class="empty" style="display:none"></div>
  <div id="t-main" style="display:none">
    <div class="tier-flow">
      <div class="box hot"><span>HOT · MergeTree (local disk)</span><b id="t-hot">–</b><span id="t-hot2"></span></div>
      <div class="arrow">→</div>
      <div class="box cold"><span>COLD · Iceberg (MinIO / S3)</span><b id="t-cold">–</b><span id="t-cold2"></span></div>
      <div class="arrow">+</div>
      <div class="box hist"><span>HISTORY · MergeTree, daily partitions</span><b id="t-histn">–</b><span id="t-hist2">copied from the previous ClickHouse</span></div>
    </div>

    <div class="card"><div class="hd"><h2>Policy</h2><p class="muted small">the job tiers a partition once it has been closed for this long</p></div>
      <div class="row">
        <label class="f">Tier after (minutes)<input type="number" id="t-age" min="0" max="1440" step="1" value="60" style="width:130px"></label>
        <label class="f">Background job<span class="row"><input type="checkbox" id="t-auto"> <span class="small">run every 30 s</span></span></label>
        <button class="btn primary" id="t-run" type="button">Run tiering now</button>
        <span class="muted small" id="t-msg"></span>
      </div>
      <p class="tier-note" id="t-ttlnote"></p>
    </div>

    <div class="card"><div class="hd"><h2>Hot partitions</h2><p class="muted small" id="t-pnote"></p></div><div id="t-parts"></div></div>

    <div class="grid2">
      <div class="card"><div class="hd"><h2>Last run</h2><p class="muted small" id="t-jobstate"></p></div><div class="tsteps" id="t-steps"></div></div>
      <div class="card"><div class="hd"><h2>Write straight to Iceberg</h2><p class="muted small">late data or a backfill, no MergeTree involved</p></div>
        <div class="row"><label class="f">Events<input type="number" id="t-n" value="5000" min="1" max="100000" style="width:130px"></label>
          <button class="btn" id="t-write" type="button">INSERT INTO Iceberg</button></div>
        <p class="tier-note" id="t-wres"></p></div>
    </div>

    <div class="card"><div class="hd"><h2>Same query, every tier</h2><p class="muted small">count + p99 on <span class="mono">obs.logs</span>, the name the dashboards use</p>
        <button class="btn" id="t-cmp" type="button" style="margin-left:auto">Run comparison</button></div>
      <div id="t-cmpres"></div></div>

    <div class="card"><div class="hd"><h2>Recent runs</h2><p class="muted small">obs.tier_runs</p></div><div id="t-runs"></div></div>
  </div>`;
  $("main").appendChild(sec);

  let timer = null, editing = false, lastPolicy = null;
  const ago = m => m < 1 ? "just closed" : m < 90 ? Math.round(m) + " min ago" : (m / 60).toFixed(1) + " h ago";

  async function load() {
    let s; try { s = await api("/api/tier/state"); } catch (e) { return; }
    const off = $("#t-off"), main = $("#t-main");
    if (!s.setup) {
      off.style.display = ""; main.style.display = "none";
      off.innerHTML = `Tiering is not set up on this database yet.${s.error ? "<br><span class='mono'>" + esc(s.error) + "</span>" : ""}<br><br>
        <span class="mono">python tier_job.py probe</span> then <span class="mono">python tier_job.py setup</span> on the host, then reload this page.`;
      return;
    }
    off.style.display = "none"; main.style.display = "";
    $("#t-histn").textContent = human(s.hist_rows); $("#t-hot").textContent = human(s.hot_rows); $("#t-hot2").textContent = `${s.partitions.length} partitions · ${bytes(s.hot_bytes)} on disk`;
    $("#t-cold").textContent = s.cold_rows == null ? "not created yet" : human(s.cold_rows);
    $("#t-cold2").textContent = s.watermark ? `everything before ${s.watermark} UTC` : "created by the first tiering run";
    if (!editing) { $("#t-age").value = s.policy.age_min; $("#t-auto").checked = s.policy.auto; }
    $("#t-ttlnote").innerHTML = `Equivalent of <span class="mono">TTL ts + INTERVAL ${esc(s.policy.age_min)} MINUTE TO VOLUME 'iceberg'</span>, run by a job (mode: <b>${esc(s.mode)}</b>). ` +
      `Order per partition: copy, verify counts, move the Hybrid watermark, drop the hot partition.`;
    $("#t-pnote").textContent = `one partition per ${s.step_min} min · server time ${s.server_now} UTC`;
    const max = Math.max(1, ...s.partitions.map(p => +p.rows));
    $("#t-parts").innerHTML = s.partitions.length ? s.partitions.map(p => {
      const open = p.age_min < 0;
      const tag = open ? `<span class="chip">open, still receiving</span>` : p.eligible ? `<span class="chip warn">ready to tier</span>` : `<span class="chip ok">tiers in ${Math.ceil(p.ready_in_min)} min</span>`;
      return `<div class="tbrow"><span class="mono">${esc(p.partition.slice(5, 16))}</span><div class="track"><i class="${p.eligible ? "ready" : ""}" style="width:${Math.max(2, 100 * p.rows / max)}%"></i></div>
        <span class="num">${human(+p.rows)} · ${bytes(+p.bytes)} ${tag}</span></div>`; }).join("") : `<div class="empty">No hot partitions. Is the simulator running?</div>`;
    const j = s.job;
    $("#t-jobstate").innerHTML = j.state === "running" ? `<span class="chip warn">running</span>` : j.state === "done" ? `<span class="chip ok">done · ${j.tiered} partition(s)</span>` :
      j.state === "error" ? `<span class="chip bad">stopped</span>` : `<span class="chip">idle</span>`;
    $("#t-steps").innerHTML = (j.steps || []).slice(-14).map(x => `<div><span class="muted num">${x.t}s</span><span class="chip ${x.status === "ok" ? "ok" : x.status === "fail" ? "bad" : "warn"}">${esc(x.name)}</span><span class="small">${esc(x.detail)}</span></div>`).join("") +
      (j.error ? `<div><span></span><span class="chip bad">error</span><span class="small">${esc(j.error)}</span></div>` : "") || `<span class="muted small">Nothing has run since the app started.</span>`;
    $("#t-runs").innerHTML = s.runs.length ? `<table><tr><th>At (UTC)</th><th>Partition</th><th class="r">Rows</th><th>Mode</th><th class="r">Time</th><th>Status</th></tr>` +
      s.runs.map(r => `<tr><td class="mono">${esc(r.at)}</td><td class="mono">${esc(r.partition)}</td><td class="r num">${nf(r.rows)}</td><td>${esc(r.mode)}</td><td class="r num">${(r.ms / 1000).toFixed(1)} s</td>
        <td><span class="chip ${r.status === "done" ? "ok" : "bad"}">${esc(r.status)}</span>${r.detail ? " <span class='small muted'>" + esc(r.detail.slice(0, 90)) + "</span>" : ""}</td></tr>`).join("") + `</table>` : `<div class="empty">No runs yet.</div>`;
    if (s.direct) $("#t-wres").innerHTML = `Currently visible through <span class="mono">obs.logs</span>: <b>${nf(s.direct)}</b> rows with attrs['source'] = 'direct-write'.`;
  }

  async function savePolicy() {
    editing = false;
    try { await post("/api/tier/policy", { age_min: +$("#t-age").value || 0, auto: $("#t-auto").checked }); } catch (e) { $("#t-msg").textContent = e.message; }
  }
  $("#t-age").oninput = () => { editing = true; };
  $("#t-age").onchange = savePolicy; $("#t-auto").onchange = savePolicy;
  $("#t-run").onclick = async () => {
    await savePolicy(); $("#t-msg").textContent = "";
    try { await post("/api/tier/run", { age_min: +$("#t-age").value || 0 }); } catch (e) { $("#t-msg").textContent = e.message; }
    load();
  };
  $("#t-write").onclick = async () => {
    const b = $("#t-write"); b.disabled = true; $("#t-wres").textContent = "writing…";
    try { const r = await post("/api/tier/write", { n: +$("#t-n").value || 1000 });
      $("#t-wres").innerHTML = `Wrote <b>${nf(r.written)}</b> events to Iceberg in ${r.ms} ms. They are visible through <span class="mono">obs.logs</span> right away: <b>${nf(r.visible)}</b> rows with source = direct-write.`;
    } catch (e) { $("#t-wres").innerHTML = `<span class="chip bad">failed</span> <span class="small">${esc(e.message)}</span>`; }
    b.disabled = false; load();
  };
  $("#t-cmp").onclick = async () => {
    const b = $("#t-cmp"); b.disabled = true; $("#t-cmpres").innerHTML = `<div class="empty">running four queries…</div>`;
    try { const r = await api("/api/tier/compare");
      $("#t-cmpres").innerHTML = `<table><tr><th>Where the rows live</th><th>Window</th><th class="r">Events</th><th class="r">p99 ms</th><th class="r">Rows read</th><th class="r">Time</th></tr>` +
        r.rows.map(x => `<tr><td><b>${esc(x.tier)}</b></td><td class="small">${esc(x.window)}</td><td class="r num">${nf(x.events)}</td><td class="r num">${nf(x.p99)}</td><td class="r num">${human(x.read_rows)}</td><td class="r num"><b>${x.ms} ms</b></td></tr>`).join("") + `</table>
        <p class="tier-note">Same SQL text each time, only the time window changes. The Hybrid watermark decides which segment is read.</p>`;
    } catch (e) { $("#t-cmpres").innerHTML = `<div class="empty">${esc(e.message)}</div>`; }
    b.disabled = false;
  };

  const baseShow = window.show;
  window.show = function (tab) {
    baseShow(tab);
    clearInterval(timer);
    if (tab === "tiering") { load(); timer = setInterval(load, 3000); }
  };

  /* ---- Code tab: add the tiering section ------------------------------------------------------ */
  const TIER_SNIPS = [
    ["bash", "Set up tiering once", "cd dashboard\n./tier.sh probe     # checks Hybrid tables, Iceberg writes, ice and MinIO\n./tier.sh setup     # logs -> logs_hist, creates logs_hot, logs_tier (Hybrid), the obs.logs view\n./tier.sh status    # watermark, hot partitions, last runs\n# undo: ./tier.sh rollback"],
    ["bash", "Run tiering from a shell", "./tier.sh run --age 60            # tier every partition closed for 60 minutes or more\n./tier.sh run --age 0 --max 1     # test: tier only the oldest closed partition\n./tier.sh run --age 60 --loop 60  # keep running, check every 60 s (or use the Tiering tab)"],
    ["sql", "The tiering objects", "obs.logs_hot   -- MergeTree, PARTITION BY 10-minute buckets; the Kafka views write here\nobs.logs_hist  -- MergeTree, the rows copied in before the cutover\nice.`obs.logs_cold` -- Iceberg table on MinIO, created by the first run\nobs.logs_tier  -- Hybrid: hot where ts >= W, Iceberg where ts < W\nobs.logs       -- view the dashboards query: logs_hist UNION ALL logs_tier\nobs.tier_meta, obs.tier_runs -- the watermark W and the log of runs"],
    ["sql", "What one run does to a partition", "-- 1 count\nSELECT count() FROM obs.logs_hot WHERE ts >= {start} AND ts < {end};\n\n-- 2 copy: first run writes Parquet and registers it with ice, later runs insert\nINSERT INTO ice.`obs.logs_cold` (ts, service, host, severity, status, duration_ms, trace_id, message, attrs, ingested_at)\nSELECT ts, service, host, severity, status, duration_ms, toString(trace_id), message, attrs, ingested_at\nFROM obs.logs_hot WHERE ts >= {start} AND ts < {end}\nSETTINGS allow_experimental_insert_into_iceberg = 1;\n\n-- 3 verify: the same range counted in Iceberg must equal step 1\nSELECT count() FROM ice.`obs.logs_cold` WHERE ts >= {start} AND ts < {end};\n\n-- 4 move the watermark: W becomes the end of the partition\nCREATE OR REPLACE TABLE obs.logs_tier AS obs.logs_hot ENGINE = Hybrid(\n  cluster('demo', obs.logs_hot), ts >= {end},\n  ice.`obs.logs_cold`, ts < {end});\n\n-- 5 drop the hot copy\nALTER TABLE obs.logs_hot DROP PARTITION ID '{partition_id}';"],
    ["sql", "Look at tiering", "-- hot partitions and their age\nSELECT partition, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS size\nFROM system.parts WHERE database = 'obs' AND table = 'logs_hot' AND active\nGROUP BY partition ORDER BY partition;\n\n-- the runs so far\nSELECT * FROM obs.tier_runs ORDER BY at DESC LIMIT 10;\n\n-- rows per tier\nSELECT 'history' AS tier, count() FROM obs.logs_hist\nUNION ALL SELECT 'hot', count() FROM obs.logs_hot\nUNION ALL SELECT 'iceberg', count() FROM ice.`obs.logs_cold`;"],
    ["sql", "Write straight to Iceberg", "-- late data or a backfill: no MergeTree involved\nINSERT INTO ice.`obs.logs_cold` (ts, service, host, severity, status, duration_ms, trace_id, message, attrs, ingested_at)\nSELECT ts, service, host, severity, status, duration_ms, toString(trace_id), message, attrs, ingested_at\nFROM my_backfill_source\nSETTINGS allow_experimental_insert_into_iceberg = 1;\n-- the rows show through obs.logs as long as ts < the watermark W"]
  ];
  const escH = x => String(x).replace(/&/g, "&amp;").replace(/</g, "&lt;");
  let tierCodeDone = false;
  async function addTierCode() {
    const body = document.getElementById("code-body");
    if (!body || tierCodeDone || !body.children.length) return;
    tierCodeDone = true;
    const html = `<div class="chead"><span class="no">06</span><img src="/static/icons/warehouse.svg" alt=""><div><h2>Tier</h2><p>Move old logs to Iceberg on object storage</p></div></div>` +
      TIER_SNIPS.map(x => `<details class="snip"><summary><span class="lang">${x[0]}</span><span>${escH(x[1])}</span><button class="btn copy" type="button">Copy</button></summary><pre><code>${escH(x[2])}</code></pre></details>`).join("");
    body.insertAdjacentHTML("beforeend", html);
    body.querySelectorAll("details.snip").forEach(d => {
      const b = d.querySelector(".copy");
      if (b && !b.dataset.t) { b.dataset.t = 1; b.onclick = e => { e.preventDefault(); e.stopPropagation(); navigator.clipboard.writeText(d.querySelector("code").textContent); b.textContent = "Copied"; setTimeout(() => b.textContent = "Copy", 1200); }; }
    });
  }
  // add the section whenever the Code tab fills its container, however the tab was opened
  (function watchCode() {
    const body = document.getElementById("code-body");
    if (!body) { setTimeout(watchCode, 300); return; }
    new MutationObserver(() => addTierCode()).observe(body, { childList: true });
    addTierCode();
  })();
})();

