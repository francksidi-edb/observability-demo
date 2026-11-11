/* Alerts / Agent tab: adds itself to the nav, so index.html only needs <script src="/static/agent.js"></script>.
   Relies on the page globals $, api, post, esc, nf, show (same as tiering.js). */
(function () {
  "use strict";
  const css = `
  .ag-checks { display:flex; flex-wrap:wrap; gap:8px; margin:4px 0 0; }
  .ag-top { display:flex; flex-wrap:wrap; gap:14px; align-items:center; }
  .ag-sw { display:inline-flex; gap:8px; align-items:center; font-weight:600; cursor:pointer; }
  .ag-sw input { width:18px; height:18px; accent-color:var(--coral); }
  .ag-rates { display:grid; grid-template-columns:repeat(auto-fill,minmax(150px,1fr)); gap:10px; }
  .ag-rate { border:1px solid var(--mist); border-radius:8px; padding:10px 12px; }
  .ag-rate.hit { border-color:var(--coral); border-left:5px solid var(--coral); }
  .ag-rate b { font:700 1.5rem var(--head); display:block; font-variant-numeric:tabular-nums; }
  .ag-rate span { font-size:.78rem; color:var(--slate); }
  .ag-alert { border-left:5px solid var(--slate); }
  .ag-alert.proposed, .ag-alert.detected, .ag-alert.investigating { border-left-color:var(--coral); }
  .ag-alert.executed, .ag-alert.resolved, .ag-alert.acknowledged { border-left-color:var(--green,#05825F); }
  .ag-alert.error { border-left-color:#c0392b; }
  .ag-head { display:flex; flex-wrap:wrap; gap:10px; align-items:baseline; margin-bottom:6px; }
  .ag-head .big { font:700 1.6rem var(--head); font-variant-numeric:tabular-nums; }
  .ag-dx { margin:8px 0; font-size:.95rem; line-height:1.45; }
  .ag-ev { margin:4px 0 8px 18px; padding:0; font-size:.86rem; color:var(--slate); }
  .ag-act { display:flex; flex-wrap:wrap; gap:10px; align-items:center; margin:10px 0 4px; }
  .ag-tr { display:grid; gap:7px; margin-top:8px; font-size:.85rem; }
  .ag-tr .st { display:grid; grid-template-columns:48px 78px 1fr; gap:8px; align-items:start; }
  .ag-tr pre { margin:4px 0 0; padding:7px 9px; background:var(--frost,#E8F2F5); border-radius:6px; overflow:auto; font-size:.78rem; white-space:pre-wrap; }
  .ag-tr table { margin-top:4px; font-size:.78rem; }
  details.ag-d > summary { cursor:pointer; color:var(--slate); font-size:.85rem; margin-top:6px; }
  `;
  const style = document.createElement("style"); style.textContent = css; document.head.appendChild(style);

  const nav = $("#nav"), codeBtn = nav.querySelector('[data-tab="code"]');
  const btn = document.createElement("button");
  btn.setAttribute("role", "tab"); btn.setAttribute("aria-selected", "false"); btn.dataset.tab = "agent"; btn.textContent = "Alerts / Agent";
  nav.insertBefore(btn, codeBtn); btn.onclick = () => show("agent");

  const sec = document.createElement("section");
  sec.className = "tab"; sec.id = "tab-agent";
  sec.innerHTML = `
  <div class="mast"><div class="eyebrow">Operations</div><h1>A bad deploy lands. Claude finds it and asks before it acts.</h1>
    <p class="sub">A detector watches the per-minute error rate of every service. When one jumps far above its own baseline, Claude investigates the logs with read-only SQL and proposes what to do. Nothing runs until you approve.</p></div>

  <div class="card"><div class="hd"><h2>Agent</h2><p class="muted small" id="ag-note"></p></div>
    <div class="ag-top">
      <label class="ag-sw"><input type="checkbox" id="ag-on"> Watch for incidents</label>
      <button class="btn" id="ag-check" type="button">Check now</button>
      <button class="btn primary" id="ag-bad" type="button">Trigger bad deploy</button>
      <button class="btn" id="ag-end" type="button">End incident</button>
      <span class="muted small" id="ag-msg"></span>
    </div>
    <div class="ag-checks" id="ag-checks"></div>
  </div>

  <div class="card"><div class="hd"><h2>Error rate now, per service</h2><p class="muted small" id="ag-rnote">current minute against the 30 minutes before it, from <span class="mono">obs.svc_1m</span></p></div>
    <div class="ag-rates" id="ag-rates"></div></div>

  <div id="ag-alerts"></div>`;
  $("main").appendChild(sec);

  const open = new Set();          // alert ids whose investigation is expanded
  let lastKey = "", timer = null;
  const hhmmss = s => new Date(s * 1000).toISOString().slice(11, 19) + " UTC";
  const pct = x => (+x).toFixed(x >= 10 ? 1 : 2) + "%";
  const SC = { detected: "warn", investigating: "warn", proposed: "warn", executed: "ok", acknowledged: "ok", resolved: "ok", dismissed: "", error: "bad" };
  const SL = { detected: "detected", investigating: "Claude is investigating", proposed: "waiting for you", executed: "action done", acknowledged: "acknowledged", resolved: "resolved", dismissed: "dismissed", error: "failed" };
  const ICON = { detect: "DETECT", start: "START", think: "THINK", query: "SQL", result: "RESULT", deploy: "DEPLOY", propose: "PROPOSE", action: "YOU", resolve: "RESOLVED", error: "ERROR" };

  function traceHtml(tr) {
    return `<div class="ag-tr">` + tr.map(x => {
      let body = `<span>${esc(x.text || "")}</span>`;
      if (x.sql) body += `<pre>${esc(x.sql)}</pre>`;
      if (x.kind === "result" && !x.error && x.rows && x.rows.length)
        body += `<table><tr>${(x.columns || []).map(c => `<th>${esc(c)}</th>`).join("")}</tr>` +
          x.rows.map(r => `<tr>${r.map(v => `<td class="num">${esc(v === null ? "" : typeof v === "number" ? nf(v) : v)}</td>`).join("")}</tr>`).join("") + `</table>`;
      return `<div class="st"><span class="muted num">${(+x.t).toFixed(1)}s</span><span class="chip ${x.error || x.kind === "error" ? "bad" : x.kind === "propose" ? "warn" : x.kind === "action" || x.kind === "resolve" ? "ok" : ""}">${ICON[x.kind] || esc(x.kind)}</span><div>${body}</div></div>`;
    }).join("") + `</div>`;
  }

  function alertHtml(a) {
    const p = a.proposal, mult = a.base_pct > 0 ? (a.recent_pct / a.base_pct) : 0;
    const dur = a.trace.length ? a.trace[a.trace.length - 1].t : 0;
    let h = `<div class="card ag-alert ${a.status}" data-id="${esc(a.id)}"><div class="ag-head">
      <span class="chip" style="font-weight:700">${esc(a.service)}</span><span class="chip ${SC[a.status] || ""}">${esc(SL[a.status] || a.status)}</span>
      <span class="big">${pct(a.recent_pct)}</span><span class="muted small">errors, baseline ${pct(a.base_pct)}${mult ? ` · ${mult.toFixed(0)}× normal` : ""} · ${nf(a.recent_events)} events · ${hhmmss(a.created)}</span></div>`;
    if (a.status === "investigating" || a.status === "detected") h += `<p class="muted small">Claude is reading the logs… ${a.trace.filter(x => x.kind === "query").length} quer${a.trace.filter(x => x.kind === "query").length === 1 ? "y" : "ies"} so far.</p>`;
    if (a.diagnosis) h += `<p class="ag-dx"><b>Diagnosis.</b> ${esc(a.diagnosis)}</p>` + (p && p.evidence && p.evidence.length ? `<ul class="ag-ev">${p.evidence.map(e => `<li>${esc(e)}</li>`).join("")}</ul>` : "");
    if (a.status === "proposed" && p) {
      const rb = p.action === "rollback_deploy";
      h += `<div class="ag-act"><span><b>Proposed:</b> ${rb ? "roll back <b>" + esc(p.service) + "</b> to the previous release" : "notify a person, no automatic action"} <span class="chip">${esc(p.confidence)} confidence</span></span>
        <button class="btn primary" data-act="approve" type="button">${rb ? "Approve rollback" : "Acknowledge"}</button><button class="btn" data-act="dismiss" type="button">Dismiss</button></div>
        <p class="tier-note">Nothing has run. The agent can only propose.</p>`;
    }
    if (a.status === "error") h += `<div class="ag-act"><span class="chip bad">failed</span><span class="small">${esc(a.result)}</span><button class="btn" data-act="again" type="button">Investigate again</button></div>`;
    else if (a.result && a.status !== "proposed") h += `<p class="small muted">${esc(a.result)}</p>`;
    h += `<details class="ag-d" ${open.has(a.id) ? "open" : ""}><summary>Investigation: ${a.trace.length} steps, ${(+dur).toFixed(0)} s</summary>${traceHtml(a.trace)}</details></div>`;
    return h;
  }

  async function load() {
    let s; try { s = await api("/api/agent/state"); } catch (e) { return; }
    const key = JSON.stringify([s.enabled, s.checks, s.rates, s.alerts, s.error || ""]);
    if (key === lastKey) return;
    lastKey = key;
    $("#ag-on").checked = !!s.enabled;
    const t = s.thresholds || {};
    $("#ag-note").textContent = t.factor ? `alert at ${t.factor}× baseline and at least ${t.min_pct}%, over ${t.min_events}+ events · looks every ${s.interval_s} s · ${s.model}` : "";
    const C = s.checks || {}, names = { tables: "alert table", api_key: "Claude API key", agent_user: "read-only SQL user", simulator: "release system" };
    $("#ag-checks").innerHTML = Object.keys(names).filter(k => C[k]).map(k => `<span class="chip ${C[k].ok ? "ok" : "bad"}" title="${esc(C[k].detail || "")}">${names[k]}${C[k].ok ? "" : ": " + esc((C[k].detail || "not ok").slice(0, 70))}</span>`).join("");
    $("#ag-rates").innerHTML = (s.rates || []).length ? s.rates.map(r => `<div class="ag-rate ${r.hit ? "hit" : ""}"><span>${esc(r.service)}</span><b>${pct(r.recent_pct)}</b><span>baseline ${pct(r.base_pct)}${r.hit ? " · over threshold" : ""}</span></div>`).join("")
      : `<div class="empty">${s.enabled ? "Waiting for the first check…" : "Switch the agent on, or click Check now."}</div>`;
    const al = s.alerts || [];
    $("#ag-alerts").innerHTML = al.length ? al.map(alertHtml).join("")
      : `<div class="card"><div class="empty">No alerts. Click <b>Trigger bad deploy</b> and watch the agent work.</div></div>`;
    document.querySelectorAll("#ag-alerts details.ag-d").forEach(d => d.addEventListener("toggle", () => {
      const id = d.closest(".ag-alert").dataset.id; d.open ? open.add(id) : open.delete(id); }));
  }

  const msg = t => { $("#ag-msg").textContent = t || ""; };
  async function act(fn) { try { msg(""); await fn(); } catch (e) { msg(e.message || String(e)); } lastKey = ""; load(); }
  $("#ag-on").onchange = () => act(() => post("/api/agent/settings", { enabled: $("#ag-on").checked }));
  $("#ag-check").onclick = () => act(async () => { msg("checking…"); const r = await post("/api/agent/check", {}); msg(r.opened && r.opened.length ? "alert opened" : "no new alert"); });
  $("#ag-bad").onclick = () => act(async () => { await post("/api/sim/incident?on=1&minutes=0", {}); msg("bad deploy started on payments; the error rate follows within a minute"); });
  $("#ag-end").onclick = () => act(async () => { await post("/api/sim/incident?on=0", {}); msg("incident ended"); });
  $("#ag-alerts").addEventListener("click", e => {
    const b = e.target.closest("button[data-act]"); if (!b) return;
    const id = b.closest(".ag-alert").dataset.id, what = b.dataset.act;
    b.disabled = true;
    act(() => post(`/api/agent/alerts/${id}/${what === "again" ? "investigate" : what}`, {}));
  });

  const baseShow = window.show;
  window.show = function (tab) {
    baseShow(tab);
    clearInterval(timer);
    if (tab === "agent") { lastKey = ""; load(); timer = setInterval(load, 2000); }
  };
})();
