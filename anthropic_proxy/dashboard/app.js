const WINDOWS = ["1m", "10m", "1h", "5h", "24h"];
function fmtDur(s) {
  if (s === null || s === undefined) return "—";
  if (s < 1)   return (s * 1000).toFixed(0) + "ms";
  if (s < 60)  return s.toFixed(2) + "s";
  if (s < 3600) return (s / 60).toFixed(1) + "m";
  return (s / 3600).toFixed(2) + "h";
}
function fmtSpan(s) {
  // Coarse "2h14m" / "47m" / "12s" for window timers.
  if (s === null || s === undefined) return "—";
  s = Math.max(0, Math.round(s));
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
  if (h > 0) return h + "h" + String(m).padStart(2, "0") + "m";
  if (m > 0) return m + "m";
  return s + "s";
}
function fmtNum(n) {
  if (n === null || n === undefined) return "—";
  if (n === 0) return "0";
  const abs = Math.abs(n);
  if (abs >= 1e9) return (n / 1e9).toFixed(2) + "B";
  if (abs >= 1e6) return (n / 1e6).toFixed(2) + "M";
  if (abs >= 1e3) return (n / 1e3).toFixed(1) + "k";
  return n.toString();
}
function fmtCost(c) {
  if (c === null || c === undefined) return "—";
  if (c === 0) return "$0.00";
  if (c < 0.01) return "<$0.01";
  if (c < 1) return "$" + c.toFixed(3);
  if (c < 100) return "$" + c.toFixed(2);
  return "$" + c.toFixed(0);
}
const METRIC_NOUN = { requests: "req", tokens: "tok", cost: "" };
function fmtWinLen(s) {
  // Budget window length: "1min", "5h", "90s". Minutes are spelled "min"
  // because card labels are uppercased — "1M" would read as a million next
  // to token counts.
  if (s === null || s === undefined) return "?";
  if (s % 3600 === 0) return (s / 3600) + "h";
  if (s % 60 === 0) return (s / 60) + "min";
  return s + "s";
}
function fmtUnits(metric, v, rounded) {
  // A quota quantity in its budget's units: $12.34 / 312.5k / 140.
  if (v === null || v === undefined) return "—";
  if (metric === "cost") return Number.isInteger(v) ? "$" + v : fmtCost(v);
  if (rounded) v = Math.round(v);
  return fmtNum(v);
}
const COLORS = {
  ok: "#3fb950", err: "#f85149",
  input: "#58a6ff", output: "#a371f7",
  cache: "#d29922",
};
function fmtTime(t, step) {
  const d = new Date(t * 1000);
  const p = (n) => String(n).padStart(2, "0");
  if (step < 86400) return p(d.getHours()) + ":" + p(d.getMinutes());
  return (d.getMonth() + 1) + "/" + d.getDate();
}
function setLegend(id, items) {
  document.getElementById(id).innerHTML = items
    .map((it) => `<span><i style="background:${it.c}"></i>${it.l}</span>`).join("");
}
// Stacked-bar chart drawn as raw SVG. `series` is a list of
// {key|fn, color}; each point's segments stack bottom-up.
function drawChart(svgId, data, series, fmtVal) {
  const svg = document.getElementById(svgId);
  const pts = data.points || [];
  const gridColor = getComputedStyle(document.documentElement)
    .getPropertyValue("--grid").trim() || "#30363d";
  const W = svg.clientWidth || svg.parentElement.clientWidth || 800;
  const H = 150, padL = 46, padR = 8, padT = 8, padB = 16;
  const plotW = W - padL - padR, plotH = H - padT - padB;
  const val = (p, s) => s.fn ? s.fn(p) : (p[s.key] || 0);
  let max = 0;
  for (const p of pts) {
    let tot = 0;
    for (const s of series) tot += val(p, s);
    if (tot > max) max = tot;
  }
  max = max || 1;
  const n = pts.length || 1;
  const bw = plotW / n;
  const x = (i) => padL + i * bw;
  const y = (v) => padT + plotH - (v / max) * plotH;
  let svgParts = [];
  // gridlines + y labels (0, max/2, max)
  for (const frac of [0, 0.5, 1]) {
    const yy = padT + plotH - frac * plotH;
    svgParts.push(`<line x1="${padL}" y1="${yy}" x2="${W - padR}" y2="${yy}" stroke="${gridColor}" stroke-width="1"/>`);
    svgParts.push(`<text x="${padL - 4}" y="${yy + 3}" text-anchor="end">${fmtVal(max * frac)}</text>`);
  }
  const gap = bw > 4 ? Math.min(2, bw * 0.2) : 0;
  for (let i = 0; i < pts.length; i++) {
    const p = pts[i];
    let base = 0;
    for (const s of series) {
      const v = val(p, s);
      if (v <= 0) continue;
      const yTop = y(base + v), yBot = y(base);
      svgParts.push(
        `<rect x="${(x(i) + gap / 2).toFixed(1)}" y="${yTop.toFixed(1)}" ` +
        `width="${Math.max(0.5, bw - gap).toFixed(1)}" height="${Math.max(0, yBot - yTop).toFixed(1)}" ` +
        `fill="${s.color}"><title>${new Date(p.t * 1000).toLocaleString()}: ${fmtVal(v)}</title></rect>`
      );
      base += v;
    }
  }
  // x labels: first, middle, last
  if (pts.length) {
    const idxs = [0, Math.floor(pts.length / 2), pts.length - 1];
    const anchors = ["start", "middle", "end"];
    idxs.forEach((i, k) => {
      const xx = padL + (i + 0.5) * bw;
      svgParts.push(`<text x="${xx.toFixed(1)}" y="${H - 4}" text-anchor="${anchors[k]}">${fmtTime(pts[i].t, data.step)}</text>`);
    });
  }
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
  svg.innerHTML = svgParts.join("");
}
function row(stats) {
  const errClass = stats.errors > 0 ? "err" : "";
  return `
    <td>${stats.count}</td>
    <td class="ok">${stats.success ?? 0}</td>
    <td class="${errClass}">${stats.errors ?? 0}</td>
    <td>${fmtDur(stats.avg_seconds)}</td>
    <td>${fmtDur(stats.p50_seconds)}</td>
    <td>${fmtDur(stats.p95_seconds)}</td>
  `;
}
function tokenRow(stats) {
  return `
    <td>${fmtNum(stats.input_tokens)}</td>
    <td>${fmtNum(stats.output_tokens)}</td>
    <td>${fmtNum(stats.cache_creation_input_tokens)}</td>
    <td>${fmtNum(stats.cache_read_input_tokens)}</td>
    <td>${fmtCost(stats.cost)}</td>
  `;
}
function renderWindowTable(data) {
  let html = "<tr><th>Window</th><th>Count</th><th>OK</th><th>Err</th><th>Avg</th><th>p50</th><th>p95</th></tr>";
  for (const w of WINDOWS) {
    const s = data[w] || {count:0, success:0, errors:0, avg_seconds:null, p50_seconds:null, p95_seconds:null};
    html += `<tr><td>${w}</td>${row(s)}</tr>`;
  }
  return html;
}
function renderTokenTable(data) {
  let html = "<tr><th>Window</th><th>Input</th><th>Output</th><th>Cache Write</th><th>Cache Read</th><th>Cost</th></tr>";
  for (const w of WINDOWS) {
    const s = data[w] || {input_tokens:0, output_tokens:0, cache_creation_input_tokens:0, cache_read_input_tokens:0, cost:null};
    html += `<tr><td>${w}</td>${tokenRow(s)}</tr>`;
  }
  return html;
}
function pct(used, max) {
  if (!max) return 0;
  return Math.min(100, (used / max) * 100);
}
async function tick() {
  try {
    const r = await fetch("/_proxy/metrics");
    if (!r.ok) throw new Error("HTTP " + r.status);
    const m = await r.json();
    const liveEl = document.getElementById("live");
    liveEl.textContent = "● live · " + new Date().toLocaleTimeString();
    liveEl.classList.remove("error");

    document.getElementById("upstream").textContent = "→ " + (m.upstream || "");

    const L = m.limiter;
    const boostBtn = document.getElementById("boost-btn");
    if (L.forced_tier) {
      boostBtn.disabled = true;
      boostBtn.textContent = "forced: " + L.forced_tier;
    } else if (L.active_tier === "high") {
      boostBtn.disabled = true;
      boostBtn.textContent = "⚡ on HIGH";
    } else {
      boostBtn.disabled = false;
      boostBtn.textContent = "⚡ Boost HIGH";
    }
    const tierClass = L.active_tier === "high" ? "tier-high" : "tier-low";
    const concPct = pct(L.in_flight, L.max_concurrent);
    const concClass = concPct >= 95 ? "crit" : concPct >= 75 ? "warn" : "";
    const rlWait = L.rate_limited_waiting || 0;
    const rlAgo = L.last_rate_limited_at ? ((Date.now()/1000) - L.last_rate_limited_at) : null;
    const rlSub = rlWait > 0
      ? "backing off upstream limit"
      : (rlAgo !== null ? "last 429 " + fmtSpan(rlAgo) + " ago" : "none");

    const W = L.window || {active:false};
    // Multi-budget quota windows: one card per budget. The old single-window
    // shape (no `windows` list) is synthesized into a one-entry list so a
    // stale poll during a live upgrade still renders.
    const wins = W.windows || (W.limit != null ? [W] : []);
    const bindingIdx = W.binding ?? 0;
    const lanes = L.lanes || {human:{in_flight:0,queued:0}, auto:{in_flight:0,queued:0,concurrency_reserve:0}, human_rate_per_min:0};
    const autoQ = lanes.auto.queued || 0;
    const laneCard = `
      <div class="stat">
        <div class="label">Lanes · human / auto</div>
        <div class="value">${lanes.human.in_flight} / ${lanes.auto.in_flight}</div>
        <div class="sub">${autoQ > 0 ? autoQ + " awaiting slot · " : ""}human ~${lanes.human_rate_per_min}/min${lanes.auto.concurrency_reserve ? " · reserve " + lanes.auto.concurrency_reserve : ""}</div>
      </div>`;

    // Automation pacing: requests held in the pacer (no slot yet) + when the
    // next one is due. "reserved" = parked because the rest of the window is
    // statistically kept for humans (releases when the window advances/resets).
    const P = m.pacer || {enabled:true, parked:0, reason:"open", next_seconds:null, rate_per_min:null};
    const paceVal = P.parked > 0 ? P.parked + " held" : (P.enabled ? "clear" : "off");
    const paceClass = P.parked > 0 ? (P.reason === "reserved" ? "warn" : "") : "";
    let paceSub;
    if (!P.enabled) paceSub = "pacing disabled";
    else if (P.reason === "reserved") paceSub = "reserved for humans · waits for window";
    else if (P.next_seconds != null && P.next_seconds >= 0.05) paceSub = "next in " + fmtSpan(P.next_seconds);
    else paceSub = P.parked > 0 ? "releasing now" : "idle";
    if (P.rate_per_min != null) paceSub += " · " + P.rate_per_min + "/min budget";
    // Which budget currently sets the pace (only interesting with >1 budget).
    if (P.binding_metric && wins.length > 1 && (P.reason === "paced" || P.reason === "reserved")) {
      paceSub += ` · ${P.binding_metric}/${fmtWinLen(P.binding_window_seconds)} binds`;
    }
    const paceCard = `
      <div class="stat">
        <div class="label">Auto Pacing</div>
        <div class="value ${paceClass}">${paceVal}</div>
        <div class="sub">${paceSub}</div>
      </div>`;

    // One quota card per budget window: "used / limit" in the budget's own
    // units, a time bar, and per-lane spend + end-of-window projections
    // (projected_human from the limiter, projected_auto from the pacer's
    // per-window status, aligned by index). The most-utilized budget is the
    // "binding" one and gets highlighted.
    const pWins = P.windows || [];
    const winCards = wins.map((w, i) => {
      const metric = w.metric || "requests";
      const noun = METRIC_NOUN[metric] ?? metric;
      const label = "Quota · " + fmtUnits(metric, w.limit) + (noun ? " " + noun : "")
        + " / " + fmtWinLen(w.window_seconds);
      const isBinding = wins.length > 1 && i === bindingIdx && w.active;
      if (!w.active) {
        return `
        <div class="stat">
          <div class="label">${label}</div>
          <div class="value">idle</div>
          <div class="sub">starts on next request</div>
          <div class="bar"><i style="width:0%"></i></div>
        </div>`;
      }
      const usePct = pct(w.count, w.limit);
      const useClass = usePct >= 95 ? "crit" : usePct >= 80 ? "warn" : "";
      // A pending LOW→HIGH switch ends the window early: count down to (and fill
      // the time bar toward) the switch, matching the shortened pacing horizon.
      const effRem = w.effective_remaining_seconds ?? w.remaining_seconds;
      const shortened = w.switch_at != null && effRem < (w.remaining_seconds - 1);
      const timePct = shortened
        ? pct(w.elapsed_seconds, w.elapsed_seconds + effRem)
        : pct(w.elapsed_seconds, w.window_seconds);
      const leftSub = shortened
        ? `${fmtSpan(effRem)} to ⏰HIGH (${fmtSpan(w.remaining_seconds)} window)`
        : `${fmtSpan(w.remaining_seconds)} left`;
      // Split + end-of-window estimate: human "so far → projected", background
      // (auto) "so far → projected". Projections are estimates; rounded units
      // read better than decimals.
      const ch = w.count_human ?? 0, ca = w.count_auto ?? 0;
      const ph = w.projected_human ?? ch;
      const pa = (pWins[i] && pWins[i].projected_auto != null)
        ? pWins[i].projected_auto : (P.projected_auto ?? ca);
      return `
        <div class="stat${isBinding ? " binding" : ""}">
          <div class="label">${label}${isBinding ? " · binding" : ""}</div>
          <div class="value ${useClass}">${fmtUnits(metric, w.count)} / ${fmtUnits(metric, w.limit)}</div>
          <div class="sub">${fmtSpan(w.elapsed_seconds)} in · ${leftSub}</div>
          <div class="sub">human ${fmtUnits(metric, ch)}→~${fmtUnits(metric, ph, true)} · auto ${fmtUnits(metric, ca)}→~${fmtUnits(metric, pa, true)}</div>
          <div class="bar"><i class="${useClass}" style="width:${timePct.toFixed(1)}%"></i></div>
        </div>`;
    }).join("");

    // Pending scheduled switches get one sub-line each (they're too long to
    // share the tier mode line without forcing the card extremely wide).
    const sched = L.schedule || {};
    const schedSubs = [];
    if (sched.pending) {
      schedSubs.push("⏰HIGH " + (sched.recurring
        ? "daily @" + sched.daily_at + " (in " + fmtSpan(sched.seconds_until) + ")"
        : "in " + fmtSpan(sched.seconds_until)));
    }
    if (sched.low_pending) {
      schedSubs.push("⏰LOW " + (sched.low_recurring
        ? "daily @" + sched.low_daily_at + " (in " + fmtSpan(sched.low_seconds_until) + ")"
        : "in " + fmtSpan(sched.low_seconds_until)));
    }
    document.getElementById("state-grid").innerHTML = `
      <div class="stat">
        <div class="label">Active Tier</div>
        <div class="value ${tierClass}">${L.active_tier.toUpperCase()}</div>
        <div class="sub">${L.forced_tier ? "forced" : "auto"}${L.probe_in_flight ? " · probing" : ""}</div>
        ${schedSubs.map((s) => `<div class="sub">${s}</div>`).join("")}
      </div>
      <div class="stat">
        <div class="label">In Flight</div>
        <div class="value ${concClass}">${L.in_flight} / ${L.max_concurrent}</div>
        <div class="sub">${concPct.toFixed(0)}%</div>
      </div>
      <div class="stat">
        <div class="label">Queued</div>
        <div class="value">${L.queued}</div>
        <div class="sub">${L.queued > 0 ? "waiting for slot" : "idle"}</div>
      </div>
      <div class="stat">
        <div class="label">Waiting on 429</div>
        <div class="value ${rlWait > 0 ? "crit" : ""}">${rlWait}</div>
        <div class="sub">${rlSub}</div>
      </div>
      ${laneCard}
      ${paceCard}
      ${winCards}
      <div class="stat">
        <div class="label">Lifetime</div>
        <div class="value">${L.totals.requests}</div>
        <div class="sub">
          ${L.totals.rate_limited} 429 · ${L.totals.promotions}↑ ${L.totals.demotions}↓ · ${L.totals.probes_sent} probes
        </div>
      </div>
    `;

    let tpHtml = "";
    for (const w of WINDOWS) {
      const s = m.overall[w] || {count:0, errors:0, avg_seconds:null, cost:null, input_tokens:0, output_tokens:0};
      const errBadge = s.errors > 0 ? ` <span class="err">(${s.errors} err)</span>` : "";
      const costBadge = s.cost !== null && s.cost !== undefined ? ` · ${fmtCost(s.cost)}` : "";
      const tokSub = `${fmtNum(s.input_tokens + (s.cache_creation_input_tokens||0) + (s.cache_read_input_tokens||0))} in / ${fmtNum(s.output_tokens)} out`;
      tpHtml += `
        <div class="stat">
          <div class="label">Last ${w}</div>
          <div class="value">${s.count}</div>
          <div class="sub">${fmtDur(s.avg_seconds)} avg${errBadge}${costBadge}</div>
          <div class="sub">${tokSub}</div>
        </div>
      `;
    }
    document.getElementById("throughput-grid").innerHTML = tpHtml;

    if (m.persistent) {
      const PS = m.persistent;  // don't shadow P (the pacer) above
      const totalCards = [
        ["24h", "24h"], ["Weekly", "7d"], ["Monthly", "30d"], ["Lifetime", "lifetime"],
      ];
      let totHtml = "";
      for (const [label, key] of totalCards) {
        const o = (PS[key] && PS[key].overall) || {count:0, errors:0, input_tokens:0, output_tokens:0, cache_creation_input_tokens:0, cache_read_input_tokens:0, cost:null};
        const inTot = o.input_tokens + (o.cache_creation_input_tokens||0) + (o.cache_read_input_tokens||0);
        const errBadge = o.errors > 0 ? ` <span class="err">(${o.errors} err)</span>` : "";
        const costBadge = o.cost !== null && o.cost !== undefined ? ` · ${fmtCost(o.cost)}` : "";
        totHtml += `
          <div class="stat">
            <div class="label">${label}</div>
            <div class="value">${fmtNum(o.count)}</div>
            <div class="sub">${fmtNum(inTot)} in / ${fmtNum(o.output_tokens)} out</div>
            <div class="sub">${fmtDur(o.avg_seconds)} avg${errBadge}${costBadge}</div>
          </div>
        `;
      }
      document.getElementById("totals-grid").innerHTML = totHtml;
    }

    document.getElementById("overall-table").innerHTML = renderWindowTable(m.overall);
    document.getElementById("overall-tokens-table").innerHTML = renderTokenTable(m.overall);

    const models = Object.keys(m.per_model).sort();
    if (models.length === 0) {
      document.getElementById("per-model").innerHTML = `<div class="empty">No model traffic yet.</div>`;
    } else {
      let html = "";
      for (const model of models) {
        const d = m.per_model[model];
        const active = d.active || 0;
        const priced = d.has_pricing ? "" : ' <span class="model-active">(no pricing)</span>';
        html += `
          <div class="model-card">
            <div class="model-head">
              <div class="model-name">${model}${priced}</div>
              <div class="model-active">active: <strong>${active}</strong></div>
            </div>
            <table>${renderWindowTable(d)}</table>
            <div style="height:8px"></div>
            <table>${renderTokenTable(d)}</table>
          </div>
        `;
      }
      document.getElementById("per-model").innerHTML = html;
    }
  } catch (e) {
    const el = document.getElementById("live");
    el.textContent = "✗ disconnected: " + e.message;
    el.classList.add("error");
  }
}
let seriesWindow = "24h";
async function drawSeries() {
  try {
    const r = await fetch("/_proxy/series?window=" + seriesWindow);
    if (!r.ok) throw new Error("HTTP " + r.status);
    const data = await r.json();
    drawChart("chart-requests", data, [
      { fn: (p) => Math.max(0, p.requests - p.errors), color: COLORS.ok },
      { key: "errors", color: COLORS.err },
    ], fmtNum);
    setLegend("chart-requests-legend", [
      { c: COLORS.ok, l: "ok" }, { c: COLORS.err, l: "errors" },
    ]);
    drawChart("chart-tokens", data, [
      { key: "input_tokens", color: COLORS.input },
      { fn: (p) => (p.cache_creation_input_tokens || 0) + (p.cache_read_input_tokens || 0), color: COLORS.cache },
      { key: "output_tokens", color: COLORS.output },
    ], fmtNum);
    setLegend("chart-tokens-legend", [
      { c: COLORS.input, l: "input" }, { c: COLORS.cache, l: "cache" }, { c: COLORS.output, l: "output" },
    ]);
  } catch (e) { /* leave previous chart in place */ }
}
document.querySelectorAll("#series-controls button").forEach((b) => {
  b.addEventListener("click", () => {
    seriesWindow = b.dataset.w;
    document.querySelectorAll("#series-controls button").forEach((x) => x.classList.remove("active"));
    b.classList.add("active");
    drawSeries();
  });
});
// ---- Theme toggle: cycles Auto -> Light -> Dark, persisted in localStorage.
// "auto" clears data-theme so the prefers-color-scheme media query governs.
const THEMES = ["auto", "light", "dark"];
const THEME_LABEL = { auto: "🖥 Auto", light: "☀ Light", dark: "🌙 Dark" };
function currentTheme() {
  const t = localStorage.getItem("theme");
  return THEMES.includes(t) ? t : "auto";
}
function applyTheme(t) {
  if (t === "auto") document.documentElement.removeAttribute("data-theme");
  else document.documentElement.setAttribute("data-theme", t);
  const btn = document.getElementById("theme-btn");
  if (btn) btn.textContent = THEME_LABEL[t];
}
document.getElementById("theme-btn").addEventListener("click", () => {
  const next = THEMES[(THEMES.indexOf(currentTheme()) + 1) % THEMES.length];
  localStorage.setItem("theme", next);
  applyTheme(next);
  drawSeries();   // repaint SVG gridlines with the new theme's --grid color
});
applyTheme(currentTheme());
// Repaint charts when the OS theme flips while in Auto mode.
if (window.matchMedia) {
  window.matchMedia("(prefers-color-scheme: light)").addEventListener("change", () => {
    if (currentTheme() === "auto") drawSeries();
  });
}
window.addEventListener("resize", drawSeries);
document.getElementById("boost-btn").addEventListener("click", async () => {
  const btn = document.getElementById("boost-btn");
  btn.disabled = true;
  try {
    const r = await fetch("/_proxy/boost", { method: "POST" });
    if (!r.ok) {
      const e = await r.json().catch(() => ({}));
      alert(e.error || ("boost failed: HTTP " + r.status));
    }
  } catch (e) {
    alert("boost failed: " + e.message);
  }
  tick();
});
tick();
setInterval(tick, 2000);
drawSeries();
setInterval(drawSeries, 15000);
