"""
Internal Insight Analytics Dashboard generator (admin-only, not deployed).

Renders a self-contained HTML file from the engine tables plus an optional
audit directory (produced by dry-run analysis scripts). Run inside the bot
container or anywhere with DATABASE_URL:

    python -m engine.dashboard [--audit DIR] [-o insight_dashboard.html]

The output is a static file — share it privately; it is never served by the
public frontend.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from datetime import datetime
from statistics import mean

import psycopg2
import psycopg2.extras


def collect_db(conn) -> dict:
    """Live stats from insights / content_items / insight_cooldowns."""
    out: dict = {"db_available": True}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT COUNT(*) AS n, AVG(score) AS avg FROM insights")
        row = cur.fetchone()
        out["insights_total"] = row["n"]
        out["avg_score"] = round(float(row["avg"]), 3) if row["avg"] else None

        cur.execute("SELECT status, COUNT(*) AS n FROM content_items GROUP BY status")
        out["content_by_status"] = {r["status"]: r["n"] for r in cur.fetchall()}

        cur.execute("""
            SELECT rule, COUNT(*) AS n, AVG(score) AS avg
            FROM insights GROUP BY rule ORDER BY n DESC
        """)
        out["insights_per_rule"] = [
            {"rule": r["rule"], "n": r["n"],
             "avg": round(float(r["avg"]), 3) if r["avg"] else None}
            for r in cur.fetchall()
        ]

        cur.execute("SELECT COUNT(*) AS n FROM insight_cooldowns")
        out["cooldowns_active"] = cur.fetchone()["n"]

        cur.execute("""
            SELECT ci.id, ci.platform, ci.status, ci.scheduled_date::text,
                   ci.slot_hour, ci.text, ci.posted_id, ci.error,
                   i.rule, i.score, i.score_components, i.payload, i.fingerprint
            FROM content_items ci LEFT JOIN insights i ON i.id = ci.insight_id
            ORDER BY ci.created_at DESC LIMIT 500
        """)
        out["recent_content"] = [dict(r) for r in cur.fetchall()]
    return out


def collect_audit(audit_dir: str) -> dict:
    """Optional dry-run audit artifacts (discovery_stats/analysis/generation)."""
    out = {}
    for name in ("discovery_stats", "analysis", "generation", "ranked_all"):
        path = os.path.join(audit_dir, f"{name}.json")
        if os.path.exists(path):
            out[name] = json.load(open(path))
    return out


def render(data: dict) -> str:
    payload = json.dumps(data, default=str)
    generated = datetime.now().strftime("%Y-%m-%d %H:%M")
    # Template uses [[DATA]] / [[TS]] placeholders — no str.format, the CSS/JS
    # braces would collide.
    return (
        _TEMPLATE
        .replace("[[DATA]]", payload)
        .replace("[[TS]]", generated)
    )


_TEMPLATE = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>CineTrace · Insight Engine Admin</title>
<style>
  :root{
    --surface:#1a1a19; --page:#0d0d0d; --ink:#fff; --ink2:#c3c2b7; --muted:#898781;
    --grid:#2c2c2a; --border:rgba(255,255,255,.10);
    --s1:#3987e5; --s2:#199e70; --s3:#c98500; --s4:#9085e9; --s5:#e66767;
    --good:#0ca30c; --warn:#fab219; --serious:#ec835a; --crit:#d03b3b;
  }
  *{box-sizing:border-box;margin:0}
  body{background:var(--page);color:var(--ink);font:14px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif;padding:24px}
  h1{font-size:20px;font-weight:650} h2{font-size:15px;font-weight:600;margin:0 0 12px;color:var(--ink)}
  .sub{color:var(--muted);font-size:12px;margin-bottom:20px}
  .grid{display:grid;gap:16px;grid-template-columns:repeat(auto-fit,minmax(320px,1fr))}
  .card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 18px;overflow:hidden}
  .card.wide{grid-column:1/-1}
  .tiles{display:grid;gap:12px;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));margin-bottom:16px}
  .tile{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px 16px}
  .tile .v{font-size:26px;font-weight:650;line-height:1.15}
  .tile .l{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.06em;margin-top:2px}
  .bar-row{display:flex;align-items:center;gap:10px;margin:4px 0;min-height:20px}
  .bar-row .name{width:190px;flex:none;color:var(--ink2);font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .bar-row .track{flex:1;display:flex;align-items:center;gap:8px}
  .bar-row .bar{height:14px;border-radius:0 4px 4px 0;min-width:2px}
  .bar-row .val{color:var(--ink2);font-size:12px;font-variant-numeric:tabular-nums}
  .hist{display:flex;align-items:flex-end;gap:2px;height:120px;padding-top:8px}
  .hist .b{flex:1;background:var(--s1);border-radius:4px 4px 0 0;position:relative;min-height:2px}
  .hist .b:hover::after{content:attr(data-t);position:absolute;bottom:100%;left:50%;transform:translateX(-50%);
    background:#000;border:1px solid var(--border);padding:3px 8px;border-radius:6px;font-size:11px;white-space:nowrap;z-index:5}
  .axis{display:flex;justify-content:space-between;color:var(--muted);font-size:10px;border-top:1px solid var(--grid);padding-top:4px;margin-top:2px}
  table{width:100%;border-collapse:collapse;font-size:12.5px}
  th{color:var(--muted);text-align:left;font-weight:500;padding:6px 8px;border-bottom:1px solid var(--grid);font-size:11px;text-transform:uppercase;letter-spacing:.05em}
  td{padding:6px 8px;border-bottom:1px solid var(--grid);vertical-align:top}
  td.num{font-variant-numeric:tabular-nums;text-align:right}
  .status{display:inline-flex;align-items:center;gap:5px;font-size:11.5px}
  .dot{width:8px;height:8px;border-radius:50%}
  .pill{background:rgba(255,255,255,.06);border:1px solid var(--border);border-radius:999px;padding:1px 9px;font-size:11px;color:var(--ink2)}
  input[type=search],select{background:var(--page);color:var(--ink);border:1px solid var(--border);border-radius:8px;padding:7px 10px;font-size:13px}
  input[type=search]{width:260px}
  .filters{display:flex;gap:10px;margin-bottom:12px;flex-wrap:wrap}
  details{border-top:1px solid var(--grid)} details summary{cursor:pointer;list-style:none;padding:8px}
  details summary:hover{background:rgba(255,255,255,.03)}
  pre{background:var(--page);border:1px solid var(--border);border-radius:8px;padding:10px;font-size:11px;overflow-x:auto;color:var(--ink2);max-height:280px}
  .tweet{background:var(--page);border:1px solid var(--border);border-radius:8px;padding:10px;white-space:pre-wrap;font-size:12.5px;margin:8px 0}
  .empty{color:var(--muted);font-style:italic;padding:12px 0}
  .legend{display:flex;gap:14px;flex-wrap:wrap;margin-top:10px;font-size:11.5px;color:var(--ink2)}
  .legend span{display:inline-flex;align-items:center;gap:5px}
  .swatch{width:10px;height:10px;border-radius:3px}
</style></head><body>
<h1>🧠 CineTrace Insight Engine — Admin Dashboard</h1>
<p class="sub">Internal only · generated [[TS]] · data: live engine tables + latest dry-run audit</p>
<div id="app"></div>
<script>
const DATA = [[DATA]];
const CAT = ['#3987e5','#199e70','#c98500','#9085e9','#e66767'];
const $ = (h) => { const d = document.createElement('div'); d.innerHTML = h; return d.firstElementChild; };
const app = document.getElementById('app');
const esc = (s) => String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const num = (x) => x == null ? '—' : Number(x).toLocaleString();

function tiles(items){
  return `<div class="tiles">` + items.map(([v,l,color]) =>
    `<div class="tile"><div class="v"${color?` style="color:${color}"`:''}>${v}</div><div class="l">${l}</div></div>`).join('') + `</div>`;
}
function bars(rows, color, fmt){
  const max = Math.max(...rows.map(r => r.v), 1);
  return rows.map((r,i) => `<div class="bar-row"><div class="name" title="${esc(r.k)}">${esc(r.k)}</div>
    <div class="track"><div class="bar" style="width:${Math.max(1,100*r.v/max)}%;background:${color||CAT[0]}"></div>
    <div class="val">${fmt ? fmt(r) : num(r.v)}</div></div></div>`).join('');
}

// ── Overview ──────────────────────────────────────────────────────────────────
const st = DATA.content_by_status || {};
const gen = (DATA.generation||{}).summary || {};
const totals = ((DATA.discovery_stats||{}).totals) || {};
app.append($(`<div>` + tiles([
  [num(DATA.insights_total ?? totals.discovered), 'insights stored'],
  [num(totals.discovered), 'last dry-run candidates'],
  [num(st.new ?? 0), 'pending approval', st.new ? 'var(--warn)' : null],
  [num(st.posted ?? 0), 'published', 'var(--good)'],
  [num(st.failed ?? 0), 'failed posts', st.failed ? 'var(--crit)' : null],
  [DATA.avg_score ?? (DATA.analysis?.score_dist?.mean ?? '—'), 'avg quality score'],
  [num(DATA.cooldowns_active ?? 0), 'active cooldowns'],
  [(gen.retry_rate_pct != null ? gen.retry_rate_pct + '%' : '—'), 'generator retry rate'],
]) + `</div>`));

// ── Discovery ─────────────────────────────────────────────────────────────────
const rs = (DATA.discovery_stats||{}).rule_stats || {};
const ruleRows = Object.entries(rs).map(([k,v]) => ({k, v: v.count, t: v.seconds, err: v.error}))
  .sort((a,b) => b.v - a.v);
const totalIns = ruleRows.reduce((s,r) => s+r.v, 0) || 1;
const health = ruleRows.map(r => {
  const cls = r.err ? ['var(--crit)','failed'] : r.v === 0 ? ['var(--warn)','empty'] :
              r.t > 2 ? ['var(--serious)','slow'] : ['var(--good)','healthy'];
  return `<tr><td><span class="status"><span class="dot" style="background:${cls[0]}"></span>${esc(r.k)}</span></td>
    <td class="num">${r.v}</td><td class="num">${(100*r.v/totalIns).toFixed(1)}%</td>
    <td class="num">${r.t}s</td><td>${cls[1]}${r.err ? ' — ' + esc(r.err) : ''}</td></tr>`;
}).join('');
app.append($(`<div class="grid" style="margin-top:4px">
  <div class="card"><h2>Discovery · insights per rule</h2>${
    ruleRows.length ? bars(ruleRows, CAT[0]) : '<div class="empty">no dry-run audit loaded</div>'}</div>
  <div class="card"><h2>Discovery · rule health</h2>
    <table><thead><tr><th>rule</th><th>n</th><th>share</th><th>time</th><th>state</th></tr></thead>
    <tbody>${health}</tbody></table></div>
</div>`));

// ── Ranking ───────────────────────────────────────────────────────────────────
const an = DATA.analysis || {};
if (an.score_dist){
  const h = an.score_dist.histogram || {};
  const keys = Object.keys(h).sort();
  const maxH = Math.max(...Object.values(h));
  const histHtml = keys.map((k,i) =>
    `<div class="b" style="height:${100*h[k]/maxH}%" data-t="${k}: ${h[k]} insights"></div>`).join('');
  const fc = an.feature_contribution || {};
  const fcRows = Object.entries(fc).map(([k,v]) => ({k, v: v.weighted_contribution}))
    .sort((a,b) => b.v - a.v);
  const perRule = Object.entries(an.avg_score_per_rule||{}).map(([k,v]) => ({k, v: v.mean, n: v.n}));
  app.append($(`<div class="grid" style="margin-top:16px">
    <div class="card"><h2>Ranking · score histogram (n=${an.score_dist.n}, mean ${an.score_dist.mean})</h2>
      <div class="hist">${histHtml}</div>
      <div class="axis"><span>${keys[0]}</span><span>${keys[keys.length-1]}</span></div></div>
    <div class="card"><h2>Ranking · weighted feature contribution</h2>${bars(fcRows, CAT[3], r => r.v.toFixed(3))}</div>
    <div class="card wide"><h2>Ranking · mean score per rule</h2>${bars(perRule, CAT[1], r => r.v.toFixed(3) + ' (n=' + r.n + ')')}</div>
  </div>`));
}

// ── Diversity ─────────────────────────────────────────────────────────────────
if (an.diversity_top100){
  const d = an.diversity_top100;
  const inds = Object.entries(d.industry_mix||{}).map(([k,v],i) => ({k, v}));
  const actors = Object.entries(d.actors_appearing_3plus||{}).map(([k,v]) => ({k, v})).sort((a,b) => b.v-a.v);
  const rules = Object.entries(d.rule_mix||{}).map(([k,v]) => ({k, v})).sort((a,b) => b.v-a.v);
  app.append($(`<div class="grid" style="margin-top:16px">
    <div class="card"><h2>Diversity · top-100 industry mix</h2>${bars(inds, CAT[2])}
      <div class="legend"><span>unique actors in top 100: <b>${d.unique_actors}</b></span></div></div>
    <div class="card"><h2>Diversity · actors appearing 3+ times</h2>${
      actors.length ? bars(actors, CAT[4]) : '<div class="empty">none — good spread</div>'}</div>
    <div class="card"><h2>Diversity · top-100 rule mix</h2>${bars(rules, CAT[1])}</div>
  </div>`));
}

// ── Dedup & Generator ─────────────────────────────────────────────────────────
const dd = an.dedup || {};
app.append($(`<div class="grid" style="margin-top:16px">
  <div class="card"><h2>Deduplication</h2>` + tiles([
    [num(dd.batch_dupe_groups), 'exact fingerprint collisions'],
    [num(dd.cross_rule_entity_overlaps), 'cross-rule entity overlaps'],
    [num(DATA.cooldowns_active ?? 0), 'cooldowns active'],
  ]) + `</div>
  <div class="card"><h2>Generator (last audit, n=${num(gen.generated_ok)})</h2>` + tiles([
    [gen.retry_rate_pct != null ? gen.retry_rate_pct+'%' : '—', 'retry rate'],
    [num(gen.hallucinated_number_attempts), 'hallucinations caught', gen.hallucinated_number_attempts ? 'var(--warn)' : null],
    [num(gen.discarded), 'discarded', gen.discarded ? 'var(--crit)' : null],
    [gen.len_mean ? Math.round(gen.len_mean)+' ch' : '—', 'avg tweet length'],
  ]) + `</div>
</div>`));

// ── Searchable insight browser ────────────────────────────────────────────────
const rows = (DATA.recent_content && DATA.recent_content.length)
  ? DATA.recent_content.map(c => ({
      kind:'content', id:c.id, rule:c.rule, score:c.score, status:c.status,
      who:(c.payload?.entities||[]).map(e=>e.name).join(', '),
      text:c.text, date:c.scheduled_date, slot:c.slot_hour, err:c.error,
      payload:c.payload, components:c.score_components, posted:c.posted_id}))
  : ((DATA.ranked_all||[]).slice(0,300).map((r,i) => ({
      kind:'audit', id:i+1, rule:r.rule, score:r.score, status:'dry-run',
      who:r.entities.map(e=>e.name).join(', '),
      text:null, payload:r, components:r.components})));
const ruleOpts = [...new Set(rows.map(r=>r.rule).filter(Boolean))].sort()
  .map(r => `<option>${esc(r)}</option>`).join('');
const statusColors = {new:'var(--warn)',approved:'var(--s1)',posted:'var(--good)',rejected:'var(--muted)',failed:'var(--crit)','dry-run':'var(--muted)'};
const browser = $(`<div class="card wide" style="margin-top:16px">
  <h2>Insight browser (${rows.length})</h2>
  <div class="filters">
    <input type="search" id="q" placeholder="search actor / director / text…">
    <select id="frule"><option value="">all rules</option>${ruleOpts}</select>
    <select id="fstatus"><option value="">all statuses</option>${
      [...new Set(rows.map(r=>r.status))].map(s=>`<option>${esc(s)}</option>`).join('')}</select>
    <select id="fsort"><option value="score">sort: score</option><option value="id">sort: newest</option></select>
  </div>
  <div id="list"></div></div>`);
app.append(browser);

function renderList(){
  const q = document.getElementById('q').value.toLowerCase();
  const fr = document.getElementById('frule').value;
  const fs = document.getElementById('fstatus').value;
  const sort = document.getElementById('fsort').value;
  let out = rows.filter(r =>
    (!fr || r.rule === fr) && (!fs || r.status === fs) &&
    (!q || (r.who + ' ' + (r.text||'') + ' ' + (r.rule||'')).toLowerCase().includes(q)));
  out.sort((a,b) => sort === 'score' ? (b.score||0)-(a.score||0) : b.id-a.id);
  document.getElementById('list').innerHTML = out.slice(0,150).map(r => `
    <details><summary>
      <span class="status"><span class="dot" style="background:${statusColors[r.status]||'var(--muted)'}"></span>
      <b>${esc(r.who)}</b></span>
      <span class="pill">${esc(r.rule||'?')}</span>
      <span class="pill">score ${r.score != null ? Number(r.score).toFixed(2) : '—'}</span>
      <span class="pill">${esc(r.status)}</span>
      ${r.date ? `<span class="pill">${esc(r.date)} ${r.slot}h</span>` : ''}
    </summary>
    <div style="padding:4px 8px 12px">
      ${r.text ? `<div class="tweet">${esc(r.text)}</div>` : ''}
      ${r.err ? `<div class="tweet" style="border-color:var(--crit)">⚠ ${esc(r.err)}</div>` : ''}
      ${r.posted ? `<div class="pill">posted: ${esc(r.posted)}</div>` : ''}
      ${r.components ? `<div class="legend">${Object.entries(r.components).map(([k,v]) =>
        `<span>${esc(k)}: <b>${Number(v).toFixed(2)}</b></span>`).join('')}</div>` : ''}
      <pre>${esc(JSON.stringify(r.payload, null, 1))}</pre>
    </div></details>`).join('') || '<div class="empty">no matches</div>';
}
['q','frule','fstatus','fsort'].forEach(id =>
  document.getElementById(id).addEventListener('input', renderList));
renderList();
</script></body></html>
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit", help="dry-run audit directory (JSON artifacts)")
    parser.add_argument("-o", "--out", default="insight_dashboard.html")
    args = parser.parse_args()

    data: dict = {}
    try:
        conn = psycopg2.connect(os.environ["DATABASE_URL"])
        data.update(collect_db(conn))
        conn.close()
    except Exception as e:
        data["db_available"] = False
        data["db_error"] = str(e)

    if args.audit:
        data.update(collect_audit(args.audit))
        # keep the embedded payload manageable
        if "ranked_all" in data:
            data["ranked_all"] = data["ranked_all"][:300]

    with open(args.out, "w") as f:
        f.write(render(data))
    print(f"dashboard written → {args.out}")


if __name__ == "__main__":
    main()
