import json
import os
from flask import Flask, request, render_template_string
from databricks.sdk import WorkspaceClient

app = Flask(__name__)
w = WorkspaceClient()

SOURCE = "workspace.chiro_hackathon"
OUTPUT = "workspace.chiro_agent_demo"
WH_ID = "57129d05302f658f"  # hardcoded SQL warehouse ID

# --- SQL helpers ---

def run_sql(sql_text):
    resp = w.statement_execution.execute_statement(
        statement=sql_text, warehouse_id=WH_ID, wait_timeout="50s"
    )
    if not resp.result or not resp.result.data_array:
        return []
    cols = [c.name for c in resp.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in resp.result.data_array]

def status_list(statuses):
    return ",".join(f"'{s.strip().lower()}'" for s in statuses.split(","))

# --- Summary computation ---

def compute_summary(location_id, snapshot_date, lookback, open_statuses, terminal_statuses):
    olist = status_list(open_statuses)
    tlist = status_list(terminal_statuses)

    loc_rows = run_sql(f"SELECT location_name FROM {SOURCE}.locations WHERE location_id = '{location_id}'")
    location_name = loc_rows[0]["location_name"] if loc_rows else location_id

    stats = run_sql(f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN lower(trim(status)) IN ({olist})
                AND converted_flag = false
                AND num_touchpoints BETWEEN 0 AND 2
                AND datediff('{snapshot_date}', created_date) BETWEEN 0 AND {lookback}
                THEN 1 ELSE 0 END) as eligible,
            SUM(CASE WHEN lower(trim(status)) IN ({olist})
                AND converted_flag = false
                AND num_touchpoints >= 3
                AND datediff('{snapshot_date}', created_date) BETWEEN 0 AND {lookback}
                THEN 1 ELSE 0 END) as contact_limit
        FROM {SOURCE}.leads WHERE assigned_location_id = '{location_id}'
    """)
    s = stats[0] if stats else {}
    eligible_count = int(s.get("eligible", 0) or 0)
    contact_limit_count = int(s.get("contact_limit", 0) or 0)
    total_at_loc = int(s.get("total", 0) or 0)
    excluded_count = total_at_loc - eligible_count - contact_limit_count

    tasks = run_sql(f"""
        SELECT task_id, lead_id, decision, evidence_json, rationale, draft_text
        FROM {OUTPUT}.followup_tasks
        WHERE location_id = '{location_id}' AND review_status = 'pending_review'
        ORDER BY lead_id
    """)
    task_cards = []
    for i, t in enumerate(tasks):
        ev = json.loads(t.get("evidence_json", "{}")) if t.get("evidence_json") else {}
        task_cards.append({
            "num": i + 1,
            "lead_id": t.get("lead_id", ""),
            "source": ev.get("source", "Unknown"),
            "status": (ev.get("status") or "unknown").title(),
            "touches": ev.get("num_touchpoints", "?"),
            "resp": ev.get("first_response_hours", "?"),
            "created": ev.get("created_date", "?"),
            "decision": (t.get("decision") or "").upper().replace("_", " "),
            "rationale": t.get("rationale", ""),
        })

    cycles = run_sql(f"""
        SELECT lead_id, cycle_number, channel, timing_days, cycle_status, draft_text
        FROM {OUTPUT}.nurturing_cycles
        WHERE location_id = '{location_id}'
        ORDER BY lead_id, cycle_number
    """)
    nurture_groups = []
    cur_lead = None
    cur_group = None
    for nc in cycles:
        if nc["lead_id"] != cur_lead:
            if cur_group:
                nurture_groups.append(cur_group)
            cur_lead = nc["lead_id"]
            cur_group = {"lead_id": cur_lead, "cycles": []}
        chan = {"phone_call": "Phone Call", "email_sms": "Email/SMS"}.get(nc["channel"], nc["channel"])
        cur_group["cycles"].append({
            "day": nc["timing_days"],
            "channel": chan,
            "status": nc["cycle_status"],
            "draft": nc.get("draft_text", ""),
        })
    if cur_group:
        nurture_groups.append(cur_group)

    g = run_sql(f"""
        SELECT
            COUNT(*) as total_leads,
            SUM(CASE WHEN converted_flag = true THEN 1 ELSE 0 END) as converted,
            SUM(CASE WHEN lower(trim(status)) IN ({olist}) AND converted_flag = false THEN 1 ELSE 0 END) as open_leads,
            SUM(CASE WHEN lower(trim(status)) IN ({tlist}) THEN 1 ELSE 0 END) as lost
        FROM {SOURCE}.leads
    """)
    gr = g[0] if g else {}
    total_leads = int(gr.get("total_leads", 0) or 0)
    total_converted = int(gr.get("converted", 0) or 0)
    total_open = int(gr.get("open_leads", 0) or 0)
    total_lost = int(gr.get("lost", 0) or 0)

    current_arr = 100_000_000
    rev_per_patient = current_arr / total_converted if total_converted else 0
    conv_rate = total_converted / total_leads if total_leads else 0
    open_potential_m = total_open * rev_per_patient / 1_000_000
    pred_lift = 0.05
    stretch_lift = 0.15
    pred_pat = int(total_open * pred_lift)
    stretch_pat = int(total_open * stretch_lift)
    pred_rev = pred_pat * rev_per_patient / 1_000_000
    stretch_rev = stretch_pat * rev_per_patient / 1_000_000
    pred_arr = (current_arr + pred_pat * rev_per_patient) / 1_000_000
    stretch_arr = (current_arr + stretch_pat * rev_per_patient) / 1_000_000
    reengage_rev = int(total_lost * 0.05) * rev_per_patient / 1_000_000
    response_rev = int(total_open * 0.02) * rev_per_patient / 1_000_000
    nurture_rev = int(total_open * 0.03) * rev_per_patient / 1_000_000
    ltv_rev = current_arr * 0.10 / 1_000_000
    combined = reengage_rev + response_rev + nurture_rev + ltv_rev
    pred_total = 100 + pred_rev + combined
    stretch_total = 100 + stretch_rev + combined
    pred_annual = 12.5
    stretch_annual = 20.0
    years = []
    for yr in [1, 2, 3, 5, 7, 10]:
        if yr == 1:
            p, s2 = pred_total, stretch_total
        else:
            p = pred_total + pred_annual * (yr - 1)
            s2 = stretch_total + stretch_annual * (yr - 1)
        years.append({"year": yr, "predicted": round(p), "stretch": round(s2)})

    return {
        "location_name": location_name, "eligible_count": eligible_count,
        "contact_limit_count": contact_limit_count, "excluded_count": excluded_count,
        "task_cards": task_cards, "nurture_groups": nurture_groups,
        "worked": len(task_cards), "total_leads": total_leads,
        "total_converted": total_converted, "total_open": total_open,
        "total_lost": total_lost, "rev_per_patient": rev_per_patient,
        "conv_rate": conv_rate, "open_potential_m": open_potential_m,
        "pred_pat": pred_pat, "pred_rev": pred_rev, "pred_arr": pred_arr,
        "stretch_pat": stretch_pat, "stretch_rev": stretch_rev, "stretch_arr": stretch_arr,
        "reengage_rev": reengage_rev, "response_rev": response_rev,
        "nurture_rev": nurture_rev, "ltv_rev": ltv_rev,
        "combined": combined, "pred_total": pred_total, "stretch_total": stretch_total,
        "years": years,
    }

# --- Routes ---

@app.route("/", methods=["GET", "POST"])
def index():
    try:
        locs = run_sql(f"SELECT location_id, location_name FROM {SOURCE}.locations ORDER BY location_id")
    except Exception as e:
        locs = []
        if request.method == "GET":
            return render_template_string(FORM_HTML, locations=[], error=f"Cannot connect to data: {e}")
    if request.method == "GET":
        return render_template_string(FORM_HTML, locations=locs)
    location_id = request.form.get("location_id", "")
    snapshot_date = request.form.get("snapshot_date", "2026-10-04")
    lookback = request.form.get("lookback", "120")
    open_statuses = request.form.get("open_statuses", "New,Contacted,Qualified")
    terminal_statuses = request.form.get("terminal_statuses", "Lost,Converted")
    try:
        summary = compute_summary(location_id, snapshot_date, int(lookback), open_statuses, terminal_statuses)
        return render_template_string(SUMMARY_HTML, **summary, location_id=location_id,
                                      snapshot_date=snapshot_date, lookback=lookback,
                                      open_statuses=open_statuses, terminal_statuses=terminal_statuses,
                                      locations=locs)
    except Exception as e:
        import traceback
        return render_template_string(FORM_HTML, locations=locs, error=f"{e}<br><pre>{traceback.format_exc()}</pre>")

@app.route("/debug")
def debug():
    import html as _html
    info = []
    info.append("<h2>Environment</h2>")
    for k in sorted(os.environ):
        if "TOKEN" in k.upper() or "SECRET" in k.upper() or "PASSWORD" in k.upper():
            info.append(f"<code>{k}</code> = <i>(hidden)</i><br>")
        else:
            info.append(f"<code>{k}</code> = {_html.escape(str(os.environ[k]))}<br>")
    info.append("<h2>SQL Test</h2>")
    try:
        info.append(f"<p>WorkspaceClient host: {w.config.host}</p>")
        rows = run_sql(f"SELECT location_id, location_name FROM {SOURCE}.locations ORDER BY location_id LIMIT 3")
        info.append(f"<p>Query returned {len(rows)} rows:</p>")
        for r in rows:
            info.append(f"<p>{_html.escape(str(r))}</p>")
    except Exception as e:
        import traceback
        info.append(f"<p style='color:red'>ERROR: {_html.escape(str(e))}</p>")
        info.append(f"<pre>{_html.escape(traceback.format_exc())}</pre>")
    return "\n".join(info)

# --- Templates ---

FORM_HTML = r"""
<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lead Recovery App</title>
<style>
  :root { --blue:#3b82f6; --blue-dark:#1e40af; --green:#10b981; --red:#ef4444;
          --bg:#f0f4f8; --card:#fff; --text:#1e293b; --muted:#64748b; --border:#e2e8f0; }
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Oxygen,sans-serif;
         background:var(--bg); color:var(--text); min-height:100vh; }
  .header { background:linear-gradient(135deg,#1e3a5f,#2563eb); color:#fff; padding:2rem 1rem; text-align:center; }
  .header h1 { font-size:1.75rem; font-weight:700; letter-spacing:-.02em; }
  .header p { margin-top:.4rem; opacity:.85; font-size:.95rem; }
  .wrap { max-width:640px; margin:2rem auto; padding:0 1rem; }
  .card { background:var(--card); border-radius:12px; padding:2rem; box-shadow:0 4px 16px rgba(0,0,0,.07); border:1px solid var(--border); }
  .card h2 { font-size:1.2rem; margin-bottom:1rem; color:var(--blue-dark); }
  label { display:block; margin-top:1rem; font-weight:600; font-size:.9rem; color:var(--text); }
  input, select { width:100%; padding:.65rem .8rem; border:1.5px solid var(--border); border-radius:8px;
                 font-size:.95rem; font-family:inherit; background:#fff; color:var(--text); transition:border .15s; }
  input:focus, select:focus { outline:none; border-color:var(--blue); box-shadow:0 0 0 3px rgba(59,130,246,.12); }
  .row { display:grid; grid-template-columns:1fr 1fr; gap:1rem; }
  button { margin-top:1.5rem; width:100%; padding:.85rem; background:var(--blue); color:#fff; border:none;
           border-radius:8px; font-size:1rem; font-weight:600; cursor:pointer; transition:background .15s; }
  button:hover { background:var(--blue-dark); }
  button:active { transform:scale(.98); }
  .error { background:#fef2f2; border:1px solid #fecaca; color:var(--red); padding:1rem; border-radius:8px;
           margin-top:1rem; font-size:.85rem; overflow-x:auto; }
  .hint { margin-top:.25rem; font-size:.8rem; color:var(--muted); }
</style></head><body>
  <div class="header">
    <h1>Lead Recovery App</h1>
    <p>Select a location and parameters to generate a recovery summary</p>
  </div>
  <div class="wrap">
    <div class="card">
      {% if error %}<div class="error">{{ error }}</div>{% endif %}
      <form method="POST">
        <label for="location_id">Location</label>
        <select name="location_id" id="location_id" required>
          {% for loc in locations %}
          <option value="{{ loc.location_id }}">{{ loc.location_id }} &mdash; {{ loc.location_name }}</option>
          {% endfor %}
        </select>
        {% if not locations %}<div class="hint">No locations loaded &mdash; check SQL warehouse status.</div>{% endif %}
        <div class="row">
          <div>
            <label for="snapshot_date">Snapshot Date</label>
            <input type="date" name="snapshot_date" id="snapshot_date" value="2026-10-04">
          </div>
          <div>
            <label for="lookback">Lookback (days)</label>
            <input type="number" name="lookback" id="lookback" value="120" min="1" max="365">
          </div>
        </div>
        <div class="row">
          <div>
            <label for="open_statuses">Open Statuses</label>
            <input type="text" name="open_statuses" id="open_statuses" value="New,Contacted,Qualified">
          </div>
          <div>
            <label for="terminal_statuses">Terminal Statuses</label>
            <input type="text" name="terminal_statuses" id="terminal_statuses" value="Lost,Converted">
          </div>
        </div>
        <button type="submit">Run Summary &rarr;</button>
      </form>
    </div>
  </div>
</body></html>
"""

SUMMARY_HTML = r"""
<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lead Recovery Summary &mdash; {{ location_name }}</title>
<style>
  :root { --blue:#3b82f6; --blue-dark:#1e40af; --green:#10b981; --green-dark:#047857;
          --bg:#f0f4f8; --card:#fff; --text:#1e293b; --muted:#64748b;
          --border:#e2e8f0; --amber:#f59e0b; --red:#ef4444; }
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Oxygen,sans-serif;
         background:var(--bg); color:var(--text); }
  .hdr { background:linear-gradient(135deg,#1e3a5f,#2563eb); color:#fff; padding:1.8rem 1rem; text-align:center; }
  .hdr h1 { font-size:1.6rem; font-weight:700; letter-spacing:-.02em; }
  .hdr .sub { margin-top:.35rem; opacity:.85; font-size:.9rem; }
  .wrap { max-width:840px; margin:0 auto; padding:1.5rem 1rem 3rem; }
  .sec-banner { background:linear-gradient(135deg,#1e293b,#334155); color:#fff; text-align:center;
                padding:.7rem; border-radius:8px; margin:1.8rem 0 .8rem; font-weight:700; font-size:.95rem; letter-spacing:.05em; }
  .sec-banner.green { background:linear-gradient(135deg,#047857,#059669); }
  .card { background:var(--card); border-radius:12px; padding:1.4rem; margin-bottom:1rem;
          box-shadow:0 2px 10px rgba(0,0,0,.06); border:1px solid var(--border); }
  .card h2 { font-size:1.1rem; margin-bottom:.75rem; color:var(--blue-dark); }
  .card h3 { font-size:.95rem; margin:1rem 0 .4rem; color:var(--text); font-weight:700; }
  .stats-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:.8rem; }
  .stat-box { background:#f8fafc; border-radius:8px; padding:1rem; text-align:center; border:1px solid var(--border); }
  .stat-box .num { font-size:1.8rem; font-weight:800; color:var(--blue); }
  .stat-box.green .num { color:var(--green); }
  .stat-box.red .num { color:var(--red); }
  .stat-box .lbl { font-size:.8rem; color:var(--muted); margin-top:.2rem; }
  .lead-card { background:#f8fafc; border-left:4px solid var(--blue); border-radius:8px;
               padding:.8rem 1rem; margin:.6rem 0; }
  .lead-card .id { font-weight:700; font-size:.95rem; color:#1e293b; }
  .lead-card .meta { color:var(--muted); font-size:.82rem; margin-top:.2rem; }
  .lead-card .rat { margin-top:.3rem; font-size:.88rem; color:#334155; }
  .badge { display:inline-block; padding:.12rem .5rem; border-radius:6px; font-size:.75rem; font-weight:600; }
  .badge.follow-up { background:#dbeafe; color:#1e40af; }
  .badge.staff-review { background:#fef3c7; color:#92400e; }
  .badge.pending { background:#e0e7ff; color:#3730a3; }
  .badge.phone { background:#ddd6fe; color:#5b21b6; }
  .badge.email { background:#fce7f3; color:#9d174d; }
  .kv { display:flex; justify-content:space-between; padding:.25rem 0; font-size:.9rem; }
  .kv .k { color:var(--muted); }
  .kv .v { font-weight:600; }
  table { width:100%; border-collapse:collapse; margin:.6rem 0; font-size:.9rem; }
  th, td { text-align:left; padding:.5rem .6rem; border-bottom:1px solid var(--border); }
  th { background:#f8fafc; font-weight:700; color:var(--muted); font-size:.8rem; text-transform:uppercase; letter-spacing:.03em; }
  td { color:var(--text); }
  .lever-row { display:flex; align-items:center; gap:.5rem; padding:.35rem 0; font-size:.9rem; }
  .lever-row .icon { width:1.5rem; text-align:center; }
  .lever-row .amt { margin-left:auto; font-weight:700; color:var(--green-dark); }
  .total-row { border-top:2px solid var(--border); margin-top:.4rem; padding-top:.5rem; }
  .total-row .v { color:var(--green-dark); font-size:1rem; }
  .note { font-size:.8rem; color:var(--muted); margin-top:.5rem; line-height:1.5; }
  .back { text-align:center; margin-top:2rem; }
  .back a { color:var(--blue); text-decoration:none; font-weight:600; }
  .back a:hover { text-decoration:underline; }
  .draft-text { padding-left:1.2rem; margin-top:.15rem; font-size:.85rem; color:#475569; font-style:italic; }
  .empty { color:var(--muted); font-style:italic; padding:.5rem 0; }
</style></head><body>

  <div class="hdr">
    <h1>Lead Recovery Summary</h1>
    <div class="sub">{{ location_name }} &nbsp;|&nbsp; {{ location_id }} &nbsp;|&nbsp; Snapshot: {{ snapshot_date }} &nbsp;|&nbsp; Lookback: {{ lookback }} days</div>
  </div>

  <div class="wrap">

    <div class="card">
      <h2>Quick Stats</h2>
      <div class="stats-grid">
        <div class="stat-box"><div class="num">{{ eligible_count }}</div><div class="lbl">Eligible Leads</div></div>
        <div class="stat-box green"><div class="num">{{ contact_limit_count }}</div><div class="lbl">Contact-Limit Skips</div></div>
        <div class="stat-box red"><div class="num">{{ excluded_count }}</div><div class="lbl">Excluded</div></div>
      </div>
    </div>

    <div class="sec-banner">STAFF ACTION ITEMS</div>

    <div class="card">
      <h2>Pending Tasks</h2>
      <p style="font-size:.85rem;color:var(--muted);margin-bottom:.8rem;">All tasks are SIMULATED &mdash; a manager must approve before any outreach.</p>
      {% if task_cards %}
        {% for t in task_cards %}
        <div class="lead-card">
          <div class="id">{{ t.num }}. {{ t.lead_id }}
            <span class="badge {{ t.decision|lower|replace(' ', '-') }}">{{ t.decision }}</span>
          </div>
          <div class="meta">{{ t.source }} &middot; {{ t.status }} &middot; Created {{ t.created }} &middot; Touches: {{ t.touches }} &middot; 1st response: {{ t.resp }} hrs</div>
          <div class="rat">{{ t.rationale }}</div>
        </div>
        {% endfor %}
      {% else %}
        <div class="empty">No pending tasks. Run the agent notebook (Cell 10) to generate new tasks.</div>
      {% endif %}
    </div>

    <div class="card">
      <h2>Nurturing Workflow</h2>
      <p style="font-size:.85rem;color:var(--muted);margin-bottom:.8rem;">3-touch staff plan: phone (day 0) &rarr; email/SMS (day 3) &rarr; phone (day 7). Drafts are suggestions for staff &mdash; no automated sending.</p>
      {% if nurture_groups %}
        {% for ng in nurture_groups %}
        <div class="lead-card">
          <div class="id">{{ loop.index }}. {{ ng.lead_id }}</div>
          {% for c in ng.cycles %}
          <div class="meta">
            Day {{ c.day }} &middot;
            <span class="badge {{ 'phone' if 'Phone' in c.channel else 'email' }}">{{ c.channel }}</span>
            &middot; <span class="badge pending">{{ c.status }}</span>
          </div>
          <div class="draft-text">"{{ c.draft }}"</div>
          {% endfor %}
        </div>
        {% endfor %}
      {% else %}
        <div class="empty">No nurturing cycles. Run the nurturing cell (Cell 13) first.</div>
      {% endif %}
    </div>

    <div class="sec-banner green">REVENUE &amp; GROWTH PROJECTIONS</div>

    <div class="card">
      <h2>Current State</h2>
      <div class="kv"><span class="k">Annual Revenue</span><span class="v">$100M ARR</span></div>
      <div class="kv"><span class="k">Total Patients</span><span class="v">{{ total_converted|int }} ({{ '%d'|format(conv_rate*100) }}% conversion)</span></div>
      <div class="kv"><span class="k">Revenue per Patient/yr</span><span class="v">${{ '%d'|format(rev_per_patient) }}</span></div>
      <div class="kv"><span class="k">Database</span><span class="v">{{ total_leads|int }} leads = {{ total_converted|int }} converted, {{ total_open|int }} open (${{ '%d'|format(open_potential_m) }}M), {{ total_lost|int }} lost</span></div>
    </div>

    <div class="card">
      <h2>Recovery Scenarios</h2>
      <div class="stats-grid">
        <div class="stat-box"><div class="num">+{{ '%d'|format(pred_rev) }}M</div><div class="lbl">Predicted (+5%)<br>{{ pred_pat|int }} new patients &rarr; ${{ '%d'|format(pred_arr) }}M ARR</div></div>
        <div class="stat-box green"><div class="num">+{{ '%d'|format(stretch_rev) }}M</div><div class="lbl">Very Positive (+15%)<br>{{ stretch_pat|int }} new patients &rarr; ${{ '%d'|format(stretch_arr) }}M ARR</div></div>
      </div>
    </div>

    <div class="card">
      <h2>Additional Revenue Levers</h2>
      <div class="lever-row"><span class="icon">1.</span> Re-engage lost leads (5% win-back)<span class="amt">${{ '%.1f'|format(reengage_rev) }}M</span></div>
      <div class="lever-row"><span class="icon">2.</span> Cut response time to &lt;1hr (+2%)<span class="amt">${{ '%.1f'|format(response_rev) }}M</span></div>
      <div class="lever-row"><span class="icon">3.</span> Staff nurturing workflow (+3%)<span class="amt">${{ '%.1f'|format(nurture_rev) }}M</span></div>
      <div class="lever-row"><span class="icon">4.</span> Raise patient LTV (+10%)<span class="amt">${{ '%.1f'|format(ltv_rev) }}M</span></div>
      <div class="lever-row"><span class="icon">5.</span> Open new locations ($5M each)<span class="amt">scales</span></div>
      <div class="lever-row total-row"><span class="icon"></span><strong>Combined Levers</strong><span class="amt">~${{ '%d'|format(combined) }}M</span></div>
      <div class="lever-row"><span class="icon"></span><strong>Predicted Total</strong><span class="amt" style="font-size:1rem;">${{ '%d'|format(pred_total) }}M ARR</span></div>
      <div class="lever-row"><span class="icon"></span><strong>Very Positive Total</strong><span class="amt" style="font-size:1rem;">${{ '%d'|format(stretch_total) }}M ARR</span></div>
    </div>

    <div class="card">
      <h2>10-Year Projection</h2>
      <p style="font-size:.85rem;color:var(--muted);margin-bottom:.6rem;">Levers compound year over year. Excluding new locations.</p>
      <table>
        <tr><th>Year</th><th>Predicted</th><th>Very Positive</th></tr>
        {% for y in years %}
        <tr><td style="font-weight:700;">{{ y.year }}</td><td>${{ y.predicted }}M</td><td style="color:var(--green-dark);font-weight:700;">${{ y.stretch }}M</td></tr>
        {% endfor %}
      </table>
      <div class="note">Predicted average reaches ~$250M by year 10 from levers alone. Very positive exceeds $300M.<br>
      New locations ($5M ARR each) would accelerate this further but are excluded. Projections are illustrative.</div>
    </div>

    <div class="back"><a href="/">&larr; Back to Input Form</a></div>
  </div>
</body></html>
"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
