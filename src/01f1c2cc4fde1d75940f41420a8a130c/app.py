import json
import os
import datetime
from flask import Flask, request, render_template_string, redirect
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

app = Flask(__name__)

# Initialize Databricks SDK with default config (auto-detects credentials)
try:
    w = WorkspaceClient()
    print(f"✅ SDK initialized. Host: {w.config.host}")
except Exception as e:
    w = None
    print(f"❌ SDK init failed: {e}")

SOURCE = "workspace.chiro_hackathon"
OUTPUT = "workspace.chiro_agent_demo"
WH_ID = "57129d05302f658f"  # hardcoded SQL warehouse ID

# --- SQL helpers ---

def run_sql(sql_text):
    """Execute SQL using Databricks SDK"""
    if not w:
        raise Exception("SDK not initialized")
    
    try:
        print(f"Executing SQL: {sql_text[:100]}...")
        resp = w.statement_execution.execute_statement(
            statement=sql_text,
            warehouse_id=WH_ID,
            wait_timeout="50s"
        )
        
        print(f"Response status: {resp.status}")
        print(f"Has result: {bool(resp.result)}")
        print(f"Has data_array: {bool(resp.result and resp.result.data_array)}")
        
        if resp.result and resp.result.data_array:
            print(f"Returned {len(resp.result.data_array)} rows")
        
        if not resp.result or not resp.result.data_array:
            print("Query returned no data")
            return []
        
        cols = [c.name for c in resp.manifest.schema.columns]
        return [dict(zip(cols, row)) for row in resp.result.data_array]
        
    except Exception as e:
        print(f"SQL ERROR: {sql_text[:200]}...")
        print(f"Error details: {e}")
        import traceback
        print(traceback.format_exc())
        raise Exception(f"Database query failed: {str(e)[:400]}")

def status_list(statuses):
    return ",".join(f"'{s.strip().lower()}'" for s in statuses.split(","))

# --- Summary computation ---

def compute_summary(location_id, snapshot_date, lookback, open_statuses, terminal_statuses, num_leads=10):
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
        SELECT 
            t.task_id, 
            t.lead_id, 
            t.decision, 
            t.evidence_json, 
            t.rationale, 
            t.draft_text, 
            t.review_status,
            array('James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','William','Elizabeth',
                  'David','Barbara','Richard','Susan','Joseph','Jessica','Thomas','Sarah','Charles','Karen',
                  'Christopher','Nancy','Daniel','Lisa','Matthew','Margaret','Anthony','Sandra','Mark','Ashley',
                  'Donald','Kimberly','Steven','Emily','Paul','Donna','Andrew','Michelle','Joshua','Carol',
                  'Kenneth','Amanda','Kevin','Melissa','Brian','Deborah','George','Stephanie','Edward','Rebecca')[abs(hash(t.lead_id)) % 50] as first_name,
            array('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez',
                  'Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin',
                  'Lee','Perez','Thompson','White','Harris','Sanchez','Clark','Ramirez','Lewis','Robinson',
                  'Walker','Young','Allen','King','Wright','Scott','Torres','Nguyen','Hill','Flores',
                  'Green','Adams','Nelson','Baker','Hall','Rivera','Campbell','Mitchell','Carter','Roberts')[abs(hash(t.lead_id)) % 50] as last_name,
            concat('(', lpad(cast((abs(hash(t.lead_id)) % 900) + 100 as string), 3, '0'), ') ', lpad(cast((abs(hash(concat(t.lead_id, 'a'))) % 900) + 100 as string), 3, '0'), '-', lpad(cast((abs(hash(concat(t.lead_id, 'b'))) % 9000) + 1000 as string), 4, '0')) as phone,
            concat(lower(array('James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','William','Elizabeth',
                  'David','Barbara','Richard','Susan','Joseph','Jessica','Thomas','Sarah','Charles','Karen',
                  'Christopher','Nancy','Daniel','Lisa','Matthew','Margaret','Anthony','Sandra','Mark','Ashley',
                  'Donald','Kimberly','Steven','Emily','Paul','Donna','Andrew','Michelle','Joshua','Carol',
                  'Kenneth','Amanda','Kevin','Melissa','Brian','Deborah','George','Stephanie','Edward','Rebecca')[abs(hash(t.lead_id)) % 50]),
                  '.',
                  lower(array('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez',
                  'Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin',
                  'Lee','Perez','Thompson','White','Harris','Sanchez','Clark','Ramirez','Lewis','Robinson',
                  'Walker','Young','Allen','King','Wright','Scott','Torres','Nguyen','Hill','Flores',
                  'Green','Adams','Nelson','Baker','Hall','Rivera','Campbell','Mitchell','Carter','Roberts')[abs(hash(t.lead_id)) % 50]),
                  '@email.com') as email
        FROM {OUTPUT}.followup_tasks t
        WHERE t.location_id = '{location_id}' AND t.review_status = 'pending_review'
        ORDER BY t.lead_id
    """)
    task_cards = []
    for i, t in enumerate(tasks):
        ev = json.loads(t.get("evidence_json", "{}")) if t.get("evidence_json") else {}
        task_cards.append({
            "num": i + 1,
            "task_id": t.get("task_id", ""),
            "lead_id": t.get("lead_id", ""),
            "first_name": t.get("first_name", ""),
            "last_name": t.get("last_name", ""),
            "phone": t.get("phone", ""),
            "email": t.get("email", ""),
            "source": ev.get("source", "Unknown"),
            "status": (ev.get("status") or "unknown").title(),
            "touches": ev.get("num_touchpoints", "?"),
            "resp": ev.get("first_response_hours", "?"),
            "created": ev.get("created_date", "?"),
            "decision": (t.get("decision") or "").upper().replace("_", " "),
            "rationale": t.get("rationale", ""),
            "draft": t.get("draft_text", ""),
        })

    cycles = run_sql(f"""
        SELECT 
            cycle_id, 
            lead_id, 
            cycle_number, 
            channel, 
            timing_days, 
            cycle_status, 
            draft_text,
            array('James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','William','Elizabeth',
                  'David','Barbara','Richard','Susan','Joseph','Jessica','Thomas','Sarah','Charles','Karen',
                  'Christopher','Nancy','Daniel','Lisa','Matthew','Margaret','Anthony','Sandra','Mark','Ashley',
                  'Donald','Kimberly','Steven','Emily','Paul','Donna','Andrew','Michelle','Joshua','Carol',
                  'Kenneth','Amanda','Kevin','Melissa','Brian','Deborah','George','Stephanie','Edward','Rebecca')[abs(hash(lead_id)) % 50] as first_name,
            array('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez',
                  'Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin',
                  'Lee','Perez','Thompson','White','Harris','Sanchez','Clark','Ramirez','Lewis','Robinson',
                  'Walker','Young','Allen','King','Wright','Scott','Torres','Nguyen','Hill','Flores',
                  'Green','Adams','Nelson','Baker','Hall','Rivera','Campbell','Mitchell','Carter','Roberts')[abs(hash(lead_id)) % 50] as last_name,
            concat('(', lpad(cast((abs(hash(lead_id)) % 900) + 100 as string), 3, '0'), ') ', lpad(cast((abs(hash(concat(lead_id, 'a'))) % 900) + 100 as string), 3, '0'), '-', lpad(cast((abs(hash(concat(lead_id, 'b'))) % 9000) + 1000 as string), 4, '0')) as phone,
            concat(lower(array('James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','William','Elizabeth',
                  'David','Barbara','Richard','Susan','Joseph','Jessica','Thomas','Sarah','Charles','Karen',
                  'Christopher','Nancy','Daniel','Lisa','Matthew','Margaret','Anthony','Sandra','Mark','Ashley',
                  'Donald','Kimberly','Steven','Emily','Paul','Donna','Andrew','Michelle','Joshua','Carol',
                  'Kenneth','Amanda','Kevin','Melissa','Brian','Deborah','George','Stephanie','Edward','Rebecca')[abs(hash(lead_id)) % 50]),
                  '.',
                  lower(array('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez',
                  'Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin',
                  'Lee','Perez','Thompson','White','Harris','Sanchez','Clark','Ramirez','Lewis','Robinson',
                  'Walker','Young','Allen','King','Wright','Scott','Torres','Nguyen','Hill','Flores',
                  'Green','Adams','Nelson','Baker','Hall','Rivera','Campbell','Mitchell','Carter','Roberts')[abs(hash(lead_id)) % 50]),
                  '@email.com') as email
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
            cur_group = {"lead_id": cur_lead, "first_name": nc.get("first_name", ""), "last_name": nc.get("last_name", ""), "phone": nc.get("phone", ""), "email": nc.get("email", ""), "cycles": []}
        chan = {"phone_call": "Phone Call", "email_sms": "Email/SMS"}.get(nc["channel"], nc["channel"])
        cur_group["cycles"].append({
            "cycle_id": nc.get("cycle_id", ""),
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

    # Fetch top leads with AI recommendations and generated contact info
    olist = status_list(open_statuses)
    top_leads_raw = run_sql(f"""
        SELECT 
            lead_id,
            source,
            status,
            num_touchpoints,
            first_response_hours,
            created_date,
            converted_flag,
            array('James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','William','Elizabeth',
                  'David','Barbara','Richard','Susan','Joseph','Jessica','Thomas','Sarah','Charles','Karen',
                  'Christopher','Nancy','Daniel','Lisa','Matthew','Margaret','Anthony','Sandra','Mark','Ashley',
                  'Donald','Kimberly','Steven','Emily','Paul','Donna','Andrew','Michelle','Joshua','Carol',
                  'Kenneth','Amanda','Kevin','Melissa','Brian','Deborah','George','Stephanie','Edward','Rebecca')[abs(hash(lead_id)) % 50] as first_name,
            array('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez',
                  'Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin',
                  'Lee','Perez','Thompson','White','Harris','Sanchez','Clark','Ramirez','Lewis','Robinson',
                  'Walker','Young','Allen','King','Wright','Scott','Torres','Nguyen','Hill','Flores',
                  'Green','Adams','Nelson','Baker','Hall','Rivera','Campbell','Mitchell','Carter','Roberts')[abs(hash(lead_id)) % 50] as last_name,
            concat('(', lpad(cast((abs(hash(lead_id)) % 900) + 100 as string), 3, '0'), ') ', lpad(cast((abs(hash(concat(lead_id, 'a'))) % 900) + 100 as string), 3, '0'), '-', lpad(cast((abs(hash(concat(lead_id, 'b'))) % 9000) + 1000 as string), 4, '0')) as phone,
            concat(lower(array('James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','William','Elizabeth',
                  'David','Barbara','Richard','Susan','Joseph','Jessica','Thomas','Sarah','Charles','Karen',
                  'Christopher','Nancy','Daniel','Lisa','Matthew','Margaret','Anthony','Sandra','Mark','Ashley',
                  'Donald','Kimberly','Steven','Emily','Paul','Donna','Andrew','Michelle','Joshua','Carol',
                  'Kenneth','Amanda','Kevin','Melissa','Brian','Deborah','George','Stephanie','Edward','Rebecca')[abs(hash(lead_id)) % 50]),
                  '.',
                  lower(array('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez',
                  'Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin',
                  'Lee','Perez','Thompson','White','Harris','Sanchez','Clark','Ramirez','Lewis','Robinson',
                  'Walker','Young','Allen','King','Wright','Scott','Torres','Nguyen','Hill','Flores',
                  'Green','Adams','Nelson','Baker','Hall','Rivera','Campbell','Mitchell','Carter','Roberts')[abs(hash(lead_id)) % 50]),
                  '@email.com') as email
        FROM {SOURCE}.leads
        WHERE assigned_location_id = '{location_id}'
          AND lower(trim(status)) IN ({olist})
          AND converted_flag = false
          AND num_touchpoints BETWEEN 0 AND 2
          AND datediff('{snapshot_date}', created_date) BETWEEN 0 AND {lookback}
        ORDER BY 
            CASE WHEN first_response_hours IS NULL OR first_response_hours > 48 THEN 1 ELSE 0 END DESC,
            num_touchpoints ASC,
            created_date DESC
        LIMIT {num_leads}
    """)
    
    # Generate AI recommendations for each lead automatically
    top_leads = []
    for lead in top_leads_raw:
        lead_dict = dict(lead)
        
        # Fetch saved staff notes for this lead
        try:
            notes_result = run_sql(f"""
                SELECT draft_text, reviewed_at, decision
                FROM {OUTPUT}.followup_tasks
                WHERE lead_id = '{lead_dict.get("lead_id")}'
                  AND decision IN ('STAFF_NOTE', 'LEAD_CLOSED')
                ORDER BY reviewed_at DESC
            """)
            lead_dict["staff_notes"] = notes_result if notes_result else []
            if notes_result:
                print(f"Found {len(notes_result)} notes for lead {lead_dict.get('lead_id')}")
        except Exception as e:
            print(f"Error fetching notes for {lead_dict.get('lead_id')}: {e}")
            import traceback
            traceback.print_exc()
            lead_dict["staff_notes"] = []
        
        # Generate AI recommendation inline
        try:
            ai_result = run_sql(f"""
                SELECT ai_gen(
                    'You are a lead recovery expert for a chiropractic wellness clinic. Analyze this lead and provide a brief, actionable recommendation.
                    
                    Lead: {lead_dict.get("lead_id")}
                    Source: {lead_dict.get("source")}
                    Status: {lead_dict.get("status")}
                    Touchpoints: {lead_dict.get("num_touchpoints")}
                    Response Time: {lead_dict.get("first_response_hours", "None")} hours
                    Created: {lead_dict.get("created_date")}
                    
                    Provide:
                    1. BEST ACTION: [Call/Email/SMS]
                    2. TIMING: [When to contact]
                    3. DRAFT MESSAGE: [Short personalized message]
                    4. KEY POINTS: [2-3 bullet points]
                    
                    Keep it concise and actionable.'
                ) as recommendation
            """)
            lead_dict["ai_recommendation"] = ai_result[0]["recommendation"] if ai_result else "AI recommendation unavailable"
        except Exception as e:
            print(f"AI generation error for {lead_dict.get('lead_id')}: {e}")
            lead_dict["ai_recommendation"] = "AI recommendation could not be generated"
        
        # Add placeholder outreach messages (will make on-demand later)
        fname = lead_dict.get('first_name', 'there')
        lname = lead_dict.get('last_name', '')
        lead_dict["suggested_sms"] = f"Hi {fname}! Thanks for your interest in our chiropractic services. We'd love to help you feel better. Ready to schedule a free consultation? Call us today at (555) 123-4567!"
        lead_dict["suggested_email"] = f"Subject: Welcome to Rodriguez, Figueroa and Sanchez Chiropractic & Wellness\n\nDear {fname} {lname},\n\nThank you for reaching out to us! We're excited to help you on your journey to better health and wellness.\n\nOur experienced chiropractors specialize in pain relief, injury recovery, and overall wellness. We'd love to offer you a complimentary initial consultation to discuss your specific needs and how we can help.\n\nTo schedule your free consultation, simply call us at (555) 123-4567 or reply to this email with your preferred date and time.\n\nWe look forward to meeting you!\n\nBest regards,\nRodriguez, Figueroa and Sanchez Chiropractic & Wellness"
        
        top_leads.append(lead_dict)

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
        "top_leads": top_leads,
    }

# --- Routes ---

# Hardcoded locations (fallback if SQL fails)
HARDCODED_LOCATIONS = [
    {"location_id": "LOC001", "location_name": "Rodriguez, Figueroa and Sanchez Chiropractic & Wellness"},
    {"location_id": "LOC002", "location_name": "Wagner Inc Chiropractic & Wellness"},
    {"location_id": "LOC003", "location_name": "Blake and Sons Chiropractic & Wellness"},
    {"location_id": "LOC004", "location_name": "Munoz-Roman Chiropractic & Wellness"},
    {"location_id": "LOC005", "location_name": "Stanley LLC Chiropractic & Wellness"},
    {"location_id": "LOC006", "location_name": "Clark-Adams Chiropractic & Wellness"},
    {"location_id": "LOC007", "location_name": "Watts, Robinson and Nguyen Chiropractic & Wellness"},
    {"location_id": "LOC008", "location_name": "Lewis-Porter Chiropractic & Wellness"},
    {"location_id": "LOC009", "location_name": "Hicks Inc Chiropractic & Wellness"},
    {"location_id": "LOC010", "location_name": "Rice-Maddox Chiropractic & Wellness"},
    {"location_id": "LOC011", "location_name": "Smith-Bowen Chiropractic & Wellness"},
    {"location_id": "LOC012", "location_name": "Brooks and Sons Chiropractic & Wellness"},
    {"location_id": "LOC013", "location_name": "Arroyo, Miller and Tucker Chiropractic & Wellness"},
    {"location_id": "LOC014", "location_name": "Henderson, Lewis and Ryan Chiropractic & Wellness"},
    {"location_id": "LOC015", "location_name": "Brown Inc Chiropractic & Wellness"},
    {"location_id": "LOC016", "location_name": "Palmer LLC Chiropractic & Wellness"},
    {"location_id": "LOC017", "location_name": "Davis Ltd Chiropractic & Wellness"},
    {"location_id": "LOC018", "location_name": "Wright and Sons Chiropractic & Wellness"},
    {"location_id": "LOC019", "location_name": "Garcia, Pearson and Fernandez Chiropractic & Wellness"},
    {"location_id": "LOC020", "location_name": "Martin Inc Chiropractic & Wellness"},
]

@app.route("/", methods=["GET", "POST"])
def index():
    # Use hardcoded locations (always works)
    locs = HARDCODED_LOCATIONS
    
    if request.method == "GET":
        return render_template_string(FORM_HTML, locations=locs, error=None)
    location_id = request.form.get("location_id", "")
    snapshot_date = request.form.get("snapshot_date", "2026-10-04")
    lookback = request.form.get("lookback", "120")
    open_statuses = request.form.get("open_statuses", "New,Contacted,Qualified")
    terminal_statuses = request.form.get("terminal_statuses", "Lost,Converted")
    num_leads = request.form.get("num_leads", "10")
    try:
        summary = compute_summary(location_id, snapshot_date, int(lookback), open_statuses, terminal_statuses, int(num_leads))
        return render_template_string(SUMMARY_HTML, **summary, location_id=location_id,
                                      snapshot_date=snapshot_date, lookback=lookback,
                                      open_statuses=open_statuses, terminal_statuses=terminal_statuses,
                                      num_leads=num_leads, locations=locs)
    except Exception as e:
        import traceback
        error_msg = f"<h3 style='color:red'>Error Loading Data</h3><p><strong>{str(e)}</strong></p><pre style='background:#f5f5f5;padding:1rem;overflow:auto'>{traceback.format_exc()}</pre>"
        return render_template_string(FORM_HTML, locations=locs, error=error_msg)

@app.route("/add_note/<lead_id>", methods=["POST"])
def add_note(lead_id):
    """Add a staff note/activity to a lead - returns JSON for AJAX"""
    from flask import jsonify
    try:
        staff_note = request.form.get("staff_note", "")
        snapshot_date = request.form.get("snapshot_date", "2026-10-04")
        
        if not staff_note.strip():
            return jsonify({"success": False, "error": "Note cannot be empty"})
        
        now = datetime.datetime.now().isoformat()
        escaped_note = staff_note.replace("'", "''")
        task_id = f"{lead_id}_note_{now.replace(':', '').replace('-', '').replace('.', '')}"
        
        print(f"Saving note for lead {lead_id}: {staff_note}")
        run_sql(f"""
            INSERT INTO {OUTPUT}.followup_tasks (
                task_id, run_id, snapshot_date, lead_id, location_id, decision, evidence_json, rationale, 
                draft_text, review_status, reviewed_at, is_simulated
            ) 
            SELECT 
                '{task_id}',
                'manual_entry',
                '{snapshot_date}',
                '{lead_id}',
                assigned_location_id,
                'STAFF_NOTE',
                '{{}}',
                'Staff activity: {escaped_note}',
                '{escaped_note}',
                'completed',
                '{now}',
                false
            FROM {SOURCE}.leads
            WHERE lead_id = '{lead_id}'
        """)
        print(f"Note saved successfully for lead {lead_id}")
        return jsonify({"success": True, "note": staff_note, "timestamp": now, "lead_id": lead_id})
    except Exception as e:
        print(f"Error adding note: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/close_lead/<lead_id>", methods=["POST"])
def close_lead(lead_id):
    """Mark a lead as closed - returns JSON for AJAX"""
    from flask import jsonify
    try:
        close_reason = request.form.get("close_reason", "Closed by staff")
        snapshot_date = request.form.get("snapshot_date", "2026-10-04")
        
        print(f"Closing lead {lead_id} with reason: {close_reason}")
        run_sql(f"""
            UPDATE {SOURCE}.leads
            SET status = 'Lost'
            WHERE lead_id = '{lead_id}'
        """)
        
        now = datetime.datetime.now().isoformat()
        if close_reason.strip():
            escaped_reason = close_reason.replace("'", "''")
            task_id = f"{lead_id}_closed_{now.replace(':', '').replace('-', '').replace('.', '')}"
            
            run_sql(f"""
                INSERT INTO {OUTPUT}.followup_tasks (
                    task_id, run_id, snapshot_date, lead_id, location_id, decision, evidence_json, rationale, 
                    draft_text, review_status, reviewed_at, is_simulated
                ) 
                SELECT 
                    '{task_id}',
                    'manual_entry',
                    '{snapshot_date}',
                    '{lead_id}',
                    assigned_location_id,
                    'LEAD_CLOSED',
                    '{{}}',
                    'Lead closed: {escaped_reason}',
                    'Closed: {escaped_reason}',
                    'completed',
                    '{now}',
                    false
                FROM {SOURCE}.leads
                WHERE lead_id = '{lead_id}'
            """)
            print(f"Lead closure logged for {lead_id}")
        
        return jsonify({"success": True, "lead_id": lead_id, "reason": close_reason, "timestamp": now})
    except Exception as e:
        print(f"Error closing lead: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/complete_task/<task_id>", methods=["POST"])
def complete_task(task_id):
    """Mark a pending task as completed"""
    from flask import jsonify
    try:
        now = datetime.datetime.now().isoformat()
        run_sql(f"""
            UPDATE {OUTPUT}.followup_tasks
            SET review_status = 'completed', reviewed_at = '{now}'
            WHERE task_id = '{task_id}'
        """)
        print(f"Task {task_id} completed")
        return jsonify({"success": True, "task_id": task_id})
    except Exception as e:
        print(f"Error completing task: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/update_task/<task_id>", methods=["POST"])
def update_task(task_id):
    """Update a task's rationale/draft"""
    from flask import jsonify
    try:
        new_rationale = request.form.get("rationale", "")
        new_draft = request.form.get("draft", "")
        escaped_rat = new_rationale.replace("'", "''")
        escaped_draft = new_draft.replace("'", "''")
        run_sql(f"""
            UPDATE {OUTPUT}.followup_tasks
            SET rationale = '{escaped_rat}', draft_text = '{escaped_draft}'
            WHERE task_id = '{task_id}'
        """)
        print(f"Task {task_id} updated")
        return jsonify({"success": True, "task_id": task_id})
    except Exception as e:
        print(f"Error updating task: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/complete_cycle/<cycle_id>", methods=["POST"])
def complete_cycle(cycle_id):
    """Mark a nurturing cycle as completed"""
    from flask import jsonify
    try:
        run_sql(f"""
            UPDATE {OUTPUT}.nurturing_cycles
            SET cycle_status = 'completed'
            WHERE cycle_id = '{cycle_id}'
        """)
        print(f"Cycle {cycle_id} completed")
        return jsonify({"success": True, "cycle_id": cycle_id})
    except Exception as e:
        print(f"Error completing cycle: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/update_cycle/<cycle_id>", methods=["POST"])
def update_cycle(cycle_id):
    """Update a nurturing cycle's draft text"""
    from flask import jsonify
    try:
        new_draft = request.form.get("draft", "")
        escaped_draft = new_draft.replace("'", "''")
        run_sql(f"""
            UPDATE {OUTPUT}.nurturing_cycles
            SET draft_text = '{escaped_draft}'
            WHERE cycle_id = '{cycle_id}'
        """)
        print(f"Cycle {cycle_id} draft updated")
        return jsonify({"success": True, "cycle_id": cycle_id})
    except Exception as e:
        print(f"Error updating cycle: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

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
        info.append(f"<p>SDK Initialized: {bool(w)}</p>")
        if w:
            info.append(f"<p>Databricks Host: {w.config.host}</p>")
        info.append(f"<p>Warehouse ID: {WH_ID}</p>")
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
        <label for="num_leads">Number of Leads to Show</label>
        <input type="number" name="num_leads" id="num_leads" value="10" min="1" max="50">
        <div class="hint">Show the top priority leads (sorted by response time and touchpoints)</div>
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
<title>Lead Recovery Dashboard &mdash; {{ location_name }}</title>
<style>
  :root { --blue:#3b82f6; --blue-dark:#1e40af; --green:#10b981; --green-dark:#047857;
          --bg:#eef2f7; --card:#fff; --text:#1e293b; --muted:#64748b;
          --border:#e2e8f0; --amber:#f59e0b; --red:#ef4444; --purple:#8b5cf6; }
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Oxygen,sans-serif;
         background:var(--bg); color:var(--text); }
  .hdr { background:linear-gradient(135deg,#1e3a5f,#2563eb); color:#fff; padding:1.2rem 1rem; }
  .hdr-inner { max-width:1300px; margin:0 auto; display:flex; justify-content:space-between; align-items:center; }
  .hdr h1 { font-size:1.4rem; font-weight:700; }
  .hdr .sub { margin-top:.2rem; opacity:.85; font-size:.82rem; }
  .hdr-back { background:rgba(255,255,255,.15); color:#fff; padding:.5rem 1rem; border-radius:8px; text-decoration:none; font-size:.85rem; font-weight:600; }
  .hdr-back:hover { background:rgba(255,255,255,.25); }
  .wrap { max-width:1300px; margin:0 auto; padding:1rem; }
  /* Tab nav */
  .tabs { display:flex; gap:.3rem; margin-bottom:1rem; flex-wrap:wrap; }
  .tab { padding:.6rem 1.2rem; border-radius:8px 8px 0 0; cursor:pointer; font-weight:600; font-size:.88rem;
         background:#e2e8f0; color:var(--muted); border:none; transition:all .2s; }
  .tab:hover { background:#d0d8e8; }
  .tab.active { background:var(--card); color:var(--blue-dark); box-shadow:0 -2px 6px rgba(0,0,0,.06); }
  .tab-content { display:none; }
  .tab-content.active { display:block; }
  /* Grid layouts */
  .grid-2 { display:grid; grid-template-columns:1fr 1fr; gap:1rem; }
  .grid-3 { display:grid; grid-template-columns:repeat(3,1fr); gap:1rem; }
  .grid-leads { display:grid; grid-template-columns:repeat(auto-fill,minmax(380px,1fr)); gap:.8rem; }
  @media(max-width:840px){ .grid-2,.grid-3,.grid-leads { grid-template-columns:1fr; } }
  @media(max-width:1000px){ #overview > div:first-child { grid-template-columns:1fr !important; } }
  /* Cards */
  .card { background:var(--card); border-radius:10px; padding:1.2rem; margin-bottom:1rem;
          box-shadow:0 2px 8px rgba(0,0,0,.05); border:1px solid var(--border); }
  .card h2 { font-size:1rem; margin-bottom:.6rem; color:var(--blue-dark); }
  .card h3 { font-size:.9rem; margin:.8rem 0 .4rem; font-weight:700; }
  /* Stat boxes */
  .stat-box { background:#f8fafc; border-radius:8px; padding:.9rem; text-align:center; border:1px solid var(--border); }
  .stat-box .num { font-size:1.6rem; font-weight:800; color:var(--blue); }
  .stat-box.green .num { color:var(--green); }
  .stat-box.red .num { color:var(--red); }
  .stat-box.purple .num { color:var(--purple); }
  .stat-box .lbl { font-size:.75rem; color:var(--muted); margin-top:.15rem; }
  /* Lead cards */
  .lead-card { background:#f8fafc; border-left:4px solid var(--blue); border-radius:8px; padding:.8rem; }
  .lead-card .id { font-weight:700; font-size:.9rem; color:#1e293b; display:flex; justify-content:space-between; align-items:center; }
  .lead-card .meta { color:var(--muted); font-size:.78rem; margin-top:.25rem; }
  .lead-card .rat { margin-top:.25rem; font-size:.82rem; color:#334155; }
  .lead-card .priority { font-size:.78rem; margin-top:.2rem; }
  .badge { display:inline-block; padding:.1rem .4rem; border-radius:4px; font-size:.7rem; font-weight:600; }
  .badge.follow-up { background:#dbeafe; color:#1e40af; }
  .badge.staff-review { background:#fef3c7; color:#92400e; }
  .badge.pending { background:#e0e7ff; color:#3730a3; }
  .badge.phone { background:#ddd6fe; color:#5b21b6; }
  .badge.email { background:#fce7f3; color:#9d174d; }
  /* KV rows */
  .kv { display:flex; justify-content:space-between; padding:.2rem 0; font-size:.85rem; }
  .kv .k { color:var(--muted); }
  .kv .v { font-weight:600; }
  table { width:100%; border-collapse:collapse; font-size:.85rem; }
  th, td { text-align:left; padding:.4rem .5rem; border-bottom:1px solid var(--border); }
  th { background:#f8fafc; font-weight:700; color:var(--muted); font-size:.75rem; text-transform:uppercase; }
  .lever-row { display:flex; align-items:center; gap:.5rem; padding:.3rem 0; font-size:.85rem; }
  .lever-row .icon { width:1.5rem; text-align:center; }
  .lever-row .amt { margin-left:auto; font-weight:700; color:var(--green-dark); }
  .total-row { border-top:2px solid var(--border); margin-top:.4rem; padding-top:.5rem; }
  .note { font-size:.78rem; color:var(--muted); margin-top:.4rem; line-height:1.4; }
  .draft-text { padding-left:1.2rem; margin-top:.15rem; font-size:.82rem; color:#475569; font-style:italic; }
  .empty { color:var(--muted); font-style:italic; padding:.5rem 0; text-align:center; }
  /* Details */
  details { margin-top:.5rem; }
  summary { cursor:pointer; padding:.4rem .6rem; background:#f1f5f9; border-radius:6px; font-weight:600; font-size:.82rem;
            color:var(--blue); border:1px solid var(--border); user-select:none; }
  summary:hover { background:#e2e8f0; }
  .details-content { margin-top:.5rem; padding:.8rem; background:#fafafa; border-radius:8px; border:1px solid var(--border); }
  .ai-rec { padding:.6rem; background:#faf5ff; border-left:3px solid #8b5cf6; border-radius:6px; margin-bottom:.8rem; }
  .ai-rec h4 { color:#6366f1; font-size:.88rem; margin-bottom:.3rem; }
  .save-msg { font-size:.82rem; padding:.3rem; margin-top:.3rem; border-radius:4px; display:none; }
  .save-msg.ok { display:block; background:#dcfce7; color:#166534; }
  .save-msg.err { display:block; background:#fee2e2; color:#991b1b; }
  .btn { padding:.45rem .9rem; border:none; border-radius:6px; font-size:.85rem; font-weight:600; cursor:pointer; }
  .btn-blue { background:var(--blue); color:#fff; }
  .btn-red { background:var(--red); color:#fff; }
  input[type=text],textarea { width:100%; padding:.5rem; border:1px solid var(--border); border-radius:6px; font-size:.85rem; font-family:inherit; }
  textarea { min-height:60px; resize:vertical; }
  .section-title { font-size:.82rem; font-weight:700; margin-bottom:.3rem; color:var(--text); }
</style></head><body>

  <div class="hdr">
    <div class="hdr-inner">
      <div>
        <h1>Lead Recovery Dashboard</h1>
        <div class="sub">{{ location_name }} &nbsp;|&nbsp; {{ location_id }} &nbsp;|&nbsp; Snapshot: {{ snapshot_date }} &nbsp;|&nbsp; Lookback: {{ lookback }} days</div>
      </div>
      <a href="/" class="hdr-back">&larr; New Search</a>
    </div>
  </div>

  <div class="wrap">
    <!-- Tab Navigation -->
    <div class="tabs">
      <button class="tab active" onclick="showTab('overview')">📊 Overview</button>
      <button class="tab" onclick="showTab('leads')">🎯 Priority Leads</button>
      <button class="tab" onclick="showTab('tasks')">📋 Tasks</button>
      <button class="tab" onclick="showTab('revenue')">💰 Revenue</button>
    </div>

    <!-- OVERVIEW TAB -->
    <div id="overview" class="tab-content active">
      <div style="display:grid;grid-template-columns:280px 1fr 280px;gap:1rem;align-items:start;">
        <!-- LEFT SIDEBAR: Stats -->
        <div>
          <div class="card">
            <h2>Lead Stats</h2>
            <div class="stat-box" style="margin-bottom:.5rem"><div class="num">{{ eligible_count }}</div><div class="lbl">Eligible Leads</div></div>
            <div class="kv"><span class="k">Contact-Limit Skips</span><span class="v" style="color:var(--green)">{{ contact_limit_count }}</span></div>
            <div class="kv"><span class="k">Excluded</span><span class="v" style="color:var(--red)">{{ excluded_count }}</span></div>
          </div>
          <div class="card">
            <h2>Database</h2>
            <div class="kv"><span class="k">Total Leads</span><span class="v">{{ total_leads|int }}</span></div>
            <div class="kv"><span class="k">Converted</span><span class="v" style="color:var(--green)">{{ total_converted|int }}</span></div>
            <div class="kv"><span class="k">Open</span><span class="v">{{ total_open|int }}</span></div>
            <div class="kv"><span class="k">Lost</span><span class="v" style="color:var(--red)">{{ total_lost|int }}</span></div>
            <div class="kv"><span class="k">Conv Rate</span><span class="v">{{ '%d'|format(conv_rate*100) }}%</span></div>
            <div class="kv"><span class="k">Rev/Patient</span><span class="v">${{ '%d'|format(rev_per_patient) }}</span></div>
          </div>
        </div>

        <!-- CENTER: Priority Leads Highlight -->
        <div class="card" style="border:2px solid var(--blue);box-shadow:0 4px 14px rgba(59,130,246,.12);">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:.8rem">
            <h2 style="margin:0">🎯 Top Priority Leads</h2>
            <button class="tab btn btn-blue" onclick="showTab('leads')" style="font-size:.8rem;padding:.3rem .7rem">View All →</button>
          </div>
          {% if top_leads %}
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:.5rem">
              {% for lead in top_leads[:4] %}
              <div class="lead-card" style="padding:.6rem">
                <div class="id"><strong>{{ lead.lead_id }}</strong> <span class="badge pending" style="font-size:.65rem">{{ lead.status }}</span></div>
                <div style="font-size:.78rem;font-weight:600;color:var(--text)">{{ lead.first_name }} {{ lead.last_name }}</div>
                <div class="meta" style="font-size:.72rem">{{ lead.phone }} · {{ lead.email }}</div>
                <div class="meta" style="font-size:.72rem">{{ lead.source }} · {{ lead.num_touchpoints }} touches · {% if lead.first_response_hours %}{{ lead.first_response_hours }}h{% else %}No response{% endif %}</div>
                <div class="priority" style="font-size:.72rem">{% if not lead.first_response_hours %}🔴 High{% elif lead.first_response_hours|float > 48 %}🔴 High{% elif lead.num_touchpoints == 0 %}🟡 Medium{% else %}🟢 Normal{% endif %}</div>
              </div>
              {% endfor %}
            </div>
            {% if top_leads|length > 4 %}
            <div style="text-align:center;margin-top:.6rem;font-size:.82rem;color:var(--muted)">+{{ top_leads|length - 4 }} more leads — <a href="#" onclick="showTab('leads');return false" style="color:var(--blue);font-weight:600">view all</a></div>
            {% endif %}
          {% else %}
            <div class="empty">No leads found matching the criteria.</div>
          {% endif %}
        </div>

        <!-- RIGHT SIDEBAR: Revenue Highlights -->
        <div>
          <div class="card">
            <h2>Revenue Outlook</h2>
            <div class="stat-box" style="margin-bottom:.5rem"><div class="num" style="font-size:1.3rem">${{ '%d'|format(pred_arr) }}M</div><div class="lbl">Predicted ARR (+5%)</div></div>
            <div class="stat-box green" style="margin-bottom:.5rem"><div class="num" style="font-size:1.3rem">${{ '%d'|format(stretch_arr) }}M</div><div class="lbl">Very Positive (+15%)</div></div>
            <div class="kv"><span class="k">Combined Levers</span><span class="v" style="color:var(--green-dark)">~${{ '%d'|format(combined) }}M</span></div>
          </div>
          <div class="card">
            <h2>Revenue Levers</h2>
            <div class="lever-row" style="font-size:.78rem"><span class="icon">1.</span> Re-engage lost<span class="amt">${{ '%.1f'|format(reengage_rev) }}M</span></div>
            <div class="lever-row" style="font-size:.78rem"><span class="icon">2.</span> Faster response<span class="amt">${{ '%.1f'|format(response_rev) }}M</span></div>
            <div class="lever-row" style="font-size:.78rem"><span class="icon">3.</span> Nurturing<span class="amt">${{ '%.1f'|format(nurture_rev) }}M</span></div>
            <div class="lever-row" style="font-size:.78rem"><span class="icon">4.</span> Raise LTV<span class="amt">${{ '%.1f'|format(ltv_rev) }}M</span></div>
          </div>
        </div>
      </div>
      <div style="max-width:600px;margin:0 auto;text-align:center">
        <div class="note" style="font-size:.78rem">Current: $100M ARR · Target: $250M ARR · Predicted Total: ${{ '%d'|format(pred_total) }}M · Stretch: ${{ '%d'|format(stretch_total) }}M</div>
      </div>
    </div>

    <!-- LEADS TAB -->
    <div id="leads" class="tab-content">
      <div class="card">
        <h2>Best {{ num_leads }} Leads (sorted by priority)</h2>
        <p style="font-size:.82rem;color:var(--muted);margin-bottom:.6rem;">Click "More Details" for AI recommendations, notes & actions</p>
        <div class="grid-leads">
        {% if top_leads %}
          {% for lead in top_leads %}
          <div class="lead-card" style="position:relative;">
            <div class="id">
              <strong>{{ lead.lead_id }}</strong>
              <span class="badge pending">{{ lead.status }}</span>
            </div>
            <div style="font-size:.9rem;font-weight:600;color:var(--text);margin-top:.2rem">{{ lead.first_name }} {{ lead.last_name }}</div>
            <div style="font-size:.8rem;color:var(--muted);margin-top:.1rem">📞 {{ lead.phone }} &nbsp; ✉️ {{ lead.email }}</div>
            <div class="meta">
              {{ lead.source }} &middot; {{ lead.num_touchpoints }} touches &middot; {% if lead.first_response_hours %}{{ lead.first_response_hours }}h response{% else %}No response{% endif %} &middot; {{ lead.created_date }}
            </div>
            <div class="priority">
              {% if not lead.first_response_hours %}🔴 High - No response{% elif lead.first_response_hours|float > 48 %}🔴 High - Slow response{% elif lead.num_touchpoints == 0 %}🟡 Medium - New{% else %}🟢 Normal{% endif %}
            </div>
            <details>
              <summary>📋 More Details & Actions</summary>
              <div class="details-content">
                <div class="ai-rec">
                  <h4>🤖 AI Recommendation</h4>
                  <pre style="white-space:pre-wrap;font-family:inherit;font-size:.82rem;line-height:1.5;margin:0;">{{ lead.ai_recommendation }}</pre>
                </div>
                
                <!-- AI-Generated Outreach Messages -->
                <div style="margin-top:.8rem;padding:.6rem;background:#f0f9ff;border-radius:6px;border:1px solid #bae6fd;">
                  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:.4rem;">
                    <h4 style="margin:0;font-size:.85rem;color:#0369a1;">📱 AI-Generated SMS</h4>
                    <button onclick="copyText('sms-{{ lead.lead_id }}')" class="btn" style="font-size:.7rem;padding:.2rem .5rem;background:#0ea5e9;color:#fff">📋 Copy</button>
                  </div>
                  <div id="sms-{{ lead.lead_id }}" style="font-size:.82rem;line-height:1.5;color:#0c4a6e;white-space:pre-wrap;">{{ lead.suggested_sms }}</div>
                </div>
                
                <div style="margin-top:.6rem;padding:.6rem;background:#f0fdf4;border-radius:6px;border:1px solid #bbf7d0;">
                  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:.4rem;">
                    <h4 style="margin:0;font-size:.85rem;color:#15803d;">✉️ AI-Generated Email</h4>
                    <button onclick="copyText('email-{{ lead.lead_id }}')" class="btn" style="font-size:.7rem;padding:.2rem .5rem;background:#22c55e;color:#fff">📋 Copy</button>
                  </div>
                  <div id="email-{{ lead.lead_id }}" style="font-size:.82rem;line-height:1.5;color:#14532d;white-space:pre-wrap;">{{ lead.suggested_email }}</div>
                </div>
                
                {% if lead.staff_notes %}
                <div id="notes-history-{{ lead.lead_id }}" style="margin-bottom:.8rem;padding:.6rem;background:#fff;border-radius:6px;border:1px solid var(--border);">
                  <div class="section-title">📚 Staff Activity History</div>
                  {% for note in lead.staff_notes %}
                  <div style="padding:.4rem;background:#f8fafc;border-left:3px solid {% if note.decision == 'LEAD_CLOSED' %}var(--red){% else %}var(--blue){% endif %};border-radius:4px;margin-bottom:.4rem;">
                    <div style="font-size:.7rem;color:var(--muted);margin-bottom:.2rem;">{% if note.decision == 'LEAD_CLOSED' %}🚫 Closed{% else %}📝 Note{% endif %} &mdash; {{ note.reviewed_at }}</div>
                    <div style="font-size:.82rem;white-space:pre-wrap;">{{ note.draft_text }}</div>
                  </div>
                  {% endfor %}
                </div>
                {% endif %}
                <div style="margin-bottom:.6rem;">
                  <div class="section-title">📝 Add Staff Note</div>
                  <textarea id="note-text-{{ lead.lead_id }}" placeholder="What did you do for this lead?" style="margin-top:.3rem;"></textarea>
                  <button onclick="saveNote('{{ lead.lead_id }}')" class="btn btn-blue" style="margin-top:.3rem;">💾 Save Note</button>
                  <div id="save-msg-{{ lead.lead_id }}" class="save-msg"></div>
                </div>
                <div style="padding-top:.6rem;border-top:1px solid var(--border);">
                  <div class="section-title" style="color:var(--red);">🚫 Close Lead</div>
                  <input type="text" id="close-reason-{{ lead.lead_id }}" placeholder="Reason for closing" style="margin-top:.3rem;margin-bottom:.3rem;">
                  <button onclick="closeLead('{{ lead.lead_id }}')" class="btn btn-red">❌ Close Lead</button>
                  <div id="close-msg-{{ lead.lead_id }}" class="save-msg"></div>
                </div>
              </div>
            </details>
          </div>
          {% endfor %}
        {% else %}
          <div class="empty">No leads found matching the criteria.</div>
        {% endif %}
        </div>
      </div>
    </div>

    <!-- TASKS TAB -->
    <div id="tasks" class="tab-content">
      <div class="grid-2">
        <div class="card">
          <h2>Pending Tasks</h2>
          <p style="font-size:.8rem;color:var(--muted);margin-bottom:.5rem;">Complete or update tasks below. All tasks require staff action.</p>
          {% if task_cards %}
            {% for t in task_cards %}
            <div class="lead-card" id="task-{{ t.task_id }}">
              <div class="id">{{ t.num }}. {{ t.lead_id }}
                <span class="badge {{ t.decision|lower|replace(' ', '-') }}">{{ t.decision }}</span>
              </div>
              <div style="font-size:.9rem;font-weight:600;color:var(--text);margin-top:.2rem">{{ t.first_name }} {{ t.last_name }}</div>
              <div style="font-size:.8rem;color:var(--muted);margin-top:.1rem">📞 {{ t.phone }} &nbsp; ✉️ {{ t.email }}</div>
              <div class="meta">{{ t.source }} &middot; {{ t.status }} &middot; Touches: {{ t.touches }} &middot; 1st response: {{ t.resp }} hrs</div>
              <div class="rat">{{ t.rationale }}</div>
              {% if t.draft %}
              <div class="draft-text" style="margin-top:.3rem">"{{ t.draft }}"</div>
              {% endif %}
              <div style="margin-top:.5rem;display:flex;gap:.4rem;flex-wrap:wrap">
                <button onclick="completeTask('{{ t.task_id }}')" class="btn btn-blue" style="font-size:.8rem;padding:.3rem .7rem">✅ Complete</button>
                <button onclick="toggleEditTask('{{ t.task_id }}')" class="btn" style="font-size:.8rem;padding:.3rem .7rem;background:#64748b;color:#fff">✏️ Edit</button>
              </div>
              <div id="edit-task-{{ t.task_id }}" style="display:none;margin-top:.5rem">
                <input type="text" id="task-rat-{{ t.task_id }}" value="{{ t.rationale }}" placeholder="Rationale" style="margin-bottom:.3rem;font-size:.82rem">
                <textarea id="task-draft-{{ t.task_id }}" placeholder="Draft message" style="font-size:.82rem;min-height:50px">{{ t.draft }}</textarea>
                <button onclick="updateTask('{{ t.task_id }}')" class="btn btn-blue" style="font-size:.8rem;padding:.3rem .7rem;margin-top:.3rem">💾 Save Changes</button>
              </div>
              <div id="task-msg-{{ t.task_id }}" class="save-msg"></div>
            </div>
            {% endfor %}
          {% else %}
            <div class="empty">No pending tasks. Run the agent notebook (Cell 10) to generate new tasks.</div>
          {% endif %}
        </div>
        <div class="card">
          <h2>Nurturing Workflow</h2>
          <p style="font-size:.8rem;color:var(--muted);margin-bottom:.5rem;">3-touch plan: phone (day 0) &rarr; email/SMS (day 3) &rarr; phone (day 7)</p>
          {% if nurture_groups %}
            {% for ng in nurture_groups %}
            <div class="lead-card">
              <div class="id">{{ loop.index }}. {{ ng.lead_id }}</div>
              <div style="font-size:.9rem;font-weight:600;color:var(--text);margin-top:.2rem">{{ ng.first_name }} {{ ng.last_name }}</div>
              <div style="font-size:.8rem;color:var(--muted);margin-top:.1rem;margin-bottom:.4rem">📞 {{ ng.phone }} &nbsp; ✉️ {{ ng.email }}</div>
              {% for c in ng.cycles %}
              <div class="meta" style="margin-top:.4rem">
                Day {{ c.day }} &middot;
                <span class="badge {{ 'phone' if 'Phone' in c.channel else 'email' }}">{{ c.channel }}</span>
                &middot; <span class="badge {% if c.status == 'completed' %}" style="background:#dcfce7;color:#166534"{% else %}pending{% endif %}">{{ c.status }}</span>
              </div>
              <div class="draft-text" style="font-size:.8rem">"{{ c.draft }}"</div>
              {% if c.status != 'completed' %}
              <div style="margin-top:.3rem;display:flex;gap:.4rem;flex-wrap:wrap">
                <button onclick="completeCycle('{{ c.cycle_id }}')" class="btn btn-blue" style="font-size:.75rem;padding:.25rem .6rem">✅ Done</button>
                <button onclick="toggleEditCycle('{{ c.cycle_id }}')" class="btn" style="font-size:.75rem;padding:.25rem .6rem;background:#64748b;color:#fff">✏️ Edit Draft</button>
              </div>
              <div id="edit-cycle-{{ c.cycle_id }}" style="display:none;margin-top:.3rem">
                <textarea id="cycle-draft-{{ c.cycle_id }}" style="font-size:.78rem;min-height:40px">{{ c.draft }}</textarea>
                <button onclick="updateCycle('{{ c.cycle_id }}')" class="btn btn-blue" style="font-size:.75rem;padding:.25rem .6rem;margin-top:.2rem">💾 Save</button>
              </div>
              <div id="cycle-msg-{{ c.cycle_id }}" class="save-msg"></div>
              {% endif %}
              {% endfor %}
            </div>
            {% endfor %}
          {% else %}
            <div class="empty">No nurturing cycles. Run the nurturing cell (Cell 13) first.</div>
          {% endif %}
        </div>
      </div>
    </div>

    <!-- REVENUE TAB -->
    <div id="revenue" class="tab-content">
      <div class="grid-2">
        <div class="card">
          <h2>Current State</h2>
          <div class="kv"><span class="k">Annual Revenue</span><span class="v">$100M ARR</span></div>
          <div class="kv"><span class="k">Total Patients</span><span class="v">{{ total_converted|int }} ({{ '%d'|format(conv_rate*100) }}% conversion)</span></div>
          <div class="kv"><span class="k">Revenue per Patient/yr</span><span class="v">${{ '%d'|format(rev_per_patient) }}</span></div>
          <div class="kv"><span class="k">Open Potential</span><span class="v">${{ '%d'|format(open_potential_m) }}M</span></div>
        </div>
        <div class="card">
          <h2>Recovery Scenarios</h2>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:.6rem;">
            <div class="stat-box"><div class="num">+{{ '%d'|format(pred_rev) }}M</div><div class="lbl">Predicted (+5%)<br>{{ pred_pat|int }} patients → ${{ '%d'|format(pred_arr) }}M ARR</div></div>
            <div class="stat-box green"><div class="num">+{{ '%d'|format(stretch_rev) }}M</div><div class="lbl">Very Positive (+15%)<br>{{ stretch_pat|int }} patients → ${{ '%d'|format(stretch_arr) }}M ARR</div></div>
          </div>
        </div>
      </div>
      <div class="grid-2">
        <div class="card">
          <h2>Revenue Levers</h2>
          <div class="lever-row"><span class="icon">1.</span> Re-engage lost leads (5%)<span class="amt">${{ '%.1f'|format(reengage_rev) }}M</span></div>
          <div class="lever-row"><span class="icon">2.</span> Cut response time &lt;1hr (+2%)<span class="amt">${{ '%.1f'|format(response_rev) }}M</span></div>
          <div class="lever-row"><span class="icon">3.</span> Staff nurturing (+3%)<span class="amt">${{ '%.1f'|format(nurture_rev) }}M</span></div>
          <div class="lever-row"><span class="icon">4.</span> Raise patient LTV (+10%)<span class="amt">${{ '%.1f'|format(ltv_rev) }}M</span></div>
          <div class="lever-row"><span class="icon">5.</span> Open new locations ($5M each)<span class="amt">scales</span></div>
          <div class="lever-row total-row"><span class="icon"></span><strong>Combined</strong><span class="amt">~${{ '%d'|format(combined) }}M</span></div>
          <div class="lever-row"><span class="icon"></span><strong>Predicted Total</strong><span class="amt" style="font-size:1rem;">${{ '%d'|format(pred_total) }}M</span></div>
          <div class="lever-row"><span class="icon"></span><strong>Very Positive Total</strong><span class="amt" style="font-size:1rem;">${{ '%d'|format(stretch_total) }}M</span></div>
        </div>
        <div class="card">
          <h2>10-Year Projection</h2>
          <p style="font-size:.8rem;color:var(--muted);margin-bottom:.4rem;">Levers compound year over year. Excluding new locations.</p>
          <table>
            <tr><th>Year</th><th>Predicted</th><th>Very Positive</th></tr>
            {% for y in years %}
            <tr><td style="font-weight:700;">{{ y.year }}</td><td>${{ y.predicted }}M</td><td style="color:var(--green-dark);font-weight:700;">${{ y.stretch }}M</td></tr>
            {% endfor %}
          </table>
          <div class="note">Predicted reaches \~$250M by year 10. Very positive exceeds $300M.</div>
        </div>
      </div>
    </div>
  </div>

<script>
var snapshotDate = '{{ snapshot_date }}';

function showTab(name) {
  document.querySelectorAll('.tab-content').forEach(function(el) { el.classList.remove('active'); });
  document.querySelectorAll('.tabs .tab').forEach(function(el) { el.classList.remove('active'); });
  document.getElementById(name).classList.add('active');
  var tabs = document.querySelectorAll('.tabs .tab');
  var tabMap = {'overview':0,'leads':1,'tasks':2,'revenue':3};
  if(tabMap[name] !== undefined && tabs[tabMap[name]]) tabs[tabMap[name]].classList.add('active');
}

function copyText(elementId) {
  var element = document.getElementById(elementId);
  var text = element.textContent;
  navigator.clipboard.writeText(text).then(function() {
    // Show success feedback
    var btn = event.target;
    var originalText = btn.textContent;
    btn.textContent = '✅ Copied!';
    btn.style.background = '#16a34a';
    setTimeout(function() {
      btn.textContent = originalText;
      btn.style.background = '';
    }, 2000);
  }).catch(function(err) {
    alert('Failed to copy: ' + err);
  });
}

function saveNote(leadId) {
  var text = document.getElementById('note-text-' + leadId).value;
  var msgDiv = document.getElementById('save-msg-' + leadId);
  
  if (!text.trim()) {
    msgDiv.className = 'save-msg err';
    msgDiv.textContent = 'Please enter a note first';
    return;
  }
  
  msgDiv.className = 'save-msg';
  msgDiv.textContent = 'Saving...';
  
  var formData = new FormData();
  formData.append('staff_note', text);
  formData.append('snapshot_date', snapshotDate);
  
  fetch('/add_note/' + leadId, { method: 'POST', body: formData })
    .then(function(r) { return r.json(); })
    .then(function(data) {
      if (data.success) {
        msgDiv.className = 'save-msg ok';
        msgDiv.textContent = '✅ Note saved!';
        document.getElementById('note-text-' + leadId).value = '';
        
        // Add note to the history section immediately
        var histDiv = document.getElementById('notes-history-' + leadId);
        if (!histDiv) {
          // Create history section if it doesn't exist
          var detailsContent = msgDiv.closest('.details-content');
          histDiv = document.createElement('div');
          histDiv.id = 'notes-history-' + leadId;
          histDiv.style.cssText = 'margin-bottom:1rem;padding:0.8rem;background:#fff;border-radius:8px;border:1px solid var(--border);';
          histDiv.innerHTML = '<strong style="font-size:0.9rem;color:var(--text);display:block;margin-bottom:0.6rem;">📚 Staff Activity History</strong>';
          detailsContent.insertBefore(histDiv, detailsContent.children[1]);
        }
        
        var noteDiv = document.createElement('div');
        noteDiv.style.cssText = 'padding:0.6rem;background:#f8fafc;border-left:3px solid var(--blue);border-radius:4px;margin-bottom:0.5rem;';
        noteDiv.innerHTML = '<div style="font-size:0.75rem;color:var(--muted);margin-bottom:0.3rem;">📝 Staff Note &mdash; ' + data.timestamp + '</div>' +
                            '<div style="font-size:0.9rem;color:var(--text);white-space:pre-wrap;">' + data.note + '</div>';
        histDiv.appendChild(noteDiv);
      } else {
        msgDiv.className = 'save-msg err';
        msgDiv.textContent = '❌ ' + (data.error || 'Failed to save');
      }
    })
    .catch(function(err) {
      msgDiv.className = 'save-msg err';
      msgDiv.textContent = '❌ Error: ' + err;
    });
}

function completeTask(taskId) {
  if (!confirm('Mark this task as completed?')) return;
  var msgDiv = document.getElementById('task-msg-' + taskId);
  msgDiv.className = 'save-msg'; msgDiv.textContent = 'Completing...';
  fetch('/complete_task/' + taskId, { method:'POST' })
    .then(function(r){return r.json();})
    .then(function(data){
      if(data.success){
        msgDiv.className='save-msg ok'; msgDiv.textContent='✅ Task completed!';
        var card=document.getElementById('task-'+taskId);
        card.style.opacity='.5'; card.style.pointerEvents='none';
      } else { msgDiv.className='save-msg err'; msgDiv.textContent='❌ '+(data.error||'Failed'); }
    })
    .catch(function(err){ msgDiv.className='save-msg err'; msgDiv.textContent='❌ '+err; });
}

function toggleEditTask(taskId){
  var el=document.getElementById('edit-task-'+taskId);
  el.style.display = el.style.display==='none' ? 'block' : 'none';
}

function updateTask(taskId){
  var rat=document.getElementById('task-rat-'+taskId).value;
  var draft=document.getElementById('task-draft-'+taskId).value;
  var msgDiv=document.getElementById('task-msg-'+taskId);
  msgDiv.className='save-msg'; msgDiv.textContent='Saving...';
  var fd=new FormData(); fd.append('rationale',rat); fd.append('draft',draft);
  fetch('/update_task/'+taskId,{method:'POST',body:fd})
    .then(function(r){return r.json();})
    .then(function(data){
      if(data.success){ msgDiv.className='save-msg ok'; msgDiv.textContent='✅ Saved!'; }
      else{ msgDiv.className='save-msg err'; msgDiv.textContent='❌ '+(data.error||'Failed'); }
    })
    .catch(function(err){ msgDiv.className='save-msg err'; msgDiv.textContent='❌ '+err; });
}

function completeCycle(cycleId){
  if(!confirm('Mark this cycle as completed?')) return;
  var msgDiv=document.getElementById('cycle-msg-'+cycleId);
  msgDiv.className='save-msg'; msgDiv.textContent='Completing...';
  fetch('/complete_cycle/'+cycleId,{method:'POST'})
    .then(function(r){return r.json();})
    .then(function(data){
      if(data.success){
        msgDiv.className='save-msg ok'; msgDiv.textContent='✅ Cycle completed!';
        var btns=msgDiv.parentElement.querySelectorAll('button,div[id^=edit-cycle]');
        btns.forEach(function(b){b.style.display='none';});
      } else { msgDiv.className='save-msg err'; msgDiv.textContent='❌ '+(data.error||'Failed'); }
    })
    .catch(function(err){ msgDiv.className='save-msg err'; msgDiv.textContent='❌ '+err; });
}

function toggleEditCycle(cycleId){
  var el=document.getElementById('edit-cycle-'+cycleId);
  el.style.display = el.style.display==='none' ? 'block' : 'none';
}

function updateCycle(cycleId){
  var draft=document.getElementById('cycle-draft-'+cycleId).value;
  var msgDiv=document.getElementById('cycle-msg-'+cycleId);
  msgDiv.className='save-msg'; msgDiv.textContent='Saving...';
  var fd=new FormData(); fd.append('draft',draft);
  fetch('/update_cycle/'+cycleId,{method:'POST',body:fd})
    .then(function(r){return r.json();})
    .then(function(data){
      if(data.success){ msgDiv.className='save-msg ok'; msgDiv.textContent='✅ Draft updated!'; }
      else{ msgDiv.className='save-msg err'; msgDiv.textContent='❌ '+(data.error||'Failed'); }
    })
    .catch(function(err){ msgDiv.className='save-msg err'; msgDiv.textContent='❌ '+err; });
}

function closeLead(leadId) {
  var reason = document.getElementById('close-reason-' + leadId).value;
  var msgDiv = document.getElementById('close-msg-' + leadId);
  
  if (!reason.trim()) {
    msgDiv.className = 'save-msg err';
    msgDiv.textContent = 'Please enter a reason for closing';
    return;
  }
  
  if (!confirm('Are you sure you want to close lead ' + leadId + '?')) return;
  
  msgDiv.className = 'save-msg';
  msgDiv.textContent = 'Closing lead...';
  
  var formData = new FormData();
  formData.append('close_reason', reason);
  formData.append('snapshot_date', snapshotDate);
  
  fetch('/close_lead/' + leadId, { method: 'POST', body: formData })
    .then(function(r) { return r.json(); })
    .then(function(data) {
      if (data.success) {
        msgDiv.className = 'save-msg ok';
        msgDiv.textContent = '✅ Lead closed successfully!';
        document.getElementById('close-reason-' + leadId).value = '';
      } else {
        msgDiv.className = 'save-msg err';
        msgDiv.textContent = '❌ ' + (data.error || 'Failed to close lead');
      }
    })
    .catch(function(err) {
      msgDiv.className = 'save-msg err';
      msgDiv.textContent = '❌ Error: ' + err;
    });
}
</script>
</body></html>
"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
