import json
import os
import datetime
from flask import Flask, request, render_template_string, jsonify
from databricks.sdk import WorkspaceClient

app = Flask(__name__)

try:
    w = WorkspaceClient()
except Exception as e:
    w = None
    print(f"SDK init failed: {e}")

SOURCE = "workspace.chiro_hackathon"
OUTPUT = "workspace.chiro_agent_demo"
WH_ID = "57129d05302f658f"
DEMO_SNAPSHOT = "2026-10-04"
DEMO_LOCATION = "LOC001"
DEMO_LOOKBACK = "120"
DEFAULT_FOLLOWUP_DAYS = 7  # Default follow-up interval when no date is selected

_FIRST = ['James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','William','Elizabeth',
           'David','Barbara','Richard','Susan','Joseph','Jessica','Thomas','Sarah','Charles','Karen',
           'Christopher','Nancy','Daniel','Lisa','Matthew','Margaret','Anthony','Sandra','Mark','Ashley',
           'Donald','Kimberly','Steven','Emily','Paul','Donna','Andrew','Michelle','Joshua','Carol',
           'Kenneth','Amanda','Kevin','Melissa','Brian','Deborah','George','Stephanie','Edward','Rebecca']
_LAST = ['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez',
         'Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin',
         'Lee','Perez','Thompson','White','Harris','Sanchez','Clark','Ramirez','Lewis','Robinson',
         'Walker','Young','Allen','King','Wright','Scott','Torres','Nguyen','Hill','Flores',
         'Green','Adams','Nelson','Baker','Hall','Rivera','Campbell','Mitchell','Carter','Roberts']

def _name_sql(col="lead_id"):
    fa = ",".join(f"'{n}'" for n in _FIRST)
    la = ",".join(f"'{n}'" for n in _LAST)
    return f"""array({fa})[abs(hash({col})) % 50] as first_name,
        array({la})[abs(hash({col})) % 50] as last_name,
        concat('(555) ', lpad(cast((abs(hash({col})) % 900) + 100 as string), 3, '0'), '-', lpad(cast((abs(hash(concat({col},'b'))) % 9000) + 1000 as string), 4, '0')) as phone,
        concat(lower(array({fa})[abs(hash({col})) % 50]), '.', lower(array({la})[abs(hash({col})) % 50]), '@email.com') as email"""

def run_sql(sql_text):
    if not w:
        raise Exception("Databricks SDK not initialized")
    resp = w.statement_execution.execute_statement(
        statement=sql_text, warehouse_id=WH_ID, wait_timeout="50s")
    if not resp.result or not resp.result.data_array:
        return []
    cols = [c.name for c in resp.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in resp.result.data_array]

def esc(s):
    return str(s).replace("'", "''") if s is not None else ""

def get_locations():
    try:
        return run_sql(f"SELECT location_id, location_name, city, state FROM {SOURCE}.locations ORDER BY location_id")
    except:
        return []

def get_metrics(location_id, snapshot_date, lookback, today=""):
    date_expr = f"'{today}'" if today else "current_date()"
    rows = run_sql(f"""
        SELECT
            COUNT(*) as total_at_clinic,
            SUM(CASE WHEN converted_flag = true THEN 1 ELSE 0 END) as converted,
            SUM(CASE WHEN lower(trim(status)) = 'lost' AND converted_flag = false THEN 1 ELSE 0 END) as lost,
            SUM(CASE WHEN lower(trim(status)) IN ('new','contacted','qualified') AND converted_flag = false THEN 1 ELSE 0 END) as open_leads,
            SUM(CASE WHEN lower(trim(status)) IN ('new','contacted','qualified')
                AND converted_flag = false AND num_touchpoints BETWEEN 0 AND 2
                AND datediff('{snapshot_date}', created_date) BETWEEN 0 AND {lookback}
                THEN 1 ELSE 0 END) as eligible,
            SUM(CASE WHEN lower(trim(status)) IN ('new','contacted','qualified')
                AND converted_flag = false AND num_touchpoints >= 3
                AND datediff('{snapshot_date}', created_date) BETWEEN 0 AND {lookback}
                THEN 1 ELSE 0 END) as contact_capped
        FROM {SOURCE}.leads WHERE assigned_location_id = '{location_id}'""")
    r = rows[0] if rows else {}
    try:
        tr = run_sql(f"""
            SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks
            WHERE location_id = '{location_id}' AND review_status = 'pending_review'
            AND (
                decision != 'FOLLOW_UP_SCHEDULED'
                OR coalesce(to_date(nullif(regexp_extract(rationale, 'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1), '')), {date_expr}) <= {date_expr}
            )""")
        followups = int(tr[0]["cnt"]) if tr else 0
    except:
        followups = 0
    try:
        ar = run_sql(f"SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks WHERE location_id = '{location_id}' AND decision = 'OUTREACH_LOGGED' AND review_status = 'completed'")
        actions_logged = int(ar[0]["cnt"]) if ar else 0
    except:
        actions_logged = 0
    return {
        "total_at_clinic": int(r.get("total_at_clinic", 0) or 0),
        "converted": int(r.get("converted", 0) or 0),
        "lost": int(r.get("lost", 0) or 0),
        "open_leads": int(r.get("open_leads", 0) or 0),
        "eligible": int(r.get("eligible", 0) or 0),
        "contact_capped": int(r.get("contact_capped", 0) or 0),
        "followups_due": followups,
        "actions_logged": actions_logged,
    }

def get_eligible_leads(location_id, snapshot_date, lookback, page=1, per_page=15, search="", today=""):
    offset = (page - 1) * per_page
    date_expr = f"'{today}'" if today else "current_date()"
    ns = _name_sql("lead_id")
    search_clause = ""
    if search:
        sl = esc(search.lower())
        search_clause = f" AND (lower(source) LIKE '%{sl}%' OR lower(lead_id) LIKE '%{sl}%')"
    total_row = run_sql(f"""
        SELECT COUNT(*) as cnt FROM {SOURCE}.leads
        WHERE assigned_location_id = '{location_id}'
          AND lower(trim(status)) IN ('new','contacted','qualified')
          AND converted_flag = false AND num_touchpoints BETWEEN 0 AND 2
          AND datediff('{snapshot_date}', created_date) BETWEEN 0 AND {lookback}""")
    total_eligible = int(total_row[0]["cnt"]) if total_row else 0
    count_row = run_sql(f"""
        SELECT COUNT(*) as cnt FROM {SOURCE}.leads
        WHERE assigned_location_id = '{location_id}'
          AND lower(trim(status)) IN ('new','contacted','qualified')
          AND converted_flag = false AND num_touchpoints BETWEEN 0 AND 2
          AND datediff('{snapshot_date}', created_date) BETWEEN 0 AND {lookback}
          {search_clause}""")
    total = int(count_row[0]["cnt"]) if count_row else 0
    leads = run_sql(f"""
        WITH fu_dates AS (
            SELECT lead_id as fu_lead_id, regexp_extract(rationale, 'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1) as follow_up_date
            FROM {OUTPUT}.followup_tasks
            WHERE location_id = '{location_id}' AND decision = 'FOLLOW_UP_SCHEDULED' AND review_status = 'pending_review'
        )
        SELECT lead_id, source, status, num_touchpoints, first_response_hours, created_date, {ns},
            CASE WHEN first_response_hours IS NULL THEN 'No response recorded'
                WHEN first_response_hours > 48 THEN CONCAT('Slow first response: ', cast(round(first_response_hours) as string), ' hrs')
                ELSE CONCAT('First response in ', cast(round(first_response_hours) as string), ' hrs') END as priority_reason,
            CASE WHEN num_touchpoints = 0 THEN 'No contact attempts'
                WHEN num_touchpoints = 1 THEN '1 prior attempt'
                ELSE CONCAT(cast(num_touchpoints as string), ' prior attempts') END as touch_info
        FROM {SOURCE}.leads
        LEFT JOIN fu_dates ON lead_id = fu_lead_id
        WHERE assigned_location_id = '{location_id}'
          AND lower(trim(status)) IN ('new','contacted','qualified')
          AND converted_flag = false AND num_touchpoints BETWEEN 0 AND 2
          AND datediff('{snapshot_date}', created_date) BETWEEN 0 AND {lookback}
          {search_clause}
        ORDER BY CASE WHEN to_date(nullif(follow_up_date, '')) > {date_expr} THEN 4
            WHEN first_response_hours IS NULL OR first_response_hours > 48 THEN 1
            WHEN first_response_hours > 24 THEN 2
            ELSE 3 END ASC,
            num_touchpoints ASC, created_date DESC
        LIMIT {per_page} OFFSET {offset}""")
    try:
        outreach_rows = run_sql(f"""
            SELECT lead_id, COUNT(*) as cnt FROM {OUTPUT}.followup_tasks
            WHERE decision = 'OUTREACH_LOGGED' AND location_id = '{location_id}'
            GROUP BY lead_id""")
        outreach_map = {r["lead_id"]: int(r["cnt"]) for r in outreach_rows}
    except:
        outreach_map = {}
    # Get recent outreach with follow-up dates for priority calculation
    try:
        recent_outreach = run_sql(f"""
            WITH latest_contact AS (
                SELECT lead_id, MAX(reviewed_at) as last_contact_at
                FROM {OUTPUT}.followup_tasks
                WHERE location_id = '{location_id}' AND decision = 'OUTREACH_LOGGED' AND review_status = 'completed'
                GROUP BY lead_id
            ),
            latest_followup AS (
                SELECT lead_id, 
                    regexp_extract(rationale, 'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1) as follow_up_date
                FROM {OUTPUT}.followup_tasks
                WHERE location_id = '{location_id}' AND decision = 'FOLLOW_UP_SCHEDULED' AND review_status = 'pending_review'
            )
            SELECT c.lead_id, c.last_contact_at, f.follow_up_date
            FROM latest_contact c
            LEFT JOIN latest_followup f ON c.lead_id = f.lead_id""")
        contact_map = {r["lead_id"]: {"last_contact": r["last_contact_at"], "follow_up_date": r.get("follow_up_date")} for r in recent_outreach}
    except:
        contact_map = {}
    
    from datetime import datetime, date
    if today:
        today = datetime.strptime(today, "%Y-%m-%d").date()
    else:
        today = date.today()
    
    for i, lead in enumerate(leads):
        lead["rank"] = offset + i + 1
        eff_touches = int(lead.get("num_touchpoints", 0) or 0) + outreach_map.get(lead["lead_id"], 0)
        lead["effective_touchpoints"] = eff_touches
        if eff_touches == 0:
            lead["touch_info"] = "No contact attempts"
        elif eff_touches == 1:
            lead["touch_info"] = "1 prior attempt"
        else:
            lead["touch_info"] = f"{eff_touches} prior attempts"
        frh = lead.get("first_response_hours")
        # Convert to float for comparison (SQL results may be strings)
        if frh is not None:
            try:
                frh = float(frh)
            except (ValueError, TypeError):
                frh = None
        
        # Check for recent outreach to adjust priority
        contact_info = contact_map.get(lead["lead_id"])
        has_recent_contact = False
        follow_up_due = False
        
        if contact_info and contact_info.get("last_contact"):
            follow_up_date_str = contact_info.get("follow_up_date")
            if follow_up_date_str:
                try:
                    follow_up_date = datetime.strptime(follow_up_date_str, "%Y-%m-%d").date()
                    if follow_up_date > today:
                        # Follow-up is in the future - recently contacted, low priority
                        has_recent_contact = True
                        lead["priority_label"] = f"Low — Recently contacted"
                        lead["priority_color"] = "success"
                        lead["priority_reason"] = f"Awaiting follow-up on {follow_up_date_str}"
                    else:
                        # Follow-up date has passed - recalculate priority
                        follow_up_due = True
                except (ValueError, TypeError):
                    pass
        
        # If no recent contact or follow-up is due, use standard priority rules
        if not has_recent_contact:
            if frh is None:
                lead["priority_label"] = "High — No response"
                lead["priority_color"] = "error"
            elif frh > 48:
                lead["priority_label"] = "High — Slow response"
                lead["priority_color"] = "error"
            elif frh > 24:
                lead["priority_label"] = "Medium — Delayed"
                lead["priority_color"] = "warning"
            else:
                lead["priority_label"] = "Standard"
                lead["priority_color"] = "success"
        # Data quality: detect inconsistent status / touchpoint / first_response
        status_lower = (lead.get("status") or "").strip().lower()
        if status_lower == "contacted" and eff_touches == 0 and frh is not None:
            lead["data_quality_warning"] = (
                f"Status shows 'Contacted' but no touchpoints recorded "
                f"despite a {frh:.1f}-hour first-response time. "
                "Contact may not have been properly logged."
            )
        elif status_lower == "contacted" and eff_touches == 0 and frh is None:
            lead["data_quality_warning"] = (
                "Status shows 'Contacted' but no contact record "
                "or response time exists. Verify actual contact history."
            )
        elif status_lower == "new" and eff_touches > 0:
            lead["data_quality_warning"] = (
                f"Status shows 'New' but {eff_touches} touchpoint(s) "
                "are recorded. Status may be stale."
            )
        elif status_lower == "qualified" and eff_touches == 0 and frh is not None:
            lead["data_quality_warning"] = (
                f"Status shows 'Qualified' but no touchpoints recorded "
                f"despite a {frh:.1f}-hour first-response time. "
                "Qualification may not have been properly logged."
            )
        elif status_lower == "qualified" and eff_touches == 0 and frh is None:
            lead["data_quality_warning"] = (
                "Status shows 'Qualified' but no contact record "
                "or response time exists. Verify actual qualification history."
            )
        else:
            lead["data_quality_warning"] = None
    
    # Re-sort leads to put recently contacted (Low priority) at the bottom
    def priority_sort_key(lead):
        priority_color = lead.get("priority_color", "success")
        # error (High) = 0, warning (Medium) = 1, success (Low/Standard) = 2
        if priority_color == "error":
            return (0, lead.get("effective_touchpoints", 0))
        elif priority_color == "warning":
            return (1, lead.get("effective_touchpoints", 0))
        else:
            # For success (Low or Standard), check if recently contacted
            if "Recently contacted" in lead.get("priority_label", ""):
                return (3, lead.get("effective_touchpoints", 0))  # Recently contacted goes last
            else:
                return (2, lead.get("effective_touchpoints", 0))  # Standard priority
    
    leads.sort(key=priority_sort_key)
    # Update ranks after sorting
    for i, lead in enumerate(leads):
        lead["rank"] = offset + i + 1
    
    return {"leads": leads, "total": total, "total_eligible": total_eligible, "page": page, "per_page": per_page, "pages": max(1, (total + per_page - 1) // per_page)}

def get_lead_detail(lead_id, location_id, snapshot_date):
    ns = _name_sql("lead_id")
    rows = run_sql(f"""
        SELECT lead_id, source, status, num_touchpoints, first_response_hours, created_date,
            converted_flag, assigned_location_id, {ns}
        FROM {SOURCE}.leads WHERE lead_id = '{lead_id}'""")
    if not rows:
        return None
    lead = rows[0]
    try:
        oc = run_sql(f"SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks WHERE lead_id = '{lead_id}' AND decision = 'OUTREACH_LOGGED'")
        lead["effective_touchpoints"] = int(lead.get("num_touchpoints", 0) or 0) + (int(oc[0]["cnt"]) if oc else 0)
    except:
        lead["effective_touchpoints"] = int(lead.get("num_touchpoints", 0) or 0)
    try:
        actions = run_sql(f"""
            SELECT task_id, decision, rationale, draft_text, review_status, reviewed_at, outcome, outcome_at, is_simulated
            FROM {OUTPUT}.followup_tasks WHERE lead_id = '{lead_id}' ORDER BY reviewed_at DESC""")
    except:
        actions = []
    lead["actions"] = actions
    try:
        cycles = run_sql(f"""
            SELECT cycle_id, cycle_number, channel, timing_days, cycle_status, draft_text
            FROM {OUTPUT}.nurturing_cycles WHERE lead_id = '{lead_id}' ORDER BY cycle_number""")
    except:
        cycles = []
    lead["nurture_cycles"] = cycles
    # Compute priority (same logic as get_eligible_leads for consistency)
    frh = lead.get("first_response_hours")
    if frh is not None:
        try:
            frh = float(frh)
        except (ValueError, TypeError):
            frh = None
    
    # Check for recent outreach to adjust priority
    from datetime import datetime, date
    today = date.today()
    has_recent_contact = False
    
    try:
        recent_contact = run_sql(f"""
            SELECT MAX(reviewed_at) as last_contact_at
            FROM {OUTPUT}.followup_tasks
            WHERE lead_id = '{lead_id}' AND decision = 'OUTREACH_LOGGED' AND review_status = 'completed'
        """)
        if recent_contact and recent_contact[0].get("last_contact_at"):
            # Check for pending follow-up
            followup = run_sql(f"""
                SELECT regexp_extract(rationale, 'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1) as follow_up_date
                FROM {OUTPUT}.followup_tasks
                WHERE lead_id = '{lead_id}' AND decision = 'FOLLOW_UP_SCHEDULED' AND review_status = 'pending_review'
                ORDER BY reviewed_at DESC LIMIT 1
            """)
            if followup and followup[0].get("follow_up_date"):
                follow_up_date_str = followup[0]["follow_up_date"]
                try:
                    follow_up_date = datetime.strptime(follow_up_date_str, "%Y-%m-%d").date()
                    if follow_up_date > today:
                        # Follow-up is in the future - recently contacted, low priority
                        has_recent_contact = True
                        lead["priority_label"] = f"Low — Recently contacted"
                        lead["priority_color"] = "success"
                        lead["priority_reason"] = f"Awaiting follow-up on {follow_up_date_str}"
                except (ValueError, TypeError):
                    pass
    except:
        pass
    
    # If no recent contact or follow-up is due, use standard priority rules
    if not has_recent_contact:
        if frh is None:
            lead["priority_label"] = "High — No response"
            lead["priority_color"] = "error"
        elif frh > 48:
            lead["priority_label"] = "High — Slow response"
            lead["priority_color"] = "error"
        elif frh > 24:
            lead["priority_label"] = "Medium — Delayed"
            lead["priority_color"] = "warning"
        else:
            lead["priority_label"] = "Standard"
            lead["priority_color"] = "success"
    # Data quality warning (same logic as get_eligible_leads)
    eff_touches = lead.get("effective_touchpoints", 0)
    status_lower = (lead.get("status") or "").strip().lower()
    if status_lower == "contacted" and eff_touches == 0 and frh is not None:
        lead["data_quality_warning"] = (
            f"Status shows 'Contacted' but no touchpoints recorded "
            f"despite a {frh:.1f}-hour first-response time. "
            "Contact may not have been properly logged."
        )
    elif status_lower == "contacted" and eff_touches == 0 and frh is None:
        lead["data_quality_warning"] = (
            "Status shows 'Contacted' but no contact record "
            "or response time exists. Verify actual contact history."
        )
    elif status_lower == "new" and eff_touches > 0:
        lead["data_quality_warning"] = (
            f"Status shows 'New' but {eff_touches} touchpoint(s) "
            "are recorded. Status may be stale."
        )
    elif status_lower == "qualified" and eff_touches == 0 and frh is not None:
        lead["data_quality_warning"] = (
            f"Status shows 'Qualified' but no touchpoints recorded "
            f"despite a {frh:.1f}-hour first-response time. "
            "Qualification may not have been properly logged."
        )
    elif status_lower == "qualified" and eff_touches == 0 and frh is None:
        lead["data_quality_warning"] = (
            "Status shows 'Qualified' but no contact record "
            "or response time exists. Verify actual qualification history."
        )
    else:
        lead["data_quality_warning"] = None
    lead["snapshot_date"] = snapshot_date
    return lead

def log_outcome(lead_id, location_id, snapshot_date, outcome, note, follow_up_date):
    now = datetime.datetime.now().isoformat()
    task_id = f"{lead_id}_act_{now.replace(':','').replace('-','').replace('.','')}"
    rationale = f"Call outcome: {outcome}" + (f" — {note}" if note else "")
    run_sql(f"""
        INSERT INTO {OUTPUT}.followup_tasks (task_id, run_id, snapshot_date, lead_id, location_id, decision, evidence_json, rationale, draft_text, review_status, reviewed_at, outcome, outcome_at, is_simulated)
        VALUES ('{task_id}', 'manual_entry', '{snapshot_date}', '{lead_id}', '{location_id}', 'OUTREACH_LOGGED', '{{}}', '{esc(rationale)}', '{esc(note)}', 'completed', '{now}', '{esc(outcome)}', '{now}', true)""")
    if outcome == "Appointment booked":
        run_sql(f"UPDATE {SOURCE}.leads SET status = 'Qualified' WHERE lead_id = '{lead_id}'")
        # Resolve/supersede ALL pending follow-ups for this lead (not just FOLLOW_UP_SCHEDULED)
        run_sql(f"""
            UPDATE {OUTPUT}.followup_tasks
            SET review_status = 'superseded'
            WHERE lead_id = '{lead_id}' AND review_status = 'pending_review'""")
    elif outcome == "No longer interested":
        run_sql(f"UPDATE {SOURCE}.leads SET status = 'Lost' WHERE lead_id = '{lead_id}'")
        # Also resolve pending follow-ups when lead is lost
        run_sql(f"""
            UPDATE {OUTPUT}.followup_tasks
            SET review_status = 'superseded'
            WHERE lead_id = '{lead_id}' AND review_status = 'pending_review'""")
    elif outcome == "Connected":
        # Increment touchpoints and ensure lead exceeds the 2-touchpoint queue cap
        run_sql(f"UPDATE {SOURCE}.leads SET num_touchpoints = GREATEST(num_touchpoints + 1, 3) WHERE lead_id = '{lead_id}'")
    # Auto-schedule follow-up unless lead is closed (booked or lost) or was previously booked
    already_booked = False
    if outcome not in ("Appointment booked", "No longer interested"):
        booking_check = run_sql(f"SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks WHERE lead_id = '{lead_id}' AND outcome = 'Appointment booked' AND review_status = 'completed'")
        already_booked = int(booking_check[0]["cnt"]) > 0 if booking_check else False
    if outcome not in ("Appointment booked", "No longer interested") and not already_booked:
        # Use provided date or calculate default follow-up date
        if not follow_up_date:
            from datetime import timedelta
            follow_up_dt = datetime.datetime.now() + timedelta(days=DEFAULT_FOLLOWUP_DAYS)
            follow_up_date = follow_up_dt.strftime("%Y-%m-%d")
        
        # Supersede any remaining pending tasks for this lead before creating new follow-up
        run_sql(f"""
            UPDATE {OUTPUT}.followup_tasks
            SET review_status = 'superseded'
            WHERE lead_id = '{lead_id}' AND review_status = 'pending_review'""")
        fu_id = f"{lead_id}_fu_{now.replace(':','').replace('-','').replace('.','')}"
        run_sql(f"""
            INSERT INTO {OUTPUT}.followup_tasks (task_id, run_id, snapshot_date, lead_id, location_id, decision, evidence_json, rationale, draft_text, review_status, is_simulated)
            VALUES ('{fu_id}', 'manual_entry', '{snapshot_date}', '{lead_id}', '{location_id}', 'FOLLOW_UP_SCHEDULED', '{{}}', 'Follow-up scheduled for {esc(follow_up_date)} after {esc(outcome)}', '', 'pending_review', true)""")
        followup_info = {"lead_id": lead_id, "decision": "FOLLOW_UP_SCHEDULED", "rationale": f"Follow-up scheduled for {follow_up_date} after {outcome}", "due_date": follow_up_date, "due_label": "Scheduled"}
    else:
        followup_info = None
    return {"task_id": task_id, "followup": followup_info}

def save_note(lead_id, location_id, snapshot_date, note):
    now = datetime.datetime.now().isoformat()
    task_id = f"{lead_id}_note_{now.replace(':','').replace('-','').replace('.','')}"
    run_sql(f"""
        INSERT INTO {OUTPUT}.followup_tasks (task_id, run_id, snapshot_date, lead_id, location_id, decision, evidence_json, rationale, draft_text, review_status, reviewed_at, is_simulated)
        VALUES ('{task_id}', 'manual_entry', '{snapshot_date}', '{lead_id}', '{location_id}', 'STAFF_NOTE', '{{}}', 'Staff note: {esc(note)}', '{esc(note)}', 'completed', '{now}', true)""")
    return task_id

def close_lead(lead_id, location_id, snapshot_date, reason):
    now = datetime.datetime.now().isoformat()
    task_id = f"{lead_id}_close_{now.replace(':','').replace('-','').replace('.','')}"
    run_sql(f"UPDATE {SOURCE}.leads SET status = 'Lost' WHERE lead_id = '{lead_id}'")
    run_sql(f"""
        INSERT INTO {OUTPUT}.followup_tasks (task_id, run_id, snapshot_date, lead_id, location_id, decision, evidence_json, rationale, draft_text, review_status, reviewed_at, is_simulated)
        VALUES ('{task_id}', 'manual_entry', '{snapshot_date}', '{lead_id}', '{location_id}', 'LEAD_CLOSED', '{{}}', 'Lead closed: {esc(reason)}', 'Closed: {esc(reason)}', 'completed', '{now}', true)""")
    return task_id

def generate_draft(lead_id, location_id):
    ns = _name_sql("lead_id")
    lead_rows = run_sql(f"SELECT lead_id, source, status, num_touchpoints, first_response_hours, created_date, converted_flag, {ns} FROM {SOURCE}.leads WHERE lead_id = '{lead_id}'")
    if not lead_rows:
        return {"error": "Lead not found"}
    lead = lead_rows[0]
    if lead.get("converted_flag"):
        return {"recommendation": "This lead is already converted. No re-engagement draft needed.", "lead_id": lead_id, "is_fallback": True, "action": "No action", "rationale": "Lead is converted — no outreach needed.", "message": "", "headline": "No Draft Needed", "badge": "None"}
    try:
        agent_rec = agent_recommend(lead_id, location_id)
        if agent_rec.get("action") == "NO_ACTION":
            return {"recommendation": f"Agent recommendation: {agent_rec.get('rationale', 'No action needed')}", "lead_id": lead_id, "is_fallback": True, "action": "No action", "rationale": agent_rec.get('rationale', 'No action needed'), "message": "", "headline": "No Draft Needed", "badge": "None"}
    except Exception:
        pass
    clinic_rows = run_sql(f"SELECT location_name, city, state FROM {SOURCE}.locations WHERE location_id = '{location_id}'")
    clinic_name = clinic_rows[0]["location_name"] if clinic_rows else "the clinic"
    fname = lead.get("first_name", "there")
    source = lead.get("source", "unknown")
    status = lead.get("status", "unknown")
    touches = lead.get("num_touchpoints", 0)
    frh = lead.get("first_response_hours")
    if frh is not None:
        try:
            frh = float(frh)
        except (ValueError, TypeError):
            frh = None
    frh_text = f"{round(frh)} hours" if frh is not None else "no response recorded"
    prompt = f"""You are a lead recovery assistant for a chiropractic clinic. Generate a brief follow-up message.

VERIFIED FACTS (use only these):
- Lead name: {fname}
- Source: {source}
- Status: {status}
- Previous contact attempts: {touches}
- First response time: {frh_text}
- Clinic name: {clinic_name}
- Converted: No (lead is not yet converted, needs re-engagement)

RULES:
- Do NOT include any phone number (clinic phone not configured)
- Do NOT offer free consultations, discounts, or promotions
- Do NOT make medical claims
- Do NOT use placeholders like [Your Name]
- Keep under 100 words

Generate:
1. RECOMMENDED ACTION: [Call or Email]
2. RATIONALE: [1-2 sentences]
3. DRAFT MESSAGE: [Professional message to {fname}]"""
    try:
        # Properly escape the prompt for SQL (double up single quotes)
        escaped_prompt = prompt.replace("'", "''")
        result = run_sql(f"SELECT ai_gen('{escaped_prompt}') as recommendation")
        rec = result[0]["recommendation"] if result else "Unable to generate"
    except Exception as gen_err:
        print(f"AI generation error: {gen_err}")
        rec = f"Template fallback: Hi {fname}, thank you for your interest in {clinic_name}. We would like to follow up about your inquiry from {source}. Please contact our office to schedule a visit at your convenience."
    # Parse the AI response into structured fields for the UI
    import re as _re
    action = ""
    rationale = ""
    message = ""
    if rec and not rec.startswith("Template fallback"):
        lines = rec.strip().split('\n')
        current_section = None
        for line in lines:
            stripped = line.strip()
            if _re.match(r'^\d+\.?\s*RECOMMENDED\s*ACTION', stripped, _re.IGNORECASE):
                current_section = 'action'
                parts = stripped.split(':', 1)
                if len(parts) > 1:
                    action = parts[1].strip()
            elif _re.match(r'^\d+\.?\s*RATIONALE', stripped, _re.IGNORECASE):
                current_section = 'rationale'
                parts = stripped.split(':', 1)
                if len(parts) > 1:
                    rationale = parts[1].strip()
            elif _re.match(r'^\d+\.?\s*DRAFT\s*MESSAGE', stripped, _re.IGNORECASE):
                current_section = 'message'
                parts = stripped.split(':', 1)
                if len(parts) > 1:
                    message = parts[1].strip()
            else:
                if current_section == 'action' and stripped:
                    action = (action + ' ' + stripped).strip() if action else stripped
                elif current_section == 'rationale' and stripped:
                    rationale = (rationale + ' ' + stripped).strip() if rationale else stripped
                elif current_section == 'message' and stripped:
                    message = (message + '\n' + stripped).strip() if message else stripped
        action = action.strip()
        rationale = rationale.strip()
        message = message.strip()
    if not action and not rationale and not message:
        message = rec
    badge = ""
    if 'call' in action.lower():
        badge = 'Call'
    elif 'email' in action.lower():
        badge = 'Email'
    elif action:
        badge = action[:20]
    return {"recommendation": rec, "lead_id": lead_id, "is_fallback": rec.startswith("Template fallback"), "action": action, "rationale": rationale, "message": message, "headline": "Draft Recommendation", "badge": badge}

# ---------------------------------------------------------------------------
# AGENT: Next-Action Recommendation Engine
# Each tool function queries the database and returns structured results.
# The agent calls tools in sequence, records evidence, and makes a decision.
# ---------------------------------------------------------------------------

def _tool_get_lead_facts(lead_id, location_id):
    """Tool: Retrieve verified lead facts from the leads table."""
    ns = _name_sql("lead_id")
    rows = run_sql(f"""
        SELECT lead_id, source, status, num_touchpoints, first_response_hours,
               created_date, converted_flag, converted_patient_id, assigned_location_id, {ns}
        FROM {SOURCE}.leads WHERE lead_id = '{lead_id}'""")
    if not rows:
        return {"found": False}
    r = rows[0]
    frh = r.get("first_response_hours")
    if frh is not None:
        try:
            frh = float(frh)
        except (ValueError, TypeError):
            frh = None
    return {
        "found": True, "lead_id": r["lead_id"], "source": r.get("source"),
        "status": r.get("status"), "num_touchpoints": int(r.get("num_touchpoints", 0) or 0),
        "first_response_hours": frh, "created_date": str(r.get("created_date", "")),
        "converted_flag": bool(r.get("converted_flag")),
        "converted_patient_id": r.get("converted_patient_id"),
        "first_name": r.get("first_name"), "last_name": r.get("last_name"),
    }

def _tool_get_contact_history(lead_id):
    """Tool: Retrieve logged contact history from followup_tasks."""
    try:
        rows = run_sql(f"""
            SELECT decision, outcome, rationale, reviewed_at, review_status
            FROM {OUTPUT}.followup_tasks
            WHERE lead_id = '{lead_id}' AND decision = 'OUTREACH_LOGGED'
            ORDER BY reviewed_at DESC""")
        return {"history": rows, "count": len(rows)}
    except Exception:
        return {"history": [], "count": 0}

def _tool_check_existing_tasks(lead_id):
    """Tool: Check for pending follow-up tasks."""
    try:
        rows = run_sql(f"""
            SELECT task_id, decision, rationale, review_status
            FROM {OUTPUT}.followup_tasks
            WHERE lead_id = '{lead_id}' AND review_status = 'pending_review'
            ORDER BY reviewed_at DESC""")
        return {"pending_tasks": rows, "count": len(rows)}
    except Exception:
        return {"pending_tasks": [], "count": 0}

def _tool_check_booking_status(lead_id, facts):
    """Tool: Check if lead is already booked/converted."""
    if facts.get("converted_flag"):
        pid = facts.get("converted_patient_id")
        appts = []
        if pid:
            try:
                appts = run_sql(f"""
                    SELECT appointment_date, status, appointment_type
                    FROM {SOURCE}.appointments WHERE patient_id = '{pid}'
                    ORDER BY appointment_date DESC LIMIT 3""")
            except Exception:
                pass
        return {"booked": True, "patient_id": pid, "appointments": appts}
    return {"booked": False}

def _tool_check_contact_eligibility(lead_id, facts):
    """Tool: Check contact eligibility — phone, email, and consent status."""
    # Phone/email are synthetic (hash-derived). No consent table exists.
    # Flag as missing consent when no prior contact record exists.
    has_phone = True   # synthetic, always present
    has_email = True   # synthetic, always present
    consent_verified = facts.get("first_response_hours") is not None
    return {
        "has_phone": has_phone, "has_email": has_email,
        "consent_verified": consent_verified,
        "note": "Contact info is synthetic (demo). Consent inferred from first_response_hours."
    }

def _tool_check_clinic_capacity(location_id):
    """Tool: Check clinic appointment capacity from appointments table."""
    try:
        rows = run_sql(f"""
            SELECT
                COUNT(*) as total_appts,
                SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'Scheduled' THEN 1 ELSE 0 END) as scheduled
            FROM {SOURCE}.appointments
            WHERE location_id = '{location_id}'
              AND appointment_date >= date_sub(current_date(), 30)""")
        if not rows:
            return {"available": True, "note": "No recent appointment data"}
        r = rows[0]
        total = int(r.get("total_appts", 0) or 0)
        scheduled = int(r.get("scheduled", 0) or 0)
        avail = scheduled < 50
        return {"available": avail, "recent_appts": total, "scheduled": scheduled,
                "note": f"{scheduled} scheduled in last 30 days" + (" — capacity available" if avail else " — high volume")}
    except Exception:
        return {"available": True, "note": "Capacity data unavailable"}

def agent_recommend(lead_id, location_id):
    """Main agent: calls tools, evaluates facts, makes recommendation."""
    tool_calls = []

    # Tool 1: Get lead facts
    facts = _tool_get_lead_facts(lead_id, location_id)
    tool_calls.append({"tool": "get_lead_facts", "description": "Retrieve lead facts from database", "result": facts})
    if not facts.get("found"):
        return {"action": "NO_ACTION", "rationale": "Lead not found in database.",
                "tool_calls": tool_calls, "is_ai": False, "is_fallback": False, "lead_id": lead_id}

    # Tool 2: Check booking status
    booking = _tool_check_booking_status(lead_id, facts)
    tool_calls.append({"tool": "check_booking_status", "description": "Check if lead already booked/converted", "result": booking})

    # Tool 3: Get contact history
    history = _tool_get_contact_history(lead_id)
    tool_calls.append({"tool": "get_contact_history", "description": "Retrieve logged contact history",
                       "result": {"count": history["count"], "recent": history["history"][:3]}})

    # Tool 4: Check existing tasks
    tasks = _tool_check_existing_tasks(lead_id)
    tool_calls.append({"tool": "check_existing_tasks", "description": "Check pending follow-up tasks",
                       "result": {"count": tasks["count"], "tasks": tasks["pending_tasks"][:3]}})

    # Tool 5: Check contact eligibility
    eligibility = _tool_check_contact_eligibility(lead_id, facts)
    tool_calls.append({"tool": "check_contact_eligibility", "description": "Check phone/email and consent", "result": eligibility})

    # Tool 6: Check clinic capacity
    capacity = _tool_check_clinic_capacity(location_id)
    tool_calls.append({"tool": "check_clinic_capacity", "description": "Check appointment availability", "result": capacity})

    # Compute effective touchpoints
    eff_touches = facts["num_touchpoints"] + history["count"]

    # ---- Decision engine (deterministic, grounded in tool results) ----
    action = None
    rationale = ""
    suggested_timing = ""
    evidence = []
    missing_info = []

    # Rule 1: Already converted/booked → suppress
    if facts["converted_flag"] or booking.get("booked"):
        action = "NO_ACTION"
        n_appts = len(booking.get("appointments", []))
        rationale = (f"Lead {lead_id} is already converted (status: {facts['status']})"
                     + (f" with {n_appts} appointment(s) on record." if n_appts else ".")
                     + " Recovery outreach is not needed.")
        evidence.append(f"converted_flag={facts['converted_flag']}")
        evidence.append(f"booking_status=booked")

    # Rule 2: Contact cap reached (3+ effective touchpoints)
    elif eff_touches >= 3:
        action = "NO_ACTION"
        rationale = (f"Contact cap reached: {eff_touches} effective touchpoints "
                     f"(static={facts['num_touchpoints']}, logged={history['count']}). "
                     "No further outreach recommended.")
        evidence.append(f"effective_touchpoints={eff_touches} (cap=3)")

    # Rule 3: Status/touchpoint inconsistency → staff review (no invented contact)
    elif facts["status"] and "contacted" in facts["status"].lower() and eff_touches == 0:
        action = "STAFF_REVIEW"
        if facts["first_response_hours"] is None:
            rationale = (f"Lead status shows \"Contacted\" but no contact record exists "
                         "and no first-response time is recorded. Staff review required "
                         "before outreach — do not assume consent.")
            missing_info.append("No first-response time recorded despite 'Contacted' status")
            missing_info.append("Consent to contact not verified")
            evidence.append(f"status={facts['status']} but touchpoints=0 and first_response_hours=None")
        else:
            rationale = (f"Lead status shows \"Contacted\" with a first-response time of "
                         f"{facts['first_response_hours']:.1f}h but no touchpoints are logged. "
                         "The contact event may not have been properly recorded. "
                         "Staff review required — do not assume the contact occurred.")
            missing_info.append("No touchpoint logged despite 'Contacted' status and first-response time")
            evidence.append(f"status={facts['status']} but touchpoints=0 and first_response_hours={facts['first_response_hours']:.1f}")

    # Rule 4: Pending follow-up → wait
    elif tasks["count"] > 0:
        pending = tasks["pending_tasks"][0] if tasks["pending_tasks"] else {}
        if pending.get("decision") == "FOLLOW_UP_SCHEDULED":
            action = "NO_ACTION"
            rationale = (f"Follow-up already scheduled ({pending.get('rationale', 'date not specified')}). "
                         "Wait for the scheduled date before taking new action.")
            evidence.append(f"pending_follow_up={pending.get('rationale')}")
        else:
            action = "STAFF_REVIEW"
            rationale = f"Pending task exists ({pending.get('decision', 'unknown')}). Review before taking new action."
            evidence.append(f"pending_task={pending.get('decision')}")

    # Rule 5: No contact attempted (New lead, 0 touchpoints, no response) → call now
    elif facts["first_response_hours"] is None and eff_touches == 0:
        action = "CALL"
        rationale = (f"No contact attempted for lead {lead_id} (source: {facts['source']}, status: {facts['status']}). "
                     "High priority — recommend immediate call.")
        suggested_timing = "Today, within business hours"
        evidence.append("first_response_hours=None")
        evidence.append("effective_touchpoints=0")

    # Rule 6: Slow initial response (>48h) + low touchpoints → call
    elif facts["first_response_hours"] is not None and facts["first_response_hours"] > 48 and eff_touches <= 1:
        action = "CALL"
        rationale = (f"Slow initial response ({facts['first_response_hours']:.0f}h) with only {eff_touches} touchpoint(s). "
                     "Recovery opportunity — recommend follow-up call.")
        suggested_timing = "Today, within business hours"
        evidence.append(f"first_response_hours={facts['first_response_hours']:.1f} (>48h threshold)")
        evidence.append(f"effective_touchpoints={eff_touches}")

    # Rule 7: Multiple unanswered attempts → switch to written outreach
    elif eff_touches >= 1 and history["count"] > 0:
        all_no_answer = all(h.get("outcome") in ("No answer", None, "") for h in history["history"])
        if all_no_answer:
            action = "DRAFT_OUTREACH"
            rationale = (f"{eff_touches} unanswered attempt(s). Recommend switching to written outreach "
                         "(email draft) instead of another call.")
            suggested_timing = "Send within 24 hours"
            evidence.append(f"unanswered_attempts={eff_touches}")
        else:
            last_outcome = history["history"][0].get("outcome", "unknown") if history["history"] else "unknown"
            action = "FOLLOW_UP"
            rationale = (f"Previous contact made (outcome: {last_outcome}) but lead not yet booked. "
                         "Recommend scheduling a follow-up.")
            suggested_timing = "Within 2 business days"
            evidence.append(f"prior_contact_outcome={last_outcome}")

    # Rule 8: Default → standard call
    else:
        action = "CALL"
        rationale = (f"Lead {lead_id} eligible for recovery outreach (status: {facts['status']}, "
                     f"{eff_touches} touchpoint(s)). Standard follow-up call recommended.")
        suggested_timing = "Within 1 business day"
        evidence.append(f"status={facts['status']}")
        evidence.append(f"effective_touchpoints={eff_touches}")

    # Add capacity info to evidence
    evidence.append(f"clinic_capacity={'available' if capacity.get('available') else 'limited'}")

    # ---- AI-enhanced rationale (optional, clearly labeled) ----
    is_ai = False
    frh_str = f"{facts['first_response_hours']:.1f}h" if facts["first_response_hours"] is not None else "No response recorded"
    try:
        ai_prompt = f"""You are a lead recovery agent for a chiropractic clinic. Based on these verified facts, write a concise 1-2 sentence rationale for the recommended action. Do not add preamble.

LEAD: {facts.get('first_name','')} {facts.get('last_name','')} (ID: {lead_id})
STATUS: {facts['status']} | SOURCE: {facts['source']}
TOUCHPOINTS: {eff_touches} effective (static={facts['num_touchpoints']}, logged={history['count']})
FIRST RESPONSE: {frh_str}
CONVERTED: {facts['converted_flag']}
PENDING TASKS: {tasks['count']}
CLINIC CAPACITY: {'Available' if capacity.get('available') else 'Limited'}

RECOMMENDED ACTION: {action}
SUGGESTED TIMING: {suggested_timing}

IMPORTANT: Status "Contacted" means the lead was reached but NOT converted. CONVERTED is {facts['converted_flag']}. Write a rationale supporting the RECOMMENDED ACTION above.

Write only the rationale:"""
        escaped = ai_prompt.replace("'", "''")
        result = run_sql(f"SELECT ai_gen('{escaped}') as rationale")
        if result and result[0].get("rationale"):
            ai_rat = result[0]["rationale"].strip()
            if action != "NO_ACTION":
                ai_lower = ai_rat.lower()
                if any(p in ai_lower for p in ["no follow-up","no action","already converted","already booked","not need","no need","no outreach"]):
                    print(f"AI rationale contradicts action={action}, using deterministic")
                else:
                    rationale = ai_rat
                    is_ai = True
            else:
                rationale = ai_rat
                is_ai = True
    except Exception as e:
        print(f"AI rationale generation failed: {e}")

    return {
        "action": action, "rationale": rationale, "suggested_timing": suggested_timing,
        "evidence": evidence, "missing_info": missing_info, "tool_calls": tool_calls,
        "is_ai": is_ai, "is_fallback": not is_ai, "lead_id": lead_id,
        "effective_touchpoints": eff_touches, "capacity_available": capacity.get("available", True),
        "agent_version": "1.0",
    }

def agent_approve(lead_id, location_id, snapshot_date, recommendation):
    """Persist an approved agent recommendation as a task. Prevents duplicates."""
    action = recommendation.get("action", "")
    if action == "NO_ACTION":
        return {"success": True, "task_id": None, "message": "No action needed — nothing persisted"}
    now = datetime.datetime.now().isoformat()
    decision_map = {
        "CALL": "OUTREACH_LOGGED", "DRAFT_OUTREACH": "OUTREACH_LOGGED",
        "FOLLOW_UP": "FOLLOW_UP_SCHEDULED", "STAFF_REVIEW": "STAFF_REVIEW",
    }
    decision = decision_map.get(action, "STAFF_REVIEW")
    # Duplicate prevention: check for existing pending task of same type
    try:
        existing = run_sql(f"""
            SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks
            WHERE lead_id = '{lead_id}' AND review_status = 'pending_review' AND decision = '{decision}'""")
        if existing and int(existing[0]["cnt"]) > 0:
            return {"success": False, "error": f"Duplicate pending task already exists ({decision})"}
    except Exception:
        pass
    task_id = f"{lead_id}_agent_{now.replace(':','').replace('-','').replace('.','')}"
    ev_json = json.dumps({"evidence": recommendation.get("evidence", []),
                          "tool_count": len(recommendation.get("tool_calls", [])),
                          "is_ai": recommendation.get("is_ai", False)})
    rationale = recommendation.get("rationale", "")
    run_sql(f"""
        INSERT INTO {OUTPUT}.followup_tasks
        (task_id, run_id, snapshot_date, lead_id, location_id, decision, evidence_json, rationale, draft_text, review_status, reviewed_at, is_simulated)
        VALUES ('{task_id}', 'agent_recommend', '{snapshot_date}', '{lead_id}', '{location_id}',
                '{esc(decision)}', '{esc(ev_json)}', '{esc(rationale)}', '', 'pending_review', '{now}', true)""")
    # If draft outreach, generate a draft message
    draft_text = ""
    if action == "DRAFT_OUTREACH":
        try:
            dr = generate_draft(lead_id, location_id)
            if not dr.get("error"):
                draft_text = dr.get("recommendation", "")
                run_sql(f"UPDATE {OUTPUT}.followup_tasks SET draft_text = '{esc(draft_text)}' WHERE task_id = '{task_id}'")
        except Exception:
            pass
    return {"success": True, "task_id": task_id, "decision": decision, "draft": draft_text}

def get_impact_metrics(location_id, snapshot_date, lookback):
    """Compact impact panel: observed, simulated, projected."""
    # --- Observed outcomes (from followup_tasks) ---
    try:
        obs = run_sql(f"""
            SELECT
                COUNT(*) as leads_worked,
                SUM(CASE WHEN outcome = 'Appointment booked' THEN 1 ELSE 0 END) as appts_booked
            FROM {OUTPUT}.followup_tasks
            WHERE location_id = '{location_id}' AND decision = 'OUTREACH_LOGGED' AND review_status = 'completed'
              AND reviewed_at <= '{snapshot_date} 23:59:59'""")
        leads_worked = int(obs[0].get("leads_worked", 0) or 0) if obs else 0
        appts_booked = int(obs[0].get("appts_booked", 0) or 0) if obs else 0
    except Exception:
        leads_worked = 0
        appts_booked = 0

    # Revenue per completed visit (from historical data)
    try:
        rev_rows = run_sql(f"""
            SELECT SUM(v.revenue) / NULLIF(COUNT(*), 0) as avg_rev
            FROM {SOURCE}.visits v
            WHERE v.location_id = '{location_id}'
              AND v.visit_date >= date_sub('{snapshot_date}', 365)
              AND v.visit_date <= '{snapshot_date}'""")
        rev_per_visit = float(rev_rows[0]["avg_rev"]) if rev_rows and rev_rows[0].get("avg_rev") else 95.0
    except Exception:
        rev_per_visit = 95.0

    # Completed visits for recovered patients (observed)
    try:
        cv_rows = run_sql(f"""
            SELECT COUNT(*) as completed_visits, SUM(v.revenue) as total_revenue
            FROM {SOURCE}.visits v
            JOIN {SOURCE}.patients p ON v.patient_id = p.patient_id
            JOIN {SOURCE}.leads l ON p.patient_id = l.converted_patient_id
            WHERE l.assigned_location_id = '{location_id}'
              AND v.status = 'Completed'
              AND v.visit_date >= date_sub('{snapshot_date}', 365)
              AND v.visit_date <= '{snapshot_date}'""")
        completed_visits = int(cv_rows[0].get("completed_visits", 0) or 0) if cv_rows else 0
        observed_revenue = float(cv_rows[0].get("total_revenue", 0) or 0) if cv_rows else 0.0
    except Exception:
        completed_visits = 0
        observed_revenue = 0.0

    # --- Eligible leads for evaluation ---
    try:
        eligible = run_sql(f"""
            SELECT lead_id, source, status, num_touchpoints, first_response_hours, created_date
            FROM {SOURCE}.leads
            WHERE assigned_location_id = '{location_id}'
              AND lower(trim(status)) IN ('new','contacted','qualified')
              AND converted_flag = false AND num_touchpoints BETWEEN 0 AND 2
              AND datediff('{snapshot_date}', created_date) BETWEEN 0 AND {lookback}""")
    except Exception:
        eligible = []

    # --- Simulated evaluation: agent vs oldest-first baseline ---
    CONTACT_BUDGET = min(10, len(eligible))

    def parse_frh(r):
        v = r.get("first_response_hours")
        if v is not None:
            try:
                return float(v)
            except (ValueError, TypeError):
                return None
        return None

    def booking_rate(frh):
        if frh is None:
            return 0.08  # no response — highest recovery value
        if frh > 48:
            return 0.06  # slow response
        return 0.04     # normal response

    def expected_revenue(leads_subset):
        return sum(booking_rate(parse_frh(r)) * rev_per_visit for r in leads_subset)

    # Agent prioritization: slow/no response first, then fewest touchpoints
    agent_sorted = sorted(eligible, key=lambda r: (
        0 if parse_frh(r) is None or parse_frh(r) > 48 else 1,
        int(r.get("num_touchpoints", 0) or 0),
    ))
    agent_top = agent_sorted[:CONTACT_BUDGET]
    agent_expected = expected_revenue(agent_top)

    # Oldest-first baseline
    oldest_sorted = sorted(eligible, key=lambda r: r.get("created_date", ""))
    oldest_top = oldest_sorted[:CONTACT_BUDGET]
    baseline_expected = expected_revenue(oldest_top)

    # --- Projected opportunity ---
    # Eligible leads x incremental booking rate x attendance rate x revenue per visit
    total_eligible = len(eligible)
    INCREMENTAL_BOOKING_RATE = 0.05  # assumption: 5% of worked leads book
    ATTENDANCE_RATE = 0.80           # assumption: 80% of booked appointments are attended
    projected_revenue = total_eligible * INCREMENTAL_BOOKING_RATE * ATTENDANCE_RATE * rev_per_visit

    # Conversion rate (observed)
    conv_rate = (appts_booked / leads_worked) if leads_worked > 0 else 0.0

    return {
        "observed": {
            "leads_worked": leads_worked,
            "appts_booked": appts_booked,
            "completed_visits": completed_visits,
            "observed_revenue": round(observed_revenue, 0),
            "conv_rate": round(conv_rate * 100, 1),
            "conv_denominator": leads_worked,
            "period": f"Last {lookback} days from {snapshot_date}",
        },
        "simulated": {
            "contact_budget": CONTACT_BUDGET,
            "agent_expected": round(agent_expected, 0),
            "baseline_expected": round(baseline_expected, 0),
            "agent_advantage": round(agent_expected - baseline_expected, 0),
            "agent_high_value": sum(1 for r in agent_top if parse_frh(r) is None or (parse_frh(r) or 0) > 48),
            "baseline_high_value": sum(1 for r in oldest_top if parse_frh(r) is None or (parse_frh(r) or 0) > 48),
        },
        "projected": {
            "total_eligible": total_eligible,
            "incremental_booking_rate": INCREMENTAL_BOOKING_RATE,
            "attendance_rate": ATTENDANCE_RATE,
            "rev_per_visit": round(rev_per_visit, 0),
            "projected_revenue": round(projected_revenue, 0),
        },
        "rev_per_visit": round(rev_per_visit, 0),
    }

def get_impact_data(location_id, snapshot_date, lookback):
    metrics = get_metrics(location_id, snapshot_date, lookback)
    eligible = metrics["eligible"]
    # Historical: avg revenue per visit (not per-patient-year, which would double-count repeat visits)
    try:
        rev_rows = run_sql(f"""
            SELECT SUM(v.revenue) / NULLIF(COUNT(*), 0) as avg_rev_per_visit
            FROM {SOURCE}.visits v JOIN {SOURCE}.patients p ON v.patient_id = p.patient_id
            WHERE p.home_location_id = '{location_id}' AND v.visit_date >= date_sub('{snapshot_date}', 365)
            AND v.visit_date <= '{snapshot_date}'""")
        avg_rev_per_visit = float(rev_rows[0]["avg_rev_per_visit"]) if rev_rows and rev_rows[0]["avg_rev_per_visit"] else 95.0
    except:
        avg_rev_per_visit = 95.0
    # Historical: visit frequency for active patients (visits per active month)
    try:
        freq_rows = run_sql(f"""
            SELECT AVG(monthly_visits) as avg_visits_per_month
            FROM (
                SELECT p.patient_id,
                    COUNT(v.visit_id) * 1.0 / GREATEST(p.tenure_months, 1) as monthly_visits
                FROM {SOURCE}.patients p
                LEFT JOIN {SOURCE}.visits v ON p.patient_id = v.patient_id
                WHERE p.home_location_id = '{location_id}' AND p.status = 'Active'
                GROUP BY p.patient_id, p.tenure_months
            ) t""")
        visits_per_month = float(freq_rows[0]["avg_visits_per_month"]) if freq_rows and freq_rows[0]["avg_visits_per_month"] else 2.0
    except:
        visits_per_month = 2.0
    # Assumption: active months in first year (derived from historical avg first-year months ~10.4, rounded down)
    ACTIVE_MONTHS = 10
    first_year_rev_per_client = avg_rev_per_visit * visits_per_month * ACTIVE_MONTHS
    scenarios = []
    for label, rate in [("Low", 0.02), ("Base", 0.05), ("High", 0.10)]:
        expected_clients = eligible * rate
        first_year_revenue = expected_clients * first_year_rev_per_client
        scenarios.append({"label": label, "rate_pct": f"{rate*100:.0f}%",
            "expected_clients": round(expected_clients, 1),
            "first_year_revenue_per_client": round(first_year_rev_per_client, 2),
            "first_year_revenue": round(first_year_revenue, 2)})
    return {"eligible": eligible, "avg_rev_per_visit": round(avg_rev_per_visit, 2),
        "visits_per_month": round(visits_per_month, 1), "active_months": ACTIVE_MONTHS,
        "first_year_rev_per_client": round(first_year_rev_per_client, 2),
        "scenarios": scenarios, "scope": location_id,
        "period": f"snapshot {snapshot_date}, {lookback}-day lookback"}

def get_arr_projection(location_id, snapshot_date, lookback):
    impact = get_impact_data(location_id, snapshot_date, lookback)
    eligible = impact["eligible"]
    first_year_rev_per_client = impact["first_year_rev_per_client"]
    # Per-clinic cohort revenue (from current eligible leads, one-time over 12 months)
    cohort_base = max(eligible * 0.05 * first_year_rev_per_client, 1.0)
    cohort_high = max(eligible * 0.10 * first_year_rev_per_client, 1.0)
    # Annual pipeline assumption (clearly labeled): new eligible leads per clinic per year
    ANNUAL_PIPELINE_LEADS = 150
    annual_base = max(ANNUAL_PIPELINE_LEADS * 0.05 * first_year_rev_per_client, 1.0)
    annual_high = max(ANNUAL_PIPELINE_LEADS * 0.10 * first_year_rev_per_client, 1.0)
    clinics_100m = max(1, int(100_000_000 / annual_base))
    clinics_250m = max(1, int(250_000_000 / annual_base))
    # Sourced addressable market: ~44,000 chiropractic clinics in the US (not individual chiropractors)
    ADDRESSABLE_CLINICS = 44000
    # Platform pricing model: explicit recurring SaaS subscription
    PLATFORM_PRICE_PER_MONTH = 500
    platform_arr_per_clinic = PLATFORM_PRICE_PER_MONTH * 12
    raw_milestones = [
        {"label": "Pilot", "clinics": 1, "icon": "\u2605", "desc": "Current clinic"},
        {"label": "Regional", "clinics": 10, "icon": "\u25cf", "desc": "10 clinics"},
        {"label": "Growth", "clinics": 50, "icon": "\u25b2", "desc": "50 clinics"},
        {"label": "Expansion", "clinics": 200, "icon": "\u26a1", "desc": "200 clinics"},
        {"label": "$100M Scale", "clinics": clinics_100m, "icon": "\u25ce", "desc": f"{clinics_100m:,} clinics"},
        {"label": "$250M Scale", "clinics": clinics_250m, "icon": "\u265b", "desc": f"{clinics_250m:,} clinics"},
    ]
    raw_milestones.sort(key=lambda m: m["clinics"])
    projections = []
    for m in raw_milestones:
        rev_base = annual_base * m["clinics"]
        rev_high = annual_high * m["clinics"]
        platform_arr = platform_arr_per_clinic * m["clinics"]
        projections.append({
            "label": m["label"], "icon": m["icon"], "desc": m["desc"],
            "clinics": m["clinics"], "arr_base": round(rev_base, 0),
            "arr_high": round(rev_high, 0),
            "platform_arr": round(platform_arr, 0),
            "in_target": rev_base >= 100_000_000,
            "exceeds_market": m["clinics"] > ADDRESSABLE_CLINICS,
        })
    return {
        "per_clinic_base": round(annual_base, 0),
        "per_clinic_high": round(annual_high, 0),
        "cohort_per_clinic_base": round(cohort_base, 0),
        "cohort_per_clinic_high": round(cohort_high, 0),
        "projections": projections,
        "clinics_100m": clinics_100m, "clinics_250m": clinics_250m,
        "eligible": eligible,
        "avg_rev_per_visit": impact["avg_rev_per_visit"],
        "visits_per_month": impact["visits_per_month"],
        "active_months": impact["active_months"],
        "first_year_rev_per_client": round(first_year_rev_per_client, 0),
        "annual_pipeline_leads": ANNUAL_PIPELINE_LEADS,
        "addressable_clinics": ADDRESSABLE_CLINICS,
        "platform_price_per_month": PLATFORM_PRICE_PER_MONTH,
        "platform_arr_per_clinic": round(platform_arr_per_clinic, 0),
    }

def get_tasks(location_id, today=""):
    date_expr = f"'{today}'" if today else "current_date()"
    try:
        ns = _name_sql("t.lead_id")
        rows = run_sql(f"""
            WITH pending AS (
                SELECT t.task_id, t.lead_id, t.decision, t.rationale, t.draft_text,
                       t.review_status, t.reviewed_at, t.outcome, {ns},
                    CASE WHEN t.decision = 'FOLLOW_UP_SCHEDULED'
                        AND to_date(nullif(regexp_extract(t.rationale, 'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1), '')) > {date_expr}
                        THEN 'Scheduled'
                        ELSE 'Due Now'
                    END as due_label,
                    CASE WHEN t.decision = 'FOLLOW_UP_SCHEDULED'
                        THEN regexp_extract(t.rationale, 'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1)
                        ELSE NULL
                    END as due_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.lead_id
                        ORDER BY
                            CASE WHEN t.decision = 'FOLLOW_UP_SCHEDULED'
                                AND to_date(nullif(regexp_extract(t.rationale, 'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1), '')) > {date_expr}
                                THEN 1 ELSE 0 END,
                            CASE WHEN t.decision = 'FOLLOW_UP_SCHEDULED' THEN 0
                                 WHEN t.decision = 'follow_up' THEN 1
                                 ELSE 2 END,
                            t.reviewed_at DESC
                    ) as rn,
                    COUNT(*) OVER (PARTITION BY t.lead_id) as total_for_lead
                FROM {OUTPUT}.followup_tasks t
                WHERE t.location_id = '{location_id}' AND t.review_status = 'pending_review'
            )
            SELECT task_id, lead_id, decision, rationale, draft_text, review_status,
                   reviewed_at, outcome, first_name, last_name, phone, email,
                   due_label, due_date, total_for_lead - 1 as duplicate_count
            FROM pending WHERE rn = 1 ORDER BY CASE WHEN due_label = 'Due Now' THEN 0 ELSE 1 END, to_date(nullif(due_date, '')) ASC NULLS LAST, lead_id""")
        # Ensure duplicate_count is int for template comparison
        for row in rows:
            if "duplicate_count" in row and row["duplicate_count"] is not None:
                try:
                    row["duplicate_count"] = int(row["duplicate_count"])
                except (ValueError, TypeError):
                    row["duplicate_count"] = 0
        return rows
    except:
        return []

@app.route("/")
def index():
    location_id = request.args.get("location_id", DEMO_LOCATION)
    snapshot_date = request.args.get("snapshot_date", DEMO_SNAPSHOT)
    lookback_raw = request.args.get("lookback", DEMO_LOOKBACK)
    try:
        lookback_int = int(lookback_raw)
    except (ValueError, TypeError):
        lookback_int = int(DEMO_LOOKBACK)
    lookback_clamped = max(1, min(365, lookback_int))
    lookback_out_of_range = lookback_int != lookback_clamped
    lookback = str(lookback_clamped)
    page = int(request.args.get("page", "1"))
    search = request.args.get("search", "").strip()
    today = request.args.get("today", "")
    try:
        locations = get_locations()
        metrics = get_metrics(location_id, snapshot_date, int(lookback), today=today)
        leads_data = get_eligible_leads(location_id, snapshot_date, int(lookback), page=page, per_page=15, search=search, today=today)
        tasks = get_tasks(location_id, today=today)
        metrics['followups_due'] = sum(1 for t in tasks if t.get('due_label') == 'Due Now')
        clinic_name = next((l["location_name"] for l in locations if l["location_id"] == location_id), location_id)
        # Impact section is hidden (display:none) — skip expensive SQL queries, pass static empty values
        impact = {"scenarios": [], "eligible": 0, "avg_rev_per_visit": 0, "visits_per_month": 0, "active_months": 0, "first_year_rev_per_client": 0}
        arr_projection = {"projections": [], "avg_rev_per_visit": 0, "visits_per_month": 0, "active_months": 0, "first_year_rev_per_client": 0, "per_clinic_base": 0, "per_clinic_high": 0, "clinics_100m": 0, "clinics_250m": 0, "addressable_clinics": 1, "annual_pipeline_leads": 0, "platform_price_per_month": 0, "platform_arr_per_clinic": 0, "eligible": 0}
        impact_metrics = {"observed": {"leads_worked": 0, "appts_booked": 0, "conv_rate": 0, "conv_denominator": 0, "completed_visits": 0, "observed_revenue": 0, "period": ""}, "simulated": {"agent_expected": 0, "baseline_expected": 0, "agent_advantage": 0, "contact_budget": 0, "agent_high_value": 0, "baseline_high_value": 0}, "projected": {"total_eligible": 0, "incremental_booking_rate": 0, "attendance_rate": 0, "rev_per_visit": 0, "projected_revenue": 0}, "rev_per_visit": 0}
        return render_template_string(DASHBOARD_HTML, locations=locations, location_id=location_id,
            clinic_name=clinic_name, snapshot_date=snapshot_date, lookback=lookback, lookback_out_of_range=lookback_out_of_range,
            page=page, search=search, today=today,
            metrics=metrics, leads_data=leads_data, impact=impact, arr_projection=arr_projection, tasks=tasks,
            impact_metrics=impact_metrics)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return render_template_string(ERROR_HTML)

@app.route("/api/lead/<lead_id>")
def api_lead_detail(lead_id):
    location_id = request.args.get("location_id", DEMO_LOCATION)
    snapshot_date = request.args.get("snapshot_date", DEMO_SNAPSHOT)
    try:
        lead = get_lead_detail(lead_id, location_id, snapshot_date)
        if not lead:
            return jsonify({"error": "Lead not found"}), 404
        def serialize(obj):
            if isinstance(obj, (datetime.datetime, datetime.date)):
                return obj.isoformat()
            return obj
        return jsonify(json.loads(json.dumps(lead, default=serialize)))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/lead/<lead_id>/draft")
def api_generate_draft(lead_id):
    location_id = request.args.get("location_id", DEMO_LOCATION)
    try:
        return jsonify(generate_draft(lead_id, location_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/lead/<lead_id>/action", methods=["POST"])
def api_log_action(lead_id):
    location_id = request.form.get("location_id", DEMO_LOCATION)
    snapshot_date = request.form.get("snapshot_date", DEMO_SNAPSHOT)
    outcome = request.form.get("outcome", "")
    note = request.form.get("note", "")
    follow_up_date = request.form.get("follow_up_date", "")
    if outcome not in ("No answer", "Connected", "Wrong number", "Appointment booked", "No longer interested"):
        return jsonify({"success": False, "error": "Invalid outcome"}), 400
    try:
        result = log_outcome(lead_id, location_id, snapshot_date, outcome, note, follow_up_date)
        resp = {"success": True, "task_id": result["task_id"], "outcome": outcome, "removed_from_queue": outcome == "Connected"}
        if result.get("followup"):
            resp["followup"] = result["followup"]
        return jsonify(resp)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/lead/<lead_id>/note", methods=["POST"])
def api_save_note(lead_id):
    location_id = request.form.get("location_id", DEMO_LOCATION)
    snapshot_date = request.form.get("snapshot_date", DEMO_SNAPSHOT)
    note = request.form.get("note", "")
    if not note.strip():
        return jsonify({"success": False, "error": "Note cannot be empty"}), 400
    try:
        task_id = save_note(lead_id, location_id, snapshot_date, note)
        return jsonify({"success": True, "task_id": task_id})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/lead/<lead_id>/close", methods=["POST"])
def api_close_lead(lead_id):
    location_id = request.form.get("location_id", DEMO_LOCATION)
    snapshot_date = request.form.get("snapshot_date", DEMO_SNAPSHOT)
    reason = request.form.get("reason", "")
    if not reason.strip():
        return jsonify({"success": False, "error": "Reason required"}), 400
    try:
        task_id = close_lead(lead_id, location_id, snapshot_date, reason)
        return jsonify({"success": True, "task_id": task_id})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/agent/recommend/<lead_id>")
def api_agent_recommend(lead_id):
    location_id = request.args.get("location_id", DEMO_LOCATION)
    try:
        rec = agent_recommend(lead_id, location_id)
        return jsonify(rec)
    except Exception as e:
        return jsonify({"action": "ERROR", "rationale": str(e), "tool_calls": [],
                        "is_ai": False, "is_fallback": False, "lead_id": lead_id}), 500

@app.route("/api/agent/approve/<lead_id>", methods=["POST"])
def api_agent_approve(lead_id):
    location_id = request.form.get("location_id", DEMO_LOCATION)
    snapshot_date = request.form.get("snapshot_date", DEMO_SNAPSHOT)
    recommendation_json = request.form.get("recommendation", "{}")
    try:
        rec = json.loads(recommendation_json)
        result = agent_approve(lead_id, location_id, snapshot_date, rec)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lead Recovery — {{ clinic_name }}</title>
<script>if(!new URLSearchParams(window.location.search).get('today')){var d=new Date();var ts=d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');var p=new URLSearchParams(window.location.search);p.set('today',ts);window.location.replace(window.location.pathname+'?'+p.toString());}</script>
<style>
:root{--bg:#F6F8FA;--surface:#FFFFFF;--text:#172B4D;--text2:#6B778C;--primary:#087F8C;--ptext:#FFFFFF;--border:#DFE1E6;--success:#0B875B;--warning:#FF991F;--error:#DE350B;--radius:12px;--pad:24px;--shadow:0 1px 3px rgba(23,43,77,0.06);}
*{margin:0;padding:0;box-sizing:border-box;}
.sr-only{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border-width:0;}
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
body{font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:var(--text);font-size:15px;line-height:1.6;}
.hdr{background:var(--surface);border-bottom:1px solid var(--border);padding:var(--pad) var(--pad);box-shadow:var(--shadow);}
.hdr-in{max-width:1400px;margin:0 auto;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:16px;}
.hdr-left h1{font-size:28px;font-weight:700;color:var(--text);margin-bottom:4px;}
.hdr-left .desc{font-size:15px;color:var(--text2);font-weight:400;}
.hdr-right{display:flex;gap:12px;align-items:center;font-size:14px;color:var(--text2);flex-wrap:wrap;}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600;}
.badge-demo{background:#FEF3C7;color:#92400E;}
.ctl{padding:20px var(--pad);background:var(--surface);border-bottom:1px solid var(--border);}
.ctl-in{max-width:1400px;margin:0 auto;display:flex;gap:20px;align-items:flex-end;flex-wrap:wrap;}
.fld{display:flex;flex-direction:column;gap:6px;min-width:200px;flex:1;}
.fld label{font-size:13px;color:var(--text2);font-weight:600;text-transform:uppercase;letter-spacing:0.03em;}
.ctl select,.ctl input{padding:10px 14px;border:1px solid var(--border);border-radius:var(--radius);font-size:14px;background:var(--surface);font-family:inherit;transition:border-color 150ms;}
.ctl select:focus,.ctl input:focus{outline:none;border-color:var(--primary);}
.ctl select{flex:3;max-width:400px;}
.ctl input[type="date"]{flex:1;min-width:160px;}
.ctl input[type="number"]{width:100px;}
.main{max-width:1400px;margin:0 auto;padding:var(--pad);}
.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:20px;margin-bottom:32px;}
@media(max-width:1024px){.metrics{grid-template-columns:repeat(2,1fr);}}
@media(max-width:600px){.metrics{grid-template-columns:1fr;}}
.mc{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px;box-shadow:var(--shadow);transition:transform 150ms,box-shadow 150ms;}
.mc:hover{transform:translateY(-2px);box-shadow:0 4px 12px rgba(23,43,77,0.1);}
.mc .lbl{font-size:12px;color:var(--text2);font-weight:600;text-transform:uppercase;letter-spacing:.05em;margin-bottom:8px;display:flex;align-items:center;gap:6px;}
.mc .val{font-size:34px;font-weight:700;margin-bottom:6px;color:var(--text);}
.mc .sub{font-size:13px;color:var(--text2);line-height:1.4;}
.mc.warning{border-left:3px solid var(--warning);}
.tabs{display:flex;gap:0;margin-bottom:24px;border-bottom:2px solid var(--border);background:var(--surface);border-radius:var(--radius) var(--radius) 0 0;overflow:hidden;}
.tab{padding:14px 28px;cursor:pointer;font-weight:600;font-size:15px;color:var(--text2);border:none;background:none;border-bottom:3px solid transparent;margin-bottom:-2px;transition:all 150ms;position:relative;}
.tab.active{color:var(--primary);border-bottom-color:var(--primary);background:rgba(8,127,140,0.05);}
.tab:hover:not(.active){color:var(--text);background:rgba(0,0,0,0.02);}
.qh{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;gap:12px;flex-wrap:wrap;}
.qh h2{font-size:18px;font-weight:700;}
.qh .cnt{font-size:14px;color:var(--text2);}
.sbox{padding:8px 12px;border:1px solid var(--border);border-radius:var(--radius);font-size:14px;width:240px;}
.qt{width:100%;border-collapse:collapse;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;box-shadow:var(--shadow);}
.qt th{text-align:left;padding:16px 20px;font-size:12px;color:var(--text2);font-weight:700;text-transform:uppercase;letter-spacing:.05em;border-bottom:2px solid var(--border);background:var(--bg);}
.qt td{padding:18px 20px;border-bottom:1px solid var(--border);font-size:14px;vertical-align:middle;}
.qt tbody tr{cursor:pointer;transition:background-color 150ms;}
.qt tbody tr:hover{background:rgba(8,127,140,0.04);}
.qt tbody tr:focus-visible{outline:2px solid var(--primary);outline-offset:-2px;background:rgba(8,127,140,0.06);}
.qt tbody tr:focus{outline:2px solid var(--primary);outline-offset:-2px;background:rgba(8,127,140,0.06);}
.qt tbody tr.sel{background:rgba(8,127,140,0.1);}
.qt tbody tr:last-child td{border-bottom:none;}
.ptag{display:inline-block;padding:4px 10px;border-radius:6px;font-size:12px;font-weight:600;}
.t-error{background:#FFEBE6;color:var(--error);}.t-warning{background:#FFF0E0;color:var(--warning);}.t-success{background:#E3FCEF;color:var(--success);}
.pg{display:flex;gap:8px;align-items:center;margin-top:16px;justify-content:center;}
.pg a{padding:8px 16px;border:1px solid var(--border);border-radius:var(--radius);text-decoration:none;color:var(--text);font-size:14px;}
.pg a:hover{background:var(--bg);}.pg .cur{background:var(--primary);color:var(--ptext);border-color:var(--primary);}.pg .dis{color:var(--text2);pointer-events:none;}
.ovr{position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.3);z-index:100;display:none;}.ovr.show{display:block;}
.dr{position:fixed;top:0;right:0;width:520px;max-width:100%;height:100%;background:var(--surface);z-index:101;overflow-y:auto;transform:translateX(100%);transition:transform 200ms cubic-bezier(0.4,0,0.2,1);box-shadow:-8px 0 32px rgba(23,43,77,0.15);}
.dr.show{transform:translateX(0);}
.dr-h{padding:16px 24px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;background:var(--surface);z-index:1;}
.dr-h h2{font-size:18px;}.dr-x{background:none;border:none;font-size:24px;cursor:pointer;color:var(--text2);padding:4px 8px;}
.dr-b{padding:24px;overflow-x:hidden;}.ds{margin-bottom:24px;}.ds h3{font-size:13px;font-weight:600;color:#718096;text-transform:none;margin-bottom:12px;letter-spacing:0.01em;}
.fr{display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid var(--border);font-size:14px;}.fr .l{color:var(--text2);}.fr .v{font-weight:600;}
.af{background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:16px;margin-top:12px;}
.af select,.af input,.af textarea{width:100%;padding:10px 12px;border:1px solid var(--border);border-radius:var(--radius);font-size:14px;margin-bottom:12px;font-family:inherit;}
.af label{font-size:13px;color:var(--text2);font-weight:600;display:block;margin-bottom:4px;}
.btn{padding:12px 24px;border:none;border-radius:var(--radius);font-size:14px;font-weight:600;cursor:pointer;min-height:44px;transition:all 150ms;font-family:inherit;}
.btn-p{background:var(--primary);color:var(--ptext);box-shadow:0 1px 2px rgba(8,127,140,0.2);}.btn-p:hover{background:#096C78;box-shadow:0 2px 4px rgba(8,127,140,0.3);}
.btn-s{background:var(--surface);color:var(--text);border:1px solid var(--border);}.btn-s:hover{background:var(--bg);border-color:var(--text2);}
.btn-e{background:#FFEBE6;color:var(--error);}.btn-e:hover{background:#FFDBDB;}
.btn-sm{padding:6px 12px;min-height:32px;font-size:13px;}
.dbox{background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:16px;margin-top:8px;}
.dbox textarea{width:100%;border:1px solid var(--border);border-radius:var(--radius);padding:10px;font-size:14px;min-height:100px;font-family:inherit;resize:vertical;}
.fb-note{font-size:13px;color:var(--warning);margin-top:8px;}
.tl{list-style:none;}.ti{padding:8px 0;border-bottom:1px solid var(--border);font-size:14px;}.ti .t{font-size:12px;color:var(--text2);}.ti .a{font-weight:600;}
.ag-badge{display:inline-block;padding:4px 10px;border-radius:6px;font-size:11px;font-weight:600;letter-spacing:0.02em;margin-left:8px;}
.ag-CALL{background:#DBEAFE;color:#1E40AF;}.ag-DRAFT_OUTREACH{background:#E0E7FF;color:#3730A3;}.ag-FOLLOW_UP{background:#FEF3C7;color:#92400E;}.ag-STAFF_REVIEW{background:#FEE2E2;color:#991B1B;}.ag-NO_ACTION{background:#F1F5F9;color:#64748B;}.ag-conflict{background:#FEE2E2;color:#991B1B;}
.ag-panel{background:white;border:1px solid #E2E8F0;border-radius:12px;padding:24px;max-width:100%;overflow-x:hidden;}
.ag-headline{font-size:18px;font-weight:600;color:#1a202c;margin:0 0 12px 0;line-height:1.4;display:flex;align-items:center;flex-wrap:wrap;}
.ag-rat{font-size:14px;line-height:1.6;color:#4a5568;margin:0 0 16px 0;}
.ag-status-note{font-size:13px;line-height:1.5;color:#d97706;margin:12px 0;padding:12px;background:#FEF3C7;border:1px solid #FDE68A;border-radius:8px;}
.ag-status-note strong{font-weight:600;color:#92400E;}
.ag-ev{display:flex;flex-wrap:wrap;gap:8px;margin:16px 0 0 0;}
.ag-ev-item{display:inline-block;font-size:13px;background:#F7FAFC;color:#2d3748;padding:6px 12px;border-radius:6px;border:1px solid #E2E8F0;font-weight:500;}
.ag-miss{font-size:13px;color:#d97706;margin:16px 0 0 0;padding:12px;background:#FEF3C7;border:1px solid #FDE68A;border-radius:8px;}
.ag-disclosure{margin:20px 0 0 0;padding:16px 0 0 0;border-top:1px solid #E2E8F0;}
.ag-disclosure-btn{display:flex;align-items:center;justify-content:flex-start;width:100%;background:none;border:none;padding:0;font:inherit;color:#718096;font-size:13px;cursor:pointer;text-align:left;font-weight:500;}
.ag-disclosure-btn:hover{color:var(--primary);}
.ag-disclosure-chevron{transition:transform 0.2s;font-size:10px;margin-right:6px;display:inline-block;}
.ag-disclosure-chevron.open{transform:rotate(90deg);}
.ag-disclosure-content{display:none;margin-top:12px;}
.ag-disclosure-content.show{display:block;}
.ag-tool{background:#F7FAFC;border:1px solid #E2E8F0;border-radius:6px;padding:12px;margin:10px 0;}
.ag-tool-name{font-weight:600;font-size:12px;margin-bottom:6px;color:#2d3748;}
.ag-tool-desc{font-size:12px;color:#718096;margin-bottom:8px;line-height:1.5;}
.ag-tool-res{font-family:ui-monospace,'Courier New',monospace;font-size:11px;background:white;padding:10px;border-radius:4px;max-height:150px;overflow-y:auto;color:#4a5568;white-space:pre-wrap;border:1px solid #E2E8F0;}
.ag-actions{display:flex;gap:10px;margin-top:20px;flex-wrap:wrap;}
.ag-impl-note{font-size:11px;color:#718096;margin-top:16px;padding-top:12px;border-top:1px solid #E2E8F0;line-height:1.5;}
.dr-sect{margin-top:16px;}.dr-sect-label{font-size:13px;font-weight:600;color:#718096;margin-bottom:6px;letter-spacing:0.01em;}.dr-msg{font-size:15px;line-height:1.6;color:#2d3748;white-space:pre-wrap;background:#F7FAFC;border:1px solid #E2E8F0;border-radius:8px;padding:16px;}
.ic{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px;}
.it{width:100%;border-collapse:collapse;margin-top:16px;}.it th,.it td{padding:10px 16px;text-align:left;border-bottom:1px solid var(--border);font-size:14px;}.it th{font-weight:600;color:var(--text2);}
.inote{font-size:13px;color:var(--text2);margin-top:16px;padding:12px;background:#F1F5F9;border-radius:var(--radius);}
.tlist{display:flex;flex-direction:column;gap:8px;}
.titem{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:12px 16px;display:flex;justify-content:space-between;align-items:center;}
.titem[role="button"]:hover{background:rgba(8,127,140,0.04);border-color:var(--primary);}
.titem[role="button"]:focus{outline:2px solid var(--primary);outline-offset:2px;}
.titem .n{font-weight:600;}.titem .m{font-size:13px;color:var(--text2);}
.sg{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-top:16px;}
.si{text-align:center;padding:16px;background:var(--bg);border-radius:var(--radius);}.si .num{font-size:24px;font-weight:700;}.si .lbl{font-size:13px;color:var(--text2);margin-top:4px;}
.sec{display:none;}.sec.active{display:block;}
.empty{text-align:center;padding:48px;color:var(--text2);}.empty h3{font-size:18px;margin-bottom:8px;}
.loading{text-align:center;padding:24px;color:var(--text2);}
.skel{background:linear-gradient(90deg,#f0f0f0 25%,#e0e0e0 50%,#f0f0f0 75%);background-size:200% 100%;animation:skel 1.5s infinite;border-radius:var(--radius);}
@keyframes skel{0%{background-position:200% 0;}100%{background-position:-200% 0;}}
.skel-line{height:16px;margin-bottom:8px;}
.skel-title{height:24px;width:60%;margin-bottom:16px;}
.bar-chart{margin:24px 0;}
.bar-row{margin-bottom:16px;}
.bar-label{font-size:13px;font-weight:600;margin-bottom:6px;display:flex;justify-content:space-between;}
.bar-track{height:32px;background:var(--bg);border-radius:6px;overflow:hidden;position:relative;}
.bar-fill{height:100%;background:var(--primary);display:flex;align-items:center;padding:0 12px;color:var(--ptext);font-size:13px;font-weight:600;transition:width 400ms cubic-bezier(0.4,0,0.2,1);}
.bar-fill.base{background:#0B9DAC;}
.exp-section{margin-top:16px;padding:16px;background:var(--bg);border-radius:var(--radius);border-left:3px solid var(--text2);}
.exp-toggle{cursor:pointer;font-size:14px;font-weight:600;color:var(--primary);display:inline-flex;align-items:center;gap:4px;user-select:none;background:none;border:none;padding:0;font-family:inherit;}
.exp-toggle:hover{text-decoration:underline;}
.exp-toggle:focus{outline:2px solid var(--primary);outline-offset:2px;border-radius:4px;}
.exp-content{display:none;margin-top:12px;font-size:14px;color:var(--text2);}
.exp-content.show{display:block;}
@media(max-width:900px){.dr{width:100%;}.fld{min-width:150px;}}
@media(max-width:700px){.hdr-in{flex-direction:column;align-items:flex-start;}.hdr-left h1{font-size:24px;}.qt{font-size:13px;}.qt th,.qt td{padding:12px 8px;}.hm{display:none;}.tabs{overflow-x:auto;-webkit-overflow-scrolling:touch;}.tab{padding:12px 20px;white-space:nowrap;flex-shrink:0;}}
@media(max-width:600px){.metrics{margin-bottom:20px;}.mc{padding:16px;}.mc .val{font-size:28px;}.mc .lbl{font-size:11px;}.mc .sub{font-size:12px;}}
.arr-hero{background:linear-gradient(135deg,#087F8C 0%,#0B9DAC 50%,#6366F1 100%);border-radius:var(--radius);padding:32px 24px;text-align:center;color:#fff;margin-bottom:24px;position:relative;overflow:hidden;}
.arr-hero::after{content:'';position:absolute;top:-50%;left:-50%;width:200%;height:200%;background:radial-gradient(circle,rgba(255,255,255,0.08) 0%,transparent 60%);animation:arr-shimmer 6s linear infinite;pointer-events:none;}
@keyframes arr-shimmer{0%{transform:rotate(0deg);}100%{transform:rotate(360deg);}}
.arr-hero-label{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.12em;opacity:0.8;margin-bottom:8px;position:relative;}
.arr-hero-range{font-size:42px;font-weight:700;margin-bottom:4px;position:relative;text-shadow:0 2px 12px rgba(0,0,0,0.15);}
.arr-hero-sub{font-size:14px;opacity:0.85;position:relative;}
.arr-traj{margin:24px 0;}
.arr-traj h3{font-size:14px;font-weight:600;color:var(--text2);text-transform:uppercase;margin-bottom:16px;}
.arr-row{display:flex;align-items:center;gap:12px;margin-bottom:10px;}
.arr-row-label{width:140px;flex-shrink:0;display:flex;align-items:center;gap:8px;}
.arr-ic{font-size:20px;}
.arr-nm{font-weight:600;font-size:13px;}
.arr-cl{font-size:11px;color:var(--text2);}
.arr-bar-wrap{flex:1;height:36px;background:var(--bg);border-radius:8px;overflow:hidden;}
.arr-bar-fill{height:100%;border-radius:8px;background:linear-gradient(90deg,#087F8C,#0B9DAC);transition:width 800ms cubic-bezier(0.4,0,0.2,1);animation:arr-grow 1.2s ease-out;}
.arr-bar-fill.target{background:linear-gradient(90deg,#0B9DAC,#6366F1);box-shadow:0 0 16px rgba(99,102,241,0.35);}
@keyframes arr-grow{from{width:0!important;}}
.arr-val{width:80px;flex-shrink:0;text-align:right;font-weight:700;font-size:14px;}
.arr-cards{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin:24px 0;}
@media(max-width:768px){.arr-cards{grid-template-columns:repeat(2,1fr);}}
@media(max-width:480px){.arr-cards{grid-template-columns:1fr;}}
.arr-card{background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:16px;text-align:center;transition:transform 200ms,box-shadow 200ms;}
.arr-card:hover{transform:translateY(-3px);box-shadow:0 4px 16px rgba(23,43,77,0.08);}
.arr-card.target{border-color:#6366F1;background:linear-gradient(135deg,rgba(99,102,241,0.04),rgba(11,157,172,0.04));}
.arr-card-ic{font-size:28px;margin-bottom:6px;}
.arr-card-nm{font-weight:700;font-size:13px;margin-bottom:2px;}
.arr-card-cl{font-size:11px;color:var(--text2);margin-bottom:8px;}
.arr-card-arr{font-size:22px;font-weight:700;color:var(--primary);}
.arr-card.target .arr-card-arr{color:#6366F1;}
.arr-card-rg{font-size:10px;color:var(--text2);margin-top:4px;}
.arr-kmetrics{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:20px 0;}
@media(max-width:600px){.arr-kmetrics{grid-template-columns:repeat(2,1fr);}}
.arr-km{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:16px;text-align:center;}
.arr-km-v{font-size:24px;font-weight:700;color:var(--primary);margin-bottom:4px;}
.arr-km-l{font-size:10px;color:var(--text2);text-transform:uppercase;letter-spacing:0.05em;}
</style></head><body>
<div class="hdr"><div class="hdr-in"><div class="hdr-left"><h1>Lead Recovery</h1><div class="desc">Find the next follow-up worth making</div></div>
<div class="hdr-right"><span style="font-weight:600">{{ clinic_name }}</span><span class="badge badge-demo">Demo</span></div></div></div>
<div class="ctl"><div class="ctl-in">
<div class="fld"><label for="clinic-select">Clinic</label><select id="clinic-select" aria-label="Select clinic location" onchange="window.location.href='?location_id='+this.value+'&snapshot_date={{ snapshot_date }}&lookback={{ lookback }}&today={{ today }}'">{% for loc in locations %}<option value="{{ loc.location_id }}" {% if loc.location_id == location_id %}selected{% endif %}>{{ loc.location_name }}</option>{% endfor %}</select></div>
<div class="fld"><label for="snapshot-date">Snapshot Date</label><input id="snapshot-date" type="date" value="{{ snapshot_date }}" aria-label="Select snapshot date for analysis" onchange="window.location.href='?location_id={{ location_id }}&snapshot_date='+this.value+'&lookback={{ lookback }}&today={{ today }}'"></div>
<div class="fld"><label for="lookback-days">Lookback (Days)</label><input id="lookback-days" type="number" value="{{ lookback }}" min="1" max="365" aria-label="Number of days to look back from snapshot date" onchange="validateLookback(this.value)"></div>
</div></div>
{% if lookback_out_of_range %}
<div style="max-width:1400px;margin:0 auto;padding:8px 24px;"><div style="background:#FFEBE6;border:1px solid var(--error);border-radius:8px;padding:12px 16px;font-size:14px;color:var(--error);"><strong>Lookback adjusted:</strong> The value you entered was outside the allowed range (1–365 days). Using {{ lookback }} days instead.</div></div>
{% endif %}
<div class="main">
<div class="metrics">
<div class="mc"><div class="lbl">Ready for Review</div><div class="val">{{ metrics.eligible }}</div><div class="sub">Eligible leads at this clinic</div></div>
<div class="mc warning"><div class="lbl">Follow-ups Due</div><div class="val">{{ metrics.followups_due }}</div><div class="sub">Due now · {{ tasks|length }} total pending</div></div>
<div class="mc"><div class="lbl">Actions Logged</div><div class="val">{{ metrics.actions_logged }}</div><div class="sub">Recorded outreach outcomes</div></div>
<div class="mc"><div class="lbl">Total Leads</div><div class="val">{{ metrics.total_at_clinic }}</div><div class="sub">All leads at this clinic</div></div>
</div>
<div class="imp-section" style="display:none">
<div class="ic" style="padding:16px">
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
<h2 style="font-size:16px;font-weight:700">Impact Summary</h2>
<span style="font-size:12px;color:var(--text2)">Observed + Simulated + Projected | {{ impact_metrics.observed.period }}</span>
</div>
<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px">
<div style="text-align:center;padding:8px;background:var(--bg);border-radius:var(--radius)">
<div style="font-size:24px;font-weight:700;color:var(--primary)">{{ impact_metrics.observed.leads_worked }}</div>
<div style="font-size:11px;color:var(--text2);text-transform:uppercase">Leads Worked</div>
</div>
<div style="text-align:center;padding:8px;background:var(--bg);border-radius:var(--radius)">
<div style="font-size:24px;font-weight:700;color:var(--success)">{{ impact_metrics.observed.appts_booked }}</div>
<div style="font-size:11px;color:var(--text2);text-transform:uppercase">Appts Booked</div>
</div>
<div style="text-align:center;padding:8px;background:var(--bg);border-radius:var(--radius)">
<div style="font-size:24px;font-weight:700;color:var(--primary)">{{ impact_metrics.observed.conv_rate }}%</div>
<div style="font-size:11px;color:var(--text2);text-transform:uppercase">Conv Rate ({{ impact_metrics.observed.conv_denominator }} worked)</div>
</div>
<div style="text-align:center;padding:8px;background:var(--bg);border-radius:var(--radius)">
<div style="font-size:24px;font-weight:700;color:var(--success)">{{ impact_metrics.observed.completed_visits }}</div>
<div style="font-size:11px;color:var(--text2);text-transform:uppercase">Completed Visits</div>
</div>
</div>
<div style="border-top:1px solid var(--border);padding-top:12px;margin-bottom:12px">
<div style="font-size:13px;font-weight:600;margin-bottom:8px">Simulated Evaluation: Agent vs Oldest-First Baseline</div>
<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;font-size:13px">
<div><strong>Agent:</strong> ${{ "{:,.0f}".format(impact_metrics.simulated.agent_expected) }} expected from {{ impact_metrics.simulated.contact_budget }} leads ({{ impact_metrics.simulated.agent_high_value }} high-value)</div>
<div><strong>Baseline:</strong> ${{ "{:,.0f}".format(impact_metrics.simulated.baseline_expected) }} expected from {{ impact_metrics.simulated.contact_budget }} leads ({{ impact_metrics.simulated.baseline_high_value }} high-value)</div>
<div><strong>Advantage:</strong> ${{ "{:,.0f}".format(impact_metrics.simulated.agent_advantage) }}</div>
</div>
<div style="font-size:12px;color:var(--text2);margin-top:4px">Same contact budget. Booking rates: 8% no-response, 6% slow (>48h), 4% normal. Simulated, not measured.</div>
</div>
<div style="border-top:1px solid var(--border);padding-top:12px">
<div style="font-size:13px;font-weight:600;margin-bottom:4px">Projected Incremental Revenue Opportunity</div>
<div style="font-size:14px">{{ impact_metrics.projected.total_eligible }} eligible leads &times; {{ (impact_metrics.projected.incremental_booking_rate * 100)|round(0) }}% booking &times; {{ (impact_metrics.projected.attendance_rate * 100)|round(0) }}% attendance &times; ${{ "{:,.0f}".format(impact_metrics.projected.rev_per_visit) }}/visit = <strong>${{ "{:,.0f}".format(impact_metrics.projected.projected_revenue) }}</strong></div>
<div style="font-size:12px;color:var(--text2);margin-top:4px">Transparent assumptions. Not attributed revenue or proven causal uplift. Scaling across clinics contributes to growth goal but does not close the entire revenue gap.</div>
</div>
</div>
</div>
<div class="tabs">
<button class="tab active" onclick="showTab('queue',this)">Recovery Queue</button>
<button class="tab" onclick="showTab('tasks',this)">Follow-ups</button>
</div>
<div id="queue" class="sec active">
<div class="qh"><div><h2>Recovery Queue</h2><div class="cnt">{% if search %}{{ leads_data.total }} match{{ 'es' if leads_data.total != 1 else '' }} of {{ leads_data.total_eligible }} eligible leads{% else %}Showing {{ leads_data.leads|length }} of {{ leads_data.total }} eligible leads{% endif %}</div></div>
<label for="searchInput" class="sr-only">Search leads</label><input type="text" class="sbox" id="searchInput" placeholder="Search by lead ID or source..." value="{{ search }}" aria-label="Search by lead ID or source" oninput="searchTimer(this.value)"></div>
{% if leads_data.leads %}
<table class="qt"><thead><tr><th>#</th><th>Lead</th><th>Why Now</th><th class="hm">Contact History</th><th>Status</th></tr></thead><tbody>
{% for lead in leads_data.leads %}
<tr onclick="openLead('{{ lead.lead_id }}')" id="row-{{ lead.lead_id }}" tabindex="0" role="button" aria-label="Open {{ lead.first_name }} {{ lead.last_name }} lead details" onkeydown="if(event.key==='Enter'||event.key===' '||event.key==='Spacebar'){event.preventDefault();openLead('{{ lead.lead_id }}')}">
<td>{{ lead.rank }}</td>
<td><div style="font-weight:600">{{ lead.first_name }} {{ lead.last_name }}</div><div style="font-size:12px;color:var(--text2)">{{ lead.lead_id }} &middot; {{ lead.source }}</div></td>
<td><span class="ptag t-{{ lead.priority_color }}">{{ lead.priority_label }}</span>{% if lead.data_quality_warning %}<div style="font-size:12px;color:var(--warning);margin-top:4px;font-weight:600">&#9888; Data quality: {{ lead.data_quality_warning }}</div>{% endif %}</td>
<td class="hm">{{ lead.touch_info }}<div style="font-size:12px;color:var(--text2)">First response: {% if lead.first_response_hours is not none %}{{ "%.1f"|format(lead.first_response_hours|float) }} hrs{% else %}Not recorded{% endif %}</div></td>
<td>{{ lead.status }}</td>
</tr>
{% endfor %}
</tbody></table>
{% if leads_data.pages > 1 %}
<div class="pg">{% if page > 1 %}<a href="?location_id={{ location_id }}&snapshot_date={{ snapshot_date }}&lookback={{ lookback }}&page={{ page - 1 }}&search={{ search|urlencode }}&today={{ today }}">&larr; Prev</a>{% else %}<a class="dis">&larr; Prev</a>{% endif %}
<span style="padding:8px;font-size:14px">Page {{ page }} of {{ leads_data.pages }}</span>
{% if page < leads_data.pages %}<a href="?location_id={{ location_id }}&snapshot_date={{ snapshot_date }}&lookback={{ lookback }}&page={{ page + 1 }}&search={{ search|urlencode }}&today={{ today }}">Next &rarr;</a>{% else %}<a class="dis">Next &rarr;</a>{% endif %}</div>
{% endif %}
{% else %}
<div class="empty"><h3>{% if search %}0 matches of {{ leads_data.total_eligible }} eligible leads{% else %}No eligible leads found{% endif %}</h3><p>{% if search %}No leads match &ldquo;{{ search }}&rdquo;. Try a different search term.{% else %}Try adjusting the snapshot date, lookback period, or search.{% endif %}</p></div>
{% endif %}
</div>
<div id="tasks" class="sec">
<h2 style="font-size:18px;font-weight:700;margin-bottom:16px">Follow-up Tasks</h2>
{% if tasks %}
<div class="tlist">{% for t in tasks %}<div class="titem" role="button" tabindex="0" onclick="openLead('{{ t.lead_id }}')" onkeypress="if(event.key==='Enter')openLead('{{ t.lead_id }}')" style="cursor:pointer;transition:all 150ms;" aria-label="Open {{ t.first_name }} {{ t.last_name }} lead details" title="Click to open lead"><div class="info" style="flex:1"><div class="n">{{ t.first_name }} {{ t.last_name }}</div><div class="m">{{ t.lead_id }} &middot; {{ t.decision|replace('_',' ')|title }}</div>{% if t.rationale %}<div class="m">{{ t.rationale }}</div>{% endif %}{% if t.duplicate_count and t.duplicate_count > 0 %}<div class="m" style="font-size:11px;color:var(--warning);font-weight:600">&#9888; {{ t.duplicate_count }} additional pending task{{ 's' if t.duplicate_count != 1 else '' }} for this lead (deduplicated)</div>{% endif %}</div>{% if t.due_label == 'Due Now' %}<span style="font-size:12px;font-weight:600;padding:4px 10px;border-radius:6px;background:#FEE2E2;color:#991B1B" aria-label="Due now">Due Now</span>{% else %}<span style="font-size:12px;font-weight:600;padding:4px 10px;border-radius:6px;background:#DBEAFE;color:#1E40AF" aria-label="Scheduled for {{ t.due_date }}">Scheduled{% if t.due_date %} · {{ t.due_date }}{% endif %}</span>{% endif %}</div>{% endfor %}</div>
{% else %}
<div class="empty"><h3>No pending tasks</h3><p>All follow-up tasks are completed.</p></div>
{% endif %}
</div>
<div id="impact" class="sec" style="display:none">
<div class="ic"><h2 style="font-size:20px;font-weight:700;margin-bottom:6px">Recovery Opportunity</h2>
<p style="font-size:14px;color:var(--text2);margin-bottom:24px">Estimated revenue scenarios based on eligible leads at this clinic</p>
<table class="it" style="margin-bottom:16px;"><thead><tr><th>Scenario</th><th>Ongoing-Client Rate</th><th>Expected Ongoing Clients</th><th style="text-align:right">First-Year Clinic Revenue Opportunity</th></tr></thead><tbody>
{% for s in impact.scenarios %}<tr{% if s.label == 'Base' %} style="background:rgba(8,127,140,0.06);"{% endif %}><td style="font-weight:{% if s.label == 'Base' %}700{% else %}400{% endif %}">{{ s.label }}</td><td>{{ s.rate_pct }}</td><td>{{ s.expected_clients }}</td><td style="text-align:right;font-weight:600">${{ "{:,.0f}".format(s.first_year_revenue) }}</td></tr>{% endfor %}
</tbody></table>
<div class="bar-chart">
{% set max_val = impact.scenarios[2].first_year_revenue if impact.scenarios[2].first_year_revenue > 0 else 1 %}
{% for s in impact.scenarios %}
<div class="bar-row"><div class="bar-label"><span>{{ s.label }} ({{ s.rate_pct }})</span><span>${{ "{:,.0f}".format(s.first_year_revenue) }}</span></div><div class="bar-track"><div class="bar-fill{% if s.label == 'Base' %} base{% endif %}" style="width:{{ (s.first_year_revenue / max_val * 100)|round(1) }}%"></div></div></div>
{% endfor %}
</div>
<div class="exp-section"><button class="exp-toggle" type="button" aria-expanded="false" aria-controls="calc-details" onclick="toggleExpand(this)"><span class="arrow">▶</span> How this is calculated</button><div id="calc-details" class="exp-content" role="region" aria-label="Calculation details"><p style="margin-bottom:8px"><strong>Expected ongoing clients</strong> = Eligible leads ({{ impact.eligible }}) &times; Ongoing-client conversion rate (scenario)</p><p style="margin-bottom:8px"><strong>First-year revenue per client</strong> = Avg revenue per visit (${{ "{:,.0f}".format(impact.avg_rev_per_visit) }}) &times; Visits per active month ({{ impact.visits_per_month }}) &times; Active months ({{ impact.active_months }}) = <strong>${{ "{:,.0f}".format(impact.first_year_rev_per_client) }}</strong></p><p style="margin-bottom:8px"><strong>First-year clinic revenue opportunity</strong> = Expected ongoing clients &times; First-year revenue per client</p><p style="font-size:13px;color:var(--text2);margin-top:12px">Eligible leads: {{ impact.eligible }} at this clinic | Lookback: {{ lookback }} days from {{ snapshot_date }}</p></div></div>
<div class="inote" style="margin-top:16px">These are <strong>cohort scenario estimates</strong>, not measured predictions. The {{ impact.eligible }} eligible leads come from a {{ lookback }}-day lookback and represent a one-time cohort, not a recurring pipeline. Revenue figures represent the cohort’s expected revenue over the 12 months after conversion, not collected revenue. Ongoing-client conversion rates and active months are assumptions unless validated experimentally. Revenue per visit and visit frequency are derived from historical clinic data.</div>
</div>
<div class="ic" style="margin-top:20px">
<h2 style="font-size:20px;font-weight:700;margin-bottom:6px">Path to $100M &ndash; $250M Revenue Opportunity</h2>
<p style="font-size:14px;color:var(--text2);margin-bottom:24px">How lead recovery scales across chiropractic clinics nationwide</p>
<div class="arr-hero"><div class="arr-hero-label">TARGET REVENUE ZONE</div><div class="arr-hero-range">$100M &ndash; $250M</div><div class="arr-hero-sub">Estimated annual clinic revenue opportunity at scale</div></div>
<div class="arr-traj"><h3 style="font-size:14px;font-weight:600;color:var(--text2);text-transform:uppercase;margin-bottom:16px">Growth Trajectory</h3>
{% set arr_max = arr_projection.projections[-1].arr_high if arr_projection.projections and arr_projection.projections[-1].arr_high > 0 else 1 %}
{% for p in arr_projection.projections %}
<div class="arr-row"><div class="arr-row-label"><span class="arr-ic">{{ p.icon }}</span><div><div class="arr-nm">{{ p.label }}</div><div class="arr-cl">{{ '{:,.0f}'.format(p.clinics) }} clinic{{ 's' if p.clinics != 1 else '' }}</div></div></div><div class="arr-bar-wrap"><div class="arr-bar-fill{% if p.in_target %} target{% endif %}" style="width:{{ (p.arr_high / arr_max * 100)|round(1) }}%"></div></div><div class="arr-val">{% if p.arr_base >= 1000000 %}${{ '{:,.1f}'.format(p.arr_base / 1000000) }}M{% elif p.arr_base >= 1000 %}${{ '{:,.0f}'.format(p.arr_base / 1000) }}K{% else %}${{ '{:,.0f}'.format(p.arr_base) }}{% endif %}</div></div>
{% endfor %}
</div>
<div class="arr-cards">
{% for p in arr_projection.projections %}
<div class="arr-card{% if p.in_target %} target{% endif %}"><div class="arr-card-ic">{{ p.icon }}</div><div class="arr-card-nm">{{ p.label }}</div><div class="arr-card-cl">{{ '{:,.0f}'.format(p.clinics) }} clinic{{ 's' if p.clinics != 1 else '' }}</div><div class="arr-card-arr">{% if p.arr_base >= 1000000 %}${{ '{:,.1f}'.format(p.arr_base / 1000000) }}M{% elif p.arr_base >= 1000 %}${{ '{:,.0f}'.format(p.arr_base / 1000) }}K{% else %}${{ '{:,.0f}'.format(p.arr_base) }}{% endif %}</div><div class="arr-card-rg">{% if p.arr_high >= 1000000 %}up to ${{ '{:,.1f}'.format(p.arr_high / 1000000) }}M{% elif p.arr_high >= 1000 %}up to ${{ '{:,.0f}'.format(p.arr_high / 1000) }}K{% else %}up to ${{ '{:,.0f}'.format(p.arr_high) }}{% endif %}</div></div>
{% endfor %}
</div>
<div class="arr-kmetrics"><div class="arr-km"><div class="arr-km-v">${{ '{:,.0f}'.format(arr_projection.avg_rev_per_visit) }}</div><div class="arr-km-l">Rev/visit (historical)</div></div><div class="arr-km"><div class="arr-km-v">{{ arr_projection.visits_per_month }}</div><div class="arr-km-l">Visits/mo (active)</div></div><div class="arr-km"><div class="arr-km-v">${{ '{:,.0f}'.format(arr_projection.first_year_rev_per_client) }}</div><div class="arr-km-l">First-year rev/client</div></div><div class="arr-km"><div class="arr-km-v">${{ '{:,.0f}'.format(arr_projection.per_clinic_base) }}</div><div class="arr-km-l">Annual rev/clinic (base)</div></div></div>
<div class="exp-section"><button class="exp-toggle" type="button" aria-expanded="false" aria-controls="arr-calc" onclick="toggleExpand(this)"><span class="arrow">&#9654;</span> How we get to $100M &ndash; $250M</button><div id="arr-calc" class="exp-content" role="region" aria-label="Revenue calculation details"><p style="margin-bottom:8px"><strong>First-year revenue per client</strong> = Avg revenue per visit (${{ '{:,.0f}'.format(arr_projection.avg_rev_per_visit) }}) &times; Visits per active month ({{ arr_projection.visits_per_month }}) &times; Active months ({{ arr_projection.active_months }}) = <strong>${{ '{:,.0f}'.format(arr_projection.first_year_rev_per_client) }}/client</strong></p><p style="margin-bottom:8px"><strong>Annual per-clinic revenue</strong> = Assumed annual pipeline ({{ arr_projection.annual_pipeline_leads }} new eligible leads/clinic/year, documented assumption) &times; Ongoing-client conversion rate &times; First-year revenue per client. At base rate (5%): <strong>${{ '{:,.0f}'.format(arr_projection.per_clinic_base) }}/clinic/year</strong>. At high rate (10%): <strong>${{ '{:,.0f}'.format(arr_projection.per_clinic_high) }}/clinic/year</strong>.</p><p style="margin-bottom:8px"><strong>Scaling model</strong> = Annual per-clinic revenue &times; Number of clinics onboarded. At <strong>{{ '{:,.0f}'.format(arr_projection.clinics_100m) }} clinics</strong> (base rate), revenue opportunity reaches <strong>$100M</strong>. At <strong>{{ '{:,.0f}'.format(arr_projection.clinics_250m) }} clinics</strong> (base rate), revenue opportunity reaches <strong>$250M</strong>; at high rate across the same clinics, up to <strong>$500M</strong>.</p><p style="margin-bottom:8px"><strong>Market context</strong> = ~{{ '{:,.0f}'.format(arr_projection.addressable_clinics) }} chiropractic clinics in the US (sourced estimate, not individual chiropractors). {{ '{:,.0f}'.format(arr_projection.clinics_250m) }} clinics represents {{ (arr_projection.clinics_250m / arr_projection.addressable_clinics * 100)|round(1) }}% market penetration.{% if arr_projection.clinics_250m > arr_projection.addressable_clinics %} <strong style="color:var(--error)">This target exceeds the total addressable market.</strong>{% endif %}</p><p style="margin-bottom:8px"><strong>Platform ARR</strong> (the product company's own revenue, separate from clinic revenue) = ${{ '{:,.0f}'.format(arr_projection.platform_price_per_month) }}/month per clinic &times; 12 = <strong>${{ '{:,.0f}'.format(arr_projection.platform_arr_per_clinic) }}/clinic/year</strong>. At {{ '{:,.0f}'.format(arr_projection.clinics_100m) }} clinics: ${{ '{:,.1f}'.format(arr_projection.platform_arr_per_clinic * arr_projection.clinics_100m / 1000000) }}M ARR. At {{ '{:,.0f}'.format(arr_projection.clinics_250m) }} clinics: ${{ '{:,.1f}'.format(arr_projection.platform_arr_per_clinic * arr_projection.clinics_250m / 1000000) }}M ARR.</p><p style="font-size:13px;color:var(--text2);margin-top:12px">These are scenario projections based on current clinic data, not measured outcomes. The current {{ arr_projection.eligible }} eligible leads represent a one-time cohort from a {{ lookback }}-day lookback; annual scaling assumes {{ arr_projection.annual_pipeline_leads }} new eligible leads per clinic per year (documented assumption). Ongoing-client conversion rates and active months are assumptions unless validated experimentally. Revenue per visit and visit frequency are derived from historical clinic data. Platform pricing (${{ '{:,.0f}'.format(arr_projection.platform_price_per_month) }}/month/clinic) is an explicit recurring SaaS subscription model, not a percentage of clinic revenue.</p></div></div>
</div>
<div class="ic" style="margin-top:20px"><h2 style="font-size:20px;font-weight:700;margin-bottom:6px">Lead Status Breakdown</h2>
<p style="font-size:14px;color:var(--text2);margin-bottom:20px">All leads at {{ clinic_name }}</p>
<div style="margin-bottom:20px;background:var(--bg);border-radius:var(--radius);overflow:hidden;height:48px;display:flex;">
{% set total = (metrics.converted + metrics.open_leads + metrics.lost) if (metrics.converted + metrics.open_leads + metrics.lost) > 0 else 1 %}
{% if total > 0 %}
<div style="background:#E3FCEF;width:{{ (metrics.converted / total * 100)|round(1) }}%;display:flex;align-items:center;justify-content:center;font-weight:600;font-size:14px;color:var(--success)">{{ metrics.converted }}</div>
<div style="background:rgba(8,127,140,0.15);width:{{ (metrics.open_leads / total * 100)|round(1) }}%;display:flex;align-items:center;justify-content:center;font-weight:600;font-size:14px;color:var(--primary)">{{ metrics.open_leads }}</div>
<div style="background:#FFEBE6;width:{{ (metrics.lost / total * 100)|round(1) }}%;display:flex;align-items:center;justify-content:center;font-weight:600;font-size:14px;color:var(--error)">{{ metrics.lost }}</div>
{% endif %}
</div>
<div style="display:flex;justify-content:space-around;text-align:center;">
<div><div style="font-size:12px;color:var(--text2);text-transform:uppercase;letter-spacing:0.05em;margin-bottom:4px">Converted</div><div style="font-size:20px;font-weight:700;color:var(--success)">{{ metrics.converted }}</div></div>
<div><div style="font-size:12px;color:var(--text2);text-transform:uppercase;letter-spacing:0.05em;margin-bottom:4px">Open</div><div style="font-size:20px;font-weight:700;color:var(--primary)">{{ metrics.open_leads }}</div></div>
<div><div style="font-size:12px;color:var(--text2);text-transform:uppercase;letter-spacing:0.05em;margin-bottom:4px">Lost</div><div style="font-size:20px;font-weight:700;color:var(--error)">{{ metrics.lost }}</div></div>
</div>
<div style="font-size:13px;color:var(--text2);margin-top:16px;padding-top:16px;border-top:1px solid var(--border)">Total leads: <strong>{{ metrics.total_at_clinic }}</strong> &middot; Contact-capped (3+ touches): <strong>{{ metrics.contact_capped }}</strong></div>
</div>
</div>
</div>
<div class="ovr" id="overlay" onclick="closeDrawer()" aria-hidden="true"></div>
<div class="dr" id="drawer" role="dialog" aria-modal="true" aria-labelledby="dtitle" aria-hidden="true" hidden><div class="dr-h"><h2 id="dtitle">Lead Detail</h2><button class="dr-x" onclick="closeDrawer()" aria-label="Close lead details">&times;</button></div><div class="dr-b" id="dbody"><div class="loading">Loading...</div></div></div>
<script>
var locId='{{ location_id }}';var snapDate='{{ snapshot_date }}';var stid;var lastFocusedElement=null;var lastAgentRec=null;
function showTab(n,b){document.querySelectorAll('.sec').forEach(s=>s.classList.remove('active'));document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));document.getElementById(n).classList.add('active');b.classList.add('active');}
function searchTimer(v){clearTimeout(stid);stid=setTimeout(function(){window.location.href='?location_id='+locId+'&snapshot_date='+snapDate+'&lookback={{ lookback }}&search='+encodeURIComponent(v)+'&today={{ today }}';},400);}
function validateLookback(v){var n=parseInt(v);if(isNaN(n)||n<1||n>365){alert('Lookback must be between 1 and 365 days. Value will be adjusted to fit the allowed range.');n=Math.max(1,Math.min(365,isNaN(n)?120:n));}window.location.href='?location_id={{ location_id }}&snapshot_date={{ snapshot_date }}&lookback='+n+'&today={{ today }}';}
function openLead(id){lastFocusedElement=document.activeElement;document.querySelectorAll('.qt tr').forEach(r=>r.classList.remove('sel'));var r=document.getElementById('row-'+id);if(r)r.classList.add('sel');var ovr=document.getElementById('overlay'),dr=document.getElementById('drawer');ovr.classList.add('show');ovr.setAttribute('aria-hidden','false');dr.classList.add('show');dr.removeAttribute('hidden');dr.setAttribute('aria-hidden','false');document.getElementById('dtitle').textContent='Loading...';document.getElementById('dbody').innerHTML='<div style="padding:24px;"><div class="skel skel-title"></div><div class="skel skel-line"></div><div class="skel skel-line"></div><div class="skel skel-line" style="width:70%"></div><div style="height:24px"></div><div class="skel skel-title"></div><div class="skel skel-line"></div><div class="skel skel-line"></div></div>';fetch('/api/lead/'+id+'?location_id='+locId+'&snapshot_date='+snapDate).then(r=>r.json()).then(d=>renderLead(d)).catch(e=>{document.getElementById('dbody').innerHTML='<div class="empty"><h3>Error loading lead</h3><p>'+e.message+'</p></div>';});setTimeout(()=>{var closeBtn=document.querySelector('.dr-x');if(closeBtn)closeBtn.focus();},100);}
function renderLead(d){if(d.error){document.getElementById('dbody').innerHTML='<div class="empty"><h3>Error</h3><p>'+d.error+'</p></div>';return;}document.getElementById('dtitle').textContent=d.first_name+' '+d.last_name;var frh=d.first_response_hours!==null&&d.first_response_hours!==undefined?parseFloat(d.first_response_hours).toFixed(1)+' hours':'Not recorded';var h='';
h+='<div class="ds"><h3>Status</h3><div style="font-size:16px;font-weight:600">'+d.status+'</div><div style="font-size:14px;color:var(--text2);margin-top:4px">'+(d.priority_label||'Standard')+'</div></div>';var frhVal=d.first_response_hours!==null&&d.first_response_hours!==undefined?parseFloat(d.first_response_hours):null;
h+='<div class="ds"><h3>Why This Lead Is Prioritized</h3><div style="font-size:14px"><strong>'+(d.priority_label||'Standard')+'</strong> — ';if(frhVal===null){h+='No response has been recorded. This lead has not been contacted.';}else if(frhVal>48){h+='First response took '+frhVal.toFixed(1)+' hours (slower than the 48-hour threshold).';}else if(frhVal>24){h+='First response took '+frhVal.toFixed(1)+' hours (delayed, between 24-48 hours).';}else{h+='First response was within '+frhVal.toFixed(1)+' hours.';}var touches=parseInt(d.effective_touchpoints)||parseInt(d.num_touchpoints)||0;h+='<div style="margin-top:4px;color:var(--text2)">'+(touches===0?'No contact attempts yet':touches+' previous contact attempts')+'</div></div></div>';
h+='<div class="ds"><h3>Verified Facts</h3><div class="fr"><span class="l">Lead ID</span><span class="v">'+d.lead_id+'</span></div><div class="fr"><span class="l">Source</span><span class="v">'+d.source+'</span></div><div class="fr"><span class="l">Status</span><span class="v">'+d.status+'</span></div><div class="fr"><span class="l">Created</span><span class="v">'+(d.created_date||'Unknown')+'</span></div><div class="fr"><span class="l">Touchpoints</span><span class="v">'+(d.effective_touchpoints!==undefined?d.effective_touchpoints:d.num_touchpoints)+'</span></div><div class="fr"><span class="l">First response</span><span class="v">'+frh+'</span></div><div class="fr"><span class="l">Phone</span><span class="v" style="font-size:12px;color:var(--text2)">'+(d.phone||'N/A')+' (synthetic)</span></div><div class="fr"><span class="l">Email</span><span class="v" style="font-size:12px;color:var(--text2)">'+(d.email||'N/A')+' (synthetic)</span></div></div>';
if(d.data_quality_warning){h+='<div class="ds"><h3>Data quality</h3><div style="font-size:14px;color:var(--warning);padding:12px;background:#FEF3C7;border:1px solid #FDE68A;border-radius:8px"><strong>Data quality warning:</strong> '+d.data_quality_warning+'</div></div>';}
h+='<div class="ds"><h3>Recommended next step</h3><div id="ag"><button class="btn btn-p" style="width:100%" type="button" onclick="runAgent(\''+d.lead_id+'\')">Run Agent Recommendation</button></div></div>';
h+='<div class="ds"><h3>Recommended Draft</h3><div id="da"><button class="btn btn-p" style="width:100%" type="button" onclick="genDraft(\''+d.lead_id+'\')" >Generate AI Draft</button></div></div>';
h+='<div class="ds"><h3>Log Call Outcome</h3><div class="af"><label for="os">Outcome</label><select id="os" aria-label="Select call outcome"><option value="">Select outcome...</option><option value="No answer">No answer</option><option value="Connected">Connected</option><option value="Wrong number">Wrong number</option><option value="Appointment booked">Appointment booked</option><option value="No longer interested">No longer interested</option></select><label for="on">Note (optional)</label><textarea id="on" aria-label="Optional note about the call" rows="2" placeholder="Add context about the call..."></textarea><label for="fd">Follow-up Date (optional)</label><input type="date" id="fd" aria-label="Optional follow-up date"><button class="btn btn-p" style="width:100%" type="button" onclick="logOut(\''+d.lead_id+'\')" >Log Outcome</button><div id="ar" style="margin-top:8px" role="alert" aria-live="polite"></div></div></div>';
h+='<div class="ds"><h3>Save Note</h3><div class="af"><label for="sn" class="sr-only">Internal note</label><textarea id="sn" aria-label="Internal note about this lead" rows="2" placeholder="Internal note about this lead..."></textarea><button class="btn btn-s" style="width:100%" type="button" onclick="saveN(\''+d.lead_id+'\')" >Save Note</button><div id="nr" style="margin-top:8px" role="alert" aria-live="polite"></div></div></div>';
if(d.actions&&d.actions.length>0){h+='<div class="ds"><h3>Recent Activity</h3><div class="tl">';d.actions.forEach(function(a){var t=a.reviewed_at?new Date(a.reviewed_at).toLocaleString():'';var badge=a.review_status==='superseded'?' <span style="font-size:10px;padding:2px 6px;border-radius:4px;background:#F1F5F9;color:var(--text2)">superseded</span>':'';h+='<div class="ti"><div class="a">'+(a.decision||'').replace(/_/g,' ')+(a.outcome?' — '+a.outcome:'')+badge+'</div><div class="t">'+t+'</div>';if(a.rationale)h+='<div style="font-size:13px;color:var(--text2)">'+a.rationale+'</div>';h+='</div>';});h+='</div></div>';}
h+='<div class="ds"><h3>Close Lead</h3><div class="af"><label for="cr">Reason (required)</label><input type="text" id="cr" aria-label="Reason for closing lead" placeholder="Why is this lead being closed?"><button class="btn btn-e" style="width:100%" type="button" onclick="closeL(\''+d.lead_id+'\')" >Close Lead</button><div id="clr" style="margin-top:8px" role="alert" aria-live="polite"></div></div></div>';
document.getElementById('dbody').innerHTML=h;}
function genDraft(id){
    var da = document.getElementById('da');
    da.innerHTML = '<div class="loading">Generating draft...</div>';
    fetch('/api/lead/' + id + '/draft?location_id=' + locId)
    .then(r => r.json())
    .then(d => {
        if (d.error) {
            da.innerHTML = '<div class="ag-panel"><div style="color:var(--error)">Error: ' + d.error + '</div><div class="ag-actions"><button class="btn btn-s btn-sm" type="button" onclick="genDraft(\'' + id + '\')">Retry</button></div></div>';
            return;
        }
        var headline = d.headline || 'Draft Recommendation';
        var badge = d.badge || '';
        var rationale = d.rationale || '';
        var action = d.action || '';
        var message = d.message || '';
        var raw = d.recommendation || 'Unable to generate';
        var badgeClass = 'ag-NO_ACTION';
        if (badge) {
            var bl = badge.toLowerCase();
            if (bl.indexOf('call') >= 0) { badgeClass = 'ag-CALL'; }
            else if (bl.indexOf('email') >= 0) { badgeClass = 'ag-DRAFT_OUTREACH'; }
        }
        var h = '<div class="ag-panel">';
        h += '<div class="ag-headline">' + headline + '</div>';
        if (action) {
            h += '<div class="dr-sect"><div class="dr-sect-label">Recommended action</div><span class="ag-badge ' + badgeClass + '">' + action + '</span></div>';
        }
        if (rationale) {
            h += '<div class="dr-sect"><div class="dr-sect-label">Rationale</div><div class="ag-rat" style="margin:0">' + rationale + '</div></div>';
        }
        var msgText = message || raw;
        h += '<div class="dr-sect"><div class="dr-sect-label">Draft message</div><div class="dr-msg" id="dt">' + msgText + '</div></div>';
        h += '<div class="ag-actions">';
        h += '<button class="btn btn-p btn-sm" type="button" onclick="copyDraftText()">Copy Message</button>';
        h += '<button class="btn btn-s btn-sm" type="button" onclick="genDraft(\''+id+'\')">Regenerate</button>';
        h += '</div>';
        h += '</div>';
        da.innerHTML = h;
    })
    .catch(e => {
        da.innerHTML = '<div class="ag-panel"><div style="color:var(--error)">Request failed: ' + e.message + '</div><div class="ag-actions"><button class="btn btn-s btn-sm" type="button" onclick="genDraft(\'' + id + '\')">Retry</button></div></div>';
    });
}

function copyDraftText(){var t=document.getElementById('dt');var txt=t.textContent||t.innerText;navigator.clipboard.writeText(txt).then(function(){alert('Draft message copied to clipboard. This is a draft, not a sent message.');}).catch(function(err){var temp=document.createElement('textarea');temp.value=txt;document.body.appendChild(temp);temp.select();document.execCommand('copy');document.body.removeChild(temp);alert('Draft message copied to clipboard. This is a draft, not a sent message.');});}

function runAgent(id){var ag=document.getElementById('ag');ag.innerHTML='<div class="loading">Analyzing...</div>';fetch('/api/agent/recommend/'+id+'?location_id='+locId).then(r=>r.json()).then(rec=>{if(rec.action==='ERROR'){ag.innerHTML='<div class="ag-panel"><div style="color:var(--error)">'+rec.rationale+'</div><button class="btn btn-s btn-sm" style="margin-top:12px" type="button" onclick="runAgent(\''+id+'\')">Retry</button></div>';return;}lastAgentRec=rec;var h='<div class="ag-panel">';var headline='Unknown';var badgeClass='NO_ACTION';var badgeText='';if(rec.action==='NO_ACTION'){headline='No follow-up needed';badgeClass='NO_ACTION';badgeText='No action';}else if(rec.action==='CALL'){headline='Follow-up call recommended';badgeClass='CALL';badgeText='Call';}else if(rec.action==='DRAFT_OUTREACH'){headline='Written outreach recommended';badgeClass='DRAFT_OUTREACH';badgeText='Message';}else if(rec.action==='FOLLOW_UP'){headline='Schedule follow-up';badgeClass='FOLLOW_UP';badgeText='Follow-up';}else if(rec.action==='STAFF_REVIEW'){headline='Status needs review';badgeClass='STAFF_REVIEW';badgeText='Review needed';}h+='<div class="ag-headline">'+headline+'<span class="ag-badge ag-'+badgeClass+'">'+badgeText+'</span></div>';var shortRat=rec.rationale||'No rationale provided.';if(shortRat.length>240){var sentences=shortRat.match(/[^.!?]+[.!?]+/g)||[shortRat];if(sentences.length>2){shortRat=sentences.slice(0,2).join(' ');}}h+='<div class="ag-rat">'+shortRat+'</div>';function humanEvidence(e){var m={'converted_flag=True':'Converted','converted_flag=False':'Not converted','converted_flag=true':'Converted','converted_flag=false':'Not converted','booking_status=booked':'Appointment booked','clinic_capacity=available':'Capacity available','clinic_capacity=limited':'Limited capacity','first_response_hours=None':'No response recorded','first_response_hours=null':'No response recorded'};for(var k in m){if(e.indexOf(k)>=0)return m[k];}if(e.indexOf('effective_touchpoints=')===0){var n=e.split('=')[1];return n+' contact'+(n==='1'?' attempt':' attempts');}if(e.indexOf('status=')===0){var s=e.split('=')[1];var statusMap={'new':'New lead','contacted':'Contacted','qualified':'Qualified','lost':'Lost'};return statusMap[s.toLowerCase()]||('Status: '+s);}if(e.indexOf('first_response_hours=')===0){var hr=e.split('=')[1];var hrs=parseFloat(hr);if(!isNaN(hrs)){if(hrs<24)return'Quick response ('+hrs.toFixed(0)+'h)';if(hrs<48)return'Response in '+hrs.toFixed(0)+' hours';return'Slow response ('+hrs.toFixed(0)+'h)';}}return e;}var statusConflict=false;if(rec.evidence){var hasConverted=rec.evidence.some(function(e){return e.indexOf('converted_flag=true')>=0||e.indexOf('converted_flag=True')>=0;});var hasQualLost=rec.evidence.some(function(e){return e.indexOf('status=')>=0&&(e.indexOf('Qualified')>=0||e.indexOf('Lost')>=0);});var statusNew=rec.evidence.some(function(e){return e.indexOf('status=')>=0&&(e.indexOf('New')>=0||e.indexOf('Contacted')>=0);});if(hasConverted&&statusNew){statusConflict=true;}else if(!hasConverted&&hasQualLost){statusConflict=true;}}if(statusConflict){h+='<div class="ag-status-note"><strong>Status needs review:</strong> Lead status and conversion flag disagree. Review data before taking action.</div>';}if(rec.evidence&&rec.evidence.length>0){h+='<div class="ag-ev">';rec.evidence.forEach(function(e){h+='<span class="ag-ev-item">'+humanEvidence(e)+'</span>';});h+='</div>';}if(rec.missing_info&&rec.missing_info.length>0){h+='<div class="ag-miss"><strong>Missing information:</strong> ';rec.missing_info.forEach(function(m,i){h+=(i>0?', ':'')+m;});h+='</div>';}if(rec.tool_calls&&rec.tool_calls.length>0){var agentCalls=rec.tool_calls.filter(function(tc){return tc.description;});if(agentCalls.length>0){h+='<div class="ag-disclosure"><button class="ag-disclosure-btn" type="button" onclick="toggleAgentDisclosure(this)" aria-expanded="false"><span class="ag-disclosure-chevron">&#9654;</span>Technical details · '+agentCalls.length+' tool call'+(agentCalls.length===1?'':'s')+'</button><div class="ag-disclosure-content">';agentCalls.forEach(function(tc){h+='<div class="ag-tool"><div class="ag-tool-name">'+tc.tool+'</div><div class="ag-tool-desc">'+tc.description+'</div><div class="ag-tool-res">'+JSON.stringify(tc.result,null,2)+'</div></div>';});h+='<div class="ag-impl-note">Implementation: '+(rec.is_fallback?'Rule-based decision engine (deterministic fallback)':'AI-enhanced analysis via ai_gen SQL function')+'</div>';h+='</div></div>';}}h+='<div class="ag-actions">';if(rec.action!=='NO_ACTION'){h+='<button class="btn btn-p btn-sm" type="button" onclick="approveAgent(\''+id+'\')">Approve & Persist</button>';}h+='<button class="btn btn-s btn-sm" type="button" onclick="runAgent(\''+id+'\')">Re-run analysis</button></div>';h+='</div>';ag.innerHTML=h;}).catch(e=>{ag.innerHTML='<div class="ag-panel"><div style="color:var(--error)">Request failed: '+e.message+'</div><button class="btn btn-s btn-sm" style="margin-top:12px" type="button" onclick="runAgent(\''+id+'\')">Retry</button></div>';});}
function approveAgent(id){if(!lastAgentRec){alert('No recommendation to approve');return;}var ag=document.getElementById('ag');ag.innerHTML='<div class="loading">Persisting recommendation...</div>';var fd=new FormData();fd.append('location_id',locId);fd.append('snapshot_date',snapDate);fd.append('recommendation',JSON.stringify(lastAgentRec));fetch('/api/agent/approve/'+id,{method:'POST',body:fd}).then(r=>r.json()).then(d=>{if(d.success){ag.innerHTML='<div class="ag-panel"><div style="color:var(--success);font-weight:600">OK: '+d.message+'</div>'+(d.task_id?'<div style="font-size:13px;color:var(--text2);margin-top:4px">Task ID: '+d.task_id+'</div>':'')+(d.draft?'<div style="margin-top:8px"><div class="fb-note">Draft generated:</div><textarea rows="4" style="width:100%;padding:8px;border:1px solid var(--border);border-radius:4px;font-size:13px;margin-top:4px">'+d.draft+'</textarea></div>':'')+'</div>';}else{ag.innerHTML='<div class="ag-panel"><div style="color:var(--error)">Failed: '+d.error+'</div><button class="btn btn-s btn-sm" style="margin-top:8px" type="button" onclick="runAgent(\''+id+'\')">Retry</button></div>';}}).catch(e=>{ag.innerHTML='<div class="ag-panel"><div style="color:var(--error)">Request failed: '+e.message+'</div></div>';});}
function toggleAgentDisclosure(btn){var c=btn.nextElementSibling;var chevron=btn.querySelector('.ag-disclosure-chevron');if(c.classList.contains('show')){c.classList.remove('show');chevron.classList.remove('open');}else{c.classList.add('show');chevron.classList.add('open');}}
function logOut(id){var o=document.getElementById('os').value,n=document.getElementById('on').value,fd=document.getElementById('fd').value,r=document.getElementById('ar');if(!o){r.innerHTML='<div style="color:var(--error);font-size:13px">Please select an outcome.</div>';return;}r.innerHTML='<div class="loading">Saving...</div>';var fd2=new FormData();fd2.append('location_id',locId);fd2.append('snapshot_date',snapDate);fd2.append('outcome',o);fd2.append('note',n);fd2.append('follow_up_date',fd);fetch('/api/lead/'+id+'/action',{method:'POST',body:fd2}).then(r=>r.json()).then(d=>{if(d.success){var ln=document.getElementById('dtitle')?document.getElementById('dtitle').textContent.trim():id;if(d.removed_from_queue){var row=document.getElementById('row-'+id);if(row)row.remove();closeDrawer();var mc=document.querySelectorAll('.mc .val');if(mc.length>=3){mc[0].textContent=parseInt(mc[0].textContent||'0')-1;mc[2].textContent=parseInt(mc[2].textContent||'0')+1;}var cnt=document.querySelector('.cnt');if(cnt){var nums=cnt.textContent.match(/\d+/g);if(nums&&nums.length>=2){var shown=parseInt(nums[0])-1;var total=parseInt(nums[1])-1;if(cnt.textContent.indexOf('Showing')>=0){cnt.textContent='Showing '+shown+' of '+total+' eligible leads';}else{cnt.textContent=shown+' matches of '+total+' eligible leads';}}}document.getElementById('os').value='';document.getElementById('on').value='';document.getElementById('fd').value='';}else{r.innerHTML='<div style="color:var(--success);font-size:14px;font-weight:600">Saved: '+d.outcome+'</div>';document.getElementById('os').value='';document.getElementById('on').value='';document.getElementById('fd').value='';}if(d.followup){addFollowup(d.followup,ln);}}else{r.innerHTML='<div style="color:var(--error);font-size:13px">Error: '+d.error+'</div>';}}).catch(e=>{r.innerHTML='<div style="color:var(--error);font-size:13px">Error: '+e.message+'</div>';});}
function addFollowup(fu,ln){var td=document.getElementById('tasks');var tl=td.querySelector('.tlist');if(!tl){var em=td.querySelector('.empty');if(em)em.remove();tl=document.createElement('div');tl.className='tlist';var h=td.querySelector('h2');if(h)h.after(tl);else td.insertBefore(tl,td.firstChild);}var ex=tl.querySelector('[data-lid="'+fu.lead_id+'"]');if(ex)ex.remove();var it=document.createElement('div');it.className='titem';it.setAttribute('data-lid',fu.lead_id);it.setAttribute('role','button');it.setAttribute('tabindex','0');it.style.cssText='cursor:pointer;transition:all 150ms;';it.setAttribute('aria-label','Open '+ln+' lead details');it.setAttribute('title','Click to open lead');it.onclick=function(){openLead(fu.lead_id);};it.onkeypress=function(e){if(e.key==='Enter')openLead(fu.lead_id);};var info=document.createElement('div');info.className='info';info.style.flex='1';var dec=(fu.decision||'follow up').replace(/_/g,' ').split(' ').map(function(w){return w.charAt(0).toUpperCase()+w.slice(1);}).join(' ');info.innerHTML='<div class="n">'+ln+'</div><div class="m">'+fu.lead_id+' &middot; '+dec+'</div>'+(fu.rationale?'<div class="m">'+fu.rationale+'</div>':'');it.appendChild(info);var badge=document.createElement('span');if(fu.due_label==='Due Now'){badge.style.cssText='font-size:12px;font-weight:600;padding:4px 10px;border-radius:6px;background:#FEE2E2;color:#991B1B';badge.textContent='Due Now';}else{badge.style.cssText='font-size:12px;font-weight:600;padding:4px 10px;border-radius:6px;background:#DBEAFE;color:#1E40AF';badge.textContent='Scheduled'+(fu.due_date?' - '+fu.due_date:'');}it.appendChild(badge);tl.insertBefore(it,tl.firstChild);var mc=document.querySelectorAll('.mc .val');if(mc.length>=2){mc[1].textContent=parseInt(mc[1].textContent||'0')+1;}}
function saveN(id){var n=document.getElementById('sn').value,r=document.getElementById('nr');if(!n.trim()){r.innerHTML='<div style="color:var(--error);font-size:13px">Note cannot be empty.</div>';return;}r.innerHTML='<div class="loading">Saving...</div>';var fd=new FormData();fd.append('location_id',locId);fd.append('snapshot_date',snapDate);fd.append('note',n);fetch('/api/lead/'+id+'/note',{method:'POST',body:fd}).then(r=>r.json()).then(d=>{if(d.success){r.innerHTML='<div style="color:var(--success);font-size:14px;font-weight:600">Note saved</div>';document.getElementById('sn').value='';}else{r.innerHTML='<div style="color:var(--error);font-size:13px">Error: '+d.error+'</div>';}});}
function closeL(id){var r=document.getElementById('cr').value,rr=document.getElementById('clr');if(!r.trim()){rr.innerHTML='<div style="color:var(--error);font-size:13px">Reason is required.</div>';return;}rr.innerHTML='<div class="loading">Closing...</div>';var fd=new FormData();fd.append('location_id',locId);fd.append('snapshot_date',snapDate);fd.append('reason',r);fetch('/api/lead/'+id+'/close',{method:'POST',body:fd}).then(r=>r.json()).then(d=>{if(d.success){rr.innerHTML='<div style="color:var(--success);font-size:14px;font-weight:600">Lead closed</div>';document.getElementById('cr').value='';}else{rr.innerHTML='<div style="color:var(--error);font-size:13px">Error: '+d.error+'</div>';}});}
function closeDrawer(){var ovr=document.getElementById('overlay'),dr=document.getElementById('drawer');ovr.classList.remove('show');ovr.setAttribute('aria-hidden','true');dr.classList.remove('show');dr.setAttribute('hidden','');dr.setAttribute('aria-hidden','true');document.querySelectorAll('.qt tr').forEach(r=>r.classList.remove('sel'));if(lastFocusedElement)lastFocusedElement.focus();}
function trapDrawerFocus(e){var dr=document.getElementById('drawer');if(!dr.classList.contains('show'))return;if(e.key==='Tab'){var f=dr.querySelectorAll('button:not([disabled]),[href],input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex="0"]');var v=[];f.forEach(function(el){if(el.offsetParent!==null||el===document.activeElement)v.push(el);});if(v.length===0)return;var first=v[0],last=v[v.length-1];if(e.shiftKey&&(document.activeElement===first||document.activeElement===dr)){e.preventDefault();last.focus();}else if(!e.shiftKey&&document.activeElement===last){e.preventDefault();first.focus();}}}
document.addEventListener('keydown',trapDrawerFocus);
function toggleExpand(btn){var targetId=btn.getAttribute('aria-controls');var content=document.getElementById(targetId);if(!content)return;var arrow=btn.querySelector('.arrow');var isExpanded=content.classList.toggle('show');arrow.textContent=isExpanded?'▼':'▶';btn.setAttribute('aria-expanded',isExpanded);}
document.addEventListener('keydown',function(e){if(e.key==='Escape'&&document.getElementById('drawer').classList.contains('show')){closeDrawer();}});
</script></body></html>
"""
import json
import os
import datetime
from flask import Flask, request, render_template_string, jsonify
from databricks.sdk import WorkspaceClient

app = Flask(__name__)

try:
    w = WorkspaceClient()
except Exception as e:
    w = None
    print(f"SDK init failed: {e}")

SOURCE = "workspace.chiro_hackathon"
OUTPUT = "workspace.chiro_agent_demo"
WH_ID = "57129d05302f658f"
DEMO_SNAPSHOT = "2026-10-04"
DEMO_LOCATION = "LOC001"
DEMO_LOOKBACK = "120"
DEFAULT_FOLLOWUP_DAYS = 7  # Default follow-up interval when no date is selected

_FIRST = ['James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','William','Elizabeth',
           'David','Barbara','Richard','Susan','Joseph','Jessica','Thomas','Sarah','Charles','Karen',
           'Christopher','Nancy','Daniel','Lisa','Matthew','Margaret','Anthony','Sandra','Mark','Ashley',
           'Donald','Kimberly','Steven','Emily','Paul','Donna','Andrew','Michelle','Joshua','Carol',
           'Kenneth','Amanda','Kevin','Melissa','Brian','Deborah','George','Stephanie','Edward','Rebecca']
_LAST = ['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez',
         'Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin',
         'Lee','Perez','Thompson','White','Harris','Sanchez','Clark','Ramirez','Lewis','Robinson',
         'Walker','Young','Allen','King','Wright','Scott','Torres','Nguyen','Hill','Flores',
         'Green','Adams','Nelson','Baker','Hall','Rivera','Campbell','Mitchell','Carter','Roberts']

def _name_sql(col="lead_id"):
    fa = ",".join(f"'{n}'" for n in _FIRST)
    la = ",".join(f"'{n}'" for n in _LAST)
    return f"""array({fa})[abs(hash({col})) % 50] as first_name,
        array({la})[abs(hash({col})) % 50] as last_name,
        concat('(555) ', lpad(cast((abs(hash({col})) % 900) + 100 as string), 3, '0'), '-', lpad(cast((abs(hash(concat({col},'b'))) % 9000) + 1000 as string), 4, '0')) as phone,
        concat(lower(array({fa})[abs(hash({col})) % 50]), '.', lower(array({la})[abs(hash({col})) % 50]), '@email.com') as email"""

def run_sql(sql_text):
    if not w:
        raise Exception("Databricks SDK not initialized")
    resp = w.statement_execution.execute_statement(
        statement=sql_text, warehouse_id=WH_ID, wait_timeout="50s")
    if not resp.result or not resp.result.data_array:
        return []
    cols = [c.name for c in resp.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in resp.result.data_array]

def esc(s):
    return str(s).replace("'", "''") if s is not None else ""

def get_locations():
    try:
        return run_sql(f"SELECT location_id, location_name, city, state FROM {SOURCE}.locations ORDER BY location_id")
    except:
        return []

def get_metrics(location_id, snapshot_date, lookback, today=""):
    date_expr = f"'{today}'" if today else "current_date()"
    rows = run_sql(f"""
        SELECT
            COUNT(*) as total_at_clinic,
            SUM(CASE WHEN converted_flag = true THEN 1 ELSE 0 END) as converted,
            SUM(CASE WHEN lower(trim(status)) = 'lost' AND converted_flag = false THEN 1 ELSE 0 END) as lost,
            SUM(CASE WHEN lower(trim(status)) IN ('new','contacted','qualified') AND converted_flag = false THEN 1 ELSE 0 END) as open_leads,
            SUM(CASE WHEN lower(trim(status)) IN ('new','contacted','qualified')
                AND converted_flag = false AND num_touchpoints BETWEEN 0 AND 2
                AND datediff('{snapshot_date}', created_date) BETWEEN 0 AND {lookback}
                THEN 1 ELSE 0 END) as eligible,
            SUM(CASE WHEN lower(trim(status)) IN ('new','contacted','qualified')
                AND converted_flag = false AND num_touchpoints >= 3
                AND datediff('{snapshot_date}', created_date) BETWEEN 0 AND {lookback}
                THEN 1 ELSE 0 END) as contact_capped
        FROM {SOURCE}.leads WHERE assigned_location_id = '{location_id}'""")
    r = rows[0] if rows else {}
    try:
        tr = run_sql(f"""
            SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks
            WHERE location_id = '{location_id}' AND review_status = 'pending_review'
            AND (
                decision != 'FOLLOW_UP_SCHEDULED'
                OR coalesce(to_date(nullif(regexp_extract(rationale, 'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1), '')), {date_expr}) <= {date_expr}
            )""")
        followups = int(tr[0]["cnt"]) if tr else 0
    except:
        followups = 0
    try:
        ar = run_sql(f"SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks WHERE location_id = '{location_id}' AND decision = 'OUTREACH_LOGGED' AND review_status = 'completed'")
        actions_logged = int(ar[0]["cnt"]) if ar else 0
    except:
        actions_logged = 0
    return {
        "total_at_clinic": int(r.get("total_at_clinic", 0) or 0),
        "converted": int(r.get("converted", 0) or 0),
        "lost": int(r.get("lost", 0) or 0),
        "open_leads": int(r.get("open_leads", 0) or 0),
        "eligible": int(r.get("eligible", 0) or 0),
        "contact_capped": int(r.get("contact_capped", 0) or 0),
        "followups_due": followups,
        "actions_logged": actions_logged,
    }

def get_eligible_leads(location_id, snapshot_date, lookback, page=1, per_page=15, search="", today=""):
    offset = (page - 1) * per_page
    date_expr = f"'{today}'" if today else "current_date()"
    ns = _name_sql("lead_id")
    search_clause = ""
    if search:
        sl = esc(search.lower())
        search_clause = f" AND (lower(source) LIKE '%{sl}%' OR lower(lead_id) LIKE '%{sl}%')"
    total_row = run_sql(f"""
        SELECT COUNT(*) as cnt FROM {SOURCE}.leads
        WHERE assigned_location_id = '{location_id}'
          AND lower(trim(status)) IN ('new','contacted','qualified')
          AND converted_flag = false AND num_touchpoints BETWEEN 0 AND 2
          AND datediff('{snapshot_date}', created_date) BETWEEN 0 AND {lookback}""")
    total_eligible = int(total_row[0]["cnt"]) if total_row else 0
    count_row = run_sql(f"""
        SELECT COUNT(*) as cnt FROM {SOURCE}.leads
        WHERE assigned_location_id = '{location_id}'
          AND lower(trim(status)) IN ('new','contacted','qualified')
          AND converted_flag = false AND num_touchpoints BETWEEN 0 AND 2
          AND datediff('{snapshot_date}', created_date) BETWEEN 0 AND {lookback}
          {search_clause}""")
    total = int(count_row[0]["cnt"]) if count_row else 0
    leads = run_sql(f"""
        WITH fu_dates AS (
            SELECT lead_id as fu_lead_id, regexp_extract(rationale, 'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1) as follow_up_date
            FROM {OUTPUT}.followup_tasks
            WHERE location_id = '{location_id}' AND decision = 'FOLLOW_UP_SCHEDULED' AND review_status = 'pending_review'
        )
        SELECT lead_id, source, status, num_touchpoints, first_response_hours, created_date, {ns},
            CASE WHEN first_response_hours IS NULL THEN 'No response recorded'
                WHEN first_response_hours > 48 THEN CONCAT('Slow first response: ', cast(round(first_response_hours) as string), ' hrs')
                ELSE CONCAT('First response in ', cast(round(first_response_hours) as string), ' hrs') END as priority_reason,
            CASE WHEN num_touchpoints = 0 THEN 'No contact attempts'
                WHEN num_touchpoints = 1 THEN '1 prior attempt'
                ELSE CONCAT(cast(num_touchpoints as string), ' prior attempts') END as touch_info
        FROM {SOURCE}.leads
        LEFT JOIN fu_dates ON lead_id = fu_lead_id
        WHERE assigned_location_id = '{location_id}'
          AND lower(trim(status)) IN ('new','contacted','qualified')
          AND converted_flag = false AND num_touchpoints BETWEEN 0 AND 2
          AND datediff('{snapshot_date}', created_date) BETWEEN 0 AND {lookback}
          {search_clause}
        ORDER BY CASE WHEN to_date(nullif(follow_up_date, '')) > {date_expr} THEN 4
            WHEN first_response_hours IS NULL OR first_response_hours > 48 THEN 1
            WHEN first_response_hours > 24 THEN 2
            ELSE 3 END ASC,
            num_touchpoints ASC, created_date DESC
        LIMIT {per_page} OFFSET {offset}""")
    try:
        outreach_rows = run_sql(f"""
            SELECT lead_id, COUNT(*) as cnt FROM {OUTPUT}.followup_tasks
            WHERE decision = 'OUTREACH_LOGGED' AND location_id = '{location_id}'
            GROUP BY lead_id""")
        outreach_map = {r["lead_id"]: int(r["cnt"]) for r in outreach_rows}
    except:
        outreach_map = {}
    # Get recent outreach with follow-up dates for priority calculation
    try:
        recent_outreach = run_sql(f"""
            WITH latest_contact AS (
                SELECT lead_id, MAX(reviewed_at) as last_contact_at
                FROM {OUTPUT}.followup_tasks
                WHERE location_id = '{location_id}' AND decision = 'OUTREACH_LOGGED' AND review_status = 'completed'
                GROUP BY lead_id
            ),
            latest_followup AS (
                SELECT lead_id, 
                    regexp_extract(rationale, 'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1) as follow_up_date
                FROM {OUTPUT}.followup_tasks
                WHERE location_id = '{location_id}' AND decision = 'FOLLOW_UP_SCHEDULED' AND review_status = 'pending_review'
            )
            SELECT c.lead_id, c.last_contact_at, f.follow_up_date
            FROM latest_contact c
            LEFT JOIN latest_followup f ON c.lead_id = f.lead_id""")
        contact_map = {r["lead_id"]: {"last_contact": r["last_contact_at"], "follow_up_date": r.get("follow_up_date")} for r in recent_outreach}
    except:
        contact_map = {}
    
    from datetime import datetime, date
    if today:
        today = datetime.strptime(today, "%Y-%m-%d").date()
    else:
        today = date.today()
    
    for i, lead in enumerate(leads):
        lead["rank"] = offset + i + 1
        eff_touches = int(lead.get("num_touchpoints", 0) or 0) + outreach_map.get(lead["lead_id"], 0)
        lead["effective_touchpoints"] = eff_touches
        if eff_touches == 0:
            lead["touch_info"] = "No contact attempts"
        elif eff_touches == 1:
            lead["touch_info"] = "1 prior attempt"
        else:
            lead["touch_info"] = f"{eff_touches} prior attempts"
        frh = lead.get("first_response_hours")
        # Convert to float for comparison (SQL results may be strings)
        if frh is not None:
            try:
                frh = float(frh)
            except (ValueError, TypeError):
                frh = None
        
        # Check for recent outreach to adjust priority
        contact_info = contact_map.get(lead["lead_id"])
        has_recent_contact = False
        follow_up_due = False
        
        if contact_info and contact_info.get("last_contact"):
            follow_up_date_str = contact_info.get("follow_up_date")
            if follow_up_date_str:
                try:
                    follow_up_date = datetime.strptime(follow_up_date_str, "%Y-%m-%d").date()
                    if follow_up_date > today:
                        # Follow-up is in the future - recently contacted, low priority
                        has_recent_contact = True
                        lead["priority_label"] = f"Low — Recently contacted"
                        lead["priority_color"] = "success"
                        lead["priority_reason"] = f"Awaiting follow-up on {follow_up_date_str}"
                    else:
                        # Follow-up date has passed - recalculate priority
                        follow_up_due = True
                except (ValueError, TypeError):
                    pass
        
        # If no recent contact or follow-up is due, use standard priority rules
        if not has_recent_contact:
            if frh is None:
                lead["priority_label"] = "High — No response"
                lead["priority_color"] = "error"
            elif frh > 48:
                lead["priority_label"] = "High — Slow response"
                lead["priority_color"] = "error"
            elif frh > 24:
                lead["priority_label"] = "Medium — Delayed"
                lead["priority_color"] = "warning"
            else:
                lead["priority_label"] = "Standard"
                lead["priority_color"] = "success"
        # Data quality: detect inconsistent status / touchpoint / first_response
        status_lower = (lead.get("status") or "").strip().lower()
        if status_lower == "contacted" and eff_touches == 0 and frh is not None:
            lead["data_quality_warning"] = (
                f"Status shows 'Contacted' but no touchpoints recorded "
                f"despite a {frh:.1f}-hour first-response time. "
                "Contact may not have been properly logged."
            )
        elif status_lower == "contacted" and eff_touches == 0 and frh is None:
            lead["data_quality_warning"] = (
                "Status shows 'Contacted' but no contact record "
                "or response time exists. Verify actual contact history."
            )
        elif status_lower == "new" and eff_touches > 0:
            lead["data_quality_warning"] = (
                f"Status shows 'New' but {eff_touches} touchpoint(s) "
                "are recorded. Status may be stale."
            )
        elif status_lower == "qualified" and eff_touches == 0 and frh is not None:
            lead["data_quality_warning"] = (
                f"Status shows 'Qualified' but no touchpoints recorded "
                f"despite a {frh:.1f}-hour first-response time. "
                "Qualification may not have been properly logged."
            )
        elif status_lower == "qualified" and eff_touches == 0 and frh is None:
            lead["data_quality_warning"] = (
                "Status shows 'Qualified' but no contact record "
                "or response time exists. Verify actual qualification history."
            )
        else:
            lead["data_quality_warning"] = None
    
    # Re-sort leads to put recently contacted (Low priority) at the bottom
    def priority_sort_key(lead):
        priority_color = lead.get("priority_color", "success")
        # error (High) = 0, warning (Medium) = 1, success (Low/Standard) = 2
        if priority_color == "error":
            return (0, lead.get("effective_touchpoints", 0))
        elif priority_color == "warning":
            return (1, lead.get("effective_touchpoints", 0))
        else:
            # For success (Low or Standard), check if recently contacted
            if "Recently contacted" in lead.get("priority_label", ""):
                return (3, lead.get("effective_touchpoints", 0))  # Recently contacted goes last
            else:
                return (2, lead.get("effective_touchpoints", 0))  # Standard priority
    
    leads.sort(key=priority_sort_key)
    # Update ranks after sorting
    for i, lead in enumerate(leads):
        lead["rank"] = offset + i + 1
    
    return {"leads": leads, "total": total, "total_eligible": total_eligible, "page": page, "per_page": per_page, "pages": max(1, (total + per_page - 1) // per_page)}

def get_lead_detail(lead_id, location_id, snapshot_date):
    ns = _name_sql("lead_id")
    rows = run_sql(f"""
        SELECT lead_id, source, status, num_touchpoints, first_response_hours, created_date,
            converted_flag, assigned_location_id, {ns}
        FROM {SOURCE}.leads WHERE lead_id = '{lead_id}'""")
    if not rows:
        return None
    lead = rows[0]
    try:
        oc = run_sql(f"SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks WHERE lead_id = '{lead_id}' AND decision = 'OUTREACH_LOGGED'")
        lead["effective_touchpoints"] = int(lead.get("num_touchpoints", 0) or 0) + (int(oc[0]["cnt"]) if oc else 0)
    except:
        lead["effective_touchpoints"] = int(lead.get("num_touchpoints", 0) or 0)
    try:
        actions = run_sql(f"""
            SELECT task_id, decision, rationale, draft_text, review_status, reviewed_at, outcome, outcome_at, is_simulated
            FROM {OUTPUT}.followup_tasks WHERE lead_id = '{lead_id}' ORDER BY reviewed_at DESC""")
    except:
        actions = []
    lead["actions"] = actions
    try:
        cycles = run_sql(f"""
            SELECT cycle_id, cycle_number, channel, timing_days, cycle_status, draft_text
            FROM {OUTPUT}.nurturing_cycles WHERE lead_id = '{lead_id}' ORDER BY cycle_number""")
    except:
        cycles = []
    lead["nurture_cycles"] = cycles
    # Compute priority (same logic as get_eligible_leads for consistency)
    frh = lead.get("first_response_hours")
    if frh is not None:
        try:
            frh = float(frh)
        except (ValueError, TypeError):
            frh = None
    
    # Check for recent outreach to adjust priority
    from datetime import datetime, date
    today = date.today()
    has_recent_contact = False
    
    try:
        recent_contact = run_sql(f"""
            SELECT MAX(reviewed_at) as last_contact_at
            FROM {OUTPUT}.followup_tasks
            WHERE lead_id = '{lead_id}' AND decision = 'OUTREACH_LOGGED' AND review_status = 'completed'
        """)
        if recent_contact and recent_contact[0].get("last_contact_at"):
            # Check for pending follow-up
            followup = run_sql(f"""
                SELECT regexp_extract(rationale, 'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1) as follow_up_date
                FROM {OUTPUT}.followup_tasks
                WHERE lead_id = '{lead_id}' AND decision = 'FOLLOW_UP_SCHEDULED' AND review_status = 'pending_review'
                ORDER BY reviewed_at DESC LIMIT 1
            """)
            if followup and followup[0].get("follow_up_date"):
                follow_up_date_str = followup[0]["follow_up_date"]
                try:
                    follow_up_date = datetime.strptime(follow_up_date_str, "%Y-%m-%d").date()
                    if follow_up_date > today:
                        # Follow-up is in the future - recently contacted, low priority
                        has_recent_contact = True
                        lead["priority_label"] = f"Low — Recently contacted"
                        lead["priority_color"] = "success"
                        lead["priority_reason"] = f"Awaiting follow-up on {follow_up_date_str}"
                except (ValueError, TypeError):
                    pass
    except:
        pass
    
    # If no recent contact or follow-up is due, use standard priority rules
    if not has_recent_contact:
        if frh is None:
            lead["priority_label"] = "High — No response"
            lead["priority_color"] = "error"
        elif frh > 48:
            lead["priority_label"] = "High — Slow response"
            lead["priority_color"] = "error"
        elif frh > 24:
            lead["priority_label"] = "Medium — Delayed"
            lead["priority_color"] = "warning"
        else:
            lead["priority_label"] = "Standard"
            lead["priority_color"] = "success"
    # Data quality warning (same logic as get_eligible_leads)
    eff_touches = lead.get("effective_touchpoints", 0)
    status_lower = (lead.get("status") or "").strip().lower()
    if status_lower == "contacted" and eff_touches == 0 and frh is not None:
        lead["data_quality_warning"] = (
            f"Status shows 'Contacted' but no touchpoints recorded "
            f"despite a {frh:.1f}-hour first-response time. "
            "Contact may not have been properly logged."
        )
    elif status_lower == "contacted" and eff_touches == 0 and frh is None:
        lead["data_quality_warning"] = (
            "Status shows 'Contacted' but no contact record "
            "or response time exists. Verify actual contact history."
        )
    elif status_lower == "new" and eff_touches > 0:
        lead["data_quality_warning"] = (
            f"Status shows 'New' but {eff_touches} touchpoint(s) "
            "are recorded. Status may be stale."
        )
    elif status_lower == "qualified" and eff_touches == 0 and frh is not None:
        lead["data_quality_warning"] = (
            f"Status shows 'Qualified' but no touchpoints recorded "
            f"despite a {frh:.1f}-hour first-response time. "
            "Qualification may not have been properly logged."
        )
    elif status_lower == "qualified" and eff_touches == 0 and frh is None:
        lead["data_quality_warning"] = (
            "Status shows 'Qualified' but no contact record "
            "or response time exists. Verify actual qualification history."
        )
    else:
        lead["data_quality_warning"] = None
    lead["snapshot_date"] = snapshot_date
    return lead

def log_outcome(lead_id, location_id, snapshot_date, outcome, note, follow_up_date):
    now = datetime.datetime.now().isoformat()
    task_id = f"{lead_id}_act_{now.replace(':','').replace('-','').replace('.','')}"
    rationale = f"Call outcome: {outcome}" + (f" — {note}" if note else "")
    run_sql(f"""
        INSERT INTO {OUTPUT}.followup_tasks (task_id, run_id, snapshot_date, lead_id, location_id, decision, evidence_json, rationale, draft_text, review_status, reviewed_at, outcome, outcome_at, is_simulated)
        VALUES ('{task_id}', 'manual_entry', '{snapshot_date}', '{lead_id}', '{location_id}', 'OUTREACH_LOGGED', '{{}}', '{esc(rationale)}', '{esc(note)}', 'completed', '{now}', '{esc(outcome)}', '{now}', true)""")
    if outcome == "Appointment booked":
        run_sql(f"UPDATE {SOURCE}.leads SET status = 'Qualified' WHERE lead_id = '{lead_id}'")
        # Resolve/supersede ALL pending follow-ups for this lead (not just FOLLOW_UP_SCHEDULED)
        run_sql(f"""
            UPDATE {OUTPUT}.followup_tasks
            SET review_status = 'superseded'
            WHERE lead_id = '{lead_id}' AND review_status = 'pending_review'""")
    elif outcome == "No longer interested":
        run_sql(f"UPDATE {SOURCE}.leads SET status = 'Lost' WHERE lead_id = '{lead_id}'")
        # Also resolve pending follow-ups when lead is lost
        run_sql(f"""
            UPDATE {OUTPUT}.followup_tasks
            SET review_status = 'superseded'
            WHERE lead_id = '{lead_id}' AND review_status = 'pending_review'""")
    elif outcome == "Connected":
        # Increment touchpoints and ensure lead exceeds the 2-touchpoint queue cap
        run_sql(f"UPDATE {SOURCE}.leads SET num_touchpoints = GREATEST(num_touchpoints + 1, 3) WHERE lead_id = '{lead_id}'")
    # Auto-schedule follow-up unless lead is closed (booked or lost) or was previously booked
    already_booked = False
    if outcome not in ("Appointment booked", "No longer interested"):
        booking_check = run_sql(f"SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks WHERE lead_id = '{lead_id}' AND outcome = 'Appointment booked' AND review_status = 'completed'")
        already_booked = int(booking_check[0]["cnt"]) > 0 if booking_check else False
    if outcome not in ("Appointment booked", "No longer interested") and not already_booked:
        # Use provided date or calculate default follow-up date
        if not follow_up_date:
            from datetime import timedelta
            follow_up_dt = datetime.datetime.now() + timedelta(days=DEFAULT_FOLLOWUP_DAYS)
            follow_up_date = follow_up_dt.strftime("%Y-%m-%d")
        
        # Supersede any remaining pending tasks for this lead before creating new follow-up
        run_sql(f"""
            UPDATE {OUTPUT}.followup_tasks
            SET review_status = 'superseded'
            WHERE lead_id = '{lead_id}' AND review_status = 'pending_review'""")
        fu_id = f"{lead_id}_fu_{now.replace(':','').replace('-','').replace('.','')}"
        run_sql(f"""
            INSERT INTO {OUTPUT}.followup_tasks (task_id, run_id, snapshot_date, lead_id, location_id, decision, evidence_json, rationale, draft_text, review_status, is_simulated)
            VALUES ('{fu_id}', 'manual_entry', '{snapshot_date}', '{lead_id}', '{location_id}', 'FOLLOW_UP_SCHEDULED', '{{}}', 'Follow-up scheduled for {esc(follow_up_date)} after {esc(outcome)}', '', 'pending_review', true)""")
        followup_info = {"lead_id": lead_id, "decision": "FOLLOW_UP_SCHEDULED", "rationale": f"Follow-up scheduled for {follow_up_date} after {outcome}", "due_date": follow_up_date, "due_label": "Scheduled"}
    else:
        followup_info = None
    return {"task_id": task_id, "followup": followup_info}

def save_note(lead_id, location_id, snapshot_date, note):
    now = datetime.datetime.now().isoformat()
    task_id = f"{lead_id}_note_{now.replace(':','').replace('-','').replace('.','')}"
    run_sql(f"""
        INSERT INTO {OUTPUT}.followup_tasks (task_id, run_id, snapshot_date, lead_id, location_id, decision, evidence_json, rationale, draft_text, review_status, reviewed_at, is_simulated)
        VALUES ('{task_id}', 'manual_entry', '{snapshot_date}', '{lead_id}', '{location_id}', 'STAFF_NOTE', '{{}}', 'Staff note: {esc(note)}', '{esc(note)}', 'completed', '{now}', true)""")
    return task_id

def close_lead(lead_id, location_id, snapshot_date, reason):
    now = datetime.datetime.now().isoformat()
    task_id = f"{lead_id}_close_{now.replace(':','').replace('-','').replace('.','')}"
    run_sql(f"UPDATE {SOURCE}.leads SET status = 'Lost' WHERE lead_id = '{lead_id}'")
    run_sql(f"""
        INSERT INTO {OUTPUT}.followup_tasks (task_id, run_id, snapshot_date, lead_id, location_id, decision, evidence_json, rationale, draft_text, review_status, reviewed_at, is_simulated)
        VALUES ('{task_id}', 'manual_entry', '{snapshot_date}', '{lead_id}', '{location_id}', 'LEAD_CLOSED', '{{}}', 'Lead closed: {esc(reason)}', 'Closed: {esc(reason)}', 'completed', '{now}', true)""")
    return task_id

def generate_draft(lead_id, location_id):
    ns = _name_sql("lead_id")
    lead_rows = run_sql(f"SELECT lead_id, source, status, num_touchpoints, first_response_hours, created_date, converted_flag, {ns} FROM {SOURCE}.leads WHERE lead_id = '{lead_id}'")
    if not lead_rows:
        return {"error": "Lead not found"}
    lead = lead_rows[0]
    if lead.get("converted_flag"):
        return {"recommendation": "This lead is already converted. No re-engagement draft needed.", "lead_id": lead_id, "is_fallback": True, "action": "No action", "rationale": "Lead is converted — no outreach needed.", "message": "", "headline": "No Draft Needed", "badge": "None"}
    try:
        agent_rec = agent_recommend(lead_id, location_id)
        if agent_rec.get("action") == "NO_ACTION":
            return {"recommendation": f"Agent recommendation: {agent_rec.get('rationale', 'No action needed')}", "lead_id": lead_id, "is_fallback": True, "action": "No action", "rationale": agent_rec.get('rationale', 'No action needed'), "message": "", "headline": "No Draft Needed", "badge": "None"}
    except Exception:
        pass
    clinic_rows = run_sql(f"SELECT location_name, city, state FROM {SOURCE}.locations WHERE location_id = '{location_id}'")
    clinic_name = clinic_rows[0]["location_name"] if clinic_rows else "the clinic"
    fname = lead.get("first_name", "there")
    source = lead.get("source", "unknown")
    status = lead.get("status", "unknown")
    touches = lead.get("num_touchpoints", 0)
    frh = lead.get("first_response_hours")
    if frh is not None:
        try:
            frh = float(frh)
        except (ValueError, TypeError):
            frh = None
    frh_text = f"{round(frh)} hours" if frh is not None else "no response recorded"
    prompt = f"""You are a lead recovery assistant for a chiropractic clinic. Generate a brief follow-up message.

VERIFIED FACTS (use only these):
- Lead name: {fname}
- Source: {source}
- Status: {status}
- Previous contact attempts: {touches}
- First response time: {frh_text}
- Clinic name: {clinic_name}
- Converted: No (lead is not yet converted, needs re-engagement)

RULES:
- Do NOT include any phone number (clinic phone not configured)
- Do NOT offer free consultations, discounts, or promotions
- Do NOT make medical claims
- Do NOT use placeholders like [Your Name]
- Keep under 100 words

Generate:
1. RECOMMENDED ACTION: [Call or Email]
2. RATIONALE: [1-2 sentences]
3. DRAFT MESSAGE: [Professional message to {fname}]"""
    try:
        # Properly escape the prompt for SQL (double up single quotes)
        escaped_prompt = prompt.replace("'", "''")
        result = run_sql(f"SELECT ai_gen('{escaped_prompt}') as recommendation")
        rec = result[0]["recommendation"] if result else "Unable to generate"
    except Exception as gen_err:
        print(f"AI generation error: {gen_err}")
        rec = f"Template fallback: Hi {fname}, thank you for your interest in {clinic_name}. We would like to follow up about your inquiry from {source}. Please contact our office to schedule a visit at your convenience."
    # Parse the AI response into structured fields for the UI
    import re as _re
    action = ""
    rationale = ""
    message = ""
    if rec and not rec.startswith("Template fallback"):
        lines = rec.strip().split('\n')
        current_section = None
        for line in lines:
            stripped = line.strip()
            if _re.match(r'^\d+\.?\s*RECOMMENDED\s*ACTION', stripped, _re.IGNORECASE):
                current_section = 'action'
                parts = stripped.split(':', 1)
                if len(parts) > 1:
                    action = parts[1].strip()
            elif _re.match(r'^\d+\.?\s*RATIONALE', stripped, _re.IGNORECASE):
                current_section = 'rationale'
                parts = stripped.split(':', 1)
                if len(parts) > 1:
                    rationale = parts[1].strip()
            elif _re.match(r'^\d+\.?\s*DRAFT\s*MESSAGE', stripped, _re.IGNORECASE):
                current_section = 'message'
                parts = stripped.split(':', 1)
                if len(parts) > 1:
                    message = parts[1].strip()
            else:
                if current_section == 'action' and stripped:
                    action = (action + ' ' + stripped).strip() if action else stripped
                elif current_section == 'rationale' and stripped:
                    rationale = (rationale + ' ' + stripped).strip() if rationale else stripped
                elif current_section == 'message' and stripped:
                    message = (message + '\n' + stripped).strip() if message else stripped
        action = action.strip()
        rationale = rationale.strip()
        message = message.strip()
    if not action and not rationale and not message:
        message = rec
    badge = ""
    if 'call' in action.lower():
        badge = 'Call'
    elif 'email' in action.lower():
        badge = 'Email'
    elif action:
        badge = action[:20]
    return {"recommendation": rec, "lead_id": lead_id, "is_fallback": rec.startswith("Template fallback"), "action": action, "rationale": rationale, "message": message, "headline": "Draft Recommendation", "badge": badge}

# ---------------------------------------------------------------------------
# AGENT: Next-Action Recommendation Engine
# Each tool function queries the database and returns structured results.
# The agent calls tools in sequence, records evidence, and makes a decision.
# ---------------------------------------------------------------------------

def _tool_get_lead_facts(lead_id, location_id):
    """Tool: Retrieve verified lead facts from the leads table."""
    ns = _name_sql("lead_id")
    rows = run_sql(f"""
        SELECT lead_id, source, status, num_touchpoints, first_response_hours,
               created_date, converted_flag, converted_patient_id, assigned_location_id, {ns}
        FROM {SOURCE}.leads WHERE lead_id = '{lead_id}'""")
    if not rows:
        return {"found": False}
    r = rows[0]
    frh = r.get("first_response_hours")
    if frh is not None:
        try:
            frh = float(frh)
        except (ValueError, TypeError):
            frh = None
    return {
        "found": True, "lead_id": r["lead_id"], "source": r.get("source"),
        "status": r.get("status"), "num_touchpoints": int(r.get("num_touchpoints", 0) or 0),
        "first_response_hours": frh, "created_date": str(r.get("created_date", "")),
        "converted_flag": bool(r.get("converted_flag")),
        "converted_patient_id": r.get("converted_patient_id"),
        "first_name": r.get("first_name"), "last_name": r.get("last_name"),
    }

def _tool_get_contact_history(lead_id):
    """Tool: Retrieve logged contact history from followup_tasks."""
    try:
        rows = run_sql(f"""
            SELECT decision, outcome, rationale, reviewed_at, review_status
            FROM {OUTPUT}.followup_tasks
            WHERE lead_id = '{lead_id}' AND decision = 'OUTREACH_LOGGED'
            ORDER BY reviewed_at DESC""")
        return {"history": rows, "count": len(rows)}
    except Exception:
        return {"history": [], "count": 0}

def _tool_check_existing_tasks(lead_id):
    """Tool: Check for pending follow-up tasks."""
    try:
        rows = run_sql(f"""
            SELECT task_id, decision, rationale, review_status
            FROM {OUTPUT}.followup_tasks
            WHERE lead_id = '{lead_id}' AND review_status = 'pending_review'
            ORDER BY reviewed_at DESC""")
        return {"pending_tasks": rows, "count": len(rows)}
    except Exception:
        return {"pending_tasks": [], "count": 0}

def _tool_check_booking_status(lead_id, facts):
    """Tool: Check if lead is already booked/converted."""
    if facts.get("converted_flag"):
        pid = facts.get("converted_patient_id")
        appts = []
        if pid:
            try:
                appts = run_sql(f"""
                    SELECT appointment_date, status, appointment_type
                    FROM {SOURCE}.appointments WHERE patient_id = '{pid}'
                    ORDER BY appointment_date DESC LIMIT 3""")
            except Exception:
                pass
        return {"booked": True, "patient_id": pid, "appointments": appts}
    return {"booked": False}

def _tool_check_contact_eligibility(lead_id, facts):
    """Tool: Check contact eligibility — phone, email, and consent status."""
    # Phone/email are synthetic (hash-derived). No consent table exists.
    # Flag as missing consent when no prior contact record exists.
    has_phone = True   # synthetic, always present
    has_email = True   # synthetic, always present
    consent_verified = facts.get("first_response_hours") is not None
    return {
        "has_phone": has_phone, "has_email": has_email,
        "consent_verified": consent_verified,
        "note": "Contact info is synthetic (demo). Consent inferred from first_response_hours."
    }

def _tool_check_clinic_capacity(location_id):
    """Tool: Check clinic appointment capacity from appointments table."""
    try:
        rows = run_sql(f"""
            SELECT
                COUNT(*) as total_appts,
                SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'Scheduled' THEN 1 ELSE 0 END) as scheduled
            FROM {SOURCE}.appointments
            WHERE location_id = '{location_id}'
              AND appointment_date >= date_sub(current_date(), 30)""")
        if not rows:
            return {"available": True, "note": "No recent appointment data"}
        r = rows[0]
        total = int(r.get("total_appts", 0) or 0)
        scheduled = int(r.get("scheduled", 0) or 0)
        avail = scheduled < 50
        return {"available": avail, "recent_appts": total, "scheduled": scheduled,
                "note": f"{scheduled} scheduled in last 30 days" + (" — capacity available" if avail else " — high volume")}
    except Exception:
        return {"available": True, "note": "Capacity data unavailable"}

def agent_recommend(lead_id, location_id):
    """Main agent: calls tools, evaluates facts, makes recommendation."""
    tool_calls = []

    # Tool 1: Get lead facts
    facts = _tool_get_lead_facts(lead_id, location_id)
    tool_calls.append({"tool": "get_lead_facts", "description": "Retrieve lead facts from database", "result": facts})
    if not facts.get("found"):
        return {"action": "NO_ACTION", "rationale": "Lead not found in database.",
                "tool_calls": tool_calls, "is_ai": False, "is_fallback": False, "lead_id": lead_id}

    # Tool 2: Check booking status
    booking = _tool_check_booking_status(lead_id, facts)
    tool_calls.append({"tool": "check_booking_status", "description": "Check if lead already booked/converted", "result": booking})

    # Tool 3: Get contact history
    history = _tool_get_contact_history(lead_id)
    tool_calls.append({"tool": "get_contact_history", "description": "Retrieve logged contact history",
                       "result": {"count": history["count"], "recent": history["history"][:3]}})

    # Tool 4: Check existing tasks
    tasks = _tool_check_existing_tasks(lead_id)
    tool_calls.append({"tool": "check_existing_tasks", "description": "Check pending follow-up tasks",
                       "result": {"count": tasks["count"], "tasks": tasks["pending_tasks"][:3]}})

    # Tool 5: Check contact eligibility
    eligibility = _tool_check_contact_eligibility(lead_id, facts)
    tool_calls.append({"tool": "check_contact_eligibility", "description": "Check phone/email and consent", "result": eligibility})

    # Tool 6: Check clinic capacity
    capacity = _tool_check_clinic_capacity(location_id)
    tool_calls.append({"tool": "check_clinic_capacity", "description": "Check appointment availability", "result": capacity})

    # Compute effective touchpoints
    eff_touches = facts["num_touchpoints"] + history["count"]

    # ---- Decision engine (deterministic, grounded in tool results) ----
    action = None
    rationale = ""
    suggested_timing = ""
    evidence = []
    missing_info = []

    # Rule 1: Already converted/booked → suppress
    if facts["converted_flag"] or booking.get("booked"):
        action = "NO_ACTION"
        n_appts = len(booking.get("appointments", []))
        rationale = (f"Lead {lead_id} is already converted (status: {facts['status']})"
                     + (f" with {n_appts} appointment(s) on record." if n_appts else ".")
                     + " Recovery outreach is not needed.")
        evidence.append(f"converted_flag={facts['converted_flag']}")
        evidence.append(f"booking_status=booked")

    # Rule 2: Contact cap reached (3+ effective touchpoints)
    elif eff_touches >= 3:
        action = "NO_ACTION"
        rationale = (f"Contact cap reached: {eff_touches} effective touchpoints "
                     f"(static={facts['num_touchpoints']}, logged={history['count']}). "
                     "No further outreach recommended.")
        evidence.append(f"effective_touchpoints={eff_touches} (cap=3)")

    # Rule 3: Status/touchpoint inconsistency → staff review (no invented contact)
    elif facts["status"] and "contacted" in facts["status"].lower() and eff_touches == 0:
        action = "STAFF_REVIEW"
        if facts["first_response_hours"] is None:
            rationale = (f"Lead status shows \"Contacted\" but no contact record exists "
                         "and no first-response time is recorded. Staff review required "
                         "before outreach — do not assume consent.")
            missing_info.append("No first-response time recorded despite 'Contacted' status")
            missing_info.append("Consent to contact not verified")
            evidence.append(f"status={facts['status']} but touchpoints=0 and first_response_hours=None")
        else:
            rationale = (f"Lead status shows \"Contacted\" with a first-response time of "
                         f"{facts['first_response_hours']:.1f}h but no touchpoints are logged. "
                         "The contact event may not have been properly recorded. "
                         "Staff review required — do not assume the contact occurred.")
            missing_info.append("No touchpoint logged despite 'Contacted' status and first-response time")
            evidence.append(f"status={facts['status']} but touchpoints=0 and first_response_hours={facts['first_response_hours']:.1f}")

    # Rule 4: Pending follow-up → wait
    elif tasks["count"] > 0:
        pending = tasks["pending_tasks"][0] if tasks["pending_tasks"] else {}
        if pending.get("decision") == "FOLLOW_UP_SCHEDULED":
            action = "NO_ACTION"
            rationale = (f"Follow-up already scheduled ({pending.get('rationale', 'date not specified')}). "
                         "Wait for the scheduled date before taking new action.")
            evidence.append(f"pending_follow_up={pending.get('rationale')}")
        else:
            action = "STAFF_REVIEW"
            rationale = f"Pending task exists ({pending.get('decision', 'unknown')}). Review before taking new action."
            evidence.append(f"pending_task={pending.get('decision')}")

    # Rule 5: No contact attempted (New lead, 0 touchpoints, no response) → call now
    elif facts["first_response_hours"] is None and eff_touches == 0:
        action = "CALL"
        rationale = (f"No contact attempted for lead {lead_id} (source: {facts['source']}, status: {facts['status']}). "
                     "High priority — recommend immediate call.")
        suggested_timing = "Today, within business hours"
        evidence.append("first_response_hours=None")
        evidence.append("effective_touchpoints=0")

    # Rule 6: Slow initial response (>48h) + low touchpoints → call
    elif facts["first_response_hours"] is not None and facts["first_response_hours"] > 48 and eff_touches <= 1:
        action = "CALL"
        rationale = (f"Slow initial response ({facts['first_response_hours']:.0f}h) with only {eff_touches} touchpoint(s). "
                     "Recovery opportunity — recommend follow-up call.")
        suggested_timing = "Today, within business hours"
        evidence.append(f"first_response_hours={facts['first_response_hours']:.1f} (>48h threshold)")
        evidence.append(f"effective_touchpoints={eff_touches}")

    # Rule 7: Multiple unanswered attempts → switch to written outreach
    elif eff_touches >= 1 and history["count"] > 0:
        all_no_answer = all(h.get("outcome") in ("No answer", None, "") for h in history["history"])
        if all_no_answer:
            action = "DRAFT_OUTREACH"
            rationale = (f"{eff_touches} unanswered attempt(s). Recommend switching to written outreach "
                         "(email draft) instead of another call.")
            suggested_timing = "Send within 24 hours"
            evidence.append(f"unanswered_attempts={eff_touches}")
        else:
            last_outcome = history["history"][0].get("outcome", "unknown") if history["history"] else "unknown"
            action = "FOLLOW_UP"
            rationale = (f"Previous contact made (outcome: {last_outcome}) but lead not yet booked. "
                         "Recommend scheduling a follow-up.")
            suggested_timing = "Within 2 business days"
            evidence.append(f"prior_contact_outcome={last_outcome}")

    # Rule 8: Default → standard call
    else:
        action = "CALL"
        rationale = (f"Lead {lead_id} eligible for recovery outreach (status: {facts['status']}, "
                     f"{eff_touches} touchpoint(s)). Standard follow-up call recommended.")
        suggested_timing = "Within 1 business day"
        evidence.append(f"status={facts['status']}")
        evidence.append(f"effective_touchpoints={eff_touches}")

    # Add capacity info to evidence
    evidence.append(f"clinic_capacity={'available' if capacity.get('available') else 'limited'}")

    # ---- AI-enhanced rationale (optional, clearly labeled) ----
    is_ai = False
    frh_str = f"{facts['first_response_hours']:.1f}h" if facts["first_response_hours"] is not None else "No response recorded"
    try:
        ai_prompt = f"""You are a lead recovery agent for a chiropractic clinic. Based on these verified facts, write a concise 1-2 sentence rationale for the recommended action. Do not add preamble.

LEAD: {facts.get('first_name','')} {facts.get('last_name','')} (ID: {lead_id})
STATUS: {facts['status']} | SOURCE: {facts['source']}
TOUCHPOINTS: {eff_touches} effective (static={facts['num_touchpoints']}, logged={history['count']})
FIRST RESPONSE: {frh_str}
CONVERTED: {facts['converted_flag']}
PENDING TASKS: {tasks['count']}
CLINIC CAPACITY: {'Available' if capacity.get('available') else 'Limited'}

RECOMMENDED ACTION: {action}
SUGGESTED TIMING: {suggested_timing}

IMPORTANT: Status "Contacted" means the lead was reached but NOT converted. CONVERTED is {facts['converted_flag']}. Write a rationale supporting the RECOMMENDED ACTION above.

Write only the rationale:"""
        escaped = ai_prompt.replace("'", "''")
        result = run_sql(f"SELECT ai_gen('{escaped}') as rationale")
        if result and result[0].get("rationale"):
            ai_rat = result[0]["rationale"].strip()
            if action != "NO_ACTION":
                ai_lower = ai_rat.lower()
                if any(p in ai_lower for p in ["no follow-up","no action","already converted","already booked","not need","no need","no outreach"]):
                    print(f"AI rationale contradicts action={action}, using deterministic")
                else:
                    rationale = ai_rat
                    is_ai = True
            else:
                rationale = ai_rat
                is_ai = True
    except Exception as e:
        print(f"AI rationale generation failed: {e}")

    return {
        "action": action, "rationale": rationale, "suggested_timing": suggested_timing,
        "evidence": evidence, "missing_info": missing_info, "tool_calls": tool_calls,
        "is_ai": is_ai, "is_fallback": not is_ai, "lead_id": lead_id,
        "effective_touchpoints": eff_touches, "capacity_available": capacity.get("available", True),
        "agent_version": "1.0",
    }

def agent_approve(lead_id, location_id, snapshot_date, recommendation):
    """Persist an approved agent recommendation as a task. Prevents duplicates."""
    action = recommendation.get("action", "")
    if action == "NO_ACTION":
        return {"success": True, "task_id": None, "message": "No action needed — nothing persisted"}
    now = datetime.datetime.now().isoformat()
    decision_map = {
        "CALL": "OUTREACH_LOGGED", "DRAFT_OUTREACH": "OUTREACH_LOGGED",
        "FOLLOW_UP": "FOLLOW_UP_SCHEDULED", "STAFF_REVIEW": "STAFF_REVIEW",
    }
    decision = decision_map.get(action, "STAFF_REVIEW")
    # Duplicate prevention: check for existing pending task of same type
    try:
        existing = run_sql(f"""
            SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks
            WHERE lead_id = '{lead_id}' AND review_status = 'pending_review' AND decision = '{decision}'""")
        if existing and int(existing[0]["cnt"]) > 0:
            return {"success": False, "error": f"Duplicate pending task already exists ({decision})"}
    except Exception:
        pass
    task_id = f"{lead_id}_agent_{now.replace(':','').replace('-','').replace('.','')}"
    ev_json = json.dumps({"evidence": recommendation.get("evidence", []),
                          "tool_count": len(recommendation.get("tool_calls", [])),
                          "is_ai": recommendation.get("is_ai", False)})
    rationale = recommendation.get("rationale", "")
    run_sql(f"""
        INSERT INTO {OUTPUT}.followup_tasks
        (task_id, run_id, snapshot_date, lead_id, location_id, decision, evidence_json, rationale, draft_text, review_status, reviewed_at, is_simulated)
        VALUES ('{task_id}', 'agent_recommend', '{snapshot_date}', '{lead_id}', '{location_id}',
                '{esc(decision)}', '{esc(ev_json)}', '{esc(rationale)}', '', 'pending_review', '{now}', true)""")
    # If draft outreach, generate a draft message
    draft_text = ""
    if action == "DRAFT_OUTREACH":
        try:
            dr = generate_draft(lead_id, location_id)
            if not dr.get("error"):
                draft_text = dr.get("recommendation", "")
                run_sql(f"UPDATE {OUTPUT}.followup_tasks SET draft_text = '{esc(draft_text)}' WHERE task_id = '{task_id}'")
        except Exception:
            pass
    return {"success": True, "task_id": task_id, "decision": decision, "draft": draft_text}

def get_impact_metrics(location_id, snapshot_date, lookback):
    """Compact impact panel: observed, simulated, projected."""
    # --- Observed outcomes (from followup_tasks) ---
    try:
        obs = run_sql(f"""
            SELECT
                COUNT(*) as leads_worked,
                SUM(CASE WHEN outcome = 'Appointment booked' THEN 1 ELSE 0 END) as appts_booked
            FROM {OUTPUT}.followup_tasks
            WHERE location_id = '{location_id}' AND decision = 'OUTREACH_LOGGED' AND review_status = 'completed'
              AND reviewed_at <= '{snapshot_date} 23:59:59'""")
        leads_worked = int(obs[0].get("leads_worked", 0) or 0) if obs else 0
        appts_booked = int(obs[0].get("appts_booked", 0) or 0) if obs else 0
    except Exception:
        leads_worked = 0
        appts_booked = 0

    # Revenue per completed visit (from historical data)
    try:
        rev_rows = run_sql(f"""
            SELECT SUM(v.revenue) / NULLIF(COUNT(*), 0) as avg_rev
            FROM {SOURCE}.visits v
            WHERE v.location_id = '{location_id}'
              AND v.visit_date >= date_sub('{snapshot_date}', 365)
              AND v.visit_date <= '{snapshot_date}'""")
        rev_per_visit = float(rev_rows[0]["avg_rev"]) if rev_rows and rev_rows[0].get("avg_rev") else 95.0
    except Exception:
        rev_per_visit = 95.0

    # Completed visits for recovered patients (observed)
    try:
        cv_rows = run_sql(f"""
            SELECT COUNT(*) as completed_visits, SUM(v.revenue) as total_revenue
            FROM {SOURCE}.visits v
            JOIN {SOURCE}.patients p ON v.patient_id = p.patient_id
            JOIN {SOURCE}.leads l ON p.patient_id = l.converted_patient_id
            WHERE l.assigned_location_id = '{location_id}'
              AND v.status = 'Completed'
              AND v.visit_date >= date_sub('{snapshot_date}', 365)
              AND v.visit_date <= '{snapshot_date}'""")
        completed_visits = int(cv_rows[0].get("completed_visits", 0) or 0) if cv_rows else 0
        observed_revenue = float(cv_rows[0].get("total_revenue", 0) or 0) if cv_rows else 0.0
    except Exception:
        completed_visits = 0
        observed_revenue = 0.0

    # --- Eligible leads for evaluation ---
    try:
        eligible = run_sql(f"""
            SELECT lead_id, source, status, num_touchpoints, first_response_hours, created_date
            FROM {SOURCE}.leads
            WHERE assigned_location_id = '{location_id}'
              AND lower(trim(status)) IN ('new','contacted','qualified')
              AND converted_flag = false AND num_touchpoints BETWEEN 0 AND 2
              AND datediff('{snapshot_date}', created_date) BETWEEN 0 AND {lookback}""")
    except Exception:
        eligible = []

    # --- Simulated evaluation: agent vs oldest-first baseline ---
    CONTACT_BUDGET = min(10, len(eligible))

    def parse_frh(r):
        v = r.get("first_response_hours")
        if v is not None:
            try:
                return float(v)
            except (ValueError, TypeError):
                return None
        return None

    def booking_rate(frh):
        if frh is None:
            return 0.08  # no response — highest recovery value
        if frh > 48:
            return 0.06  # slow response
        return 0.04     # normal response

    def expected_revenue(leads_subset):
        return sum(booking_rate(parse_frh(r)) * rev_per_visit for r in leads_subset)

    # Agent prioritization: slow/no response first, then fewest touchpoints
    agent_sorted = sorted(eligible, key=lambda r: (
        0 if parse_frh(r) is None or parse_frh(r) > 48 else 1,
        int(r.get("num_touchpoints", 0) or 0),
    ))
    agent_top = agent_sorted[:CONTACT_BUDGET]
    agent_expected = expected_revenue(agent_top)

    # Oldest-first baseline
    oldest_sorted = sorted(eligible, key=lambda r: r.get("created_date", ""))
    oldest_top = oldest_sorted[:CONTACT_BUDGET]
    baseline_expected = expected_revenue(oldest_top)

    # --- Projected opportunity ---
    # Eligible leads x incremental booking rate x attendance rate x revenue per visit
    total_eligible = len(eligible)
    INCREMENTAL_BOOKING_RATE = 0.05  # assumption: 5% of worked leads book
    ATTENDANCE_RATE = 0.80           # assumption: 80% of booked appointments are attended
    projected_revenue = total_eligible * INCREMENTAL_BOOKING_RATE * ATTENDANCE_RATE * rev_per_visit

    # Conversion rate (observed)
    conv_rate = (appts_booked / leads_worked) if leads_worked > 0 else 0.0

    return {
        "observed": {
            "leads_worked": leads_worked,
            "appts_booked": appts_booked,
            "completed_visits": completed_visits,
            "observed_revenue": round(observed_revenue, 0),
            "conv_rate": round(conv_rate * 100, 1),
            "conv_denominator": leads_worked,
            "period": f"Last {lookback} days from {snapshot_date}",
        },
        "simulated": {
            "contact_budget": CONTACT_BUDGET,
            "agent_expected": round(agent_expected, 0),
            "baseline_expected": round(baseline_expected, 0),
            "agent_advantage": round(agent_expected - baseline_expected, 0),
            "agent_high_value": sum(1 for r in agent_top if parse_frh(r) is None or (parse_frh(r) or 0) > 48),
            "baseline_high_value": sum(1 for r in oldest_top if parse_frh(r) is None or (parse_frh(r) or 0) > 48),
        },
        "projected": {
            "total_eligible": total_eligible,
            "incremental_booking_rate": INCREMENTAL_BOOKING_RATE,
            "attendance_rate": ATTENDANCE_RATE,
            "rev_per_visit": round(rev_per_visit, 0),
            "projected_revenue": round(projected_revenue, 0),
        },
        "rev_per_visit": round(rev_per_visit, 0),
    }

def get_impact_data(location_id, snapshot_date, lookback):
    metrics = get_metrics(location_id, snapshot_date, lookback)
    eligible = metrics["eligible"]
    # Historical: avg revenue per visit (not per-patient-year, which would double-count repeat visits)
    try:
        rev_rows = run_sql(f"""
            SELECT SUM(v.revenue) / NULLIF(COUNT(*), 0) as avg_rev_per_visit
            FROM {SOURCE}.visits v JOIN {SOURCE}.patients p ON v.patient_id = p.patient_id
            WHERE p.home_location_id = '{location_id}' AND v.visit_date >= date_sub('{snapshot_date}', 365)
            AND v.visit_date <= '{snapshot_date}'""")
        avg_rev_per_visit = float(rev_rows[0]["avg_rev_per_visit"]) if rev_rows and rev_rows[0]["avg_rev_per_visit"] else 95.0
    except:
        avg_rev_per_visit = 95.0
    # Historical: visit frequency for active patients (visits per active month)
    try:
        freq_rows = run_sql(f"""
            SELECT AVG(monthly_visits) as avg_visits_per_month
            FROM (
                SELECT p.patient_id,
                    COUNT(v.visit_id) * 1.0 / GREATEST(p.tenure_months, 1) as monthly_visits
                FROM {SOURCE}.patients p
                LEFT JOIN {SOURCE}.visits v ON p.patient_id = v.patient_id
                WHERE p.home_location_id = '{location_id}' AND p.status = 'Active'
                GROUP BY p.patient_id, p.tenure_months
            ) t""")
        visits_per_month = float(freq_rows[0]["avg_visits_per_month"]) if freq_rows and freq_rows[0]["avg_visits_per_month"] else 2.0
    except:
        visits_per_month = 2.0
    # Assumption: active months in first year (derived from historical avg first-year months ~10.4, rounded down)
    ACTIVE_MONTHS = 10
    first_year_rev_per_client = avg_rev_per_visit * visits_per_month * ACTIVE_MONTHS
    scenarios = []
    for label, rate in [("Low", 0.02), ("Base", 0.05), ("High", 0.10)]:
        expected_clients = eligible * rate
        first_year_revenue = expected_clients * first_year_rev_per_client
        scenarios.append({"label": label, "rate_pct": f"{rate*100:.0f}%",
            "expected_clients": round(expected_clients, 1),
            "first_year_revenue_per_client": round(first_year_rev_per_client, 2),
            "first_year_revenue": round(first_year_revenue, 2)})
    return {"eligible": eligible, "avg_rev_per_visit": round(avg_rev_per_visit, 2),
        "visits_per_month": round(visits_per_month, 1), "active_months": ACTIVE_MONTHS,
        "first_year_rev_per_client": round(first_year_rev_per_client, 2),
        "scenarios": scenarios, "scope": location_id,
        "period": f"snapshot {snapshot_date}, {lookback}-day lookback"}

def get_arr_projection(location_id, snapshot_date, lookback):
    impact = get_impact_data(location_id, snapshot_date, lookback)
    eligible = impact["eligible"]
    first_year_rev_per_client = impact["first_year_rev_per_client"]
    # Per-clinic cohort revenue (from current eligible leads, one-time over 12 months)
    cohort_base = max(eligible * 0.05 * first_year_rev_per_client, 1.0)
    cohort_high = max(eligible * 0.10 * first_year_rev_per_client, 1.0)
    # Annual pipeline assumption (clearly labeled): new eligible leads per clinic per year
    ANNUAL_PIPELINE_LEADS = 150
    annual_base = max(ANNUAL_PIPELINE_LEADS * 0.05 * first_year_rev_per_client, 1.0)
    annual_high = max(ANNUAL_PIPELINE_LEADS * 0.10 * first_year_rev_per_client, 1.0)
    clinics_100m = max(1, int(100_000_000 / annual_base))
    clinics_250m = max(1, int(250_000_000 / annual_base))
    # Sourced addressable market: ~44,000 chiropractic clinics in the US (not individual chiropractors)
    ADDRESSABLE_CLINICS = 44000
    # Platform pricing model: explicit recurring SaaS subscription
    PLATFORM_PRICE_PER_MONTH = 500
    platform_arr_per_clinic = PLATFORM_PRICE_PER_MONTH * 12
    raw_milestones = [
        {"label": "Pilot", "clinics": 1, "icon": "\u2605", "desc": "Current clinic"},
        {"label": "Regional", "clinics": 10, "icon": "\u25cf", "desc": "10 clinics"},
        {"label": "Growth", "clinics": 50, "icon": "\u25b2", "desc": "50 clinics"},
        {"label": "Expansion", "clinics": 200, "icon": "\u26a1", "desc": "200 clinics"},
        {"label": "$100M Scale", "clinics": clinics_100m, "icon": "\u25ce", "desc": f"{clinics_100m:,} clinics"},
        {"label": "$250M Scale", "clinics": clinics_250m, "icon": "\u265b", "desc": f"{clinics_250m:,} clinics"},
    ]
    raw_milestones.sort(key=lambda m: m["clinics"])
    projections = []
    for m in raw_milestones:
        rev_base = annual_base * m["clinics"]
        rev_high = annual_high * m["clinics"]
        platform_arr = platform_arr_per_clinic * m["clinics"]
        projections.append({
            "label": m["label"], "icon": m["icon"], "desc": m["desc"],
            "clinics": m["clinics"], "arr_base": round(rev_base, 0),
            "arr_high": round(rev_high, 0),
            "platform_arr": round(platform_arr, 0),
            "in_target": rev_base >= 100_000_000,
            "exceeds_market": m["clinics"] > ADDRESSABLE_CLINICS,
        })
    return {
        "per_clinic_base": round(annual_base, 0),
        "per_clinic_high": round(annual_high, 0),
        "cohort_per_clinic_base": round(cohort_base, 0),
        "cohort_per_clinic_high": round(cohort_high, 0),
        "projections": projections,
        "clinics_100m": clinics_100m, "clinics_250m": clinics_250m,
        "eligible": eligible,
        "avg_rev_per_visit": impact["avg_rev_per_visit"],
        "visits_per_month": impact["visits_per_month"],
        "active_months": impact["active_months"],
        "first_year_rev_per_client": round(first_year_rev_per_client, 0),
        "annual_pipeline_leads": ANNUAL_PIPELINE_LEADS,
        "addressable_clinics": ADDRESSABLE_CLINICS,
        "platform_price_per_month": PLATFORM_PRICE_PER_MONTH,
        "platform_arr_per_clinic": round(platform_arr_per_clinic, 0),
    }

def get_tasks(location_id, today=""):
    date_expr = f"'{today}'" if today else "current_date()"
    try:
        ns = _name_sql("t.lead_id")
        rows = run_sql(f"""
            WITH pending AS (
                SELECT t.task_id, t.lead_id, t.decision, t.rationale, t.draft_text,
                       t.review_status, t.reviewed_at, t.outcome, {ns},
                    CASE WHEN t.decision = 'FOLLOW_UP_SCHEDULED'
                        AND to_date(nullif(regexp_extract(t.rationale, 'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1), '')) > {date_expr}
                        THEN 'Scheduled'
                        ELSE 'Due Now'
                    END as due_label,
                    CASE WHEN t.decision = 'FOLLOW_UP_SCHEDULED'
                        THEN regexp_extract(t.rationale, 'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1)
                        ELSE NULL
                    END as due_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.lead_id
                        ORDER BY
                            CASE WHEN t.decision = 'FOLLOW_UP_SCHEDULED'
                                AND to_date(nullif(regexp_extract(t.rationale, 'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1), '')) > {date_expr}
                                THEN 1 ELSE 0 END,
                            CASE WHEN t.decision = 'FOLLOW_UP_SCHEDULED' THEN 0
                                 WHEN t.decision = 'follow_up' THEN 1
                                 ELSE 2 END,
                            t.reviewed_at DESC
                    ) as rn,
                    COUNT(*) OVER (PARTITION BY t.lead_id) as total_for_lead
                FROM {OUTPUT}.followup_tasks t
                WHERE t.location_id = '{location_id}' AND t.review_status = 'pending_review'
            )
            SELECT task_id, lead_id, decision, rationale, draft_text, review_status,
                   reviewed_at, outcome, first_name, last_name, phone, email,
                   due_label, due_date, total_for_lead - 1 as duplicate_count
            FROM pending WHERE rn = 1 ORDER BY CASE WHEN due_label = 'Due Now' THEN 0 ELSE 1 END, to_date(nullif(due_date, '')) ASC NULLS LAST, lead_id""")
        # Ensure duplicate_count is int for template comparison
        for row in rows:
            if "duplicate_count" in row and row["duplicate_count"] is not None:
                try:
                    row["duplicate_count"] = int(row["duplicate_count"])
                except (ValueError, TypeError):
                    row["duplicate_count"] = 0
        return rows
    except:
        return []

@app.route("/")
def index():
    location_id = request.args.get("location_id", DEMO_LOCATION)
    snapshot_date = request.args.get("snapshot_date", DEMO_SNAPSHOT)
    lookback_raw = request.args.get("lookback", DEMO_LOOKBACK)
    try:
        lookback_int = int(lookback_raw)
    except (ValueError, TypeError):
        lookback_int = int(DEMO_LOOKBACK)
    lookback_clamped = max(1, min(365, lookback_int))
    lookback_out_of_range = lookback_int != lookback_clamped
    lookback = str(lookback_clamped)
    page = int(request.args.get("page", "1"))
    search = request.args.get("search", "").strip()
    today = request.args.get("today", "")
    try:
        locations = get_locations()
        metrics = get_metrics(location_id, snapshot_date, int(lookback), today=today)
        leads_data = get_eligible_leads(location_id, snapshot_date, int(lookback), page=page, per_page=15, search=search, today=today)
        tasks = get_tasks(location_id, today=today)
        metrics['followups_due'] = sum(1 for t in tasks if t.get('due_label') == 'Due Now')
        clinic_name = next((l["location_name"] for l in locations if l["location_id"] == location_id), location_id)
        # Impact section is hidden (display:none) — skip expensive SQL queries, pass static empty values
        impact = {"scenarios": [], "eligible": 0, "avg_rev_per_visit": 0, "visits_per_month": 0, "active_months": 0, "first_year_rev_per_client": 0}
        arr_projection = {"projections": [], "avg_rev_per_visit": 0, "visits_per_month": 0, "active_months": 0, "first_year_rev_per_client": 0, "per_clinic_base": 0, "per_clinic_high": 0, "clinics_100m": 0, "clinics_250m": 0, "addressable_clinics": 1, "annual_pipeline_leads": 0, "platform_price_per_month": 0, "platform_arr_per_clinic": 0, "eligible": 0}
        impact_metrics = {"observed": {"leads_worked": 0, "appts_booked": 0, "conv_rate": 0, "conv_denominator": 0, "completed_visits": 0, "observed_revenue": 0, "period": ""}, "simulated": {"agent_expected": 0, "baseline_expected": 0, "agent_advantage": 0, "contact_budget": 0, "agent_high_value": 0, "baseline_high_value": 0}, "projected": {"total_eligible": 0, "incremental_booking_rate": 0, "attendance_rate": 0, "rev_per_visit": 0, "projected_revenue": 0}, "rev_per_visit": 0}
        return render_template_string(DASHBOARD_HTML, locations=locations, location_id=location_id,
            clinic_name=clinic_name, snapshot_date=snapshot_date, lookback=lookback, lookback_out_of_range=lookback_out_of_range,
            page=page, search=search, today=today,
            metrics=metrics, leads_data=leads_data, impact=impact, arr_projection=arr_projection, tasks=tasks,
            impact_metrics=impact_metrics)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return render_template_string(ERROR_HTML)

@app.route("/api/lead/<lead_id>")
def api_lead_detail(lead_id):
    location_id = request.args.get("location_id", DEMO_LOCATION)
    snapshot_date = request.args.get("snapshot_date", DEMO_SNAPSHOT)
    try:
        lead = get_lead_detail(lead_id, location_id, snapshot_date)
        if not lead:
            return jsonify({"error": "Lead not found"}), 404
        def serialize(obj):
            if isinstance(obj, (datetime.datetime, datetime.date)):
                return obj.isoformat()
            return obj
        return jsonify(json.loads(json.dumps(lead, default=serialize)))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/lead/<lead_id>/draft")
def api_generate_draft(lead_id):
    location_id = request.args.get("location_id", DEMO_LOCATION)
    try:
        return jsonify(generate_draft(lead_id, location_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/lead/<lead_id>/action", methods=["POST"])
def api_log_action(lead_id):
    location_id = request.form.get("location_id", DEMO_LOCATION)
    snapshot_date = request.form.get("snapshot_date", DEMO_SNAPSHOT)
    outcome = request.form.get("outcome", "")
    note = request.form.get("note", "")
    follow_up_date = request.form.get("follow_up_date", "")
    if outcome not in ("No answer", "Connected", "Wrong number", "Appointment booked", "No longer interested"):
        return jsonify({"success": False, "error": "Invalid outcome"}), 400
    try:
        result = log_outcome(lead_id, location_id, snapshot_date, outcome, note, follow_up_date)
        resp = {"success": True, "task_id": result["task_id"], "outcome": outcome, "removed_from_queue": outcome == "Connected"}
        if result.get("followup"):
            resp["followup"] = result["followup"]
        return jsonify(resp)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/lead/<lead_id>/note", methods=["POST"])
def api_save_note(lead_id):
    location_id = request.form.get("location_id", DEMO_LOCATION)
    snapshot_date = request.form.get("snapshot_date", DEMO_SNAPSHOT)
    note = request.form.get("note", "")
    if not note.strip():
        return jsonify({"success": False, "error": "Note cannot be empty"}), 400
    try:
        task_id = save_note(lead_id, location_id, snapshot_date, note)
        return jsonify({"success": True, "task_id": task_id})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/lead/<lead_id>/close", methods=["POST"])
def api_close_lead(lead_id):
    location_id = request.form.get("location_id", DEMO_LOCATION)
    snapshot_date = request.form.get("snapshot_date", DEMO_SNAPSHOT)
    reason = request.form.get("reason", "")
    if not reason.strip():
        return jsonify({"success": False, "error": "Reason required"}), 400
    try:
        task_id = close_lead(lead_id, location_id, snapshot_date, reason)
        return jsonify({"success": True, "task_id": task_id})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/agent/recommend/<lead_id>")
def api_agent_recommend(lead_id):
    location_id = request.args.get("location_id", DEMO_LOCATION)
    try:
        rec = agent_recommend(lead_id, location_id)
        return jsonify(rec)
    except Exception as e:
        return jsonify({"action": "ERROR", "rationale": str(e), "tool_calls": [],
                        "is_ai": False, "is_fallback": False, "lead_id": lead_id}), 500

@app.route("/api/agent/approve/<lead_id>", methods=["POST"])
def api_agent_approve(lead_id):
    location_id = request.form.get("location_id", DEMO_LOCATION)
    snapshot_date = request.form.get("snapshot_date", DEMO_SNAPSHOT)
    recommendation_json = request.form.get("recommendation", "{}")
    try:
        rec = json.loads(recommendation_json)
        result = agent_approve(lead_id, location_id, snapshot_date, rec)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lead Recovery — {{ clinic_name }}</title>
<script>if(!new URLSearchParams(window.location.search).get('today')){var d=new Date();var ts=d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');var p=new URLSearchParams(window.location.search);p.set('today',ts);window.location.replace(window.location.pathname+'?'+p.toString());}</script>
<style>
:root{--bg:#F6F8FA;--surface:#FFFFFF;--text:#172B4D;--text2:#6B778C;--primary:#087F8C;--ptext:#FFFFFF;--border:#DFE1E6;--success:#0B875B;--warning:#FF991F;--error:#DE350B;--radius:12px;--pad:24px;--shadow:0 1px 3px rgba(23,43,77,0.06);}
*{margin:0;padding:0;box-sizing:border-box;}
.sr-only{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border-width:0;}
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
body{font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:var(--text);font-size:15px;line-height:1.6;}
.hdr{background:var(--surface);border-bottom:1px solid var(--border);padding:var(--pad) var(--pad);box-shadow:var(--shadow);}
.hdr-in{max-width:1400px;margin:0 auto;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:16px;}
.hdr-left h1{font-size:28px;font-weight:700;color:var(--text);margin-bottom:4px;}
.hdr-left .desc{font-size:15px;color:var(--text2);font-weight:400;}
.hdr-right{display:flex;gap:12px;align-items:center;font-size:14px;color:var(--text2);flex-wrap:wrap;}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600;}
.badge-demo{background:#FEF3C7;color:#92400E;}
.ctl{padding:20px var(--pad);background:var(--surface);border-bottom:1px solid var(--border);}
.ctl-in{max-width:1400px;margin:0 auto;display:flex;gap:20px;align-items:flex-end;flex-wrap:wrap;}
.fld{display:flex;flex-direction:column;gap:6px;min-width:200px;flex:1;}
.fld label{font-size:13px;color:var(--text2);font-weight:600;text-transform:uppercase;letter-spacing:0.03em;}
.ctl select,.ctl input{padding:10px 14px;border:1px solid var(--border);border-radius:var(--radius);font-size:14px;background:var(--surface);font-family:inherit;transition:border-color 150ms;}
.ctl select:focus,.ctl input:focus{outline:none;border-color:var(--primary);}
.ctl select{flex:3;max-width:400px;}
.ctl input[type="date"]{flex:1;min-width:160px;}
.ctl input[type="number"]{width:100px;}
.main{max-width:1400px;margin:0 auto;padding:var(--pad);}
.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:20px;margin-bottom:32px;}
@media(max-width:1024px){.metrics{grid-template-columns:repeat(2,1fr);}}
@media(max-width:600px){.metrics{grid-template-columns:1fr;}}
.mc{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px;box-shadow:var(--shadow);transition:transform 150ms,box-shadow 150ms;}
.mc:hover{transform:translateY(-2px);box-shadow:0 4px 12px rgba(23,43,77,0.1);}
.mc .lbl{font-size:12px;color:var(--text2);font-weight:600;text-transform:uppercase;letter-spacing:.05em;margin-bottom:8px;display:flex;align-items:center;gap:6px;}
.mc .val{font-size:34px;font-weight:700;margin-bottom:6px;color:var(--text);}
.mc .sub{font-size:13px;color:var(--text2);line-height:1.4;}
.mc.warning{border-left:3px solid var(--warning);}
.tabs{display:flex;gap:0;margin-bottom:24px;border-bottom:2px solid var(--border);background:var(--surface);border-radius:var(--radius) var(--radius) 0 0;overflow:hidden;}
.tab{padding:14px 28px;cursor:pointer;font-weight:600;font-size:15px;color:var(--text2);border:none;background:none;border-bottom:3px solid transparent;margin-bottom:-2px;transition:all 150ms;position:relative;}
.tab.active{color:var(--primary);border-bottom-color:var(--primary);background:rgba(8,127,140,0.05);}
.tab:hover:not(.active){color:var(--text);background:rgba(0,0,0,0.02);}
.qh{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;gap:12px;flex-wrap:wrap;}
.qh h2{font-size:18px;font-weight:700;}
.qh .cnt{font-size:14px;color:var(--text2);}
.sbox{padding:8px 12px;border:1px solid var(--border);border-radius:var(--radius);font-size:14px;width:240px;}
.qt{width:100%;border-collapse:collapse;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;box-shadow:var(--shadow);}
.qt th{text-align:left;padding:16px 20px;font-size:12px;color:var(--text2);font-weight:700;text-transform:uppercase;letter-spacing:.05em;border-bottom:2px solid var(--border);background:var(--bg);}
.qt td{padding:18px 20px;border-bottom:1px solid var(--border);font-size:14px;vertical-align:middle;}
.qt tbody tr{cursor:pointer;transition:background-color 150ms;}
.qt tbody tr:hover{background:rgba(8,127,140,0.04);}
.qt tbody tr:focus-visible{outline:2px solid var(--primary);outline-offset:-2px;background:rgba(8,127,140,0.06);}
.qt tbody tr:focus{outline:2px solid var(--primary);outline-offset:-2px;background:rgba(8,127,140,0.06);}
.qt tbody tr.sel{background:rgba(8,127,140,0.1);}
.qt tbody tr:last-child td{border-bottom:none;}
.ptag{display:inline-block;padding:4px 10px;border-radius:6px;font-size:12px;font-weight:600;}
.t-error{background:#FFEBE6;color:var(--error);}.t-warning{background:#FFF0E0;color:var(--warning);}.t-success{background:#E3FCEF;color:var(--success);}
.pg{display:flex;gap:8px;align-items:center;margin-top:16px;justify-content:center;}
.pg a{padding:8px 16px;border:1px solid var(--border);border-radius:var(--radius);text-decoration:none;color:var(--text);font-size:14px;}
.pg a:hover{background:var(--bg);}.pg .cur{background:var(--primary);color:var(--ptext);border-color:var(--primary);}.pg .dis{color:var(--text2);pointer-events:none;}
.ovr{position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.3);z-index:100;display:none;}.ovr.show{display:block;}
.dr{position:fixed;top:0;right:0;width:520px;max-width:100%;height:100%;background:var(--surface);z-index:101;overflow-y:auto;transform:translateX(100%);transition:transform 200ms cubic-bezier(0.4,0,0.2,1);box-shadow:-8px 0 32px rgba(23,43,77,0.15);}
.dr.show{transform:translateX(0);}
.dr-h{padding:16px 24px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;background:var(--surface);z-index:1;}
.dr-h h2{font-size:18px;}.dr-x{background:none;border:none;font-size:24px;cursor:pointer;color:var(--text2);padding:4px 8px;}
.dr-b{padding:24px;overflow-x:hidden;}.ds{margin-bottom:24px;}.ds h3{font-size:13px;font-weight:600;color:#718096;text-transform:none;margin-bottom:12px;letter-spacing:0.01em;}
.fr{display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid var(--border);font-size:14px;}.fr .l{color:var(--text2);}.fr .v{font-weight:600;}
.af{background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:16px;margin-top:12px;}
.af select,.af input,.af textarea{width:100%;padding:10px 12px;border:1px solid var(--border);border-radius:var(--radius);font-size:14px;margin-bottom:12px;font-family:inherit;}
.af label{font-size:13px;color:var(--text2);font-weight:600;display:block;margin-bottom:4px;}
.btn{padding:12px 24px;border:none;border-radius:var(--radius);font-size:14px;font-weight:600;cursor:pointer;min-height:44px;transition:all 150ms;font-family:inherit;}
.btn-p{background:var(--primary);color:var(--ptext);box-shadow:0 1px 2px rgba(8,127,140,0.2);}.btn-p:hover{background:#096C78;box-shadow:0 2px 4px rgba(8,127,140,0.3);}
.btn-s{background:var(--surface);color:var(--text);border:1px solid var(--border);}.btn-s:hover{background:var(--bg);border-color:var(--text2);}
.btn-e{background:#FFEBE6;color:var(--error);}.btn-e:hover{background:#FFDBDB;}
.btn-sm{padding:6px 12px;min-height:32px;font-size:13px;}
.dbox{background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:16px;margin-top:8px;}
.dbox textarea{width:100%;border:1px solid var(--border);border-radius:var(--radius);padding:10px;font-size:14px;min-height:100px;font-family:inherit;resize:vertical;}
.fb-note{font-size:13px;color:var(--warning);margin-top:8px;}
.tl{list-style:none;}.ti{padding:8px 0;border-bottom:1px solid var(--border);font-size:14px;}.ti .t{font-size:12px;color:var(--text2);}.ti .a{font-weight:600;}
.ag-badge{display:inline-block;padding:4px 10px;border-radius:6px;font-size:11px;font-weight:600;letter-spacing:0.02em;margin-left:8px;}
.ag-CALL{background:#DBEAFE;color:#1E40AF;}.ag-DRAFT_OUTREACH{background:#E0E7FF;color:#3730A3;}.ag-FOLLOW_UP{background:#FEF3C7;color:#92400E;}.ag-STAFF_REVIEW{background:#FEE2E2;color:#991B1B;}.ag-NO_ACTION{background:#F1F5F9;color:#64748B;}.ag-conflict{background:#FEE2E2;color:#991B1B;}
.ag-panel{background:white;border:1px solid #E2E8F0;border-radius:12px;padding:24px;max-width:100%;overflow-x:hidden;}
.ag-headline{font-size:18px;font-weight:600;color:#1a202c;margin:0 0 12px 0;line-height:1.4;display:flex;align-items:center;flex-wrap:wrap;}
.ag-rat{font-size:14px;line-height:1.6;color:#4a5568;margin:0 0 16px 0;}
.ag-status-note{font-size:13px;line-height:1.5;color:#d97706;margin:12px 0;padding:12px;background:#FEF3C7;border:1px solid #FDE68A;border-radius:8px;}
.ag-status-note strong{font-weight:600;color:#92400E;}
.ag-ev{display:flex;flex-wrap:wrap;gap:8px;margin:16px 0 0 0;}
.ag-ev-item{display:inline-block;font-size:13px;background:#F7FAFC;color:#2d3748;padding:6px 12px;border-radius:6px;border:1px solid #E2E8F0;font-weight:500;}
.ag-miss{font-size:13px;color:#d97706;margin:16px 0 0 0;padding:12px;background:#FEF3C7;border:1px solid #FDE68A;border-radius:8px;}
.ag-disclosure{margin:20px 0 0 0;padding:16px 0 0 0;border-top:1px solid #E2E8F0;}
.ag-disclosure-btn{display:flex;align-items:center;justify-content:flex-start;width:100%;background:none;border:none;padding:0;font:inherit;color:#718096;font-size:13px;cursor:pointer;text-align:left;font-weight:500;}
.ag-disclosure-btn:hover{color:var(--primary);}
.ag-disclosure-chevron{transition:transform 0.2s;font-size:10px;margin-right:6px;display:inline-block;}
.ag-disclosure-chevron.open{transform:rotate(90deg);}
.ag-disclosure-content{display:none;margin-top:12px;}
.ag-disclosure-content.show{display:block;}
.ag-tool{background:#F7FAFC;border:1px solid #E2E8F0;border-radius:6px;padding:12px;margin:10px 0;}
.ag-tool-name{font-weight:600;font-size:12px;margin-bottom:6px;color:#2d3748;}
.ag-tool-desc{font-size:12px;color:#718096;margin-bottom:8px;line-height:1.5;}
.ag-tool-res{font-family:ui-monospace,'Courier New',monospace;font-size:11px;background:white;padding:10px;border-radius:4px;max-height:150px;overflow-y:auto;color:#4a5568;white-space:pre-wrap;border:1px solid #E2E8F0;}
.ag-actions{display:flex;gap:10px;margin-top:20px;flex-wrap:wrap;}
.ag-impl-note{font-size:11px;color:#718096;margin-top:16px;padding-top:12px;border-top:1px solid #E2E8F0;line-height:1.5;}
.dr-sect{margin-top:16px;}.dr-sect-label{font-size:13px;font-weight:600;color:#718096;margin-bottom:6px;letter-spacing:0.01em;}.dr-msg{font-size:15px;line-height:1.6;color:#2d3748;white-space:pre-wrap;background:#F7FAFC;border:1px solid #E2E8F0;border-radius:8px;padding:16px;}
.ic{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px;}
.it{width:100%;border-collapse:collapse;margin-top:16px;}.it th,.it td{padding:10px 16px;text-align:left;border-bottom:1px solid var(--border);font-size:14px;}.it th{font-weight:600;color:var(--text2);}
.inote{font-size:13px;color:var(--text2);margin-top:16px;padding:12px;background:#F1F5F9;border-radius:var(--radius);}
.tlist{display:flex;flex-direction:column;gap:8px;}
.titem{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:12px 16px;display:flex;justify-content:space-between;align-items:center;}
.titem[role="button"]:hover{background:rgba(8,127,140,0.04);border-color:var(--primary);}
.titem[role="button"]:focus{outline:2px solid var(--primary);outline-offset:2px;}
.titem .n{font-weight:600;}.titem .m{font-size:13px;color:var(--text2);}
.sg{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-top:16px;}
.si{text-align:center;padding:16px;background:var(--bg);border-radius:var(--radius);}.si .num{font-size:24px;font-weight:700;}.si .lbl{font-size:13px;color:var(--text2);margin-top:4px;}
.sec{display:none;}.sec.active{display:block;}
.empty{text-align:center;padding:48px;color:var(--text2);}.empty h3{font-size:18px;margin-bottom:8px;}
.loading{text-align:center;padding:24px;color:var(--text2);}
.skel{background:linear-gradient(90deg,#f0f0f0 25%,#e0e0e0 50%,#f0f0f0 75%);background-size:200% 100%;animation:skel 1.5s infinite;border-radius:var(--radius);}
@keyframes skel{0%{background-position:200% 0;}100%{background-position:-200% 0;}}
.skel-line{height:16px;margin-bottom:8px;}
.skel-title{height:24px;width:60%;margin-bottom:16px;}
.bar-chart{margin:24px 0;}
.bar-row{margin-bottom:16px;}
.bar-label{font-size:13px;font-weight:600;margin-bottom:6px;display:flex;justify-content:space-between;}
.bar-track{height:32px;background:var(--bg);border-radius:6px;overflow:hidden;position:relative;}
.bar-fill{height:100%;background:var(--primary);display:flex;align-items:center;padding:0 12px;color:var(--ptext);font-size:13px;font-weight:600;transition:width 400ms cubic-bezier(0.4,0,0.2,1);}
.bar-fill.base{background:#0B9DAC;}
.exp-section{margin-top:16px;padding:16px;background:var(--bg);border-radius:var(--radius);border-left:3px solid var(--text2);}
.exp-toggle{cursor:pointer;font-size:14px;font-weight:600;color:var(--primary);display:inline-flex;align-items:center;gap:4px;user-select:none;background:none;border:none;padding:0;font-family:inherit;}
.exp-toggle:hover{text-decoration:underline;}
.exp-toggle:focus{outline:2px solid var(--primary);outline-offset:2px;border-radius:4px;}
.exp-content{display:none;margin-top:12px;font-size:14px;color:var(--text2);}
.exp-content.show{display:block;}
@media(max-width:900px){.dr{width:100%;}.fld{min-width:150px;}}
@media(max-width:700px){.hdr-in{flex-direction:column;align-items:flex-start;}.hdr-left h1{font-size:24px;}.qt{font-size:13px;}.qt th,.qt td{padding:12px 8px;}.hm{display:none;}.tabs{overflow-x:auto;-webkit-overflow-scrolling:touch;}.tab{padding:12px 20px;white-space:nowrap;flex-shrink:0;}}
@media(max-width:600px){.metrics{margin-bottom:20px;}.mc{padding:16px;}.mc .val{font-size:28px;}.mc .lbl{font-size:11px;}.mc .sub{font-size:12px;}}
.arr-hero{background:linear-gradient(135deg,#087F8C 0%,#0B9DAC 50%,#6366F1 100%);border-radius:var(--radius);padding:32px 24px;text-align:center;color:#fff;margin-bottom:24px;position:relative;overflow:hidden;}
.arr-hero::after{content:'';position:absolute;top:-50%;left:-50%;width:200%;height:200%;background:radial-gradient(circle,rgba(255,255,255,0.08) 0%,transparent 60%);animation:arr-shimmer 6s linear infinite;pointer-events:none;}
@keyframes arr-shimmer{0%{transform:rotate(0deg);}100%{transform:rotate(360deg);}}
.arr-hero-label{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.12em;opacity:0.8;margin-bottom:8px;position:relative;}
.arr-hero-range{font-size:42px;font-weight:700;margin-bottom:4px;position:relative;text-shadow:0 2px 12px rgba(0,0,0,0.15);}
.arr-hero-sub{font-size:14px;opacity:0.85;position:relative;}
.arr-traj{margin:24px 0;}
.arr-traj h3{font-size:14px;font-weight:600;color:var(--text2);text-transform:uppercase;margin-bottom:16px;}
.arr-row{display:flex;align-items:center;gap:12px;margin-bottom:10px;}
.arr-row-label{width:140px;flex-shrink:0;display:flex;align-items:center;gap:8px;}
.arr-ic{font-size:20px;}
.arr-nm{font-weight:600;font-size:13px;}
.arr-cl{font-size:11px;color:var(--text2);}
.arr-bar-wrap{flex:1;height:36px;background:var(--bg);border-radius:8px;overflow:hidden;}
.arr-bar-fill{height:100%;border-radius:8px;background:linear-gradient(90deg,#087F8C,#0B9DAC);transition:width 800ms cubic-bezier(0.4,0,0.2,1);animation:arr-grow 1.2s ease-out;}
.arr-bar-fill.target{background:linear-gradient(90deg,#0B9DAC,#6366F1);box-shadow:0 0 16px rgba(99,102,241,0.35);}
@keyframes arr-grow{from{width:0!important;}}
.arr-val{width:80px;flex-shrink:0;text-align:right;font-weight:700;font-size:14px;}
.arr-cards{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin:24px 0;}
@media(max-width:768px){.arr-cards{grid-template-columns:repeat(2,1fr);}}
@media(max-width:480px){.arr-cards{grid-template-columns:1fr;}}
.arr-card{background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:16px;text-align:center;transition:transform 200ms,box-shadow 200ms;}
.arr-card:hover{transform:translateY(-3px);box-shadow:0 4px 16px rgba(23,43,77,0.08);}
.arr-card.target{border-color:#6366F1;background:linear-gradient(135deg,rgba(99,102,241,0.04),rgba(11,157,172,0.04));}
.arr-card-ic{font-size:28px;margin-bottom:6px;}
.arr-card-nm{font-weight:700;font-size:13px;margin-bottom:2px;}
.arr-card-cl{font-size:11px;color:var(--text2);margin-bottom:8px;}
.arr-card-arr{font-size:22px;font-weight:700;color:var(--primary);}
.arr-card.target .arr-card-arr{color:#6366F1;}
.arr-card-rg{font-size:10px;color:var(--text2);margin-top:4px;}
.arr-kmetrics{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:20px 0;}
@media(max-width:600px){.arr-kmetrics{grid-template-columns:repeat(2,1fr);}}
.arr-km{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:16px;text-align:center;}
.arr-km-v{font-size:24px;font-weight:700;color:var(--primary);margin-bottom:4px;}
.arr-km-l{font-size:10px;color:var(--text2);text-transform:uppercase;letter-spacing:0.05em;}
</style></head><body>
<div class="hdr"><div class="hdr-in"><div class="hdr-left"><h1>Lead Recovery</h1><div class="desc">Find the next follow-up worth making</div></div>
<div class="hdr-right"><span style="font-weight:600">{{ clinic_name }}</span><span class="badge badge-demo">Demo</span></div></div></div>
<div class="ctl"><div class="ctl-in">
<div class="fld"><label for="clinic-select">Clinic</label><select id="clinic-select" aria-label="Select clinic location" onchange="window.location.href='?location_id='+this.value+'&snapshot_date={{ snapshot_date }}&lookback={{ lookback }}&today={{ today }}'">{% for loc in locations %}<option value="{{ loc.location_id }}" {% if loc.location_id == location_id %}selected{% endif %}>{{ loc.location_name }}</option>{% endfor %}</select></div>
<div class="fld"><label for="snapshot-date">Snapshot Date</label><input id="snapshot-date" type="date" value="{{ snapshot_date }}" aria-label="Select snapshot date for analysis" onchange="window.location.href='?location_id={{ location_id }}&snapshot_date='+this.value+'&lookback={{ lookback }}&today={{ today }}'"></div>
<div class="fld"><label for="lookback-days">Lookback (Days)</label><input id="lookback-days" type="number" value="{{ lookback }}" min="1" max="365" aria-label="Number of days to look back from snapshot date" onchange="validateLookback(this.value)"></div>
</div></div>
{% if lookback_out_of_range %}
<div style="max-width:1400px;margin:0 auto;padding:8px 24px;"><div style="background:#FFEBE6;border:1px solid var(--error);border-radius:8px;padding:12px 16px;font-size:14px;color:var(--error);"><strong>Lookback adjusted:</strong> The value you entered was outside the allowed range (1–365 days). Using {{ lookback }} days instead.</div></div>
{% endif %}
<div class="main">
<div class="metrics">
<div class="mc"><div class="lbl">Ready for Review</div><div class="val">{{ metrics.eligible }}</div><div class="sub">Eligible leads at this clinic</div></div>
<div class="mc warning"><div class="lbl">Follow-ups Due</div><div class="val">{{ metrics.followups_due }}</div><div class="sub">Due now · {{ tasks|length }} total pending</div></div>
<div class="mc"><div class="lbl">Actions Logged</div><div class="val">{{ metrics.actions_logged }}</div><div class="sub">Recorded outreach outcomes</div></div>
<div class="mc"><div class="lbl">Total Leads</div><div class="val">{{ metrics.total_at_clinic }}</div><div class="sub">All leads at this clinic</div></div>
</div>
<div class="imp-section" style="display:none">
<div class="ic" style="padding:16px">
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
<h2 style="font-size:16px;font-weight:700">Impact Summary</h2>
<span style="font-size:12px;color:var(--text2)">Observed + Simulated + Projected | {{ impact_metrics.observed.period }}</span>
</div>
<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px">
<div style="text-align:center;padding:8px;background:var(--bg);border-radius:var(--radius)">
<div style="font-size:24px;font-weight:700;color:var(--primary)">{{ impact_metrics.observed.leads_worked }}</div>
<div style="font-size:11px;color:var(--text2);text-transform:uppercase">Leads Worked</div>
</div>
<div style="text-align:center;padding:8px;background:var(--bg);border-radius:var(--radius)">
<div style="font-size:24px;font-weight:700;color:var(--success)">{{ impact_metrics.observed.appts_booked }}</div>
<div style="font-size:11px;color:var(--text2);text-transform:uppercase">Appts Booked</div>
</div>
<div style="text-align:center;padding:8px;background:var(--bg);border-radius:var(--radius)">
<div style="font-size:24px;font-weight:700;color:var(--primary)">{{ impact_metrics.observed.conv_rate }}%</div>
<div style="font-size:11px;color:var(--text2);text-transform:uppercase">Conv Rate ({{ impact_metrics.observed.conv_denominator }} worked)</div>
</div>
<div style="text-align:center;padding:8px;background:var(--bg);border-radius:var(--radius)">
<div style="font-size:24px;font-weight:700;color:var(--success)">{{ impact_metrics.observed.completed_visits }}</div>
<div style="font-size:11px;color:var(--text2);text-transform:uppercase">Completed Visits</div>
</div>
</div>
<div style="border-top:1px solid var(--border);padding-top:12px;margin-bottom:12px">
<div style="font-size:13px;font-weight:600;margin-bottom:8px">Simulated Evaluation: Agent vs Oldest-First Baseline</div>
<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;font-size:13px">
<div><strong>Agent:</strong> ${{ "{:,.0f}".format(impact_metrics.simulated.agent_expected) }} expected from {{ impact_metrics.simulated.contact_budget }} leads ({{ impact_metrics.simulated.agent_high_value }} high-value)</div>
<div><strong>Baseline:</strong> ${{ "{:,.0f}".format(impact_metrics.simulated.baseline_expected) }} expected from {{ impact_metrics.simulated.contact_budget }} leads ({{ impact_metrics.simulated.baseline_high_value }} high-value)</div>
<div><strong>Advantage:</strong> ${{ "{:,.0f}".format(impact_metrics.simulated.agent_advantage) }}</div>
</div>
<div style="font-size:12px;color:var(--text2);margin-top:4px">Same contact budget. Booking rates: 8% no-response, 6% slow (>48h), 4% normal. Simulated, not measured.</div>
</div>
<div style="border-top:1px solid var(--border);padding-top:12px">
<div style="font-size:13px;font-weight:600;margin-bottom:4px">Projected Incremental Revenue Opportunity</div>
<div style="font-size:14px">{{ impact_metrics.projected.total_eligible }} eligible leads &times; {{ (impact_metrics.projected.incremental_booking_rate * 100)|round(0) }}% booking &times; {{ (impact_metrics.projected.attendance_rate * 100)|round(0) }}% attendance &times; ${{ "{:,.0f}".format(impact_metrics.projected.rev_per_visit) }}/visit = <strong>${{ "{:,.0f}".format(impact_metrics.projected.projected_revenue) }}</strong></div>
<div style="font-size:12px;color:var(--text2);margin-top:4px">Transparent assumptions. Not attributed revenue or proven causal uplift. Scaling across clinics contributes to growth goal but does not close the entire revenue gap.</div>
</div>
</div>
</div>
<div class="tabs">
<button class="tab active" onclick="showTab('queue',this)">Recovery Queue</button>
<button class="tab" onclick="showTab('tasks',this)">Follow-ups</button>
</div>
<div id="queue" class="sec active">
<div class="qh"><div><h2>Recovery Queue</h2><div class="cnt">{% if search %}{{ leads_data.total }} match{{ 'es' if leads_data.total != 1 else '' }} of {{ leads_data.total_eligible }} eligible leads{% else %}Showing {{ leads_data.leads|length }} of {{ leads_data.total }} eligible leads{% endif %}</div></div>
<label for="searchInput" class="sr-only">Search leads</label><input type="text" class="sbox" id="searchInput" placeholder="Search by lead ID or source..." value="{{ search }}" aria-label="Search by lead ID or source" oninput="searchTimer(this.value)"></div>
{% if leads_data.leads %}
<table class="qt"><thead><tr><th>#</th><th>Lead</th><th>Why Now</th><th class="hm">Contact History</th><th>Status</th></tr></thead><tbody>
{% for lead in leads_data.leads %}
<tr onclick="openLead('{{ lead.lead_id }}')" id="row-{{ lead.lead_id }}" tabindex="0" role="button" aria-label="Open {{ lead.first_name }} {{ lead.last_name }} lead details" onkeydown="if(event.key==='Enter'||event.key===' '||event.key==='Spacebar'){event.preventDefault();openLead('{{ lead.lead_id }}')}">
<td>{{ lead.rank }}</td>
<td><div style="font-weight:600">{{ lead.first_name }} {{ lead.last_name }}</div><div style="font-size:12px;color:var(--text2)">{{ lead.lead_id }} &middot; {{ lead.source }}</div></td>
<td><span class="ptag t-{{ lead.priority_color }}">{{ lead.priority_label }}</span>{% if lead.data_quality_warning %}<div style="font-size:12px;color:var(--warning);margin-top:4px;font-weight:600">&#9888; Data quality: {{ lead.data_quality_warning }}</div>{% endif %}</td>
<td class="hm">{{ lead.touch_info }}<div style="font-size:12px;color:var(--text2)">First response: {% if lead.first_response_hours is not none %}{{ "%.1f"|format(lead.first_response_hours|float) }} hrs{% else %}Not recorded{% endif %}</div></td>
<td>{{ lead.status }}</td>
</tr>
{% endfor %}
</tbody></table>
{% if leads_data.pages > 1 %}
<div class="pg">{% if page > 1 %}<a href="?location_id={{ location_id }}&snapshot_date={{ snapshot_date }}&lookback={{ lookback }}&page={{ page - 1 }}&search={{ search|urlencode }}&today={{ today }}">&larr; Prev</a>{% else %}<a class="dis">&larr; Prev</a>{% endif %}
<span style="padding:8px;font-size:14px">Page {{ page }} of {{ leads_data.pages }}</span>
{% if page < leads_data.pages %}<a href="?location_id={{ location_id }}&snapshot_date={{ snapshot_date }}&lookback={{ lookback }}&page={{ page + 1 }}&search={{ search|urlencode }}&today={{ today }}">Next &rarr;</a>{% else %}<a class="dis">Next &rarr;</a>{% endif %}</div>
{% endif %}
{% else %}
<div class="empty"><h3>{% if search %}0 matches of {{ leads_data.total_eligible }} eligible leads{% else %}No eligible leads found{% endif %}</h3><p>{% if search %}No leads match &ldquo;{{ search }}&rdquo;. Try a different search term.{% else %}Try adjusting the snapshot date, lookback period, or search.{% endif %}</p></div>
{% endif %}
</div>
<div id="tasks" class="sec">
<h2 style="font-size:18px;font-weight:700;margin-bottom:16px">Follow-up Tasks</h2>
{% if tasks %}
<div class="tlist">{% for t in tasks %}<div class="titem" role="button" tabindex="0" onclick="openLead('{{ t.lead_id }}')" onkeypress="if(event.key==='Enter')openLead('{{ t.lead_id }}')" style="cursor:pointer;transition:all 150ms;" aria-label="Open {{ t.first_name }} {{ t.last_name }} lead details" title="Click to open lead"><div class="info" style="flex:1"><div class="n">{{ t.first_name }} {{ t.last_name }}</div><div class="m">{{ t.lead_id }} &middot; {{ t.decision|replace('_',' ')|title }}</div>{% if t.rationale %}<div class="m">{{ t.rationale }}</div>{% endif %}{% if t.duplicate_count and t.duplicate_count > 0 %}<div class="m" style="font-size:11px;color:var(--warning);font-weight:600">&#9888; {{ t.duplicate_count }} additional pending task{{ 's' if t.duplicate_count != 1 else '' }} for this lead (deduplicated)</div>{% endif %}</div>{% if t.due_label == 'Due Now' %}<span style="font-size:12px;font-weight:600;padding:4px 10px;border-radius:6px;background:#FEE2E2;color:#991B1B" aria-label="Due now">Due Now</span>{% else %}<span style="font-size:12px;font-weight:600;padding:4px 10px;border-radius:6px;background:#DBEAFE;color:#1E40AF" aria-label="Scheduled for {{ t.due_date }}">Scheduled{% if t.due_date %} · {{ t.due_date }}{% endif %}</span>{% endif %}</div>{% endfor %}</div>
{% else %}
<div class="empty"><h3>No pending tasks</h3><p>All follow-up tasks are completed.</p></div>
{% endif %}
</div>
<div id="impact" class="sec" style="display:none">
<div class="ic"><h2 style="font-size:20px;font-weight:700;margin-bottom:6px">Recovery Opportunity</h2>
<p style="font-size:14px;color:var(--text2);margin-bottom:24px">Estimated revenue scenarios based on eligible leads at this clinic</p>
<table class="it" style="margin-bottom:16px;"><thead><tr><th>Scenario</th><th>Ongoing-Client Rate</th><th>Expected Ongoing Clients</th><th style="text-align:right">First-Year Clinic Revenue Opportunity</th></tr></thead><tbody>
{% for s in impact.scenarios %}<tr{% if s.label == 'Base' %} style="background:rgba(8,127,140,0.06);"{% endif %}><td style="font-weight:{% if s.label == 'Base' %}700{% else %}400{% endif %}">{{ s.label }}</td><td>{{ s.rate_pct }}</td><td>{{ s.expected_clients }}</td><td style="text-align:right;font-weight:600">${{ "{:,.0f}".format(s.first_year_revenue) }}</td></tr>{% endfor %}
</tbody></table>
<div class="bar-chart">
{% set max_val = impact.scenarios[2].first_year_revenue if impact.scenarios[2].first_year_revenue > 0 else 1 %}
{% for s in impact.scenarios %}
<div class="bar-row"><div class="bar-label"><span>{{ s.label }} ({{ s.rate_pct }})</span><span>${{ "{:,.0f}".format(s.first_year_revenue) }}</span></div><div class="bar-track"><div class="bar-fill{% if s.label == 'Base' %} base{% endif %}" style="width:{{ (s.first_year_revenue / max_val * 100)|round(1) }}%"></div></div></div>
{% endfor %}
</div>
<div class="exp-section"><button class="exp-toggle" type="button" aria-expanded="false" aria-controls="calc-details" onclick="toggleExpand(this)"><span class="arrow">▶</span> How this is calculated</button><div id="calc-details" class="exp-content" role="region" aria-label="Calculation details"><p style="margin-bottom:8px"><strong>Expected ongoing clients</strong> = Eligible leads ({{ impact.eligible }}) &times; Ongoing-client conversion rate (scenario)</p><p style="margin-bottom:8px"><strong>First-year revenue per client</strong> = Avg revenue per visit (${{ "{:,.0f}".format(impact.avg_rev_per_visit) }}) &times; Visits per active month ({{ impact.visits_per_month }}) &times; Active months ({{ impact.active_months }}) = <strong>${{ "{:,.0f}".format(impact.first_year_rev_per_client) }}</strong></p><p style="margin-bottom:8px"><strong>First-year clinic revenue opportunity</strong> = Expected ongoing clients &times; First-year revenue per client</p><p style="font-size:13px;color:var(--text2);margin-top:12px">Eligible leads: {{ impact.eligible }} at this clinic | Lookback: {{ lookback }} days from {{ snapshot_date }}</p></div></div>
<div class="inote" style="margin-top:16px">These are <strong>cohort scenario estimates</strong>, not measured predictions. The {{ impact.eligible }} eligible leads come from a {{ lookback }}-day lookback and represent a one-time cohort, not a recurring pipeline. Revenue figures represent the cohort’s expected revenue over the 12 months after conversion, not collected revenue. Ongoing-client conversion rates and active months are assumptions unless validated experimentally. Revenue per visit and visit frequency are derived from historical clinic data.</div>
</div>
<div class="ic" style="margin-top:20px">
<h2 style="font-size:20px;font-weight:700;margin-bottom:6px">Path to $100M &ndash; $250M Revenue Opportunity</h2>
<p style="font-size:14px;color:var(--text2);margin-bottom:24px">How lead recovery scales across chiropractic clinics nationwide</p>
<div class="arr-hero"><div class="arr-hero-label">TARGET REVENUE ZONE</div><div class="arr-hero-range">$100M &ndash; $250M</div><div class="arr-hero-sub">Estimated annual clinic revenue opportunity at scale</div></div>
<div class="arr-traj"><h3 style="font-size:14px;font-weight:600;color:var(--text2);text-transform:uppercase;margin-bottom:16px">Growth Trajectory</h3>
{% set arr_max = arr_projection.projections[-1].arr_high if arr_projection.projections and arr_projection.projections[-1].arr_high > 0 else 1 %}
{% for p in arr_projection.projections %}
<div class="arr-row"><div class="arr-row-label"><span class="arr-ic">{{ p.icon }}</span><div><div class="arr-nm">{{ p.label }}</div><div class="arr-cl">{{ '{:,.0f}'.format(p.clinics) }} clinic{{ 's' if p.clinics != 1 else '' }}</div></div></div><div class="arr-bar-wrap"><div class="arr-bar-fill{% if p.in_target %} target{% endif %}" style="width:{{ (p.arr_high / arr_max * 100)|round(1) }}%"></div></div><div class="arr-val">{% if p.arr_base >= 1000000 %}${{ '{:,.1f}'.format(p.arr_base / 1000000) }}M{% elif p.arr_base >= 1000 %}${{ '{:,.0f}'.format(p.arr_base / 1000) }}K{% else %}${{ '{:,.0f}'.format(p.arr_base) }}{% endif %}</div></div>
{% endfor %}
</div>
<div class="arr-cards">
{% for p in arr_projection.projections %}
<div class="arr-card{% if p.in_target %} target{% endif %}"><div class="arr-card-ic">{{ p.icon }}</div><div class="arr-card-nm">{{ p.label }}</div><div class="arr-card-cl">{{ '{:,.0f}'.format(p.clinics) }} clinic{{ 's' if p.clinics != 1 else '' }}</div><div class="arr-card-arr">{% if p.arr_base >= 1000000 %}${{ '{:,.1f}'.format(p.arr_base / 1000000) }}M{% elif p.arr_base >= 1000 %}${{ '{:,.0f}'.format(p.arr_base / 1000) }}K{% else %}${{ '{:,.0f}'.format(p.arr_base) }}{% endif %}</div><div class="arr-card-rg">{% if p.arr_high >= 1000000 %}up to ${{ '{:,.1f}'.format(p.arr_high / 1000000) }}M{% elif p.arr_high >= 1000 %}up to ${{ '{:,.0f}'.format(p.arr_high / 1000) }}K{% else %}up to ${{ '{:,.0f}'.format(p.arr_high) }}{% endif %}</div></div>
{% endfor %}
</div>
<div class="arr-kmetrics"><div class="arr-km"><div class="arr-km-v">${{ '{:,.0f}'.format(arr_projection.avg_rev_per_visit) }}</div><div class="arr-km-l">Rev/visit (historical)</div></div><div class="arr-km"><div class="arr-km-v">{{ arr_projection.visits_per_month }}</div><div class="arr-km-l">Visits/mo (active)</div></div><div class="arr-km"><div class="arr-km-v">${{ '{:,.0f}'.format(arr_projection.first_year_rev_per_client) }}</div><div class="arr-km-l">First-year rev/client</div></div><div class="arr-km"><div class="arr-km-v">${{ '{:,.0f}'.format(arr_projection.per_clinic_base) }}</div><div class="arr-km-l">Annual rev/clinic (base)</div></div></div>
<div class="exp-section"><button class="exp-toggle" type="button" aria-expanded="false" aria-controls="arr-calc" onclick="toggleExpand(this)"><span class="arrow">&#9654;</span> How we get to $100M &ndash; $250M</button><div id="arr-calc" class="exp-content" role="region" aria-label="Revenue calculation details"><p style="margin-bottom:8px"><strong>First-year revenue per client</strong> = Avg revenue per visit (${{ '{:,.0f}'.format(arr_projection.avg_rev_per_visit) }}) &times; Visits per active month ({{ arr_projection.visits_per_month }}) &times; Active months ({{ arr_projection.active_months }}) = <strong>${{ '{:,.0f}'.format(arr_projection.first_year_rev_per_client) }}/client</strong></p><p style="margin-bottom:8px"><strong>Annual per-clinic revenue</strong> = Assumed annual pipeline ({{ arr_projection.annual_pipeline_leads }} new eligible leads/clinic/year, documented assumption) &times; Ongoing-client conversion rate &times; First-year revenue per client. At base rate (5%): <strong>${{ '{:,.0f}'.format(arr_projection.per_clinic_base) }}/clinic/year</strong>. At high rate (10%): <strong>${{ '{:,.0f}'.format(arr_projection.per_clinic_high) }}/clinic/year</strong>.</p><p style="margin-bottom:8px"><strong>Scaling model</strong> = Annual per-clinic revenue &times; Number of clinics onboarded. At <strong>{{ '{:,.0f}'.format(arr_projection.clinics_100m) }} clinics</strong> (base rate), revenue opportunity reaches <strong>$100M</strong>. At <strong>{{ '{:,.0f}'.format(arr_projection.clinics_250m) }} clinics</strong> (base rate), revenue opportunity reaches <strong>$250M</strong>; at high rate across the same clinics, up to <strong>$500M</strong>.</p><p style="margin-bottom:8px"><strong>Market context</strong> = ~{{ '{:,.0f}'.format(arr_projection.addressable_clinics) }} chiropractic clinics in the US (sourced estimate, not individual chiropractors). {{ '{:,.0f}'.format(arr_projection.clinics_250m) }} clinics represents {{ (arr_projection.clinics_250m / arr_projection.addressable_clinics * 100)|round(1) }}% market penetration.{% if arr_projection.clinics_250m > arr_projection.addressable_clinics %} <strong style="color:var(--error)">This target exceeds the total addressable market.</strong>{% endif %}</p><p style="margin-bottom:8px"><strong>Platform ARR</strong> (the product company's own revenue, separate from clinic revenue) = ${{ '{:,.0f}'.format(arr_projection.platform_price_per_month) }}/month per clinic &times; 12 = <strong>${{ '{:,.0f}'.format(arr_projection.platform_arr_per_clinic) }}/clinic/year</strong>. At {{ '{:,.0f}'.format(arr_projection.clinics_100m) }} clinics: ${{ '{:,.1f}'.format(arr_projection.platform_arr_per_clinic * arr_projection.clinics_100m / 1000000) }}M ARR. At {{ '{:,.0f}'.format(arr_projection.clinics_250m) }} clinics: ${{ '{:,.1f}'.format(arr_projection.platform_arr_per_clinic * arr_projection.clinics_250m / 1000000) }}M ARR.</p><p style="font-size:13px;color:var(--text2);margin-top:12px">These are scenario projections based on current clinic data, not measured outcomes. The current {{ arr_projection.eligible }} eligible leads represent a one-time cohort from a {{ lookback }}-day lookback; annual scaling assumes {{ arr_projection.annual_pipeline_leads }} new eligible leads per clinic per year (documented assumption). Ongoing-client conversion rates and active months are assumptions unless validated experimentally. Revenue per visit and visit frequency are derived from historical clinic data. Platform pricing (${{ '{:,.0f}'.format(arr_projection.platform_price_per_month) }}/month/clinic) is an explicit recurring SaaS subscription model, not a percentage of clinic revenue.</p></div></div>
</div>
<div class="ic" style="margin-top:20px"><h2 style="font-size:20px;font-weight:700;margin-bottom:6px">Lead Status Breakdown</h2>
<p style="font-size:14px;color:var(--text2);margin-bottom:20px">All leads at {{ clinic_name }}</p>
<div style="margin-bottom:20px;background:var(--bg);border-radius:var(--radius);overflow:hidden;height:48px;display:flex;">
{% set total = (metrics.converted + metrics.open_leads + metrics.lost) if (metrics.converted + metrics.open_leads + metrics.lost) > 0 else 1 %}
{% if total > 0 %}
<div style="background:#E3FCEF;width:{{ (metrics.converted / total * 100)|round(1) }}%;display:flex;align-items:center;justify-content:center;font-weight:600;font-size:14px;color:var(--success)">{{ metrics.converted }}</div>
<div style="background:rgba(8,127,140,0.15);width:{{ (metrics.open_leads / total * 100)|round(1) }}%;display:flex;align-items:center;justify-content:center;font-weight:600;font-size:14px;color:var(--primary)">{{ metrics.open_leads }}</div>
<div style="background:#FFEBE6;width:{{ (metrics.lost / total * 100)|round(1) }}%;display:flex;align-items:center;justify-content:center;font-weight:600;font-size:14px;color:var(--error)">{{ metrics.lost }}</div>
{% endif %}
</div>
<div style="display:flex;justify-content:space-around;text-align:center;">
<div><div style="font-size:12px;color:var(--text2);text-transform:uppercase;letter-spacing:0.05em;margin-bottom:4px">Converted</div><div style="font-size:20px;font-weight:700;color:var(--success)">{{ metrics.converted }}</div></div>
<div><div style="font-size:12px;color:var(--text2);text-transform:uppercase;letter-spacing:0.05em;margin-bottom:4px">Open</div><div style="font-size:20px;font-weight:700;color:var(--primary)">{{ metrics.open_leads }}</div></div>
<div><div style="font-size:12px;color:var(--text2);text-transform:uppercase;letter-spacing:0.05em;margin-bottom:4px">Lost</div><div style="font-size:20px;font-weight:700;color:var(--error)">{{ metrics.lost }}</div></div>
</div>
<div style="font-size:13px;color:var(--text2);margin-top:16px;padding-top:16px;border-top:1px solid var(--border)">Total leads: <strong>{{ metrics.total_at_clinic }}</strong> &middot; Contact-capped (3+ touches): <strong>{{ metrics.contact_capped }}</strong></div>
</div>
</div>
</div>
<div class="ovr" id="overlay" onclick="closeDrawer()" aria-hidden="true"></div>
<div class="dr" id="drawer" role="dialog" aria-modal="true" aria-labelledby="dtitle" aria-hidden="true" hidden><div class="dr-h"><h2 id="dtitle">Lead Detail</h2><button class="dr-x" onclick="closeDrawer()" aria-label="Close lead details">&times;</button></div><div class="dr-b" id="dbody"><div class="loading">Loading...</div></div></div>
<script>
var locId='{{ location_id }}';var snapDate='{{ snapshot_date }}';var stid;var lastFocusedElement=null;var lastAgentRec=null;
function showTab(n,b){document.querySelectorAll('.sec').forEach(s=>s.classList.remove('active'));document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));document.getElementById(n).classList.add('active');b.classList.add('active');}
function searchTimer(v){clearTimeout(stid);stid=setTimeout(function(){window.location.href='?location_id='+locId+'&snapshot_date='+snapDate+'&lookback={{ lookback }}&search='+encodeURIComponent(v)+'&today={{ today }}';},400);}
function validateLookback(v){var n=parseInt(v);if(isNaN(n)||n<1||n>365){alert('Lookback must be between 1 and 365 days. Value will be adjusted to fit the allowed range.');n=Math.max(1,Math.min(365,isNaN(n)?120:n));}window.location.href='?location_id={{ location_id }}&snapshot_date={{ snapshot_date }}&lookback='+n+'&today={{ today }}';}
function openLead(id){lastFocusedElement=document.activeElement;document.querySelectorAll('.qt tr').forEach(r=>r.classList.remove('sel'));var r=document.getElementById('row-'+id);if(r)r.classList.add('sel');var ovr=document.getElementById('overlay'),dr=document.getElementById('drawer');ovr.classList.add('show');ovr.setAttribute('aria-hidden','false');dr.classList.add('show');dr.removeAttribute('hidden');dr.setAttribute('aria-hidden','false');document.getElementById('dtitle').textContent='Loading...';document.getElementById('dbody').innerHTML='<div style="padding:24px;"><div class="skel skel-title"></div><div class="skel skel-line"></div><div class="skel skel-line"></div><div class="skel skel-line" style="width:70%"></div><div style="height:24px"></div><div class="skel skel-title"></div><div class="skel skel-line"></div><div class="skel skel-line"></div></div>';fetch('/api/lead/'+id+'?location_id='+locId+'&snapshot_date='+snapDate).then(r=>r.json()).then(d=>renderLead(d)).catch(e=>{document.getElementById('dbody').innerHTML='<div class="empty"><h3>Error loading lead</h3><p>'+e.message+'</p></div>';});setTimeout(()=>{var closeBtn=document.querySelector('.dr-x');if(closeBtn)closeBtn.focus();},100);}
function renderLead(d){if(d.error){document.getElementById('dbody').innerHTML='<div class="empty"><h3>Error</h3><p>'+d.error+'</p></div>';return;}document.getElementById('dtitle').textContent=d.first_name+' '+d.last_name;var frh=d.first_response_hours!==null&&d.first_response_hours!==undefined?parseFloat(d.first_response_hours).toFixed(1)+' hours':'Not recorded';var h='';
h+='<div class="ds"><h3>Status</h3><div style="font-size:16px;font-weight:600">'+d.status+'</div><div style="font-size:14px;color:var(--text2);margin-top:4px">'+(d.priority_label||'Standard')+'</div></div>';var frhVal=d.first_response_hours!==null&&d.first_response_hours!==undefined?parseFloat(d.first_response_hours):null;
h+='<div class="ds"><h3>Why This Lead Is Prioritized</h3><div style="font-size:14px"><strong>'+(d.priority_label||'Standard')+'</strong> — ';if(frhVal===null){h+='No response has been recorded. This lead has not been contacted.';}else if(frhVal>48){h+='First response took '+frhVal.toFixed(1)+' hours (slower than the 48-hour threshold).';}else if(frhVal>24){h+='First response took '+frhVal.toFixed(1)+' hours (delayed, between 24-48 hours).';}else{h+='First response was within '+frhVal.toFixed(1)+' hours.';}var touches=parseInt(d.effective_touchpoints)||parseInt(d.num_touchpoints)||0;h+='<div style="margin-top:4px;color:var(--text2)">'+(touches===0?'No contact attempts yet':touches+' previous contact attempts')+'</div></div></div>';
h+='<div class="ds"><h3>Verified Facts</h3><div class="fr"><span class="l">Lead ID</span><span class="v">'+d.lead_id+'</span></div><div class="fr"><span class="l">Source</span><span class="v">'+d.source+'</span></div><div class="fr"><span class="l">Status</span><span class="v">'+d.status+'</span></div><div class="fr"><span class="l">Created</span><span class="v">'+(d.created_date||'Unknown')+'</span></div><div class="fr"><span class="l">Touchpoints</span><span class="v">'+(d.effective_touchpoints!==undefined?d.effective_touchpoints:d.num_touchpoints)+'</span></div><div class="fr"><span class="l">First response</span><span class="v">'+frh+'</span></div><div class="fr"><span class="l">Phone</span><span class="v" style="font-size:12px;color:var(--text2)">'+(d.phone||'N/A')+' (synthetic)</span></div><div class="fr"><span class="l">Email</span><span class="v" style="font-size:12px;color:var(--text2)">'+(d.email||'N/A')+' (synthetic)</span></div></div>';
if(d.data_quality_warning){h+='<div class="ds"><h3>Data quality</h3><div style="font-size:14px;color:var(--warning);padding:12px;background:#FEF3C7;border:1px solid #FDE68A;border-radius:8px"><strong>Data quality warning:</strong> '+d.data_quality_warning+'</div></div>';}
h+='<div class="ds"><h3>Recommended next step</h3><div id="ag"><button class="btn btn-p" style="width:100%" type="button" onclick="runAgent(\''+d.lead_id+'\')">Run Agent Recommendation</button></div></div>';
h+='<div class="ds"><h3>Recommended Draft</h3><div id="da"><button class="btn btn-p" style="width:100%" type="button" onclick="genDraft(\''+d.lead_id+'\')" >Generate AI Draft</button></div></div>';
h+='<div class="ds"><h3>Log Call Outcome</h3><div class="af"><label for="os">Outcome</label><select id="os" aria-label="Select call outcome"><option value="">Select outcome...</option><option value="No answer">No answer</option><option value="Connected">Connected</option><option value="Wrong number">Wrong number</option><option value="Appointment booked">Appointment booked</option><option value="No longer interested">No longer interested</option></select><label for="on">Note (optional)</label><textarea id="on" aria-label="Optional note about the call" rows="2" placeholder="Add context about the call..."></textarea><label for="fd">Follow-up Date (optional)</label><input type="date" id="fd" aria-label="Optional follow-up date"><button class="btn btn-p" style="width:100%" type="button" onclick="logOut(\''+d.lead_id+'\')" >Log Outcome</button><div id="ar" style="margin-top:8px" role="alert" aria-live="polite"></div></div></div>';
h+='<div class="ds"><h3>Save Note</h3><div class="af"><label for="sn" class="sr-only">Internal note</label><textarea id="sn" aria-label="Internal note about this lead" rows="2" placeholder="Internal note about this lead..."></textarea><button class="btn btn-s" style="width:100%" type="button" onclick="saveN(\''+d.lead_id+'\')" >Save Note</button><div id="nr" style="margin-top:8px" role="alert" aria-live="polite"></div></div></div>';
if(d.actions&&d.actions.length>0){h+='<div class="ds"><h3>Recent Activity</h3><div class="tl">';d.actions.forEach(function(a){var t=a.reviewed_at?new Date(a.reviewed_at).toLocaleString():'';var badge=a.review_status==='superseded'?' <span style="font-size:10px;padding:2px 6px;border-radius:4px;background:#F1F5F9;color:var(--text2)">superseded</span>':'';h+='<div class="ti"><div class="a">'+(a.decision||'').replace(/_/g,' ')+(a.outcome?' — '+a.outcome:'')+badge+'</div><div class="t">'+t+'</div>';if(a.rationale)h+='<div style="font-size:13px;color:var(--text2)">'+a.rationale+'</div>';h+='</div>';});h+='</div></div>';}
h+='<div class="ds"><h3>Close Lead</h3><div class="af"><label for="cr">Reason (required)</label><input type="text" id="cr" aria-label="Reason for closing lead" placeholder="Why is this lead being closed?"><button class="btn btn-e" style="width:100%" type="button" onclick="closeL(\''+d.lead_id+'\')" >Close Lead</button><div id="clr" style="margin-top:8px" role="alert" aria-live="polite"></div></div></div>';
document.getElementById('dbody').innerHTML=h;}
function genDraft(id){
    var da = document.getElementById('da');
    da.innerHTML = '<div class="loading">Generating draft...</div>';
    fetch('/api/lead/' + id + '/draft?location_id=' + locId)
    .then(r => r.json())
    .then(d => {
        if (d.error) {
            da.innerHTML = '<div class="ag-panel"><div style="color:var(--error)">Error: ' + d.error + '</div><div class="ag-actions"><button class="btn btn-s btn-sm" type="button" onclick="genDraft(\'' + id + '\')">Retry</button></div></div>';
            return;
        }
        var headline = d.headline || 'Draft Recommendation';
        var badge = d.badge || '';
        var rationale = d.rationale || '';
        var action = d.action || '';
        var message = d.message || '';
        var raw = d.recommendation || 'Unable to generate';
        var badgeClass = 'ag-NO_ACTION';
        if (badge) {
            var bl = badge.toLowerCase();
            if (bl.indexOf('call') >= 0) { badgeClass = 'ag-CALL'; }
            else if (bl.indexOf('email') >= 0) { badgeClass = 'ag-DRAFT_OUTREACH'; }
        }
        var h = '<div class="ag-panel">';
        h += '<div class="ag-headline">' + headline + '</div>';
        if (action) {
            h += '<div class="dr-sect"><div class="dr-sect-label">Recommended action</div><span class="ag-badge ' + badgeClass + '">' + action + '</span></div>';
        }
        if (rationale) {
            h += '<div class="dr-sect"><div class="dr-sect-label">Rationale</div><div class="ag-rat" style="margin:0">' + rationale + '</div></div>';
        }
        var msgText = message || raw;
        h += '<div class="dr-sect"><div class="dr-sect-label">Draft message</div><div class="dr-msg" id="dt">' + msgText + '</div></div>';
        h += '<div class="ag-actions">';
        h += '<button class="btn btn-p btn-sm" type="button" onclick="copyDraftText()">Copy Message</button>';
        h += '<button class="btn btn-s btn-sm" type="button" onclick="genDraft(\''+id+'\')">Regenerate</button>';
        h += '</div>';
        h += '</div>';
        da.innerHTML = h;
    })
    .catch(e => {
        da.innerHTML = '<div class="ag-panel"><div style="color:var(--error)">Request failed: ' + e.message + '</div><div class="ag-actions"><button class="btn btn-s btn-sm" type="button" onclick="genDraft(\'' + id + '\')">Retry</button></div></div>';
    });
}

function copyDraftText(){var t=document.getElementById('dt');var txt=t.textContent||t.innerText;navigator.clipboard.writeText(txt).then(function(){alert('Draft message copied to clipboard. This is a draft, not a sent message.');}).catch(function(err){var temp=document.createElement('textarea');temp.value=txt;document.body.appendChild(temp);temp.select();document.execCommand('copy');document.body.removeChild(temp);alert('Draft message copied to clipboard. This is a draft, not a sent message.');});}

function runAgent(id){var ag=document.getElementById('ag');ag.innerHTML='<div class="loading">Analyzing...</div>';fetch('/api/agent/recommend/'+id+'?location_id='+locId).then(r=>r.json()).then(rec=>{if(rec.action==='ERROR'){ag.innerHTML='<div class="ag-panel"><div style="color:var(--error)">'+rec.rationale+'</div><button class="btn btn-s btn-sm" style="margin-top:12px" type="button" onclick="runAgent(\''+id+'\')">Retry</button></div>';return;}lastAgentRec=rec;var h='<div class="ag-panel">';var headline='Unknown';var badgeClass='NO_ACTION';var badgeText='';if(rec.action==='NO_ACTION'){headline='No follow-up needed';badgeClass='NO_ACTION';badgeText='No action';}else if(rec.action==='CALL'){headline='Follow-up call recommended';badgeClass='CALL';badgeText='Call';}else if(rec.action==='DRAFT_OUTREACH'){headline='Written outreach recommended';badgeClass='DRAFT_OUTREACH';badgeText='Message';}else if(rec.action==='FOLLOW_UP'){headline='Schedule follow-up';badgeClass='FOLLOW_UP';badgeText='Follow-up';}else if(rec.action==='STAFF_REVIEW'){headline='Status needs review';badgeClass='STAFF_REVIEW';badgeText='Review needed';}h+='<div class="ag-headline">'+headline+'<span class="ag-badge ag-'+badgeClass+'">'+badgeText+'</span></div>';var shortRat=rec.rationale||'No rationale provided.';if(shortRat.length>240){var sentences=shortRat.match(/[^.!?]+[.!?]+/g)||[shortRat];if(sentences.length>2){shortRat=sentences.slice(0,2).join(' ');}}h+='<div class="ag-rat">'+shortRat+'</div>';function humanEvidence(e){var m={'converted_flag=True':'Converted','converted_flag=False':'Not converted','converted_flag=true':'Converted','converted_flag=false':'Not converted','booking_status=booked':'Appointment booked','clinic_capacity=available':'Capacity available','clinic_capacity=limited':'Limited capacity','first_response_hours=None':'No response recorded','first_response_hours=null':'No response recorded'};for(var k in m){if(e.indexOf(k)>=0)return m[k];}if(e.indexOf('effective_touchpoints=')===0){var n=e.split('=')[1];return n+' contact'+(n==='1'?' attempt':' attempts');}if(e.indexOf('status=')===0){var s=e.split('=')[1];var statusMap={'new':'New lead','contacted':'Contacted','qualified':'Qualified','lost':'Lost'};return statusMap[s.toLowerCase()]||('Status: '+s);}if(e.indexOf('first_response_hours=')===0){var hr=e.split('=')[1];var hrs=parseFloat(hr);if(!isNaN(hrs)){if(hrs<24)return'Quick response ('+hrs.toFixed(0)+'h)';if(hrs<48)return'Response in '+hrs.toFixed(0)+' hours';return'Slow response ('+hrs.toFixed(0)+'h)';}}return e;}var statusConflict=false;if(rec.evidence){var hasConverted=rec.evidence.some(function(e){return e.indexOf('converted_flag=true')>=0||e.indexOf('converted_flag=True')>=0;});var hasQualLost=rec.evidence.some(function(e){return e.indexOf('status=')>=0&&(e.indexOf('Qualified')>=0||e.indexOf('Lost')>=0);});var statusNew=rec.evidence.some(function(e){return e.indexOf('status=')>=0&&(e.indexOf('New')>=0||e.indexOf('Contacted')>=0);});if(hasConverted&&statusNew){statusConflict=true;}else if(!hasConverted&&hasQualLost){statusConflict=true;}}if(statusConflict){h+='<div class="ag-status-note"><strong>Status needs review:</strong> Lead status and conversion flag disagree. Review data before taking action.</div>';}if(rec.evidence&&rec.evidence.length>0){h+='<div class="ag-ev">';rec.evidence.forEach(function(e){h+='<span class="ag-ev-item">'+humanEvidence(e)+'</span>';});h+='</div>';}if(rec.missing_info&&rec.missing_info.length>0){h+='<div class="ag-miss"><strong>Missing information:</strong> ';rec.missing_info.forEach(function(m,i){h+=(i>0?', ':'')+m;});h+='</div>';}if(rec.tool_calls&&rec.tool_calls.length>0){var agentCalls=rec.tool_calls.filter(function(tc){return tc.description;});if(agentCalls.length>0){h+='<div class="ag-disclosure"><button class="ag-disclosure-btn" type="button" onclick="toggleAgentDisclosure(this)" aria-expanded="false"><span class="ag-disclosure-chevron">&#9654;</span>Technical details · '+agentCalls.length+' tool call'+(agentCalls.length===1?'':'s')+'</button><div class="ag-disclosure-content">';agentCalls.forEach(function(tc){h+='<div class="ag-tool"><div class="ag-tool-name">'+tc.tool+'</div><div class="ag-tool-desc">'+tc.description+'</div><div class="ag-tool-res">'+JSON.stringify(tc.result,null,2)+'</div></div>';});h+='<div class="ag-impl-note">Implementation: '+(rec.is_fallback?'Rule-based decision engine (deterministic fallback)':'AI-enhanced analysis via ai_gen SQL function')+'</div>';h+='</div></div>';}}h+='<div class="ag-actions">';if(rec.action!=='NO_ACTION'){h+='<button class="btn btn-p btn-sm" type="button" onclick="approveAgent(\''+id+'\')">Approve & Persist</button>';}h+='<button class="btn btn-s btn-sm" type="button" onclick="runAgent(\''+id+'\')">Re-run analysis</button></div>';h+='</div>';ag.innerHTML=h;}).catch(e=>{ag.innerHTML='<div class="ag-panel"><div style="color:var(--error)">Request failed: '+e.message+'</div><button class="btn btn-s btn-sm" style="margin-top:12px" type="button" onclick="runAgent(\''+id+'\')">Retry</button></div>';});}
function approveAgent(id){if(!lastAgentRec){alert('No recommendation to approve');return;}var ag=document.getElementById('ag');ag.innerHTML='<div class="loading">Persisting recommendation...</div>';var fd=new FormData();fd.append('location_id',locId);fd.append('snapshot_date',snapDate);fd.append('recommendation',JSON.stringify(lastAgentRec));fetch('/api/agent/approve/'+id,{method:'POST',body:fd}).then(r=>r.json()).then(d=>{if(d.success){ag.innerHTML='<div class="ag-panel"><div style="color:var(--success);font-weight:600">OK: '+d.message+'</div>'+(d.task_id?'<div style="font-size:13px;color:var(--text2);margin-top:4px">Task ID: '+d.task_id+'</div>':'')+(d.draft?'<div style="margin-top:8px"><div class="fb-note">Draft generated:</div><textarea rows="4" style="width:100%;padding:8px;border:1px solid var(--border);border-radius:4px;font-size:13px;margin-top:4px">'+d.draft+'</textarea></div>':'')+'</div>';}else{ag.innerHTML='<div class="ag-panel"><div style="color:var(--error)">Failed: '+d.error+'</div><button class="btn btn-s btn-sm" style="margin-top:8px" type="button" onclick="runAgent(\''+id+'\')">Retry</button></div>';}}).catch(e=>{ag.innerHTML='<div class="ag-panel"><div style="color:var(--error)">Request failed: '+e.message+'</div></div>';});}
function toggleAgentDisclosure(btn){var c=btn.nextElementSibling;var chevron=btn.querySelector('.ag-disclosure-chevron');if(c.classList.contains('show')){c.classList.remove('show');chevron.classList.remove('open');}else{c.classList.add('show');chevron.classList.add('open');}}
function logOut(id){var o=document.getElementById('os').value,n=document.getElementById('on').value,fd=document.getElementById('fd').value,r=document.getElementById('ar');if(!o){r.innerHTML='<div style="color:var(--error);font-size:13px">Please select an outcome.</div>';return;}r.innerHTML='<div class="loading">Saving...</div>';var fd2=new FormData();fd2.append('location_id',locId);fd2.append('snapshot_date',snapDate);fd2.append('outcome',o);fd2.append('note',n);fd2.append('follow_up_date',fd);fetch('/api/lead/'+id+'/action',{method:'POST',body:fd2}).then(r=>r.json()).then(d=>{if(d.success){var ln=document.getElementById('dtitle')?document.getElementById('dtitle').textContent.trim():id;if(d.removed_from_queue){var row=document.getElementById('row-'+id);if(row)row.remove();closeDrawer();var mc=document.querySelectorAll('.mc .val');if(mc.length>=3){mc[0].textContent=parseInt(mc[0].textContent||'0')-1;mc[2].textContent=parseInt(mc[2].textContent||'0')+1;}var cnt=document.querySelector('.cnt');if(cnt){var nums=cnt.textContent.match(/\d+/g);if(nums&&nums.length>=2){var shown=parseInt(nums[0])-1;var total=parseInt(nums[1])-1;if(cnt.textContent.indexOf('Showing')>=0){cnt.textContent='Showing '+shown+' of '+total+' eligible leads';}else{cnt.textContent=shown+' matches of '+total+' eligible leads';}}}document.getElementById('os').value='';document.getElementById('on').value='';document.getElementById('fd').value='';}else{r.innerHTML='<div style="color:var(--success);font-size:14px;font-weight:600">Saved: '+d.outcome+'</div>';document.getElementById('os').value='';document.getElementById('on').value='';document.getElementById('fd').value='';}if(d.followup){addFollowup(d.followup,ln);}}else{r.innerHTML='<div style="color:var(--error);font-size:13px">Error: '+d.error+'</div>';}}).catch(e=>{r.innerHTML='<div style="color:var(--error);font-size:13px">Error: '+e.message+'</div>';});}
function addFollowup(fu,ln){var td=document.getElementById('tasks');var tl=td.querySelector('.tlist');if(!tl){var em=td.querySelector('.empty');if(em)em.remove();tl=document.createElement('div');tl.className='tlist';var h=td.querySelector('h2');if(h)h.after(tl);else td.insertBefore(tl,td.firstChild);}var ex=tl.querySelector('[data-lid="'+fu.lead_id+'"]');if(ex)ex.remove();var it=document.createElement('div');it.className='titem';it.setAttribute('data-lid',fu.lead_id);it.setAttribute('role','button');it.setAttribute('tabindex','0');it.style.cssText='cursor:pointer;transition:all 150ms;';it.setAttribute('aria-label','Open '+ln+' lead details');it.setAttribute('title','Click to open lead');it.onclick=function(){openLead(fu.lead_id);};it.onkeypress=function(e){if(e.key==='Enter')openLead(fu.lead_id);};var info=document.createElement('div');info.className='info';info.style.flex='1';var dec=(fu.decision||'follow up').replace(/_/g,' ').split(' ').map(function(w){return w.charAt(0).toUpperCase()+w.slice(1);}).join(' ');info.innerHTML='<div class="n">'+ln+'</div><div class="m">'+fu.lead_id+' &middot; '+dec+'</div>'+(fu.rationale?'<div class="m">'+fu.rationale+'</div>':'');it.appendChild(info);var badge=document.createElement('span');if(fu.due_label==='Due Now'){badge.style.cssText='font-size:12px;font-weight:600;padding:4px 10px;border-radius:6px;background:#FEE2E2;color:#991B1B';badge.textContent='Due Now';}else{badge.style.cssText='font-size:12px;font-weight:600;padding:4px 10px;border-radius:6px;background:#DBEAFE;color:#1E40AF';badge.textContent='Scheduled'+(fu.due_date?' - '+fu.due_date:'');}it.appendChild(badge);tl.insertBefore(it,tl.firstChild);var mc=document.querySelectorAll('.mc .val');if(mc.length>=2){mc[1].textContent=parseInt(mc[1].textContent||'0')+1;}}
function saveN(id){var n=document.getElementById('sn').value,r=document.getElementById('nr');if(!n.trim()){r.innerHTML='<div style="color:var(--error);font-size:13px">Note cannot be empty.</div>';return;}r.innerHTML='<div class="loading">Saving...</div>';var fd=new FormData();fd.append('location_id',locId);fd.append('snapshot_date',snapDate);fd.append('note',n);fetch('/api/lead/'+id+'/note',{method:'POST',body:fd}).then(r=>r.json()).then(d=>{if(d.success){r.innerHTML='<div style="color:var(--success);font-size:14px;font-weight:600">Note saved</div>';document.getElementById('sn').value='';}else{r.innerHTML='<div style="color:var(--error);font-size:13px">Error: '+d.error+'</div>';}});}
function closeL(id){var r=document.getElementById('cr').value,rr=document.getElementById('clr');if(!r.trim()){rr.innerHTML='<div style="color:var(--error);font-size:13px">Reason is required.</div>';return;}rr.innerHTML='<div class="loading">Closing...</div>';var fd=new FormData();fd.append('location_id',locId);fd.append('snapshot_date',snapDate);fd.append('reason',r);fetch('/api/lead/'+id+'/close',{method:'POST',body:fd}).then(r=>r.json()).then(d=>{if(d.success){rr.innerHTML='<div style="color:var(--success);font-size:14px;font-weight:600">Lead closed</div>';document.getElementById('cr').value='';}else{rr.innerHTML='<div style="color:var(--error);font-size:13px">Error: '+d.error+'</div>';}});}
function closeDrawer(){var ovr=document.getElementById('overlay'),dr=document.getElementById('drawer');ovr.classList.remove('show');ovr.setAttribute('aria-hidden','true');dr.classList.remove('show');dr.setAttribute('hidden','');dr.setAttribute('aria-hidden','true');document.querySelectorAll('.qt tr').forEach(r=>r.classList.remove('sel'));if(lastFocusedElement)lastFocusedElement.focus();}
function trapDrawerFocus(e){var dr=document.getElementById('drawer');if(!dr.classList.contains('show'))return;if(e.key==='Tab'){var f=dr.querySelectorAll('button:not([disabled]),[href],input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex="0"]');var v=[];f.forEach(function(el){if(el.offsetParent!==null||el===document.activeElement)v.push(el);});if(v.length===0)return;var first=v[0],last=v[v.length-1];if(e.shiftKey&&(document.activeElement===first||document.activeElement===dr)){e.preventDefault();last.focus();}else if(!e.shiftKey&&document.activeElement===last){e.preventDefault();first.focus();}}}
document.addEventListener('keydown',trapDrawerFocus);
function toggleExpand(btn){var targetId=btn.getAttribute('aria-controls');var content=document.getElementById(targetId);if(!content)return;var arrow=btn.querySelector('.arrow');var isExpanded=content.classList.toggle('show');arrow.textContent=isExpanded?'▼':'▶';btn.setAttribute('aria-expanded',isExpanded);}
document.addEventListener('keydown',function(e){if(e.key==='Escape'&&document.getElementById('drawer').classList.contains('show')){closeDrawer();}});
</script></body></html>
"""

ERROR_HTML = r"""
<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Error — Lead Recovery</title>
<style>
body{font-family:-apple-system,sans-serif;background:#F8FAFC;color:#0F172A;padding:48px;}
.ebox{max-width:640px;margin:0 auto;background:#fff;border:1px solid #E2E8F0;border-radius:10px;padding:24px;}
.ebox h2{color:#B91C1C;margin-bottom:12px;}.ebox p{margin-bottom:12px;}.ebox pre{background:#F1F5F9;padding:12px;border-radius:8px;overflow:auto;font-size:13px;}
</style></head><body>
<div class="ebox"><h2>Something went wrong</h2><p>We couldn't load the dashboard for this clinic and date range. This may happen when no leads match the selected filters.</p><p style="color:#64748B;font-size:14px">Try a wider lookback period or a different snapshot date. If the problem persists, contact your administrator.</p><a href="/" style="color:#2563EB;font-weight:600">&larr; Back to start</a></div>
</body></html>
"""

if __name__ == "__main__":
    port = int(os.environ.get("DATABRICKS_APP_PORT", "8080"))
    app.run(host="0.0.0.0", port=port)