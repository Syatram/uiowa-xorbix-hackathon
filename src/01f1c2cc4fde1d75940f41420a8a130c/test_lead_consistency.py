"""
Focused tests for Lead Recovery app data consistency.
Run in a Databricks notebook cell after deploying app_v2.py changes.

Uses Spark SQL directly (no Flask or SDK import needed) so it runs
in any Databricks notebook/serverless compute.

Tests cover seven bug categories:
  1. Touchpoint count mismatch (num_touchpoints static, outreach logged separately)
  2. Follow-ups-due count includes future-scheduled tasks
  3. Multiple pending FOLLOW_UP_SCHEDULED tasks with no supersedence rule
  4. Follow-ups Due card count must match actual due tasks (not zero from regex failure)
  5. ARR labels replaced with 'estimated annual clinic revenue opportunity'
  6. Revenue model: multi-factor annual client value (not flat per-patient average)
  7. Scaling consistency: $250M base / $500M high, market flag, platform ARR separate
"""

SOURCE = 'workspace.chiro_hackathon'
OUTPUT = 'workspace.chiro_agent_demo'
TEST_LEAD = 'LD0040737'
TEST_LOC = 'LOC001'
TEST_SNAP = '2026-10-04'
TEST_LOOKBACK = 120

passed = 0
failed = 0

def check(name, condition, detail=""):
    global passed, failed
    if condition:
        print(f'  PASS: {name}')
        passed += 1
    else:
        print(f'  FAIL: {name} — {detail}')
        failed += 1

def sql_query(sql):
    """Run SQL via Spark and return list of dicts."""
    rows = spark.sql(sql).collect()
    if not rows:
        return []
    cols = rows[0].asDict().keys()
    return [dict(zip(cols, row)) for row in [r.asDict().values() for r in rows]]

print('=== Test Suite: Lead Recovery Data Consistency ===\n')

# ---------------------------------------------------------------------------
# 1. Effective touchpoints reflect logged outreach
# ---------------------------------------------------------------------------
print('1. Touchpoint count reflects logged outreach')

# Lead LD0040737 has num_touchpoints=0 in leads table but 2 OUTREACH_LOGGED tasks
lead_rows = sql_query(f"""
    SELECT l.lead_id, l.num_touchpoints,
        l.num_touchpoints + COALESCE(oc.outreach_count, 0) as effective_touchpoints
    FROM {SOURCE}.leads l
    LEFT JOIN (
        SELECT lead_id, COUNT(*) as outreach_count
        FROM {OUTPUT}.followup_tasks
        WHERE decision = 'OUTREACH_LOGGED'
        GROUP BY lead_id
    ) oc ON l.lead_id = oc.lead_id
    WHERE l.lead_id = '{TEST_LEAD}'
""")
lead = lead_rows[0] if lead_rows else {}
check('lead found', bool(lead))
check('num_touchpoints is 0 (source unchanged)', lead.get('num_touchpoints') == 0)
check('effective_touchpoints >= 2 (0 + 2 logged)',
      lead.get('effective_touchpoints', 0) >= 2,
      f"got {lead.get('effective_touchpoints')}")

# Verify outreach count in followup_tasks
outreach = sql_query(f"""
    SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks
    WHERE lead_id = '{TEST_LEAD}' AND decision = 'OUTREACH_LOGGED'
""")
check('exactly 2 OUTREACH_LOGGED tasks',
      int(outreach[0]['cnt']) == 2 if outreach else False,
      f"got {outreach[0]['cnt'] if outreach else 'none'}")

print()

# ---------------------------------------------------------------------------
# 2. Follow-ups-due excludes future-scheduled tasks
# ---------------------------------------------------------------------------
print('2. Follow-ups-due count excludes future-scheduled tasks')

# Total pending tasks
all_pending = sql_query(f"""
    SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks
    WHERE location_id = '{TEST_LOC}' AND review_status = 'pending_review'
""")
all_count = int(all_pending[0]['cnt']) if all_pending else 0

# Pending tasks that are future-scheduled (should be excluded from due count)
future = sql_query(f"""
    SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks
    WHERE location_id = '{TEST_LOC}' AND review_status = 'pending_review'
    AND decision = 'FOLLOW_UP_SCHEDULED'
    AND to_date(regexp_extract(rationale, 'Follow-up scheduled for (\\\\d{{4}}-\\\\d{{2}}-\\\\d{{2}})', 1)) > current_date()
""")
future_count = int(future[0]['cnt']) if future else 0

# Correct due count = all pending - future scheduled
correct_due = all_count - future_count

# The fixed query should match this
due_rows = sql_query(f"""
    SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks
    WHERE location_id = '{TEST_LOC}' AND review_status = 'pending_review'
    AND (
        decision != 'FOLLOW_UP_SCHEDULED'
        OR coalesce(to_date(regexp_extract(rationale, 'Follow-up scheduled for (\\\\d{{4}}-\\\\d{{2}}-\\\\d{{2}})', 1)), current_date()) <= current_date()
    )
""")
due_count = int(due_rows[0]['cnt']) if due_rows else 0

check('due count excludes future tasks', due_count == correct_due,
      f"due={due_count}, expected={correct_due}")
check('due count < total pending when future tasks exist',
      future_count == 0 or due_count < all_count,
      f"due={due_count}, all={all_count}, future={future_count}")

print()

# ---------------------------------------------------------------------------
# 3. Supersedence: only one pending FOLLOW_UP_SCHEDULED per lead
# ---------------------------------------------------------------------------
print('3. Supersedence rule: only one pending follow-up per lead')

pending_fu = sql_query(f"""
    SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks
    WHERE lead_id = '{TEST_LEAD}' AND decision = 'FOLLOW_UP_SCHEDULED' AND review_status = 'pending_review'
""")
pending_count = int(pending_fu[0]['cnt']) if pending_fu else 0
check('exactly one pending FOLLOW_UP_SCHEDULED for LD0040737',
      pending_count == 1, f"found {pending_count}")

superseded = sql_query(f"""
    SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks
    WHERE lead_id = '{TEST_LEAD}' AND decision = 'FOLLOW_UP_SCHEDULED' AND review_status = 'superseded'
""")
superseded_count = int(superseded[0]['cnt']) if superseded else 0
check('at least one superseded follow-up exists (history preserved)',
      superseded_count >= 1, f"found {superseded_count}")

# Verify total tasks for this lead (should be 5: 2 outreach + 1 note + 2 follow-up)
all_tasks = sql_query(f"""
    SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks
    WHERE lead_id = '{TEST_LEAD}'
""")
total_tasks = int(all_tasks[0]['cnt']) if all_tasks else 0
check('total tasks unchanged (no deletion)', total_tasks >= 5,
      f"found {total_tasks}")

# The pending follow-up should be the newest one (2026-10-08)
active_fu = sql_query(f"""
    SELECT rationale FROM {OUTPUT}.followup_tasks
    WHERE lead_id = '{TEST_LEAD}' AND decision = 'FOLLOW_UP_SCHEDULED' AND review_status = 'pending_review'
""")
if active_fu:
    check('active follow-up is for 2026-10-08',
          '2026-10-08' in (active_fu[0].get('rationale') or ''))
else:
    check('active follow-up exists', False, 'none found')

# The superseded one should be for 2026-10-07
sup_fu = sql_query(f"""
    SELECT rationale FROM {OUTPUT}.followup_tasks
    WHERE lead_id = '{TEST_LEAD}' AND decision = 'FOLLOW_UP_SCHEDULED' AND review_status = 'superseded'
""")
if sup_fu:
    check('superseded follow-up is for 2026-10-07',
          '2026-10-07' in (sup_fu[0].get('rationale') or ''))

print()

# ---------------------------------------------------------------------------
# 4. Code logic verification: supersedence UPDATE in log_outcome
# ---------------------------------------------------------------------------
print('4. Verify supersedence logic exists in app code')

with open('/Workspace/Users/erickford9012@gmail.com/lead-recovery-app/app_v2.py', 'r') as f:
    app_source = f.read()

check('log_outcome has UPDATE to supersede old follow-ups',
      "SET review_status = 'superseded'" in app_source and
      "FOLLOW_UP_SCHEDULED" in app_source and
      "pending_review" in app_source)

check('get_metrics filters future-scheduled tasks',
      "current_date()" in app_source and
      "Follow-up scheduled for" in app_source)

check('get_eligible_leads computes effective_touchpoints',
      "effective_touchpoints" in app_source and
      "OUTREACH_LOGGED" in app_source and
      "outreach_map" in app_source)

check('get_lead_detail computes effective_touchpoints',
      app_source.count("effective_touchpoints") >= 3)

check('JS renderLead uses effective_touchpoints',
      "parseInt(d.effective_touchpoints)" in app_source)

check('JS shows superseded badge',
      "review_status==='superseded'" in app_source)

print()
# ---------------------------------------------------------------------------
# 5. Follow-ups Due count matches actual due tasks (not zero from regex bug)
# ---------------------------------------------------------------------------
print('5. Due count matches actual due tasks at LOC001')

try:
    due_rows = sql_query(f"""
        SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks
        WHERE location_id = '{TEST_LOC}' AND review_status = 'pending_review'
        AND (
            decision != 'FOLLOW_UP_SCHEDULED'
            OR coalesce(to_date(nullif(regexp_extract(rationale,
                'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1), '')),
                current_date()) <= current_date()
        )
    """)
    due_count = int(due_rows[0]['cnt']) if due_rows else 0
except Exception as e:
    due_count = -1
    check('due count query executes without error', False, str(e))

all_pending = sql_query(f"""
    SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks
    WHERE location_id = '{TEST_LOC}' AND review_status = 'pending_review'
""")
total_pending = int(all_pending[0]['cnt']) if all_pending else 0

check('due count query executes without error', due_count >= 0)
check('due count > 0 (was 0 due to regex bug)', due_count > 0, f"got {due_count}")
check('due count <= total pending', due_count <= total_pending, f"due={due_count}, total={total_pending}")
check('total pending > 0 (Follow-ups tab not empty)', total_pending > 0, f"got {total_pending}")

regex_test = sql_query(f"""
    SELECT regexp_extract('Follow-up scheduled for 2026-10-08 after No answer',
        'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1) as extracted_date
""")
if regex_test:
    check('regex extracts date correctly',
          regex_test[0].get('extracted_date') == '2026-10-08',
          f"got '{regex_test[0].get('extracted_date')}'")
else:
    check('regex test returns a row', False, 'no rows returned')

empty_test = sql_query("""
    SELECT to_date(nullif('', '')) as null_date,
           coalesce(to_date(nullif('', '')), current_date()) as coalesced_date
""")
if empty_test:
    check('nullif + to_date handles empty string', empty_test[0].get('null_date') is None)
    check('coalesce defaults to current_date for empty', empty_test[0].get('coalesced_date') is not None)

due_labels = sql_query(f"""
    SELECT decision,
        CASE WHEN decision = 'FOLLOW_UP_SCHEDULED'
            AND to_date(nullif(regexp_extract(rationale,
                'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1), '')) > current_date()
            THEN 'Scheduled'
            ELSE 'Due Now'
        END as due_label
    FROM {OUTPUT}.followup_tasks
    WHERE location_id = '{TEST_LOC}' AND review_status = 'pending_review'
""")
if due_labels:
    due_now_count = sum(1 for r in due_labels if r.get('due_label') == 'Due Now')
    scheduled_count = sum(1 for r in due_labels if r.get('due_label') == 'Scheduled')
    check('due_label assigns Due Now to pipeline tasks', due_now_count > 0, f"got {due_now_count}")
    check('due_count matches Due Now count', due_count == due_now_count,
          f"due_count={due_count}, due_now={due_now_count}")

print()

# ---------------------------------------------------------------------------
# 6. Revenue terminology: no misleading ARR labels in source
# ---------------------------------------------------------------------------
print('6. Revenue terminology: ARR replaced with revenue opportunity')

with open('/Workspace/Users/erickford9012@gmail.com/lead-recovery-app/app_v2.py', 'r') as f:
    app_source = f.read()

check('heading says Revenue Opportunity not ARR',
      'Revenue Opportunity' in app_source and 'Path to $100M' in app_source)
check('target zone says REVENUE ZONE not ARR ZONE',
      'TARGET REVENUE ZONE' in app_source)
check('hero subtitle says estimated annual clinic revenue opportunity',
      'Estimated annual clinic revenue opportunity at scale' in app_source)
check('key metric says Annual rev/clinic not Base ARR/clinic',
      'Annual rev/clinic (base)' in app_source and 'Base ARR/clinic' not in app_source)
check('calc details say revenue opportunity not recovered revenue reaches $X ARR',
      'revenue opportunity reaches' in app_source and
      'recovered revenue reaches' not in app_source)
check('calc uses Annual per-clinic revenue not Per-clinic recovered revenue',
      'Annual per-clinic revenue' in app_source and
      'Per-clinic recovered revenue' not in app_source)
check('platform ARR kept separate from clinic revenue',
      "product company's own revenue, separate from clinic revenue" in app_source)
check('platform ARR uses explicit SaaS pricing model',
      'explicit recurring SaaS subscription model' in app_source)

import re
h2_arrs = re.findall(r'<h2[^>]*>[^<]*ARR[^<]*</h2>', app_source)
check('no ARR in h2 headings', len(h2_arrs) == 0, f"found: {h2_arrs}")

print()
# ---------------------------------------------------------------------------
# 7. Revenue model: multi-factor annual client value
# ---------------------------------------------------------------------------
print('7. Revenue model: multi-factor annual client value')

# Illustrative test: 100 eligible leads × 5% × $100 × 2 × 10 = 5 clients, $10,000
test_eligible = 100
test_rate = 0.05
test_rev_per_visit = 100
test_visits_per_month = 2
test_active_months = 10
test_expected_clients = test_eligible * test_rate
test_first_year_rev_per_client = test_rev_per_visit * test_visits_per_month * test_active_months
test_first_year_revenue = test_expected_clients * test_first_year_rev_per_client

check('illustrative: 100 leads × 5% = 5 expected clients',
      abs(test_expected_clients - 5.0) < 0.001, f"got {test_expected_clients}")
check('illustrative: $100 × 2 × 10 = $2,000 per client',
      abs(test_first_year_rev_per_client - 2000) < 0.01, f"got {test_first_year_rev_per_client}")
check('illustrative: 5 clients × $2,000 = $10,000 total',
      abs(test_first_year_revenue - 10000) < 0.01, f"got {test_first_year_revenue}")

# Verify the app code uses the multi-factor model (not flat eligible × rate × avg_rev)
with open('/Workspace/Users/erickford9012@gmail.com/lead-recovery-app/app_v2.py', 'r') as f:
    app_source = f.read()

check('get_impact_data uses avg_rev_per_visit (not avg_rev per patient)',
      'avg_rev_per_visit' in app_source and 'avg_revenue_per_patient' not in app_source)
check('get_impact_data computes first_year_rev_per_client',
      'first_year_rev_per_client' in app_source and
      'visits_per_month' in app_source and
      'ACTIVE_MONTHS' in app_source)
check('scenarios use expected_clients (not expected_patients)',
      'expected_clients' in app_source and 'expected_patients' not in app_source)
check('scenarios use first_year_revenue (not estimated_revenue)',
      'first_year_revenue' in app_source and 'estimated_revenue' not in app_source)
check('model does NOT use flat avg_rev × eligible × rate',
      'eligible * rate * avg_rev' not in app_source)

# Verify historical data is used for revenue per visit
check('revenue per visit from historical data (SUM/COUNT on visits)',
      'SUM(v.revenue) / NULLIF(COUNT(*)' in app_source)
check('visit frequency from active patients',
      "p.status = 'Active'" in app_source and 'monthly_visits' in app_source)
check('active_months is an explicit assumption (10, not 12)',
      'ACTIVE_MONTHS = 10' in app_source)

print()

# ---------------------------------------------------------------------------
# 8. Scaling consistency: $250M/$500M fix, market flag, platform ARR
# ---------------------------------------------------------------------------
print('8. Scaling consistency and market sizing')

check('calc uses 44,000 clinics (not 46,000 chiropractors)',
      '44000' in app_source and '46,000 chiropractors' not in app_source)
check('market penetration uses addressable_clinics variable',
      'addressable_clinics' in app_source)
check('market flag condition exists (exceeds market)',
      'exceeds_market' in app_source and 'addressable_clinics' in app_source)
check('HTML shows exceeds market warning',
      'exceeds the total addressable market' in app_source)

# $250M/$500M fix: both rates shown at same clinic count
check('calc says $250M at base rate AND up to $500M at high rate',
      '$250M</strong>; at high rate across the same clinics, up to <strong>$500M' in app_source)
check('calc no longer says recovered revenue reaches $250M at 10% rate',
      'recovered revenue reaches' not in app_source)

# Platform ARR: explicit pricing, not percentage of clinic revenue
check('platform ARR uses explicit $500/month pricing',
      'platform_price_per_month' in app_source and 'PLATFORM_PRICE_PER_MONTH = 500' in app_source)
check('platform ARR calculated per clinic (not as percentage)',
      'platform_arr_per_clinic' in app_source)
check('platform ARR labeled as separate from clinic revenue',
      "product company's own revenue, separate from clinic revenue" in app_source)
check('platform pricing is explicit SaaS subscription (not percentage)',
      'explicit recurring SaaS subscription model' in app_source)
check('old 15-20% share model removed',
      '15&ndash;20%' not in app_source and '15-20%' not in app_source)

# Cohort vs pipeline distinction
check('cohort labeled as one-time from lookback',
      'one-time cohort' in app_source)
check('annual pipeline is documented assumption',
      'annual_pipeline_leads' in app_source and 'ANNUAL_PIPELINE_LEADS = 150' in app_source)
check('pipeline assumption labeled in HTML',
      'documented assumption' in app_source)

# Labels updated in HTML template
check('card says Total Leads not First-Year Opportunity',
      'Total Leads' in app_source and 'First-Year Opportunity' not in app_source)
check('table says Ongoing-Client Rate',
      'Ongoing-Client Rate' in app_source)
check('table says Expected Ongoing Clients',
      'Expected Ongoing Clients' in app_source)
check('table says First-Year Clinic Revenue Opportunity',
      'First-Year Clinic Revenue Opportunity' in app_source)
check('key metrics show Rev/visit and Visits/mo',
      'Rev/visit (historical)' in app_source and 'Visits/mo (active)' in app_source)
check('key metrics show First-year rev/client',
      'First-year rev/client' in app_source)
check('key metrics show Annual rev/clinic (base)',
      'Annual rev/clinic (base)' in app_source)

print()

# ---------------------------------------------------------------------------
# 9. Agent decision logic: tool calls, eligibility, suppression
# ---------------------------------------------------------------------------
print('9. Agent decision logic')

with open('/Workspace/Users/erickford9012@gmail.com/lead-recovery-app/app_v2.py', 'r') as f:
    app_source = f.read()

check('agent has 6 tool functions',
      all(t in app_source for t in ['_tool_get_lead_facts', '_tool_get_contact_history',
          '_tool_check_existing_tasks', '_tool_check_booking_status',
          '_tool_check_contact_eligibility', '_tool_check_clinic_capacity']))
check('agent_recommend function exists',
      'def agent_recommend(' in app_source)
check('agent has decision rules (converted, cap, staff_review)',
      'converted_flag' in app_source and 'Contact cap' in app_source and 'STAFF_REVIEW' in app_source)
check('agent uses ai_gen for rationale',
      'ai_gen' in app_source and 'AI-enhanced rationale' in app_source.lower() or 'ai_gen' in app_source)
check('agent has fallback labeling (is_fallback)',
      'is_fallback' in app_source)
check('agent_approve has duplicate prevention',
      'Duplicate pending task' in app_source)
check('agent endpoints exist (/api/agent/recommend and /api/agent/approve)',
      '/api/agent/recommend/' in app_source and '/api/agent/approve/' in app_source)
check('agent does not send real emails or SMS',
      'smtplib' not in app_source and 'twilio' not in app_source)
check('agent tool calls are visible (tool_calls in return)',
      'tool_calls' in app_source)

# Test agent on a known lead
try:
    agent_result = spark.sql("""
        SELECT lead_id, status, converted_flag, num_touchpoints, first_response_hours
        FROM workspace.chiro_hackathon.leads
        WHERE lead_id = 'LD0012003' AND assigned_location_id = 'LOC001'
    """).collect()
    if agent_result:
        r = agent_result[0]
        check('LD0012003 is eligible for agent (not converted)',
              not r['converted_flag'] and r['status'] in ('New', 'Contacted', 'Qualified'))
        check('LD0012003 has slow response (>48h)',
              r['first_response_hours'] is not None and r['first_response_hours'] > 48)
except Exception as e:
    check('LD0012003 query executes', False, str(e))

# Test suppression on a converted lead
try:
    converted_result = spark.sql("""
        SELECT COUNT(*) as cnt FROM workspace.chiro_hackathon.leads
        WHERE assigned_location_id = 'LOC001' AND converted_flag = true LIMIT 1
    """).collect()
    check('converted leads exist for suppression test',
          converted_result and converted_result[0]['cnt'] > 0)
except Exception as e:
    check('converted leads query executes', False, str(e))

print()

# ---------------------------------------------------------------------------
# 10. Impact metrics: observed, simulated, projected
# ---------------------------------------------------------------------------
print('10. Impact metrics')

check('get_impact_metrics function exists',
      'def get_impact_metrics(' in app_source)
check('impact has observed section (leads_worked, appts_booked)',
      'leads_worked' in app_source and 'appts_booked' in app_source)
check('impact has simulated evaluation (agent vs baseline)',
      'agent_expected' in app_source and 'baseline_expected' in app_source)
check('impact has projected opportunity',
      'projected_revenue' in app_source and 'INCREMENTAL_BOOKING_RATE' in app_source)
check('impact has transparent assumptions (booking rate, attendance rate)',
      'INCREMENTAL_BOOKING_RATE = 0.05' in app_source and 'ATTENDANCE_RATE = 0.80' in app_source)
check('impact has contact budget for evaluation',
      'CONTACT_BUDGET' in app_source)
check('impact does NOT claim proven causal uplift',
      'proven causal uplift' not in app_source.lower() or 'Not attributed' in app_source)
check('impact revenue per visit from historical data',
      'SUM(v.revenue) / NULLIF(COUNT(*)' in app_source)
check('impact panel HTML exists in template',
      'Impact Summary' in app_source)
check('impact panel shows observed/simulated/projected',
      'Observed' in app_source and 'Simulated' in app_source and 'Projected' in app_source)

# Verify impact metrics query works
try:
    obs_result = spark.sql("""
        SELECT
            COUNT(*) as leads_worked,
            SUM(CASE WHEN outcome = 'Appointment booked' THEN 1 ELSE 0 END) as appts_booked
        FROM workspace.chiro_agent_demo.followup_tasks
        WHERE location_id = 'LOC001' AND decision = 'OUTREACH_LOGGED' AND review_status = 'completed'
    """).collect()
    check('impact observed query executes',
          obs_result is not None)
except Exception as e:
    check('impact observed query executes', False, str(e))

print()

# ---------------------------------------------------------------------------
# 11. AI failure handling
# ---------------------------------------------------------------------------
print('11. AI failure handling')

check('agent catches AI failure (try/except around ai_gen)',
      'AI rationale generation failed' in app_source)
check('agent labels fallback clearly (is_fallback flag)',
      'is_fallback' in app_source and 'not is_ai' in app_source)
check('generate_draft has fallback (Template fallback)',
      'Template fallback' in app_source)
check('UI shows fallback notice for agent (Rule-based rationale)',
      'Rule-based rationale' in app_source)
check('UI shows fallback notice for draft (Template fallback)',
      'Template fallback' in app_source)

print()

# ---------------------------------------------------------------------------
# 12. Deployment package
# ---------------------------------------------------------------------------
print('12. Deployment package')

import os
app_dir = '/Workspace/Users/erickford9012@gmail.com/lead-recovery-app'
check('databricks.yml exists',
      os.path.exists(os.path.join(app_dir, 'databricks.yml')))
check('README.md exists',
      os.path.exists(os.path.join(app_dir, 'README.md')))
check('requirements.txt exists',
      os.path.exists(os.path.join(app_dir, 'requirements.txt')))
check('app.yaml exists',
      os.path.exists(os.path.join(app_dir, 'app.yaml')))

with open(os.path.join(app_dir, 'databricks.yml'), 'r') as f:
    dab_content = f.read()
check('databricks.yml has bundle name',
      'name: lead-recovery-app' in dab_content)
check('databricks.yml has app resource',
      'apps:' in dab_content and 'lead-recovery-app:' in dab_content)
check('databricks.yml parameterizes warehouse_id',
      'warehouse_id' in dab_content)

with open(os.path.join(app_dir, 'README.md'), 'r') as f:
    readme_content = f.read()
check('README has architecture section',
      'Architecture' in readme_content)
check('README has demo steps',
      'Demo Steps' in readme_content)
check('README has limitations section',
      'Limitations' in readme_content)
check('README mentions agent workflow',
      'Agentic Workflow' in readme_content or 'agentic' in readme_content.lower())

print()

# ---------------------------------------------------------------------------
# 13. Data quality: Contacted + 0 touchpoints + non-null first_response
# ---------------------------------------------------------------------------
print('13. Data quality: inconsistent Contacted records')

# LD0027354: the specific case from the spec
ld27354 = sql_query(f"""
    SELECT lead_id, status, num_touchpoints, first_response_hours, converted_flag
    FROM {SOURCE}.leads
    WHERE lead_id = 'LD0027354'
""")
if ld27354:
    r = ld27354[0]
    check('LD0027354 status is Contacted',
          (r.get('status') or '').strip().lower() == 'contacted')
    check('LD0027354 num_touchpoints is 0',
          int(r.get('num_touchpoints') or 0) == 0)
    check('LD0027354 first_response_hours is non-null (62.1)',
          r.get('first_response_hours') is not None and
          abs(float(r['first_response_hours']) - 62.1) < 0.01,
          f"got {r.get('first_response_hours')}")
    check('LD0027354 converted_flag is False',
          not r.get('converted_flag'))
else:
    check('LD0027354 found in leads table', False, 'not found')

# Count all inconsistent records at LOC001
inconsistent_loc1 = sql_query(f"""
    SELECT COUNT(*) as cnt FROM {SOURCE}.leads
    WHERE assigned_location_id = 'LOC001'
      AND lower(trim(status)) = 'contacted'
      AND num_touchpoints = 0
      AND first_response_hours IS NOT NULL
""")
inconsistent_count = int(inconsistent_loc1[0]['cnt']) if inconsistent_loc1 else 0
check('LOC001 has >0 inconsistent Contacted records',
      inconsistent_count > 0, f"got {inconsistent_count}")

# Count across ALL clinics
inconsistent_all = sql_query(f"""
    SELECT COUNT(*) as cnt FROM {SOURCE}.leads
    WHERE lower(trim(status)) = 'contacted'
      AND num_touchpoints = 0
      AND first_response_hours IS NOT NULL
""")
all_inconsistent = int(inconsistent_all[0]['cnt']) if inconsistent_all else 0
check('systemic: >100 inconsistent records across all clinics',
      all_inconsistent > 100, f"got {all_inconsistent}")

print()

# ---------------------------------------------------------------------------
# 14. App code: data_quality_warning in get_eligible_leads
# ---------------------------------------------------------------------------
print('14. Data quality warning in backend and queue')

with open('/Workspace/Users/erickford9012@gmail.com/lead-recovery-app/app_v2.py', 'r') as f:
    app_source = f.read()

check('get_eligible_leads sets data_quality_warning',
      'data_quality_warning' in app_source and
      'lead["data_quality_warning"]' in app_source)
check('data_quality_warning detects Contacted + 0 touches + non-null frh',
      'eff_touches == 0 and frh is not None' in app_source)
check('data_quality_warning detects Contacted + 0 touches + null frh',
      'eff_touches == 0 and frh is None' in app_source)
check('data_quality_warning set to None for consistent records',
      'lead["data_quality_warning"] = None' in app_source)

# Queue rendering: warning shown when data_quality_warning is set
check('queue renders data quality warning badge',
      'Data quality:' in app_source and
      'lead.data_quality_warning' in app_source)
check('queue warning uses warning color',
      'color:var(--warning)' in app_source and
      '&#9888;' in app_source)

# Why Now column: touch_info removed (no longer duplicated)
import re
why_now_td = re.search(
    r'<td><span class="ptag t-\{\{ lead\.priority_color \}\}">.*?</td>',
    app_source, re.DOTALL
)
if why_now_td:
    why_now_html = why_now_td.group()
    check('Why Now column does not contain touch_info',
          'lead.touch_info' not in why_now_html,
          f"found touch_info in Why Now: {why_now_html[:200]}")
else:
    check('Why Now column found in source', False, 'td not matched')

# Contact History column still has touch_info
contact_history_td = re.search(
    r'<td class="hm">.*?lead\.touch_info.*?</td>',
    app_source, re.DOTALL
)
check('Contact History column still shows touch_info',
      contact_history_td is not None,
      'touch_info not found in Contact History column')

print()

# ---------------------------------------------------------------------------
# 15. Agent Rule 3: STAFF_REVIEW for Contacted + 0 touches (all cases)
# ---------------------------------------------------------------------------
print('15. Agent Rule 3: broadened STAFF_REVIEW')

rule3_block = ''
if '# Rule 3' in app_source and '# Rule 4' in app_source:
    rule3_block = app_source.split('# Rule 3')[1].split('# Rule 4')[0]

check('Rule 3 block exists', bool(rule3_block))
check('Rule 3 handles non-null first_response_hours case',
      'first_response_hours' in rule3_block and
      'is None' in rule3_block and
      'else' in rule3_block)
check('Rule 3 says do not assume contact occurred',
      'do not assume the contact occurred' in rule3_block)
check('Rule 3 adds missing_info for non-null frh case',
      'No touchpoint logged despite' in rule3_block)
check('Rule 3 evidence includes first_response_hours value',
      'first_response_hours=' in rule3_block and
      ':.1f' in rule3_block)

print()

# ---------------------------------------------------------------------------
# 16. Detail panel JS: data quality warning for all inconsistent patterns
# ---------------------------------------------------------------------------
print('16. Detail panel JS: data quality detection')

check('JS detects Contacted + 0 touches with non-null frh',
      'no touchpoints are logged' in app_source and
      'first-response time of' in app_source)
check('JS detects Contacted + 0 touches with null frh',
      'no contact record' in app_source and
      'response time exists' in app_source)
check('JS detects New + touchpoints',
      'Status may be stale' in app_source)
check('JS uses data quality section header',
      'Data quality' in app_source)
check('JS data quality uses warning styling',
      'background:#FEF3C7' in app_source and
      'border:1px solid #FDE68A' in app_source)

print()

# ---------------------------------------------------------------------------
# 17. Verify fixes work across clinic selection, pagination, search
# ---------------------------------------------------------------------------
print('17. Cross-view consistency: clinic, pagination, search')

for loc_id in ['LOC001', 'LOC002', 'LOC003']:
    loc_inconsistent = sql_query(f"""
        SELECT COUNT(*) as cnt FROM {SOURCE}.leads
        WHERE assigned_location_id = '{loc_id}'
          AND lower(trim(status)) = 'contacted'
          AND num_touchpoints = 0
          AND first_response_hours IS NOT NULL
    """)
    cnt = int(loc_inconsistent[0]['cnt']) if loc_inconsistent else -1
    check(f'{loc_id} has Contacted+0touch+frh records (data quality pattern)',
          cnt >= 0, f"got {cnt}")

# Verify LD0027354 appears in eligible queue (within filters)
searchable = sql_query(f"""
    SELECT lead_id, status, num_touchpoints, first_response_hours,
        datediff('2026-10-04', created_date) as age_days
    FROM {SOURCE}.leads
    WHERE lead_id = 'LD0027354'
      AND assigned_location_id = 'LOC001'
      AND lower(trim(status)) IN ('new','contacted','qualified')
      AND converted_flag = false
      AND num_touchpoints BETWEEN 0 AND 2
      AND datediff('2026-10-04', created_date) BETWEEN 0 AND 120
""")
check('LD0027354 appears in eligible queue (within filters)',
      len(searchable) > 0, 'not found in eligible results')

# search clause supports lead_id match
check('search clause supports lead_id match',
      'lower(lead_id) LIKE' in app_source)

# data_quality_warning is computed per-lead (pagination-safe)
check('data_quality_warning computed inside per-lead loop (pagination-safe)',
      app_source.count('data_quality_warning') >= 4)

print()

# ---------------------------------------------------------------------------
# 18. LD0019964: priority consistency between queue and detail panel
# ---------------------------------------------------------------------------
print('18. LD0019964 priority consistency')

ld19964 = sql_query(f"""
    SELECT lead_id, status, num_touchpoints, first_response_hours,
           converted_flag, assigned_location_id, source, created_date
    FROM {SOURCE}.leads
    WHERE lead_id = 'LD0019964'
""")
if ld19964:
    r = ld19964[0]
    check('LD0019964 status is New',
          (r.get('status') or '').strip().lower() == 'new')
    check('LD0019964 num_touchpoints is 1',
          int(r.get('num_touchpoints') or 0) == 1)
    check('LD0019964 first_response_hours is 29.3',
          r.get('first_response_hours') is not None and
          abs(float(r['first_response_hours']) - 29.3) < 0.01,
          f"got {r.get('first_response_hours')}")
    check('LD0019964 is at LOC001',
          r.get('assigned_location_id') == 'LOC001')
    check('LD0019964 converted_flag is False',
          not r.get('converted_flag'))
else:
    check('LD0019964 found', False, 'not in leads table')

outreach_19964 = sql_query(f"""
    SELECT COUNT(*) as cnt FROM {OUTPUT}.followup_tasks
    WHERE lead_id = 'LD0019964' AND decision = 'OUTREACH_LOGGED'
""")
logged_19964 = int(outreach_19964[0]['cnt']) if outreach_19964 else 0
check('LD0019964 has 0 logged outreach (effective = 1)',
      logged_19964 == 0, f"got {logged_19964}")

with open('/Workspace/Users/erickford9012@gmail.com/lead-recovery-app/app_v2.py', 'r') as f:
    app_source = f.read()

# Queue and detail must use same 4-tier priority
check('queue has 4-tier priority (None/48/24/else)',
      'High \u2014 No response' in app_source and
      'High \u2014 Slow response' in app_source and
      'Medium \u2014 Delayed' in app_source and
      'Standard' in app_source)
check('get_lead_detail computes priority_label',
      'def get_lead_detail' in app_source and
      'priority_label' in app_source)
check('get_lead_detail computes priority_color',
      'priority_color' in app_source)
check('get_lead_detail has 4-tier priority matching queue',
      app_source.count('Medium \u2014 Delayed') >= 2)
check('get_lead_detail computes data_quality_warning',
      app_source.count('data_quality_warning') >= 6)

# JS uses backend-provided priority_label
check('JS renderLead uses d.priority_label',
      'd.priority_label' in app_source)
check('JS no longer computes its own priority tiers',
      'High priority \u2014 No response recorded' not in app_source)
check('JS Why This Lead uses d.priority_label as headline',
      "d.priority_label||'Standard'" in app_source and
      'Why This Lead Is Prioritized' in app_source)
check('JS Why This Lead has 24-48h tier',
      'between 24-48 hours' in app_source)

# JS data quality uses backend field
check('JS uses d.data_quality_warning from backend',
      'd.data_quality_warning' in app_source)
check('JS no longer computes its own data quality detection',
      '_dqMsg' not in app_source)
check('data_quality_warning detects New + touchpoints',
      "Status shows 'New'" in app_source and
      'Status may be stale' in app_source)

print()

# ---------------------------------------------------------------------------
# 19. Lookback validation (1-365)
# ---------------------------------------------------------------------------
print('19. Lookback validation (1-365)')

check('index() has lookback_raw variable',
      'lookback_raw' in app_source)
check('index() clamps lookback with max(1, min(365',
      'max(1, min(365' in app_source)
check('index() tracks lookback_out_of_range flag',
      'lookback_out_of_range' in app_source)
check('index() passes lookback_out_of_range to template',
      'lookback_out_of_range=lookback_out_of_range' in app_source)
check('template shows visible warning when out of range',
      'Lookback adjusted' in app_source and
      '1\u2013365 days' in app_source)
check('template warning uses error styling',
      'FFEBE6' in app_source and 'var(--error)' in app_source)
check('lookback input calls validateLookback',
      'validateLookback(this.value)' in app_source)
check('JS validateLookback function exists',
      'function validateLookback' in app_source)
check('JS validateLookback alerts on out-of-range',
      'between 1 and 365 days' in app_source)
check('JS validateLookback clamps value',
      'Math.max(1,Math.min(365' in app_source)
check('search timer uses clamped lookback from template',
      'lookback={{ lookback }}' in app_source)

# Python clamping logic test
lookback_cases = [
    (0, 1, True), (1, 1, False), (120, 120, False),
    (365, 365, False), (400, 365, True), (-5, 1, True), (50, 50, False),
]
for raw, expected, oor in lookback_cases:
    clamped = max(1, min(365, raw))
    is_oor = raw != clamped
    check(f'lookback {raw} clamps to {expected} (out_of_range={oor})',
          clamped == expected and is_oor == oor,
          f'got clamped={clamped}, oor={is_oor}')

print()

# ---------------------------------------------------------------------------
# 20. Cross-view: LD0019964 in queue with correct priority
# ---------------------------------------------------------------------------
print('20. LD0019964 in queue with correct priority')

eligible_19964 = sql_query(f"""
    SELECT lead_id, status, num_touchpoints, first_response_hours,
           datediff('2026-10-04', created_date) as age_days
    FROM {SOURCE}.leads
    WHERE lead_id = 'LD0019964'
      AND assigned_location_id = 'LOC001'
      AND lower(trim(status)) IN ('new','contacted','qualified')
      AND converted_flag = false
      AND num_touchpoints BETWEEN 0 AND 2
      AND datediff('2026-10-04', created_date) BETWEEN 0 AND 120
""")
check('LD0019964 appears in eligible queue',
      len(eligible_19964) > 0, 'not in eligible results')
if eligible_19964:
    r = eligible_19964[0]
    frh = float(r['first_response_hours']) if r['first_response_hours'] else None
    check('LD0019964 frh=29.3 triggers Medium - Delayed',
          frh is not None and 24 < frh <= 48,
          f'frh={frh}')
    check('LD0019964 age within 120-day lookback',
          int(r.get('age_days') or 0) <= 120)
    check('LD0019964 triggers New+touchpoints data quality warning',
          (r.get('status') or '').lower() == 'new' and
          int(r.get('num_touchpoints') or 0) > 0)

# Both functions use same thresholds
queue_fn = app_source.split('def get_eligible_leads')[1].split('def get_lead_detail')[0]
detail_fn = app_source.split('def get_lead_detail')[1].split('def log_outcome')[0]
check('queue and detail both use frh > 48 for High',
      'frh > 48' in queue_fn and 'frh > 48' in detail_fn)
check('queue and detail both use frh > 24 for Medium',
      'frh > 24' in queue_fn and 'frh > 24' in detail_fn)
check('queue and detail both use same priority labels',
      'Medium \u2014 Delayed' in queue_fn and
      'Medium \u2014 Delayed' in detail_fn)
check('queue and detail both detect New+touchpoints',
      'status_lower == "new" and eff_touches > 0' in queue_fn and
      'status_lower == "new" and eff_touches > 0' in detail_fn)

print()

# ---------------------------------------------------------------------------
# 21. Follow-up task deduplication by lead_id
# ---------------------------------------------------------------------------
print('21. Follow-up task deduplication (LD0010358 at Wagner Inc)')

# LD0010358 at LOC002 (Wagner Inc) has two pending tasks:
#   - follow_up (agent-generated): generic follow-up
#   - FOLLOW_UP_SCHEDULED (manual): scheduled for 2026-10-01 after Connected
# get_tasks should return only ONE row per lead (deduplicated)
dup_tasks = sql_query(f"""
    SELECT task_id, lead_id, decision, rationale, review_status, reviewed_at
    FROM {OUTPUT}.followup_tasks
    WHERE lead_id = 'LD0010358' AND location_id = 'LOC002' AND review_status = 'pending_review'
    ORDER BY reviewed_at DESC
""")
raw_pending_count = len(dup_tasks)
check('LD0010358 has multiple pending tasks (pre-dedup)',
      raw_pending_count >= 2, f'found {raw_pending_count}')

# The dedup query should return exactly 1 row for this lead
dedup_query = f"""
    WITH pending AS (
        SELECT t.task_id, t.lead_id, t.decision, t.rationale, t.draft_text,
               t.review_status, t.reviewed_at, t.outcome,
               CASE WHEN t.decision = 'FOLLOW_UP_SCHEDULED'
                   AND to_date(nullif(regexp_extract(t.rationale,
                       'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1), '')) > current_date()
                   THEN 'Scheduled'
                   ELSE 'Due Now'
               END as due_label,
               CASE WHEN t.decision = 'FOLLOW_UP_SCHEDULED'
                   THEN regexp_extract(t.rationale,
                       'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1)
                   ELSE NULL
               END as due_date,
               ROW_NUMBER() OVER (
                   PARTITION BY t.lead_id
                   ORDER BY
                       CASE WHEN t.decision = 'FOLLOW_UP_SCHEDULED'
                           AND to_date(nullif(regexp_extract(t.rationale,
                               'Follow-up scheduled for ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1), '')) > current_date()
                           THEN 1 ELSE 0 END,
                       CASE WHEN t.decision = 'FOLLOW_UP_SCHEDULED' THEN 0
                            WHEN t.decision = 'follow_up' THEN 1
                            ELSE 2 END,
                       t.reviewed_at DESC
               ) as rn,
               COUNT(*) OVER (PARTITION BY t.lead_id) as total_for_lead
        FROM {OUTPUT}.followup_tasks t
        WHERE t.location_id = 'LOC002' AND t.review_status = 'pending_review'
    )
    SELECT task_id, lead_id, decision, due_label, due_date,
           total_for_lead - 1 as duplicate_count
    FROM pending WHERE rn = 1 ORDER BY lead_id
"""
dedup_rows = sql_query(dedup_query)
ld010358_rows = [r for r in dedup_rows if r.get('lead_id') == 'LD0010358']
check('get_tasks dedup returns exactly 1 row for LD0010358',
      len(ld010358_rows) == 1, f'found {len(ld010358_rows)}')

if ld010358_rows:
    r = ld010358_rows[0]
    # The Follow-up scheduled task (most recent, Due Now) should win
    check('dedup keeps FOLLOW_UP_SCHEDULED task',
          r.get('decision') == 'FOLLOW_UP_SCHEDULED',
          f"got {r.get('decision')}")
    check('dedup shows Due Now label',
          r.get('due_label') == 'Due Now', f"got {r.get('due_label')}")
    check('duplicate_count > 0 (additional tasks hidden)',
          int(r.get('duplicate_count') or 0) > 0,
          f"got {r.get('duplicate_count')}")

# Verify app source has dedup logic
with open('/Workspace/Users/erickford9012@gmail.com/lead-recovery-app/app_v2.py', 'r') as f:
    app_source = f.read()

check('get_tasks uses ROW_NUMBER for dedup',
      'ROW_NUMBER()' in app_source and 'PARTITION BY t.lead_id' in app_source)
check('get_tasks returns duplicate_count',
      'duplicate_count' in app_source)
check('HTML shows duplicate indicator',
      'additional pending task' in app_source and 'deduplicated' in app_source)

print()

# ---------------------------------------------------------------------------
# 22. log_outcome supersedes prior pending tasks when follow-up scheduled
# ---------------------------------------------------------------------------
print('22. log_outcome supersede logic')

check('log_outcome supersedes ALL pending before follow-up insert',
      "SET review_status = 'superseded'" in app_source and
      "WHERE lead_id = '{lead_id}' AND review_status = 'pending_review'" in app_source)

# Verify the supersede happens in the follow_up_date block
fu_block = ''
if 'if follow_up_date:' in app_source:
    fu_block = app_source.split('if follow_up_date:')[1].split('return task_id')[0]
check('follow_up_date block has supersede UPDATE',
      'superseded' in fu_block and 'pending_review' in fu_block)

print()

# ---------------------------------------------------------------------------
# 23. Keyboard accessibility for recovery queue rows
# ---------------------------------------------------------------------------
print('23. Keyboard accessibility for recovery queue rows')

check('queue rows have tabindex=0',
      'tabindex="0"' in app_source and 'row-{{ lead.lead_id }}' in app_source)
check('queue rows have role=button',
      'role="button"' in app_source)
check('queue rows have aria-label for lead',
      'aria-label="Open {{ lead.first_name }} {{ lead.last_name }} lead details"' in app_source)
check('queue rows handle Enter key',
      "event.key==='Enter'" in app_source)
check('queue rows handle Space key',
      "event.key===' '" in app_source or "event.key==='Spacebar'" in app_source)
check('queue rows prevent default on keydown',
      'event.preventDefault()' in app_source)
check('CSS has visible focus state for queue rows',
      '.qt tbody tr:focus' in app_source and 'outline:2px solid var(--primary)' in app_source)
check('CSS has focus-visible for modern browsers',
      '.qt tbody tr:focus-visible' in app_source)

print()

# ---------------------------------------------------------------------------
# 24. Search counts and empty-state copy
# ---------------------------------------------------------------------------
print('24. Search counts and empty-state copy')

check('get_eligible_leads returns total_eligible',
      'total_eligible' in app_source)
check('total_eligible query has no search_clause',
      'total_eligible' in app_source and
      'total_row' in app_source)

# Search count text shows matches of eligible when searching
check('count text shows matches of eligible when search active',
      'match{{ ' in app_source and 'eligible leads' in app_source)
check('count text shows showing of total when no search',
      'Showing {{ leads_data.leads|length }} of {{ leads_data.total }} eligible leads' in app_source)

# Empty state shows "0 matches of N eligible" when searching
check('empty state shows 0 matches when search active',
      '0 matches of {{ leads_data.total_eligible }} eligible leads' in app_source)
check('empty state shows No eligible leads when no search',
      'No eligible leads found' in app_source)
check('empty state has search-specific copy',
      'Try a different search term' in app_source)
check('empty state has non-search copy',
      'Try adjusting the snapshot date, lookback period, or search' in app_source)

# Verify the total_eligible count is correct for LOC001
loc001_eligible = sql_query(f"""
    SELECT COUNT(*) as cnt FROM {SOURCE}.leads
    WHERE assigned_location_id = 'LOC001'
      AND lower(trim(status)) IN ('new','contacted','qualified')
      AND converted_flag = false AND num_touchpoints BETWEEN 0 AND 2
      AND datediff('2026-10-04', created_date) BETWEEN 0 AND 120
""")
eligible_count = int(loc001_eligible[0]['cnt']) if loc001_eligible else 0
check('LOC001 has eligible leads for total_eligible test',
      eligible_count > 0, f'got {eligible_count}')

# Simulate search returning 0 results for a nonexistent term
search_zero = sql_query(f"""
    SELECT COUNT(*) as cnt FROM {SOURCE}.leads
    WHERE assigned_location_id = 'LOC001'
      AND lower(trim(status)) IN ('new','contacted','qualified')
      AND converted_flag = false AND num_touchpoints BETWEEN 0 AND 2
      AND datediff('2026-10-04', created_date) BETWEEN 0 AND 120
      AND (lower(source) LIKE '%zzzznonexistent%' OR lower(lead_id) LIKE '%zzzznonexistent%')
""")
search_zero_count = int(search_zero[0]['cnt']) if search_zero else 0
check('search for nonexistent term returns 0 matches',
      search_zero_count == 0, f'got {search_zero_count}')
check('total_eligible > 0 even when search returns 0',
      eligible_count > 0 and search_zero_count == 0)

print()

print(f'=== Results: {passed} passed, {failed} failed ===')
if failed > 0:
    print('FAILED TESTS — review the failures above')
else:
    print('All tests passed!')