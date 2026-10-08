# Lead Recovery App

An agentic lead recovery workflow for chiropractic clinics, built on Databricks Apps.

## Business Problem

Chiropractic clinics lose revenue when leads slip through the cracks — slow initial responses, unanswered follow-ups, and inconsistent contact logging. This app helps clinic staff recover at-risk leads through an AI-assisted, tool-using agent that recommends the next action for each lead, with human approval before any task is persisted.

## Architecture

```
+-------------------+     +------------------+     +---------------------+
|   Databricks App  |     |  Databricks SDK  |     |  SQL Warehouse      |
|  (Flask, app_v2.py)|<-->|  WorkspaceClient |<-->|  (Statement Exec)  |
|                   |     +------------------+     +---------------------+
|  - Recovery Queue |                                    |
|  - Lead Details   |     +------------------+           |
|  - Agent Panel    |     | ai_gen() SQL FN  |<----------+
|  - Impact Summary |     +------------------+           |
|  - Follow-ups     |                                    |
+-------------------+     +------------------+           |
                          | workspace.       |<----------+
                          | chiro_hackathon  |  (leads, locations, appointments,
                          | workspace.       |   providers, patients, visits)
                          | chiro_agent_demo |  (followup_tasks, nurturing_cycles)
                          +------------------+
```

**Databricks' role:** The app runs on Databricks Apps (managed compute), queries data via the Databricks SQL Statement Execution API, uses the built-in `ai_gen()` SQL function for AI rationale generation, and persists agent decisions to Unity Catalog tables.

## Setup

### Prerequisites
- A Databricks workspace with a SQL warehouse
- Unity Catalog schema `workspace.chiro_hackathon` with tables: leads, locations, appointments, providers, patients, visits
- Unity Catalog schema `workspace.chiro_agent_demo` with table: followup_tasks

### Deploy
1. The app is already deployed as `lead-recovery-app` in this workspace.
2. To redeploy: `databricks apps deploy lead-recovery-app --source-code-path /Workspace/Users/<user>/lead-recovery-app`
3. Or via bundle: `databricks bundle deploy --target dev`

### Dependencies
- `flask` — web framework
- `databricks-sdk` — Databricks SDK for SQL execution and workspace access

## The Agentic Workflow

The agent uses 6 tool functions to gather facts, then a deterministic decision engine makes a recommendation:

1. **get_lead_facts** — retrieves lead from database (status, touchpoints, response time)
2. **check_booking_status** — checks if lead is already converted/booked (suppression)
3. **get_contact_history** — retrieves logged outreach from followup_tasks
4. **check_existing_tasks** — checks for pending follow-up tasks
5. **check_contact_eligibility** — verifies phone/email and consent status
6. **check_clinic_capacity** — checks appointment availability from appointments table

**Decision rules (deterministic, grounded in tool results):**
- Already converted → NO_ACTION (suppression)
- 3+ touchpoints → NO_ACTION (contact cap)
- Missing consent + inconsistent data → STAFF_REVIEW
- Pending follow-up → NO_ACTION (wait)
- No contact attempted → CALL (high priority)
- Slow response (>48h) + low touchpoints → CALL (recovery)
- Multiple no-answers → DRAFT_OUTREACH (switch channel)
- Connected but not booked → FOLLOW_UP

AI enhancement: `ai_gen()` generates a natural-language rationale from the facts. If unavailable, a rule-based rationale is used and clearly labeled as fallback.

Staff approve/reject the recommendation. Approval persists a task in `followup_tasks` with duplicate prevention.

## Impact Measurement

The Impact Summary panel distinguishes:
- **Observed:** Leads worked, appointments booked, conversion rate (with denominator), completed visits, revenue
- **Simulated:** Agent vs oldest-first baseline on same contact budget, with transparent booking rate assumptions
- **Projected:** Eligible leads × booking rate × attendance rate × revenue per visit

All figures use transparent, editable assumptions. Simulated outcomes are not treated as evidence of real-world effectiveness.

## Demo Steps (2 minutes)

1. **Open the app** — select clinic LOC001, snapshot 2026-10-04, lookback 120 days
2. **Recovery Queue** — show prioritized leads (slow/no response first)
3. **Open a lead** — click a lead with slow response (e.g., LD0012003, 93.4h response)
4. **Run Agent** — click "Run Agent Recommendation" — watch 6 tool calls execute
5. **Inspect evidence** — expand "Tool Activity" to see each tool call and result
6. **Review recommendation** — agent recommends CALL with AI-enhanced rationale
7. **Approve** — click "Approve & Persist" — task is written to followup_tasks
8. **Record outcome** — log "Appointment booked" in the Log Call Outcome section
9. **Impact Summary** — observe leads worked and conversion rate update
10. **Test suppression** — open an already-converted lead → agent returns NO_ACTION

## Validation

Run tests: `python test_lead_consistency.py` (in a Databricks notebook cell)

Tests cover:
- Touchpoint count consistency (static + logged)
- Follow-ups-due excludes future-scheduled tasks
- Supersedence rule (no duplicate pending follow-ups)
- Revenue terminology (no ARR labels on clinic revenue)
- Agent decision logic (tool calls, eligibility, suppression)
- Impact metrics (observed, simulated, projected)

## Limitations

- All data is synthetic. Revenue figures are not real revenue.
- `ai_gen()` is Databricks' built-in SQL AI function; no external AI service is used.
- Contact info (phone, email) is hash-derived synthetic data.
- Booking rate assumptions (8%/6%/4%) are illustrative, not validated.
- The simulated evaluation compares prioritization strategies, not measured outcomes.
- The app does not send real emails or SMS.
- Scaling projections are scenario estimates, not business forecasts.

## Files

| File | Purpose |
|---|---|
| `app_v2.py` | Main Flask app (agent, UI, API, impact metrics) |
| `app.yaml` | App command configuration |
| `databricks.yml` | DAB bundle for reproducible deployment |
| `requirements.txt` | Python dependencies |
| `test_lead_consistency.py` | Test suite |
| `README.md` | This file |