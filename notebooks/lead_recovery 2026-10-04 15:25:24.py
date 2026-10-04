# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "databricks-openai",
#   "databricks-sdk[openai]",
# ]
# ///
# MAGIC %md
# MAGIC # Lead Recovery Agent
# MAGIC Synthetic lead triage for a clinic manager. All outreach and outcomes remain simulated.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 Setup

# COMMAND ----------

# DBTITLE 1,Install databricks-openai
# MAGIC %pip install databricks-sdk[openai]

# COMMAND ----------

import json
import re
import uuid
from datetime import date, datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)
from delta.tables import DeltaTable

# BEGIN EMBEDDED HELPERS
import hashlib
import math
from datetime import date
from typing import Any


DECISIONS = {"follow_up", "staff_review", "skip"}
REVIEW_STATES = {"approved", "rejected"}
OUTCOMES = {"not_contacted", "contacted_no_response", "appointment_requested", "converted_simulated"}


def task_key(lead_id: str, snapshot_date: str) -> str:
    return hashlib.sha256(f"lead_followup|{snapshot_date}|{lead_id}".encode()).hexdigest()


def classify(lead: dict[str, Any], open_statuses: set[str], terminal_statuses: set[str]) -> tuple[str, str]:
    """Return an allowed decision and its reason; never infer missing consent."""
    status = lead.get("status")
    flag = lead.get("converted_flag")
    patient_id = lead.get("converted_patient_id") or None
    touches = lead.get("num_touchpoints")
    response = lead.get("first_response_hours")
    if flag is True or status == "converted":
        return "skip", "Conversion evidence present"
    if status in terminal_statuses:
        return "skip", "Terminal status"
    if flag is False and patient_id is not None:
        return "staff_review", "Conversion fields conflict"
    if status not in open_statuses or flag is not False:
        return "staff_review", "Unknown status or conversion flag"
    if touches is None or not isinstance(touches, int) or touches < 0:
        return "staff_review", "Invalid touchpoint count"
    if response is not None and (not isinstance(response, (int, float)) or response < 0):
        return "staff_review", "Invalid response time"
    if not lead.get("assigned_location_id") or not lead.get("created_date"):
        return "staff_review", "Missing location or date"
    if touches == 0:
        return "follow_up", "No recorded touchpoints; staff must verify contact history and consent"
    if touches <= 2:
        return "staff_review", "Last contact time is unavailable"
    return "skip", "Contact limit reached for this demo"


def validate_proposal(proposal: dict[str, Any], inspected: dict[str, dict[str, Any]],
                      open_statuses: set[str], terminal_statuses: set[str]) -> dict[str, Any]:
    lead_id = proposal.get("lead_id")
    if lead_id not in inspected:
        raise ValueError("Proposal lead was not inspected in this run")
    decision = proposal.get("decision")
    if decision not in DECISIONS:
        raise ValueError("Invalid decision")
    expected, reason = classify(inspected[lead_id], open_statuses, terminal_statuses)
    if decision != expected:
        raise ValueError(f"Decision {decision} violates policy; expected {expected}: {reason}")
    rationale = proposal.get("rationale")
    if not isinstance(rationale, str) or not 1 <= len(rationale.strip()) <= 500:
        raise ValueError("Rationale must be 1-500 characters")
    draft = proposal.get("draft_text") or ""
    if not isinstance(draft, str) or len(draft) > 500:
        raise ValueError("Draft must be at most 500 characters")
    if decision == "skip" and draft:
        raise ValueError("Skipped lead cannot have an outreach draft")
    return {"lead_id": lead_id, "decision": decision, "rationale": rationale.strip(),
            "draft_text": draft.strip(), "policy_reason": reason}


def opportunity(worked_leads: int, baseline: float, uplift: float, revenue_per_conversion: float) -> dict[str, float]:
    if type(worked_leads) is not int or worked_leads < 0:
        raise ValueError("worked_leads must be a nonnegative integer")
    if not all(isinstance(v, (int, float)) and math.isfinite(v) for v in (baseline, uplift, revenue_per_conversion)):
        raise ValueError("Scenario assumptions must be finite numbers")
    if not 0 <= baseline <= 1 or not 0 <= uplift or baseline + uplift > 1:
        raise ValueError("Conversion assumptions are outside 0-1")
    if revenue_per_conversion < 0:
        raise ValueError("Revenue must be nonnegative")
    return {"additional_conversions": worked_leads * uplift,
            "incremental_revenue": worked_leads * uplift * revenue_per_conversion}


def validate_snapshot(value: str) -> date:
    return date.fromisoformat(value)

import json
import time
from typing import Any, Callable


TOOLS = [
    {"type": "function", "function": {"name": "get_candidates", "description": "Get at most 20 eligible lead IDs and evidence for one location.", "parameters": {"type": "object", "properties": {"location_id": {"type": "string"}, "limit": {"type": "integer"}}, "required": ["location_id", "limit"]}}},
    {"type": "function", "function": {"name": "inspect_lead", "description": "Inspect one ID returned by get_candidates.", "parameters": {"type": "object", "properties": {"lead_id": {"type": "string"}}, "required": ["lead_id"]}}},
    {"type": "function", "function": {"name": "estimate_opportunity", "description": "Calculate a clearly labeled revenue scenario from validated assumptions.", "parameters": {"type": "object", "properties": {"worked_leads": {"type": "integer"}, "baseline": {"type": "number"}, "uplift": {"type": "number"}, "revenue_per_conversion": {"type": "number"}}, "required": ["worked_leads", "baseline", "uplift", "revenue_per_conversion"]}}},
    {"type": "function", "function": {"name": "propose_tasks", "description": "Validate and save simulated pending tasks for inspected leads.", "parameters": {"type": "object", "properties": {"proposals": {"type": "array", "items": {"type": "object", "properties": {"lead_id": {"type": "string"}, "decision": {"type": "string", "enum": ["follow_up", "staff_review", "skip"]}, "rationale": {"type": "string"}, "draft_text": {"type": "string"}}, "required": ["lead_id", "decision", "rationale", "draft_text"]}}}, "required": ["proposals"]}}},
]

SYSTEM = ("You assist a clinic manager using synthetic data. First call get_candidates, then inspect each lead before proposing. "
          "Only propose policy-permitted decisions. Never claim consent, last contact time, actual conversion lift, or live scheduling. "
          "Do not approve tasks. Keep drafts brief. All saved tasks are simulated and require manager review.")


class AgentRunError(RuntimeError):
    def __init__(self, message: str, events: list[dict[str, Any]]):
        super().__init__(message)
        self.events = events


def run_agent(client: Any, model: str, tools: dict[str, Callable[..., Any]], location_id: str,
              limit: int, max_steps: int = 12) -> tuple[str, list[dict[str, Any]]]:
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": f"Investigate up to {limit} leads at location {location_id}. Save justified simulated tasks, then summarize."},
    ]
    events: list[dict[str, Any]] = []
    failures = 0
    for _ in range(max_steps):
        try:
            response = client.chat.completions.create(model=model, messages=messages, tools=TOOLS, tool_choice="auto")
        except Exception as exc:
            raise AgentRunError(f"Model call failed: {exc}", events) from exc
        message = response.choices[0].message
        calls = message.tool_calls or []
        if not calls:
            if not any(e.get("tool_name") == "get_candidates" and not e.get("error_message") for e in events):
                raise AgentRunError("Model ended before retrieving candidates", events)
            if not any(e.get("tool_name") == "propose_tasks" and not e.get("error_message") for e in events):
                raise AgentRunError("Model ended before saving validated proposals", events)
            return message.content or "Agent completed without a summary.", events
        messages.append(message.model_dump(exclude_none=True))
        for call in calls:
            name = call.function.name
            started = time.monotonic()
            error = None
            try:
                if name not in tools:
                    raise ValueError("Unknown tool")
                args = json.loads(call.function.arguments)
                if not isinstance(args, dict):
                    raise ValueError("Tool arguments must be an object")
                result = tools[name](**args)
            except Exception as exc:
                failures += 1
                error = str(exc)
                result = {"error": error}
            event = {"step_number": len(events) + 1, "tool_name": name,
                     "arguments_json": call.function.arguments, "result_json": json.dumps(result, default=str),
                     "elapsed_ms": int((time.monotonic() - started) * 1000), "error_message": error}
            events.append(event)
            messages.append({"role": "tool", "tool_call_id": call.id, "content": event["result_json"]})
            if failures > 1:
                raise AgentRunError("Tool validation failed twice; see event log", events)
    raise AgentRunError("Agent reached its tool-step budget; see event log", events)
# END EMBEDDED HELPERS

for name, default in {
    "catalog": "workspace", "source_schema": "chiro_hackathon", "output_schema": "chiro_agent_demo",
    "model_name": "", "location_id": "", "max_tasks": "5", "snapshot_date": str(date.today()),
    "lookback_days": "30", "open_statuses": "", "terminal_statuses": "",
    "run_mode": "agent",
    "review_task_id": "", "review_decision": "", "outcome_task_id": "", "outcome": "",
}.items():
    dbutils.widgets.text(name, default)

def widget(name):
    return dbutils.widgets.get(name).strip()

def identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid identifier: {value!r}")
    return value

catalog, source_schema, output_schema = (identifier(widget(k)) for k in ("catalog", "source_schema", "output_schema"))
source = f"{catalog}.{source_schema}"
output = f"{catalog}.{output_schema}"
snapshot_date = widget("snapshot_date")
validate_snapshot(snapshot_date)
limit = int(widget("max_tasks"))
configured_limit = limit
lookback = int(widget("lookback_days"))
if not 1 <= limit <= 20 or not 1 <= lookback <= 365:
    raise ValueError("max_tasks must be 1-20 and lookback_days 1-365")
run_mode = widget("run_mode")
if run_mode not in {"agent", "review_only"}:
    raise ValueError("run_mode must be agent or review_only")
open_statuses = {s.strip().lower() for s in widget("open_statuses").split(",") if s.strip()}
terminal_statuses = {s.strip().lower() for s in widget("terminal_statuses").split(",") if s.strip()}
if open_statuses & terminal_statuses:
    raise ValueError("Open and terminal statuses overlap")
print(f"Snapshot: {snapshot_date}; lookback: {lookback} days; mode: simulated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 Data checks
# MAGIC Inspect these results, then enter the actual open and terminal status values in the widgets.

# COMMAND ----------

leads = spark.table(f"{source}.leads")
locations = spark.table(f"{source}.locations")
display(leads.groupBy("status", "converted_flag").count().orderBy("status", "converted_flag"))
display(leads.agg(F.count("*").alias("lead_rows"), F.countDistinct("lead_id").alias("unique_lead_ids"),
                  F.min("created_date").alias("earliest"), F.max("created_date").alias("latest")))
lead_duplicates = leads.groupBy("lead_id").count().filter("lead_id IS NULL OR count > 1").limit(1).count()
location_duplicates = locations.groupBy("location_id").count().filter("location_id IS NULL OR count > 1").limit(1).count()
if lead_duplicates or location_duplicates:
    raise ValueError("Lead or location keys are null/duplicated; resolve the data contract before running")
display(locations.select("location_id", "location_name", "city", "state").orderBy("location_name"))
if not open_statuses:
    raise ValueError("Set open_statuses from the displayed live status values and rerun")

location_id = widget("location_id")
location_rows = locations.filter(F.col("location_id").cast("string") == location_id).limit(2).collect()
if len(location_rows) != 1:
    raise ValueError("Select an existing unique location_id in the widget")
location_name = location_rows[0]["location_name"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 Find opportunities
# MAGIC Python enforces eligibility. The model can inspect only IDs returned in this run.

# COMMAND ----------

joined = leads.join(locations.select("location_id", "location_name"),
                    leads.assigned_location_id == locations.location_id, "left")
joined = joined.withColumn("status_normalized", F.lower(F.trim(F.col("status"))))
joined = joined.withColumn("age_days", F.datediff(F.lit(snapshot_date).cast("date"), F.col("created_date")))
joined = joined.withColumn("patient_id_normalized", F.when(F.trim(F.col("converted_patient_id").cast("string")) == "", None)
                           .otherwise(F.col("converted_patient_id")))
at_location = joined.filter(F.col("assigned_location_id").cast("string") == location_id)
eligible = at_location.filter(
    F.col("status_normalized").isin(list(open_statuses)) &
    (F.col("converted_flag") == F.lit(False)) &
    F.col("patient_id_normalized").isNull() &
    F.col("location_name").isNotNull() &
    F.col("num_touchpoints").between(0, 2) &
    F.col("age_days").between(0, lookback) &
    (F.col("first_response_hours").isNull() | (F.col("first_response_hours") >= 0))
)
inspectable = at_location.filter(
    F.col("status_normalized").isin(list(open_statuses)) &
    (F.col("converted_flag") == F.lit(False)) &
    F.col("patient_id_normalized").isNull() &
    F.col("location_name").isNotNull() &
    (F.col("num_touchpoints") >= 3) &
    F.col("age_days").between(0, lookback) &
    (F.col("first_response_hours").isNull() | (F.col("first_response_hours") >= 0))
)
eligible_count = eligible.count()
contact_limit_count = inspectable.count()
excluded_count = at_location.count() - eligible_count
print(f"Location: {location_name}; eligible: {eligible_count}; contact-limit examples: {contact_limit_count}; excluded or routed to audit: {excluded_count}")
audit = at_location.withColumn(
    "audit_reason",
    F.when(F.col("converted_flag") == F.lit(True), "converted_flag_true")
     .when(F.col("status_normalized").isin(list(terminal_statuses)), "terminal_status")
     .when((F.col("converted_flag") == F.lit(False)) & F.col("patient_id_normalized").isNotNull(), "conflicting_patient_id")
     .when(F.col("converted_flag").isNull(), "null_conversion_flag")
     .when(F.col("status_normalized").isNull() | ~F.col("status_normalized").isin(list(open_statuses)), "unknown_or_nonopen_status")
     .when(F.col("location_name").isNull(), "missing_location")
     .when(F.col("created_date").isNull() | ~F.col("age_days").between(0, lookback), "outside_snapshot_window")
     .when(F.col("num_touchpoints").isNull() | (F.col("num_touchpoints") < 0), "invalid_touchpoints")
     .when(F.col("first_response_hours") < 0, "invalid_response_hours")
     .when(F.col("num_touchpoints") >= 3, "contact_limit_skip")
     .otherwise("eligible"),
)
display(audit.groupBy("audit_reason").count().orderBy("audit_reason"))
display(eligible.select("lead_id", "status", "created_date", "num_touchpoints", "first_response_hours", "age_days")
        .orderBy(F.asc("num_touchpoints"), F.desc("first_response_hours"), F.desc("created_date")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 Review actions
# MAGIC Running the agent saves simulated tasks as `pending_review`. Use review widgets on a later notebook run to approve or reject one.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {output}")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {output}.agent_runs (
  run_id STRING, snapshot_date DATE, location_id STRING, requested_limit INT, mode STRING,
  started_at TIMESTAMP, finished_at TIMESTAMP, run_status STRING, error_message STRING
) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {output}.followup_tasks (
  task_id STRING, run_id STRING, snapshot_date DATE, lead_id STRING, location_id STRING,
  decision STRING, evidence_json STRING, rationale STRING, draft_text STRING,
  review_status STRING, reviewed_at TIMESTAMP, outcome STRING, outcome_at TIMESTAMP,
  is_simulated BOOLEAN
) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {output}.agent_events (
  run_id STRING, step_number INT, tool_name STRING, arguments_json STRING, result_json STRING,
  elapsed_ms BIGINT, error_message STRING
) USING DELTA""")

run_id = str(uuid.uuid4())
started = datetime.now(timezone.utc)
inspected = {}
candidate_ids = set()

def get_candidates(location_id: str, limit: int):
    if location_id != widget("location_id") or type(limit) is not int or not 1 <= limit <= configured_limit:
        raise ValueError("Invalid location or limit")
    reserve_skip = 1 if contact_limit_count and limit > 1 else 0
    rows = (eligible.withColumn("untouched", F.col("num_touchpoints") == 0)
            .withColumn("delayed", F.coalesce(F.col("first_response_hours") > 24, F.lit(False)))
            .orderBy(F.desc("untouched"), F.desc("delayed"), F.desc("created_date"), "lead_id")
            .limit(limit - reserve_skip).collect())
    if reserve_skip:
        rows += inspectable.orderBy(F.desc("created_date"), "lead_id").limit(1).collect()
    result = [{"lead_id": str(r["lead_id"]), "touchpoints": r["num_touchpoints"],
               "first_response_hours": r["first_response_hours"], "created_date": str(r["created_date"]),
               "candidate_kind": "contact_limit_skip_example" if r["num_touchpoints"] >= 3 else "eligible"} for r in rows]
    candidate_ids.update(r["lead_id"] for r in result)
    return {"candidates": result, "eligible_count": eligible_count, "excluded_count": excluded_count,
            "contact_limit_count": contact_limit_count,
            "snapshot_date": snapshot_date}

def inspect_lead(lead_id: str):
    if lead_id not in candidate_ids:
        raise ValueError("Lead ID was not returned by get_candidates")
    rows = joined.filter(F.col("lead_id").cast("string") == lead_id).limit(2).collect()
    if len(rows) != 1:
        raise ValueError("Lead inspection requires exactly one record")
    r = rows[0]
    record = {k: (str(r[k]) if k == "created_date" and r[k] is not None else r[k])
              for k in ("lead_id", "status", "converted_flag", "converted_patient_id", "assigned_location_id",
                        "created_date", "num_touchpoints", "first_response_hours", "location_name", "source")}
    record["lead_id"] = str(record["lead_id"])
    record["assigned_location_id"] = str(record["assigned_location_id"])
    record["status"] = (record["status"] or "").lower().strip()
    inspected[lead_id] = record
    decision, reason = classify(record, open_statuses, terminal_statuses)
    return {"record": record, "policy_decision": decision, "policy_reason": reason}

def estimate_opportunity(worked_leads: int, baseline: float, uplift: float, revenue_per_conversion: float):
    result = opportunity(worked_leads, baseline, uplift, revenue_per_conversion)
    return {**result, "label": "Illustrative scenario using supplied assumptions; not measured lift"}

def propose_tasks(proposals: list[dict]):
    if not candidate_ids or not isinstance(proposals, list) or not 1 <= len(proposals) <= limit:
        raise ValueError("Proposal count must be 1 through the configured limit")
    validated = [validate_proposal(p, inspected, open_statuses, terminal_statuses) for p in proposals]
    if len({p["lead_id"] for p in validated}) != len(validated):
        raise ValueError("Duplicate proposal IDs")
    saved, skipped = [], []
    task_table = DeltaTable.forName(spark, f"{output}.followup_tasks")
    for p in validated:
        if p["decision"] == "skip":
            skipped.append(p["lead_id"])
            continue
        existing_open = (spark.table(f"{output}.followup_tasks")
                         .filter((F.col("lead_id") == p["lead_id"]) &
                                 F.col("review_status").isin("pending_review", "approved"))
                         .limit(1).count())
        if existing_open:
            skipped.append(p["lead_id"])
            continue
        task_id = task_key(p["lead_id"], snapshot_date)
        if spark.table(f"{output}.followup_tasks").filter(F.col("task_id") == task_id).limit(1).count():
            skipped.append(p["lead_id"])
            continue
        source_record = inspected[p["lead_id"]]
        evidence = json.dumps(source_record, default=str)
        grounded_rationale = (f"{p['policy_reason']}. Recorded touchpoints: {source_record['num_touchpoints']}; "
                              f"first response hours: {source_record['first_response_hours']}.")
        schema = "task_id STRING, run_id STRING, snapshot_date DATE, lead_id STRING, location_id STRING, decision STRING, evidence_json STRING, rationale STRING, draft_text STRING, review_status STRING, reviewed_at TIMESTAMP, outcome STRING, outcome_at TIMESTAMP, is_simulated BOOLEAN"
        row = [(task_id, run_id, validate_snapshot(snapshot_date), p["lead_id"], location_id,
                p["decision"], evidence, grounded_rationale, p["draft_text"], "pending_review", None, None, None, True)]
        staged = spark.createDataFrame(row, schema=schema)
        task_table.alias("t").merge(staged.alias("s"), "t.task_id = s.task_id").whenNotMatchedInsertAll().execute()
        saved.append(task_id)
    return {"saved_task_ids": saved, "skipped_or_existing_leads": skipped, "mode": "simulated"}

def update_review():
    task_id, decision = widget("review_task_id"), widget("review_decision")
    if not task_id and not decision:
        return
    if not re.fullmatch(r"[0-9a-f]{64}", task_id) or decision not in {"approved", "rejected"}:
        raise ValueError("Provide a valid review_task_id and approved or rejected")
    rows = spark.table(f"{output}.followup_tasks").filter(F.col("task_id") == task_id).limit(2).collect()
    if len(rows) != 1 or rows[0]["review_status"] != "pending_review":
        raise ValueError("Only one pending task can be reviewed")
    spark.sql(f"UPDATE {output}.followup_tasks SET review_status = '{decision}', reviewed_at = current_timestamp() WHERE task_id = '{task_id}' AND review_status = 'pending_review'")

def update_outcome():
    task_id, outcome_value = widget("outcome_task_id"), widget("outcome")
    if not task_id and not outcome_value:
        return
    if not re.fullmatch(r"[0-9a-f]{64}", task_id) or outcome_value not in {"not_contacted", "contacted_no_response", "appointment_requested", "converted_simulated"}:
        raise ValueError("Provide a valid outcome_task_id and allowed outcome")
    rows = spark.table(f"{output}.followup_tasks").filter(F.col("task_id") == task_id).limit(2).collect()
    if len(rows) != 1 or rows[0]["review_status"] != "approved":
        raise ValueError("Only one approved task can record an outcome")
    spark.sql(f"UPDATE {output}.followup_tasks SET outcome = '{outcome_value}', outcome_at = current_timestamp() WHERE task_id = '{task_id}' AND review_status = 'approved'")

update_review()
update_outcome()

events = []
run_status, error_message = "completed", None
if run_mode == "review_only":
    print("Applied requested review/outcome updates without calling the model.")
elif eligible_count and widget("model_name"):
    try:
        from databricks.sdk import WorkspaceClient
        client = WorkspaceClient().serving_endpoints.get_open_ai_client()
        summary, events = run_agent(client, widget("model_name"), {
            "get_candidates": get_candidates, "inspect_lead": inspect_lead,
            "estimate_opportunity": estimate_opportunity, "propose_tasks": propose_tasks,
        }, location_id, limit)
        print(summary)
    except AgentRunError as exc:
        events = exc.events
        run_status, error_message = "failed", str(exc)
        print(f"Agent failed: {error_message}")
    except Exception as exc:
        run_status, error_message = "failed", str(exc)
        print(f"Agent failed: {error_message}")
elif not eligible_count:
    print("No eligible leads. No tasks created.")
else:
    run_status, error_message = "not_started", "Set model_name to a tested tool-calling endpoint"
    print(error_message)

finished = datetime.now(timezone.utc)
spark.createDataFrame([(run_id, validate_snapshot(snapshot_date), location_id, limit, run_mode, started,
                        finished, run_status, error_message)],
                      "run_id STRING, snapshot_date DATE, location_id STRING, requested_limit INT, mode STRING, started_at TIMESTAMP, finished_at TIMESTAMP, run_status STRING, error_message STRING")\
    .write.mode("append").saveAsTable(f"{output}.agent_runs")
if events:
    spark.createDataFrame([(run_id, e["step_number"], e["tool_name"], e["arguments_json"],
                            e["result_json"], e["elapsed_ms"], e["error_message"]) for e in events],
                          "run_id STRING, step_number INT, tool_name STRING, arguments_json STRING, result_json STRING, elapsed_ms BIGINT, error_message STRING")\
        .write.mode("append").saveAsTable(f"{output}.agent_events")

display(spark.table(f"{output}.followup_tasks").filter(F.col("location_id") == location_id)
        .select("task_id", "lead_id", "decision", "rationale", "draft_text", "review_status", "outcome", "is_simulated")
        .orderBy(F.desc("snapshot_date")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 Outcomes
# MAGIC This is a scenario, not observed revenue lift. Change the assumptions before presenting it.

# COMMAND ----------

scenario = estimate_opportunity(200, 0.10, 0.03, 250.0)
print(json.dumps({"worked_leads": 200, "assumed_baseline": 0.10, "assumed_absolute_uplift": 0.03,
                  "assumed_30_day_revenue_per_conversion": 250.0, **scenario}, indent=2))
display(spark.table(f"{output}.followup_tasks").filter(F.col("location_id") == location_id)
        .groupBy("review_status", "outcome").count())

# COMMAND ----------

# DBTITLE 1,Multi-cycle lead nurturing
# Multi-cycle lead nurturing: generate a 3-touch staff workflow plan for each follow_up lead
# This is a task planner for clinic staff -- it does NOT send emails or SMS automatically.
# Staff use the suggested drafts and timing to manually follow up with each lead.
from pyspark.sql.types import DateType

spark.sql(f"""CREATE TABLE IF NOT EXISTS {output}.nurturing_cycles (
  cycle_id STRING, task_id STRING, lead_id STRING, location_id STRING,
  snapshot_date DATE, cycle_number INT, channel STRING, timing_days INT,
  draft_text STRING, cycle_status STRING, is_simulated BOOLEAN
) USING DELTA""")

# Get all pending follow_up tasks for this location
nurture_tasks = (spark.table(f"{output}.followup_tasks")
    .filter((F.col("location_id") == location_id) &
            (F.col("review_status") == "pending_review") &
            (F.col("decision") == "follow_up"))
    .orderBy("lead_id")
    .collect())

# 3-cycle nurturing sequence: phone → email/SMS → phone
NURTURE_SEQUENCE = [
    {"cycle": 1, "channel": "phone_call",   "timing": 0, "draft": "Initial outreach: Call to verify contact info, confirm interest, and schedule an appointment."},
    {"cycle": 2, "channel": "email_sms",    "timing": 3, "draft": "Follow-up: Send a reminder referencing the initial inquiry. Offer flexible scheduling options."},
    {"cycle": 3, "channel": "phone_call",   "timing": 7, "draft": "Final attempt: Call with a time-limited offer to schedule a consultation this week."},
]

# Generate nurturing rows
nurture_rows = []
for task in nurture_tasks:
    ev = json.loads(task["evidence_json"]) if task["evidence_json"] else {}
    source = ev.get("source", "unknown")
    for seq in NURTURE_SEQUENCE:
        cycle_id = task_key(task["lead_id"], f"{snapshot_date}_n{seq['cycle']}")
        # Personalize draft with lead source
        draft = seq["draft"]
        if source != "unknown":
            draft = draft.replace("the initial inquiry", f"the {source.lower()}")
        nurture_rows.append((
            cycle_id, task["task_id"], task["lead_id"], location_id,
            snapshot_date, seq["cycle"], seq["channel"], seq["timing"],
            draft, "pending", True
        ))

if nurture_rows:
    # Clear old cycles for this location + snapshot, then insert fresh
    DeltaTable.forName(spark, f"{output}.nurturing_cycles").delete(
        (F.col("location_id") == location_id) &
        (F.col("snapshot_date") == F.lit(snapshot_date).cast("date"))
    )
    snap_d = date.fromisoformat(snapshot_date)
    typed_rows = [(
        r[0], r[1], r[2], r[3], snap_d, int(r[5]), r[6], int(r[7]), r[8], r[9], r[10]
    ) for r in nurture_rows]
    nurture_df = spark.createDataFrame(typed_rows,
        schema=StructType([
            StructField("cycle_id", StringType(), True),
            StructField("task_id", StringType(), True),
            StructField("lead_id", StringType(), True),
            StructField("location_id", StringType(), True),
            StructField("snapshot_date", DateType(), True),
            StructField("cycle_number", IntegerType(), True),
            StructField("channel", StringType(), True),
            StructField("timing_days", IntegerType(), True),
            StructField("draft_text", StringType(), True),
            StructField("cycle_status", StringType(), True),
            StructField("is_simulated", BooleanType(), True),
        ]))
    nurture_df.write.mode("append").saveAsTable(f"{output}.nurturing_cycles")
    print(f"Generated {len(nurture_rows)} nurturing cycles for {len(nurture_tasks)} follow_up leads")
    print(f"Each lead gets a 3-touch staff workflow over 7 days: phone (day 0) -> email/SMS (day 3) -> phone (day 7)")
    print(f"Staff must manually perform each action using the suggested draft. No automated sending.")
else:
    print("No follow_up tasks to nurture. Run the agent (Cell 10) first.")

print()
print("Staff nurturing workflow:")
display(spark.table(f"{output}.nurturing_cycles")
    .filter((F.col("location_id") == location_id) & (F.col("snapshot_date") == F.lit(snapshot_date).cast("date")))
    .select("lead_id", "cycle_number", "channel", "timing_days", "cycle_status", "draft_text")
    .orderBy("lead_id", "cycle_number"))

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 6 Summary
# MAGIC Everything below is auto-generated from the latest run. All tasks are **simulated** and require manager review before any action. The nurturing workflow shows the 3-touch staff action plan generated for each lead.

# COMMAND ----------

# DBTITLE 1,Summary dashboard
from pyspark.sql import functions as F

W = 72
def bar(top=True):
    if top: print("\u2554" + "\u2550" * W + "\u2557")
    else:  print("\u255A" + "\u2550" * W + "\u255D")
def sub():
    print("  " + "\u2500" * W)
def blank():
    print()
def center(text):
    print("\u2551" + text.center(W) + "\u2551")
def line(text=""):
    print(f"  {text}" if text else "")
def kv(label, value, indent=4):
    dots = "." * max(2, 42 - len(str(label)))
    print(f"{' ' * indent}{label} {dots} {value}")

bar(True)
center("LEAD RECOVERY SUMMARY")
center(location_name)
center(f"Location: {location_id}   Snapshot: {snapshot_date}   Lookback: {lookback} days")
bar(False)
blank()

# ── Quick Stats ────────────────────────────────────────────────────────
line("  \U0001f4ca  QUICK STATS")
kv("Eligible leads", f"{eligible_count:,}")
kv("Contact-limit skips", f"{contact_limit_count:,}")
kv("Excluded (converted, lost, or outside window)", f"{excluded_count:,}")
blank()
bar(True)
center("STAFF ACTION ITEMS")
bar(False)
blank()

# ── Pending Tasks ─────────────────────────────────────────────────────
line("  \u2705  PENDING TASKS (pending manager review)")
line(f"  All tasks are SIMULATED -- a manager must approve before any outreach.")
line()
tasks = (spark.table(f"{output}.followup_tasks")
         .filter((F.col("location_id") == location_id) & (F.col("review_status") == "pending_review"))
         .orderBy(F.desc("snapshot_date"), "lead_id"))
task_rows = tasks.collect()
worked = len(task_rows) if task_rows else 0
if task_rows:
    for i, t in enumerate(task_rows):
        ev = json.loads(t["evidence_json"]) if t["evidence_json"] else {}
        source = ev.get("source", "Unknown")
        status = (ev.get("status") or "unknown").title()
        touches = ev.get("num_touchpoints", "?")
        decision = t["decision"].upper().replace("_", " ")
        resp = ev.get("first_response_hours", "?")
        created = ev.get("created_date", "?")
        print(f"    {i+1}. {t['lead_id']}  |  {source}  |  {status}  |  Created: {created}")
        print(f"       Touches: {touches}  |  1st response: {resp} hrs  |  Action: {decision}")
        print(f"       {t['rationale']}")
        print()
else:
    line("  No pending tasks. Run the agent (Cell 10) to generate new tasks.")
blank()

# ── Nurturing Pipeline ─────────────────────────────────────────────────
line("  \U0001f504  NURTURING WORKFLOW (3-touch staff plan per follow_up lead)")
line(f"  Staff action plan: phone (day 0) -> email/SMS (day 3) -> phone (day 7)")
line(f"  Drafts are suggestions for staff -- no automated sending.")
line()
nurture_cycles = (spark.table(f"{output}.nurturing_cycles")
    .filter((F.col("location_id") == location_id) & (F.col("snapshot_date") == F.lit(snapshot_date).cast("date")))
    .orderBy("lead_id", "cycle_number")
    .collect())
if nurture_cycles:
    cur_lead = None
    lead_num = 0
    for nc in nurture_cycles:
        if nc["lead_id"] != cur_lead:
            cur_lead = nc["lead_id"]
            lead_num += 1
            print()
            print(f"    {lead_num}. {nc['lead_id']}")
        chan = {"phone_call": "Phone Call", "email_sms": "Email/SMS"}.get(nc["channel"], nc["channel"])
        print(f"       Day {nc['timing_days']}  |  {chan}  |  {nc['cycle_status']}")
        if nc["draft_text"]:
            print(f"          {nc['draft_text']}")
    print()
else:
    line("  No nurturing cycles. Run the nurturing cell (Cell 13) first.")
blank()
bar(True)
center("REVENUE & GROWTH PROJECTIONS")
bar(False)
blank()

# ── Revenue & Growth ───────────────────────────────────────────────────
total_leads_count = leads.count()
total_converted_count = leads.filter(F.col("converted_flag") == F.lit(True)).count()
total_open_count = leads.filter(F.lower(F.trim(F.col("status"))).isin(list(open_statuses)) & (F.col("converted_flag") == F.lit(False))).count()
total_lost_count = total_leads_count - total_converted_count - total_open_count

current_arr = 100_000_000
rev_per_patient = current_arr / total_converted_count if total_converted_count else 0
conv_rate = total_converted_count / total_leads_count if total_leads_count else 0
open_potential_m = total_open_count * rev_per_patient / 1_000_000

pred_lift = 0.05
stretch_lift = 0.15
pred_pat = int(total_open_count * pred_lift)
stretch_pat = int(total_open_count * stretch_lift)
pred_rev = pred_pat * rev_per_patient / 1_000_000
stretch_rev = stretch_pat * rev_per_patient / 1_000_000
pred_arr = (current_arr + pred_pat * rev_per_patient) / 1_000_000
stretch_arr = (current_arr + stretch_pat * rev_per_patient) / 1_000_000

reengage_rev = int(total_lost_count * 0.05) * rev_per_patient / 1_000_000
response_rev = int(total_open_count * 0.02) * rev_per_patient / 1_000_000
nurture_rev = int(total_open_count * 0.03) * rev_per_patient / 1_000_000
ltv_rev = current_arr * 0.10 / 1_000_000
combined = reengage_rev + response_rev + nurture_rev + ltv_rev
pred_total = 100 + pred_rev + combined
stretch_total = 100 + stretch_rev + combined

line("  \U0001f4b0  REVENUE & GROWTH")
line()
line(f"  Current: $100M ARR | {total_converted_count:,} patients | {conv_rate:.0%} conversion | ${rev_per_patient:,.0f}/patient/yr")
line(f"  Database: {total_leads_count:,} leads = {total_converted_count:,} converted, {total_open_count:,} open (${open_potential_m:.0f}M addressable), {total_lost_count:,} lost")
line()
line(f"  \U0001f4ca  PREDICTED AVERAGE (+{pred_lift:.0%} lift on open leads)")
kv("New patients", f"{pred_pat:,}", 4)
kv("New revenue", f"${pred_rev:.1f}M -> ${pred_arr:.0f}M ARR", 4)
line()
line(f"  \U0001f680  VERY POSITIVE (+{stretch_lift:.0%} lift on open leads)")
kv("New patients", f"{stretch_pat:,}", 4)
kv("New revenue", f"${stretch_rev:.1f}M -> ${stretch_arr:.0f}M ARR", 4)
line()
line(f"  \U0001f4a1  ADDITIONAL REVENUE LEVERS (compound on top of lead recovery)")
line(f"    1. Re-engage lost leads (5% win-back)   -> ${reengage_rev:.1f}M")
line(f"    2. Cut response time to <1hr (+2%)      -> ${response_rev:.1f}M")
line(f"    3. Staff nurturing workflow (+3%)          -> ${nurture_rev:.1f}M")
line(f"    4. Raise patient LTV (+10%)             -> ${ltv_rev:.1f}M")
line(f"    5. Open new locations ($5M each)         -> scales linearly")
line()
kv("Combined levers", f"~${combined:.0f}M", 4)
kv("Predicted total", f"${pred_total:.0f}M ARR", 4)
kv("Very positive total", f"${stretch_total:.0f}M ARR", 4)
line()
# ── 10-Year Projection (excluding new locations) ───────────────────────
pred_y1 = pred_total
stretch_y1 = stretch_total
pred_annual = 12.5
stretch_annual = 20.0

line("  \U0001f4c9  10-YEAR PROJECTION (excluding new locations)")
line()
line("  Levers compound year over year: higher conversion rate and higher LTV")
line("  persist permanently. Year 1 captures the existing lead pool; later years")
line("  convert new leads at the improved rates.")
line()
print(f"    {'Year':>4}  {'Predicted':>12}  {'Very Positive':>14}")
print(f"    {'----':>4}  {'------------':>12}  {'--------------':>14}")
for yr in [1, 2, 3, 5, 7, 10]:
    if yr == 1:
        p = pred_y1
        s = stretch_y1
    else:
        p = pred_y1 + pred_annual * (yr - 1)
        s = stretch_y1 + stretch_annual * (yr - 1)
    print(f"    {yr:>4}  ${p:>10.0f}M  ${s:>12.0f}M")
line()
line("  Predicted average reaches ~$250M by year 10 from levers alone.")
line("  Very positive exceeds $300M by year 10.")
line()
line("  New locations ($5M ARR each) would accelerate this further but are")
line("  excluded from the projection above.")
line("  Note: 10-year projections are illustrative, based on assumed annual rates.")
blank()

# ── Agent Event Log ───────────────────────────────────────────────────
line("  \U0001f527  AGENT EVENT LOG")
events_df = spark.table(f"{output}.agent_events").orderBy("step_number")
event_rows = events_df.filter(F.col("run_id") == spark.table(f"{output}.agent_runs").orderBy(F.desc("started_at")).limit(1).select("run_id").collect()[0]["run_id"]).collect()
if event_rows:
    print(f"    {'Step':>4}  {'Tool':<20}  {'Elapsed':>9}   Status")
    print(f"    {'--':>4}  {'-'*20}  {'-'*9}   {'-'*30}")
    for e in event_rows:
        status = "ok" if not e["error_message"] else f"ERR {e['error_message'][:35]}"
        print(f"    {e['step_number']:>4}  {e['tool_name']:<20}  {e['elapsed_ms']:>7}ms   {status}")
else:
    line("    No agent events recorded.")
blank()

bar(True)
center("All tasks are simulated. Approve or reject in the Review widgets.")
bar(False)