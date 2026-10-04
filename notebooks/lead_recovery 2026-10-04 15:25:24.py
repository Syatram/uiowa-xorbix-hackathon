# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Lead Recovery Agent
# MAGIC Synthetic lead triage for a clinic manager. All outreach and outcomes remain simulated.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 Setup

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