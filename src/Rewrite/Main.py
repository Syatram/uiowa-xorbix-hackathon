import json
import re
import uuid
from datetime import date, datetime, timezone
import math
import hashlib
from typing import Any

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

from Rewrite.Enums import Decisions, Outcomes
from delta.tables import DeltaTable
from databricks.sdk import WorkspaceClient, dbutils

from Rewrite.LeadRecoveryAgent import LeadRecoveryAgent, LeadRecoveryAgentBuilder
from Rewrite.Enums import ReviewStates
### Constants ###

# System prompt
SYSTEM = ("You assist a clinic manager using synthetic data. First call get_candidates, then inspect each lead before proposing. "
          "Only propose policy-permitted decisions. Never claim consent, last contact time, actual conversion lift, or live scheduling. "
          "Do not approve tasks. Keep drafts brief. All saved tasks are simulated and require manager review.")

# Widgets that can be edited
WIDGETS = {
    "catalog": "workspace", 
    "source_schema": "chiro_hackathon", 
    "output_schema": "chiro_agent_demo",
    "model_name": "", 
    "location_id": "", 
    "lead_limit": "5", 
    "snapshot_date": str(date.today()),
    "lookback_days": "30", 
    "open_statuses": "", 
    "terminal_statuses": "",
    "run_mode": "agent",
    "review_task_id": "", 
    "review_decision": "", 
    "outcome_task_id": "", 
    "outcome": "",
}

MAX_LEAD_LIMIT = 20
MAX_LOOKBACK = 365

### End of constants ###

# If we're going to use an app it would be better
# to just comment this out
def get_widget_value(name : str) -> str:
    """Gets the value of the specified widget"""
    return dbutils.widgets.get(name).strip()

def validate_identifier(value : str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("Bad identifier")
    return 

def validate_snapshot(value : str) -> date:
    return date.isoformat(value)

def do_duplicate_leads_exist(leads : DataFrame):
    lead_duplicates = leads.groupBy("lead_id").count().filter("lead_id IS NULL OR count > 1").limit(1).count()

    return lead_duplicates > 0

def do_duplicate_locations_exist(locations : DataFrame):
    location_duplicates = locations.groupBy("location_id").count().filter("location_id IS NULL OR count > 1").limit(1).count()

    return location_duplicates > 0

def get_all_widget_values() -> dict[str, str | date | int | list[str]]: 
    for k, v in WIDGETS:
        dbutils.widgets.text(k, v)

    # Get widget values
    catalog, source_schema, output_schema = (validate_identifier(get_widget_value(k)) for k in ("catalog", "source_schema", "output_schema"))
    snapshot_date = validate_snapshot(get_widget_value("snapshot_date"))
    lead_limit = int(get_widget_value("lead_limit"))
    lookback = int(get_widget_value("lookback_days"))
    run_mode = get_widget_value("run_mode")
    # Not sure what these are doing
    open_statuses = [s.strip().lower() for s in get_widget_value("open_statuses").split(",") if s.strip()]
    terminal_statuses = [s.strip().lower() for s in get_widget_value("terminal_statuses").split(",") if s.strip()]

    source = f"{catalog}.{source_schema}"
    output = f"{catalog}.{output_schema}"

    # Input validation
    if not 1 <= lead_limit <= MAX_LEAD_LIMIT:
        raise ValueError(f"lead_limit: {lead_limit} outside of bounds")

    if run_mode not in {"agent", "review_ony"}:
        raise ValueError(f"run_mode: {run_mode} could not be found")

    if open_statuses & terminal_statuses:
        raise ValueError("Open and terminal statuses overlap")

    if not open_statuses:
        raise ValueError("Open status not set")


    return {
        "catalog" : catalog,
        "source_schema" : source_schema,
        "output_schema" : output_schema,
        "snapshot_date" : snapshot_date,
        "lead_limit" : lead_limit,
        "lookback" : lookback,
        "run_mode" : run_mode,
        "open_statuses" : open_statuses,
    }

def task_key(lead_id: str, snapshot_date: str) -> str:
    return hashlib.sha256(f"lead_followup|{snapshot_date}|{lead_id}".encode()).hexdigest()



def classify(lead: dict[str, Any], open_statuses: list[str], terminal_statuses: list[str]) -> tuple[str, str]:
    """Return an allowed decision and its reason; never infer missing consent."""
    status = lead.get("status")
    converted = lead.get("converted_flag")
    patient_id = lead.get("converted_patient_id") or None
    touches = lead.get("num_touchpoints")
    response = lead.get("first_response_hours")

    if converted is True:
        return Decisions.SKIP, "Converted"

    if status in terminal_statuses:
        return Decisions.SKIP, "Terminal status"

    if converted is False and patient_id is not None:
        return Decisions.STAFF_REVIEW, "Not converted but has patient id"

    if status not in open_statuses:
        return Decisions.STAFF_REVIEW, "Unknown status"

    if touches is None or not isinstance(touches, int) or touches < 0:
        return Decisions.STAFF_REVIEW, "Invalid touchpoint count"

    if response is not None and (not isinstance(response, (int, float)) or response < 0):
        return Decisions.STAFF_REVIEW, "Invalid response time"

    if not lead.get("assigned_location_id") or not lead.get("created_date"):
        return "staff_review", "Missing location or date"

    if touches <= 2:
        return "staff_review", "Last contact time is unavailable"

    return Decisions.STAFF_REVIEW, "Could not classify"

def get_candidates():
    """Get at most 20 eligible lead IDs and evidence for one location."""
    print()

def inspect_lead():
    """Inspect one ID returned by get_candidates."""
    print()

def estimate_opportunity():
    """Calculate a clearly labeled revenue scenario from validated assumptions."""
    print()

def propose_task():
    """Validate and save simulated pending tasks for inspected leads."""
    print()

def init():
    client = WorkspaceClient().serving_endpoints.get_open_ai_client()
    widgets = get_all_widget_values()

    agent_builder = LeadRecoveryAgentBuilder(client, widgets.get("model_name"))
    agent_builder.with_lead_limit(widgets.get("lead_limit"))
    agent_builder.with_location_id(widgets.get("location_id"))
    agent_builder.with_system_prompt(SYSTEM)
    agent_builder.with_tools({
        "get_candidates" : get_candidates,
        "inspect_lead" : inspect_lead,
        "estimate_opportunity" : estimate_opportunity,
        "propose_task": propose_task
    })

    agent = LeadRecoveryAgent(agent_builder)

    agent.run()

init()