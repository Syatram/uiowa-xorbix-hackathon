from databricks.sdk import WorkspaceClient
from typing import Any, Callable
import json
import time

class LeadRecoveryAgentBuilder:
    def __init__(self, client : WorkspaceClient, model : str) -> LeadRecoveryAgentBuilder:
        self.client = client
        self.model = model
        self.max_steps = 12

    def with_system_prompt(self : LeadRecoveryAgentBuilder, system_prompt : str):
        self.system_prompt = system_prompt

    def with_tools(self : LeadRecoveryAgentBuilder, tools : dict[str, Callable[..., Any]]):
        self.tools = tools

    def with_location_id(self : LeadRecoveryAgentBuilder, location_id : str):
        self.location_id = location_id

    def with_lead_limit(self : LeadRecoveryAgentBuilder, lead_limit : int):
        self.lead_limit = lead_limit

    def with_max_steps(self : LeadRecoveryAgentBuilder, max_steps : int):
        self.max_steps = max_steps

class AgentRunError(RuntimeError):
    def __init__(self, message: str, events: list[dict[str, Any]]):
        super().__init__(message)
        self.events = events

class LeadRecoveryAgent:
    def __init__(self : LeadRecoveryAgent, builder : LeadRecoveryAgentBuilder) -> None:
       self.client = builder.client
       self.model = builder.model
       self.system_prompt = builder.system_prompt
       self.tools = builder.tools
       self.max_steps = builder.max_steps
       self.location_id = builder.location_id
       self.lead_limit = builder.lead_limit

    # I let the AI have this one
    # I can't be bothered to figure it all out
    def run(self : LeadRecoveryAgent):
        messages: list[dict[str, Any]] = [
        {"role": "system", "content": self.system_prompt},
        {"role": "user", "content": f"Investigate up to {self.lead_limit} leads at location {self.location_id}. Save justified simulated tasks, then summarize."},
        ]
        events: list[dict[str, Any]] = []
        failures = 0
        for _ in range(self.max_steps):
            try:
                response = self.client.chat.completions.create(model=self.model,
                                                                messages=messages, 
                                                                tools=self.tools, 
                                                                tool_choice="auto")
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
