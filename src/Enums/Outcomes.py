from enum import Enum

class Outcomes:
    NOT_CONTACTED = "not_contacted"
    CONTACTED_NO_RESPONSE = "contacted_no_response"
    APPOINTMENT_REQUESTED = "appointment_requested"
    CONVERTED_SIMULATED = "converted_simulated"

NOT_CONTACTED = Outcomes.NOT_CONTACTED
CONTACTED_NO_RESPONSE = Outcomes.CONTACTED_NO_RESPONSE
APPOINTMENT_REQUESTED = Outcomes.APPOINTMENT_REQUESTED
CONVERTED_SIMULATED = Outcomes.CONVERTED_SIMULATED