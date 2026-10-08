from enum import Enum

class Decisions(Enum):
    FOLLOW_UP = "follow_up"
    STAFF_REVIEW = "staff_review"
    SKIP = "skip"

FOLLOW_UP = Decisions.FOLLOW_UP
STAFF_REVIEW = Decisions.STAFF_REVIEW
SKIP = Decisions.SKIP