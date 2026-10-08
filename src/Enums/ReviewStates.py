from enum import Enum

class ReviewStates(Enum):
    APPROVED = "approved"
    REJECTED = "rejected"

APPROVED = ReviewStates.APPROVED
REJECTED = ReviewStates.REJECTED