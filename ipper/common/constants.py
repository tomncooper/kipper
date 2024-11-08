from enum import StrEnum

UNKNOWN_STR: str = "unknown"


class IPState(StrEnum):
    """Enum representing the status of an improvement proposal."""

    ACCEPTED: str = "accepted"
    UNDER_DISCUSSION: str = "under discussion"
    NOT_ACCEPTED: str = "not accepted"
    UNKNOWN: str = "unknown"
