from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import uuid


@dataclass
class EventEnvelope:
    type: str
    correlation_id: str
    version: int
    timestamp: str
    payload: dict
    is_rate_limited: bool = False

    @staticmethod
    def create(
        type: str,
        payload: dict,
        version: int = 1,
        correlation_id: str = str(uuid.uuid4()),
        is_rate_limited: bool = False,
        timestamp: str = datetime.now(timezone.utc).isoformat()
    ) -> "EventEnvelope":
        return EventEnvelope(
            type=type,
            version=version,
            correlation_id=correlation_id,
            timestamp=timestamp,
            payload=payload,
            is_rate_limited=is_rate_limited,
        )

    @staticmethod
    def from_dict(d: dict) -> "EventEnvelope":
        return EventEnvelope(
            type=d.get("type", ""),
            version=int(d.get("version", 1)),
            correlation_id=d.get("correlation_id", ""),
            timestamp=d.get("timestamp", ""),
            payload=d.get("payload", {}),
            is_rate_limited=d.get("is_rate_limited", False),
        )

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)
