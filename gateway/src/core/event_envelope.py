from dataclasses import asdict, dataclass
import json

# event = EventEnvelope.from_dict(json.loads(message.body.decode()))


@dataclass
class EventEnvelope:
    type: str
    version: int
    correlation_id: str
    timestamp: str
    payload: dict
    is_rate_limited: bool = False

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
