

from datetime import datetime, timezone
import uuid
from src.core.event_envelope import EventEnvelope
from src.core.service_container import ServiceContainer


async def downloads_cleanup_dispatcher(ctx: ServiceContainer, max_delete: int):
    correlation_id = str(uuid.uuid4())
    event = EventEnvelope(type="commands.gateway.downloads-cleanup",
                          correlation_id=correlation_id,
                          timestamp=datetime.now(
                              timezone.utc).isoformat(),
                          payload={'max_delete': max_delete},
                          version=1)
    event_as_json = event.to_json()

    await ctx.safe_publish(
        routing_key='gateway_events', body=event_as_json, exchange_name=''
    )
