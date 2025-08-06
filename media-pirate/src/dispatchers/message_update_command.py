

from datetime import datetime, timezone
from src.core.event_envelope import EventEnvelope
from src.core.logging_context import get_correlation_id
from src.core.service_container import ServiceContainer


async def message_update_command_dispatcher(ctx: ServiceContainer, chat_id: int | str, message_id: int, text: str):
    correlation_id = get_correlation_id()
    event = EventEnvelope(type="commands.gateway.message-update",
                          correlation_id=correlation_id,
                          timestamp=datetime.now(
                              timezone.utc).isoformat(),
                          payload={'chat_id': chat_id,
                                   'message_id': message_id,
                                   'text': text},
                          version=1)
    event_as_json = event.to_json()

    await ctx.safe_publish(
        routing_key='gateway_events', body=event_as_json, exchange_name=''
    )


def safe_int(value):
    if value is None:
        return None
    try:
        # Redis may return bytes, decode first
        if isinstance(value, bytes):
            value = value.decode()
        return int(value)
    except (ValueError, TypeError):
        return None


async def download_error_message_dispatcher(ctx: ServiceContainer):
    correlation_id = get_correlation_id()
    composite_key = f"correlation_id:{correlation_id}:optimistic_reply"

    message_id_str, chat_id_str = await ctx.redis.hmget(composite_key, 'message_id', 'chat_id')
    message_id = safe_int(message_id_str)
    chat_id = safe_int(chat_id_str)

    if message_id is None:
        ctx.logger.warning(
            "Missing message_id")
        return

    if chat_id is None:
        ctx.logger.warning(
            "Missing chat_id")
        return

    await message_update_command_dispatcher(ctx=ctx, chat_id=chat_id, message_id=message_id, text="💣 Unsupported source")
    return
