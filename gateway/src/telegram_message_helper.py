

from hydrogram import Client
from src.core.logging_context import get_correlation_id
from src.core.service_container import ServiceContainer


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


async def optimistic_reply_cleanup(ctx: ServiceContainer, telegram_app: Client) -> None:

    correlation_id = get_correlation_id()
    composite_key = f"correlation_id:{correlation_id}:optimistic_reply"

    message_id_str, chat_id_str = await ctx.redis.hmget(composite_key, 'message_id', 'chat_id')

    message_id = safe_int(message_id_str)
    chat_id = safe_int(chat_id_str)

    if message_id is not None and chat_id is not None:
        await telegram_app.delete_messages(chat_id, message_id)
        await ctx.redis.hdel(composite_key, 'message_id', 'chat_id')

    ctx.logger.info("Handled optimistic reply cleanup", extra={
        'message_id': message_id_str,
        'chat_id': chat_id_str,
        'key': composite_key
    })

    return
