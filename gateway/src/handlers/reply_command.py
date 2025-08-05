

from typing import Optional
from hydrogram import Client
from src.core.logging_context import get_correlation_id
from src.core.service_container import ServiceContainer


async def reply_command_handler(ctx: ServiceContainer, telegram_app: Client, payload: object):
    correlation_id = get_correlation_id()

    chat_id: Optional[str] = payload.get(
        'chat_id', '')
    text: Optional[str] = payload.get(
        'text', '')
    reply_to_message_id: Optional[str] = payload.get(
        'reply_to_message_id', '')
    persistence_key: Optional[str] = payload.get(
        'persistence_key', '')

    if not all([chat_id, text, reply_to_message_id]):
        ctx.logger.error(
            "Malformed payload. Aborting...",
            extra=payload)
        return

    msg = await telegram_app.send_message(
        chat_id=chat_id,
        text=text,
        reply_to_message_id=reply_to_message_id
    )

    if persistence_key:
        composite_key = f"correlation_id:{correlation_id}:{persistence_key}"
        await ctx.redis.hset(
            composite_key, mapping={
                'message_id': msg.id,
                'chat_id': msg.chat.id
            })
        # expires in 10 minutes (600 seconds)
        # await ctx.redis.expire(composite_key, 600)

    return
