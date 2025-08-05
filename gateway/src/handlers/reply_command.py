

from typing import Optional
from hydrogram import Client
from src.core.service_container import ServiceContainer


async def reply_command_handler(ctx: ServiceContainer, telegram_app: Client, payload: object):
    chat_id: Optional[str] = payload.get(
        'chat_id', '')
    text: Optional[str] = payload.get(
        'text', '')
    reply_to_message_id: Optional[str] = payload.get(
        'reply_to_message_id', '')

    if not all([chat_id, text, reply_to_message_id]):
        ctx.logger.error(
            "Malformed payload. Aborting...",
            extra=payload)
        return

    await telegram_app.send_message(
        chat_id=chat_id,
        text=text,
        reply_to_message_id=reply_to_message_id
    )
