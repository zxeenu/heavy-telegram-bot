

from typing import Optional
from hydrogram import Client
from src.core.service_container import ServiceContainer


async def update_message_command_handler(ctx: ServiceContainer, telegram_app: Client, payload: object):

    chat_id: Optional[str] = payload.get(
        'chat_id', '')
    text: Optional[str] = payload.get(
        'text', '')
    message_id: Optional[str] = payload.get(
        'message_id', '')

    if not all([chat_id, text, message_id]):
        ctx.logger.error(
            "Malformed payload. Aborting...",
            extra=payload)
        return

    await telegram_app.edit_message_caption(
        chat_id=chat_id,
        message_id=message_id,
        caption=text,
    )

    return
