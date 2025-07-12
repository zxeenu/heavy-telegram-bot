import asyncio
import os
import json
from hydrogram import Client
from hydrogram.handlers import MessageHandler
from hydrogram.types import Message
from src.app_context import AsyncAppContext
import uuid
from datetime import datetime, timezone

# GLOBALS
ctx = AsyncAppContext()


# Decorator: inject shared AppContext into handlers
def with_app_context(func):

    async def wrapper(client, message):
        if ctx.connection is None or ctx.connection.is_closed:
            await ctx.connect()
        try:
            await func(ctx, client, message)
        except Exception:
            ctx.logger.exception("Error in handler")
            raise
    return wrapper


# Convert message to serializable JSON
def to_serializable(obj):
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    elif isinstance(obj, list):
        return [to_serializable(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: to_serializable(v) for k, v in obj.items()}
    elif hasattr(obj, "__dict__"):
        return {k: to_serializable(v) for k, v in vars(obj).items() if not k.startswith("_")}
    elif hasattr(obj, "_asdict"):
        return to_serializable(obj._asdict())
    elif hasattr(obj, "isoformat"):
        return obj.isoformat()
    else:
        return str(obj)


# Handler with context injection
@with_app_context
async def event_bus_handler(ctx: AsyncAppContext, client: Client, message: Message):
    message_dict = to_serializable(obj=message)

    event = {
        "type": "telegram.message",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": message_dict,
    }
    json_str = json.dumps(event, indent=2)
    await ctx.safe_publish(
        routing_key='telegram_events', body=json_str, exchange_name=''
    )
    
    # Extract basic info
    user = getattr(message.from_user, 'username', None) or getattr(message.from_user, 'id', 'UnknownUser')
    text = getattr(message, 'text', None)
    chat_id = getattr(message.chat, 'id', 'UnknownChat')
    chat_type = getattr(message.chat, 'type', 'UnknownType')
    group_id = chat_id if chat_type in ('group', 'supergroup') else None
    group_name = getattr(message.chat, 'title', None) if chat_type in ('group', 'supergroup') else None

    # Text preview
    if text is None:
        text_preview = "<no text>"
    else:
        text_preview = text[:50] + ("..." if len(text) > 50 else "")

    # Check if reply
    reply_to = None
    if getattr(message, 'reply_to_message', None):
        replied = message.reply_to_message
        reply_user = getattr(replied.from_user, 'username', None) or getattr(replied.from_user, 'id', 'UnknownUser')
        reply_text = getattr(replied, 'text', '')
        reply_preview = reply_text[:30] + ("..." if len(reply_text) > 30 else "")
        reply_to = f"Reply to {reply_user}: \"{reply_preview}\""

    # Check for photos
    photo_info = None
    photos = getattr(message, 'photo', None)
    if photos:
        photo_count = len(photos)
        largest_photo = photos[-1]
        photo_info = f"Photo(s): {photo_count}, largest size: {largest_photo.width}x{largest_photo.height}"

    # Check for document
    doc_info = None
    document = getattr(message, 'document', None)
    if document:
        doc_name = getattr(document, 'file_name', 'unknown')
        doc_mime = getattr(document, 'mime_type', 'unknown')
        doc_info = f"Document: {doc_name} ({doc_mime})"

    # Check for sticker
    sticker_info = None
    sticker = getattr(message, 'sticker', None)
    if sticker:
        emoji = getattr(sticker, 'emoji', None)
        set_name = getattr(sticker, 'set_name', None)
        sticker_info = f"Sticker: {emoji or ''} from set {set_name or 'unknown'}"

    # Build log parts
    log_parts = [
        f"User: {user}",
        f"Text: {text_preview}",
        f"Chat ID: {chat_id}",
        f"Group ID: {group_id or 'N/A'}",
        f"Group Name: {group_name or 'N/A'}"
    ]

    if reply_to:
        log_parts.append(reply_to)
    if photo_info:
        log_parts.append(photo_info)
    if doc_info:
        log_parts.append(doc_info)
    if sticker_info:
        log_parts.append(sticker_info)

    log_msg = " | ".join(log_parts)
    ctx.logger.info(log_msg)


# Background task example
async def background_task(ctx: AsyncAppContext):
    while True:
        # ctx.logger.info("Doing other stuff...") # comment out later when doing stuff to propagate messages out
        await asyncio.sleep(5)


# Main entry point — directly manages the context lifecycle
async def main():
    await ctx.connect()

    try:
        ctx.logger.info("Service started")
        telegram_app = Client(
            "account_session",
            api_id=os.environ["TELEGRAM_ID"],
            api_hash=os.environ["TELEGRAM_HASH"]
        )

        await ctx.channel.declare_queue(name='telegram_events', durable=False)
        telegram_app.add_handler(MessageHandler(event_bus_handler))
        await telegram_app.start()

        await asyncio.gather(
            background_task(ctx=ctx),
            asyncio.Event().wait()
        )
    finally:
        await telegram_app.stop()
        await ctx.close()


if __name__ == '__main__':
    asyncio.run(main())
