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


@with_app_context
async def event_bus_handler(ctx: AsyncAppContext, client: Client, message: Message):
    message_dict = to_serializable(obj=message)

    event = {
        "type": "events.telegram.raw",
        "correlation_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": message_dict,
    }
    json_str = json.dumps(event, indent=2)
    await ctx.safe_publish(
        routing_key='telegram_events', body=json_str, exchange_name=''
    )

    # Extract basic info
    user = getattr(message.from_user, 'username', None) or getattr(
        message.from_user, 'id', 'UnknownUser')
    user_id = getattr(message.from_user, 'id', 'UnknownUser')
    chat_id = getattr(message.chat, 'id', 'UnknownChat')
    chat_type = getattr(message.chat, 'type', 'UnknownType')
    message_id = getattr(message, 'message_id', 'UnknownID')
    message_time = getattr(message, 'date', None)
    message_time_str = message_time.isoformat() if message_time else 'UnknownTime'

    # Determine message type
    if getattr(message, 'sticker', None):
        message_type = 'sticker'
    elif getattr(message, 'photo', None):
        message_type = 'photo'
    elif getattr(message, 'document', None):
        message_type = 'document'
    elif getattr(message, 'video', None):
        message_type = 'video'
    elif getattr(message, 'audio', None):
        message_type = 'audio'
    elif getattr(message, 'voice', None):
        message_type = 'voice'
    elif getattr(message, 'location', None):
        message_type = 'location'
    else:
        message_type = 'text'

    # Text or caption preview
    text = getattr(message, 'text', None)
    caption = getattr(message, 'caption', None)
    text_or_caption = text or caption
    text_preview = text_or_caption[:50] + ("..." if len(
        text_or_caption) > 50 else "") if text_or_caption else "<no text>"

    # Check if reply
    reply_to = None
    if getattr(message, 'reply_to_message', None):
        replied = message.reply_to_message
        reply_user = getattr(replied.from_user, 'username', None) or getattr(
            replied.from_user, 'id', 'UnknownUser')
        reply_text = getattr(replied, 'text', None) or getattr(
            replied, 'caption', None)

        if reply_text:
            reply_preview = reply_text[:30] + \
                ("..." if len(reply_text) > 30 else "")
        else:
            reply_preview = "<no text>"

        reply_to = f"Reply to {reply_user}: \"{reply_preview}\""

    # Photo info
    photo_info = None
    photos = getattr(message, 'photo', None)
    if photos:
        if isinstance(photos, list):
            photo_count = len(photos)
            largest_photo = photos[-1]
        else:
            photo_count = 1
            largest_photo = photos
        width = getattr(largest_photo, 'width', '?')
        height = getattr(largest_photo, 'height', '?')
        photo_info = f"Photo(s): {photo_count}, largest size: {width}x{height}"

    # Document info
    doc_info = None
    document = getattr(message, 'document', None)
    if document:
        doc_name = getattr(document, 'file_name', 'unknown')
        doc_mime = getattr(document, 'mime_type', 'unknown')
        doc_info = f"Document: {doc_name} ({doc_mime})"

    # Sticker info
    sticker_info = None
    sticker = getattr(message, 'sticker', None)
    if sticker:
        emoji = getattr(sticker, 'emoji', None)
        set_name = getattr(sticker, 'set_name', None)
        sticker_info = f"Sticker: {emoji or ''} from set {set_name or 'unknown'}"

    # Location info
    location_info = None
    location = getattr(message, 'location', None)
    if location:
        lat = getattr(location, 'latitude', '?')
        lon = getattr(location, 'longitude', '?')
        location_info = f"Location: {lat}, {lon}"

    # Command info
    command_info = None
    if text and text.startswith('/'):
        command_info = f"Command: {text.split()[0]}"

    # Build log parts
    log_parts = [
        f"User: {user}",
        f"User ID: {user_id}",
        f"Message ID: {message_id}",
        f"Message Time: {message_time_str}",
        f"Chat Type: {chat_type}",
        f"Chat ID: {chat_id}",
        f"Message Type: {message_type}",
        f"Text: {text_preview}",
    ]

    if reply_to:
        log_parts.append(reply_to)
    if command_info:
        log_parts.append(command_info)
    if photo_info:
        log_parts.append(photo_info)
    if doc_info:
        log_parts.append(doc_info)
    if sticker_info:
        log_parts.append(sticker_info)
    if location_info:
        log_parts.append(location_info)

    # Final log
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
        ctx.logger.info("Gateway Service started")
        telegram_app = Client(
            name="account_session",
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
