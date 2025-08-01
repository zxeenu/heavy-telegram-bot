import asyncio
import hashlib
import json
import logging
import os
from typing import Optional
from hydrogram import Client
from hydrogram.handlers import MessageHandler
from hydrogram.types import Message
import urllib
from src.core.service_container import ServiceContainer
import uuid
from datetime import datetime, timezone
from src.core.event_envelope import EventEnvelope
from src.core.logging_context import set_correlation_id
import aiohttp
import aiofiles


# global singleton
ctx = ServiceContainer(log_name="Gateway", log_level=logging.INFO)


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


async def event_bus_handler(client: Client, message: Message):

    correlation_id = str(uuid.uuid4())
    set_correlation_id(correlation_id)

    try:
        message_dict = to_serializable(obj=message)
        ctx.logger.info(f"Message serialized successfully: {message_dict}")
    except Exception as e:
        ctx.logger.error(f"Failed to serialize message: {e}")
        return
    
    event = EventEnvelope(type="events.telegram.raw",
                          correlation_id=correlation_id,
                          timestamp=datetime.now(timezone.utc).isoformat(),
                          payload=message_dict,
                          version=1)
    event_as_json = event.to_json()

    await ctx.safe_publish(
        routing_key='telegram_events', body=event_as_json, exchange_name=''
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

    # Collect structured log context
    extras = {
        "user": user,
        "user_id": user_id,
        "chat_id": chat_id,
        "message_id": message_id,
        "message_time_str": message_time_str,
        "chat_type": chat_type,
        "message_type": message_type,
        "text_preview": text_preview,
        "location_info": location_info,
        "sticker_info": sticker_info,
        "doc_info": doc_info,
        "photo_info": photo_info,
        "reply_to": reply_to
    }

    # Optionally filter out None values for a cleaner log
    filtered_extras = {k: v for k, v in extras.items() if v is not None}
    ctx.logger.info("Message sent to event bus!", extra=filtered_extras)


# Background task example
async def background_task(telegram_app: Client):

    async with ctx.connection as connection:
        channel = await connection.channel()

        queue = await channel.declare_queue(
            name='telegram_events',
            auto_delete=False
        )

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    body_str = message.body.decode()

                    try:
                        body = json.loads(body_str)
                    except json.JSONDecodeError:
                        ctx.logger.error(
                            f"Invalid JSON in message")
                        # Do something with the malformed JSON's later
                        continue
                    except Exception as e:
                        ctx.logger.error(
                            f"Error processing: {e}")
                        continue

                    event_type: str = body.get('type', '')
                    version = int(body.get('version')) if body.get(
                        'version') is not None else None
                    correlation_id: str = body.get('correlation_id', '')
                    timestamp: str = body.get('timestamp', '')

                    set_correlation_id(correlation_id)

                    if version is None:
                        ctx.logger.info(
                            f"Event does not have a version. Malformed event payload.")
                        continue

                    # Proper formatting with placeholders

                    if event_type == 'events.dl.video.ready':
                        payload = body.get('payload', {})
                        ctx.logger.info(
                            f"Event received successfully",
                            extra={
                                'data': payload
                            })

                        pre_signed_url: Optional[str] = payload.get(
                            'presigned_url', '')
                        message_id: Optional[str] = payload.get(
                            'message_id', '')
                        chat_id: Optional[str] = payload.get(
                            'chat_id', '')

                        if not all([pre_signed_url, message_id, chat_id]):
                            ctx.logger.error(
                                "Malformed payload. Aborting...")
                            return

                        parsed = urllib.parse.urlparse(pre_signed_url)
                        base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

                        # Generate SHA-256-based object name
                        object_name = hashlib.sha256(
                            base_url.encode()).hexdigest()

                        # Define the file path (same directory or use a specific folder)
                        # you can use "." if not using a subfolder
                        file_path = os.path.join("downloads", object_name)

                        # Check if file already exists
                        if not os.path.exists(file_path):
                            async with aiohttp.ClientSession() as session:
                                async with session.get(pre_signed_url) as resp:
                                    if resp.status != 200:
                                        ctx.logger.error(
                                            "Failed to download video.")
                                        continue
                                    else:
                                        # Make sure the folder exists
                                        os.makedirs(os.path.dirname(
                                            file_path), exist_ok=True)

                                        # Write to file
                                        async with aiofiles.open(file_path, "wb") as f:
                                            await f.write(await resp.read())
                                        ctx.logger.info(
                                            "File written to downloads directory.")
                        else:
                            ctx.logger.info(
                                f"File already exists: {file_path}")

                        ctx.logger.info(payload)
                        # await telegram_app.send_message("me", "this is a reply", reply_to_message_id=message_id)
                        # Keep track of the progress while uploading

                        # TODO: keep track of documents uploaded to telegram, so we can reuse them

                        async def progress(current, total):
                            ctx.logger.info(f"{current * 100 / total:.1f}%")
                        await telegram_app.send_video(chat_id, file_path, progress=progress, reply_to_message_id=message_id, caption='Downloaded...')


# Main entry point — directly manages the context lifecycle
async def main() -> None:
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
            background_task(telegram_app),
            asyncio.Event().wait()
        )
    finally:
        await telegram_app.stop()
        await ctx.close()


if __name__ == '__main__':
    asyncio.run(main())
