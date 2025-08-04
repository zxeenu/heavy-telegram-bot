import asyncio
import json
import logging
import os
from hydrogram import Client
from hydrogram.handlers import MessageHandler
from hydrogram.types import Message
from src.core.service_container import ServiceContainer
from time import time
import uuid
from datetime import datetime, timezone
from src.core.event_envelope import EventEnvelope
from src.core.logging_context import set_correlation_id
from src.dispatchers.disk_cleanup_command import downloads_cleanup_dispatcher
from src.handlers.audio_ready_event import audio_ready_event_handler
from src.handlers.download_cleanup_command import download_cleanup_command_handler
from src.handlers.video_ready_event import video_ready_event_handler


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


def make_event_bus_handler(ctx: ServiceContainer):
    async def event_bus_handler(client: Client, message: Message):

        # Extract basic info
        user = getattr(message.from_user, 'username', None) or getattr(
            message.from_user, 'id', 'UnknownUser')
        user_id = getattr(message.from_user, 'id', 'UnknownUser')
        chat_id = getattr(message.chat, 'id', 'UnknownChat')
        chat_type = getattr(message.chat, 'type', 'UnknownType')
        message_id = getattr(message, 'message_id', 'UnknownID')
        message_time = getattr(message, 'date', None)
        message_time_str = message_time.isoformat() if message_time else 'UnknownTime'

        admin_user_id = int(os.environ["TELEGRAM_ADMIN_USER_ID"])

        # allowed_user_ids = set([admin_user_id])
        authorized: bool = False

        if admin_user_id == user_id:
            authorized = True

        correlation_id = str(uuid.uuid4())

        set_correlation_id(correlation_id)

        if authorized:
            try:
                message_dict = to_serializable(obj=message)
                # ctx.logger.info(f"Message serialized successfully: {message_dict}")
            except Exception as e:
                ctx.logger.error(f"Failed to serialize message: {e}")
                return

            await ctx.redis.hset(
                f"correlation_id:{correlation_id}", 'start_time', time())

            event = EventEnvelope(type="events.telegram.raw",
                                  correlation_id=correlation_id,
                                  timestamp=datetime.now(
                                      timezone.utc).isoformat(),
                                  payload=message_dict,
                                  version=1)
            event_as_json = event.to_json()

            await ctx.safe_publish(
                routing_key='telegram_events', body=event_as_json, exchange_name=''
            )

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

        if authorized:
            ctx.logger.info("Message sent to event bus!",
                            extra=filtered_extras)
        else:
            ctx.logger.warning("Message skipped", extra=filtered_extras)
    return event_bus_handler


# Background task example
async def background_task(telegram_app: Client, ctx: ServiceContainer):

    async def after_event_handling():
        key = "cleanup_event_counter"
        count = await ctx.redis.incr(key)
        # Set TTL so it doesn't live forever (e.g., 1 day)
        await ctx.redis.expire(key, 86400)
        if count >= 100:
            await ctx.redis.delete(key)
            await downloads_cleanup_dispatcher(ctx=ctx, max_delete=100)
        pass

    async def cleanup_redis(correlation_id_str: str):
        await ctx.redis.hdel(
            f"correlation_id:{correlation_id_str}", 'start_time')
        pass

    async with ctx.connection as connection:
        channel = await connection.channel()

        queue = await channel.declare_queue(
            name='gateway_events',
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
                            "Event does not have a version. Malformed event payload.")
                        continue

                    match event_type:
                        case 'events.dl.video.ready':
                            await video_ready_event_handler(
                                ctx=ctx, telegram_app=telegram_app, payload=body.get('payload', {}))
                        case 'events.dl.audio.ready':
                            await audio_ready_event_handler(
                                ctx=ctx, telegram_app=telegram_app, payload=body.get('payload', {}))
                        case 'commands.gateway.downloads-cleanup':
                            await download_cleanup_command_handler(ctx=ctx, payload=body.get('payload', {}))

                        # Add more cases here as needed
                        case _:
                            ctx.logger.warning(
                                "Unknown event_type received.", extra={
                                    event_type: event_type
                                })
                            pass

                    await cleanup_redis(correlation_id)
                    await after_event_handling()


# Main entry point — directly manages the context lifecycle
async def main() -> None:
    ctx = await ServiceContainer.create(log_name="Gateway", log_level=logging.INFO)

    try:
        ctx.logger.info("Gateway Service started!")
        telegram_app = Client(
            name="account_session",
            api_id=os.environ["TELEGRAM_ID"],
            api_hash=os.environ["TELEGRAM_HASH"]
        )

        await ctx.channel.declare_queue(name='telegram_events', durable=False)
        telegram_app.add_handler(MessageHandler(make_event_bus_handler(ctx)))
        await telegram_app.start()

        await asyncio.gather(
            background_task(telegram_app=telegram_app, ctx=ctx),
            asyncio.Event().wait()
        )
    finally:
        await telegram_app.stop()
        await ctx.close()


if __name__ == '__main__':
    asyncio.run(main())
