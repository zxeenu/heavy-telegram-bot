import asyncio
import logging
import json
from typing import Optional
from src.core.event_envelope import EventEnvelope
from src.core.logging_context import set_correlation_id
from src.core.rate_limiter import FixedWindowRateLimiter
from src.core.service_container import ServiceContainer
from src.handlers.dl_command import video_dl_command_handler, audio_dl_command_handler
from src.handlers.normalized_telegram_payload import NormalizedTelegramPayload
from datetime import datetime, timezone


def normalize_telegram_payload(payload: dict) -> NormalizedTelegramPayload:
    """
    Normalizes a Telegram message payload into a structured format.

    Args:
        payload (dict): The raw Telegram message payload.

    Returns:
        NormalizedTelegramPayload: A structured representation of the payload.
    """
    from_user = payload.get('from_user') or {}
    reply_to_message = payload.get('reply_to_message') or {}
    chat = payload.get('chat') or {}

    try:
        from_user_id = int(from_user.get('id')) if from_user.get(
            'id') is not None else None
    except (TypeError, ValueError) as e:
        from_user_id = None

    try:
        chat_id = int(chat.get('id')) if chat.get('id') is not None else None
    except (TypeError, ValueError) as e:
        chat_id = None

    text = str(payload.get('text') or '')
    parts = text.split()
    filtered_parts = list(filter(None, parts))

    return NormalizedTelegramPayload(
        message_id=payload.get('id', ''),
        chat_id=chat_id,
        text=text,
        filtered_parts=filtered_parts,
        from_user_id=from_user_id,
        from_user_name=str(from_user.get('username', '')),
        reply_to_message_id=payload.get('reply_to_message_id'),
        reply_text=str(reply_to_message.get('text', '')),
    )


TELEGRAM_COMMAND_TO_EVENT = {
    '.vdl': 'commands.media.video_download',
    '.adl': 'commands.media.audio_download',
}


async def main() -> None:
    ctx = await ServiceContainer.create(log_name="MediaPirate", log_level=logging.INFO)
    ctx.logger.info("MediaPirate Service started!")

    rate_limiter = FixedWindowRateLimiter(redis=ctx.redis)

    async def after_event_handling(user_id: str):
        meaningul_use_count = await rate_limiter.increment(user_id=user_id)
        ctx.logger.info("Rate limit incremented", extra={
            'from_user_id': user_id,
            'meaningul_use_count': meaningul_use_count
        })
        pass

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
                    match event_type:
                        case 'events.telegram.raw':
                            payload = body.get('payload', {})
                            data = normalize_telegram_payload(payload)

                            ctx.logger.info(
                                f"Event received successfully",
                                extra=data)
                            # pprint.pprint(payload, indent=2, width=60)
                            # ctx.logger.info(payload)
                            filtered_parts = data.get("filtered_parts", [])
                            command_word: Optional[str] = filtered_parts[0] if filtered_parts else None
                            ctx.logger.info("Command word located.", extra={
                                            "command_word": command_word})

                            if not command_word:
                                ctx.logger.warning(
                                    "Message does not contain any actionable keywords. Skipping.")
                                continue

                            event_to_dispatch = TELEGRAM_COMMAND_TO_EVENT.get(
                                command_word)
                            if not event_to_dispatch:
                                ctx.logger.error("Failed to process command. No configured mappings!", extra={
                                    "event_type": event_type,
                                })
                                continue

                            event = EventEnvelope(type=event_to_dispatch,
                                                  correlation_id=correlation_id,
                                                  timestamp=datetime.now(
                                                      timezone.utc).isoformat(),
                                                  payload=payload,
                                                  version=1)
                            event_as_json = event.to_json()
                            await ctx.safe_publish(
                                routing_key='telegram_events', body=event_as_json, exchange_name=''
                            )
                            ctx.logger.info("Telegram command mapped to a command handler", extra={
                                'event_type': event_to_dispatch
                            })
                            await after_event_handling(user_id=data['from_user_id'])

                        # Add more cases here as needed
                        case 'commands.media.video_download':
                            payload = body.get(
                                'payload', {})
                            data = normalize_telegram_payload(payload)
                            try:
                                await video_dl_command_handler(ctx=ctx, correlation_id=correlation_id, event_type=event_type, timestamp=timestamp, version=version, payload=data)
                            except Exception:
                                ctx.logger.exception(
                                    "Handler invocation failed")

                        case 'commands.media.audio_download':
                            payload = body.get(
                                'payload', {})
                            data = normalize_telegram_payload(payload)
                            try:
                                await audio_dl_command_handler(ctx=ctx, correlation_id=correlation_id, event_type=event_type, timestamp=timestamp, version=version, payload=data)
                            except Exception:
                                ctx.logger.exception(
                                    "Handler invocation failed")

                        case _:
                            ctx.logger.warning(
                                "Unknown event_type received.",
                                extra={"event_type": event_type}
                            )


if __name__ == "__main__":
    asyncio.run(main())
