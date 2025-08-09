import asyncio
import logging
import json
import os
from typing import Optional
import yt_dlp
from src.core.event_envelope import EventEnvelope
from src.core.event_router import EventRouter
from src.core.logging_context import get_correlation_id, set_correlation_id
from src.core.rate_limiter import FixedWindowRateLimiter
from src.core.service_container import ServiceContainer
from src.dispatchers.message_update_command import download_error_message_dispatcher
from src.handlers.dl_command import video_dl_command_handler, audio_dl_command_handler
from src.handlers.normalized_telegram_payload import NormalizedTelegramPayload


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


async def bootstrap(ctx: ServiceContainer) -> None:
    ctx.logger.info("Booting up MediaPirate!")
    bucket_name = os.environ.get("S3_BUCKET_NAME")

    if not bucket_name:
        raise Exception("Bucket name not set!")

    minio_client = ctx.minio

    bucket_exists = await asyncio.to_thread(minio_client.bucket_exists, bucket_name)
    if not bucket_exists:
        await asyncio.to_thread(minio_client.make_bucket, bucket_name)
        ctx.logger.info(
            f"Bucket '{bucket_name}' created (private by default).")
    else:
        ctx.logger.info(f"Bucket '{bucket_name}' already exists.")


router = EventRouter()


@router.route(event_type="events.telegram.raw",
              version=1,
              options={
                  'middleware_before': ["correlation_guard_prepare"],
                  'middleware_after': ["correlation_guard_validate", 'maybe_rate_limit_increment'],
              })
async def handle_raw_telegram_events(
        envelope: EventEnvelope,
        ctx: ServiceContainer,
        rate_limiter: FixedWindowRateLimiter):
    correlation_id = get_correlation_id()

    data = normalize_telegram_payload(envelope.payload)
    ctx.logger.info(
        f"Event received successfully",
        extra=data)

    filtered_parts = data.get("filtered_parts", [])
    command_word: Optional[str] = filtered_parts[0] if filtered_parts else None
    ctx.logger.info("Command word located.", extra={
                    "command_word": command_word})

    if not command_word:
        ctx.logger.warning(
            "Message does not contain any actionable keywords. Skipping.")
        return

    event_to_dispatch = TELEGRAM_COMMAND_TO_EVENT.get(
        command_word)
    if not event_to_dispatch:
        ctx.logger.error("Failed to process command. No configured mappings!", extra={
            "event_type": "events.telegram.raw",
        })
        return

    # Check if rate limited
    is_not_rate_limited = await rate_limiter.is_allowed(data["from_user_id"])
    is_rate_limited = not is_not_rate_limited
    # is_rate_limited = True # for testing

    if is_rate_limited:
        rate_limit_payload = {
            'chat_id': data["chat_id"],
            'text': "⏳ Too many requests. Please try again shortly.",
            'reply_to_message_id': data['message_id']
        }
        rate_limit_response_event = EventEnvelope.create(type='commands.gateway.reply',
                                                         correlation_id=correlation_id,
                                                         payload=rate_limit_payload,
                                                         is_rate_limited=is_rate_limited)
        await ctx.safe_publish(
            routing_key='gateway_events', body=rate_limit_response_event.to_json(), exchange_name=''
        )
        ctx.logger.info("Request will not be handled. Received from rate limited user", extra={
            'event_type': event_to_dispatch,
            'payload': rate_limit_payload,
        })
        return

    # TODO: if events are too old, do not process them

    success_event = EventEnvelope.create(type=event_to_dispatch,
                                         correlation_id=correlation_id,
                                         payload=envelope.payload)
    await ctx.safe_publish(
        routing_key='telegram_events', body=success_event.to_json(), exchange_name=''
    )
    ctx.logger.info("Telegram command mapped to a command handler", extra={
        'event_type': event_to_dispatch
    })
    envelope.payload["_increase_rate_limit"] = True


@router.route(event_type="commands.media.video_download",
              version=1,
              options={
                  'retry_attempt': 1,
                  'middleware_before': ["correlation_guard_prepare"],
                  'middleware_after': ["correlation_guard_validate"],
              })
async def handle_video_download_command(envelope: EventEnvelope,
                                        ctx: ServiceContainer):
    correlation_id = get_correlation_id()
    data = normalize_telegram_payload(envelope.payload)
    try:
        await video_dl_command_handler(ctx=ctx, correlation_id=correlation_id, event_type=envelope.type, timestamp=envelope.timestamp, version=envelope.version, payload=data)
    except yt_dlp.utils.DownloadError as e:
        await download_error_message_dispatcher(ctx=ctx)
        ctx.logger.warning(f"DownloadError: {e}")
    except Exception:
        # TODO: send off to QuarterMaster
        ctx.logger.exception(
            "Handler invocation failed")


@router.route(event_type="commands.media.audio_download",
              version=1,
              options={
                  'retry_attempt': 1,
                  'middleware_before': ["correlation_guard_prepare"],
                  'middleware_after': ["correlation_guard_validate"],
              })
async def handle_audio_download_command(envelope: EventEnvelope,
                                        ctx: ServiceContainer):
    correlation_id = get_correlation_id()
    data = normalize_telegram_payload(envelope.payload)
    try:
        await audio_dl_command_handler(ctx=ctx, correlation_id=correlation_id, event_type=envelope.type, timestamp=envelope.timestamp, version=envelope.version, payload=data)
    except yt_dlp.utils.DownloadError as e:
        await download_error_message_dispatcher(ctx=ctx)
        ctx.logger.warning(f"DownloadError: {e}")
    except Exception:
        # TODO: send off to QuarterMaster
        ctx.logger.exception(
            "Handler invocation failed")


@router.register_middleware(name='maybe_rate_limit_increment')
async def handle_rate_limit_usage_increment(
        envelope: EventEnvelope,
        ctx: ServiceContainer,
        rate_limiter: FixedWindowRateLimiter):

    control_flag = envelope.payload.get("_increase_rate_limit")
    if not control_flag:
        return True

    correlation_id = get_correlation_id()

    data = normalize_telegram_payload(envelope.payload)

    user_id = data['from_user_id']
    meaningul_use_count = await rate_limiter.increment(user_id=user_id)
    ctx.logger.info("Rate limit incremented", extra={
        'from_user_id': user_id,
        'meaningul_use_count': meaningul_use_count
    })

    # lets be optimistic and let the user know we are doing __something__
    payload = {
        'chat_id': data["chat_id"],
        'text': "🫡 Let me process that for you.",
        'reply_to_message_id': data['message_id'],
        'persistence_key': "optimistic_reply"
    }

    event_envelope = EventEnvelope.create(type='commands.gateway.reply',
                                          correlation_id=correlation_id,
                                          payload=payload,
                                          is_rate_limited=False)
    event_as_json = event_envelope.to_json()
    await ctx.safe_publish(
        routing_key='gateway_events', body=event_as_json, exchange_name=''
    )
    ctx.logger.info("Letting the user know that we have begun processing his request", extra={
        'payload': payload,
    })
    return True


@router.register_before_middleware(name="correlation_guard_prepare")
async def correlation_guard(envelope: EventEnvelope):
    envelope.payload["_correlation_snapshot"] = get_correlation_id()
    return True


@router.register_after_middleware(name="correlation_guard_validate")
async def correlation_guard_validate(envelope: EventEnvelope):
    expected = envelope.payload.get("_correlation_snapshot")
    actual = get_correlation_id()
    if expected != actual:
        raise RuntimeError("Context corruption detected")
    return True


@router.register_before_middleware(name='logger')
async def log_event_start(envelope: EventEnvelope, ctx: ServiceContainer):
    ctx.logger.info(f"⏱️ Handling {envelope.type}")
    return True


# TODO: need to implement a retry mechanism


async def main() -> None:

    ctx = await ServiceContainer.create(log_name="MediaPirate", log_level=logging.INFO)

    await bootstrap(ctx=ctx)
    rate_limiter = FixedWindowRateLimiter(redis=ctx.redis)
    ctx.logger.info("MediaPirate Service started!")

    # override logger
    router.set_logger(ctx.logger)

    # register dependencies
    router.register(ctx)
    router.register(rate_limiter)

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

                    correlation_id: str = body.get('correlation_id', '')

                    if not correlation_id:
                        ctx.logger.error(
                            "Fatal: Missing correlation_id in event payload")
                        raise ValueError(
                            "correlation_id is required for all events")

                    set_correlation_id(correlation_id)
                    envelope = EventEnvelope.from_dict(body)

                    route = router.get_route(envelope=envelope)

                    if not route:
                        ctx.logger.warning(
                            "Unknown event_type received.",
                            extra={"event_type": envelope.type}
                        )

                    result_set = await router.dispatch(
                        envelope=envelope
                    )
                    ctx.logger.info(result_set)
                    continue


if __name__ == "__main__":
    asyncio.run(main())
