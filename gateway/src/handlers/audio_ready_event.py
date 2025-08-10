import asyncio
from datetime import datetime, timezone
import hashlib
import os
import random
from typing import Optional, TypedDict
import urllib
import aiofiles
import aiohttp
import humanize
from hydrogram import Client
from src.core.event_envelope import EventEnvelope
from src.core.logging_context import get_correlation_id
from src.core.service_container import ServiceContainer
from time import time

from src.telegram_message_helper import optimistic_reply_cleanup

AUDIO_CACHE_TTL = 600  # 10 minutes
AUDIO_CACHE_INTEREST_ACC_TTL = 500


class ElapsedTime(TypedDict):
    now_unix: float
    elapsed_seconds: float
    human_readable_elapsed_time: str


async def audio_ready_event_handler(ctx: ServiceContainer, telegram_app: Client, payload: object):
    correlation_id = get_correlation_id()

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

    async def progress(current, total):
        ctx.logger.debug(f"Sending to user {current * 100 / total:.1f}%")

    # Generate SHA-256-based object name
    object_name = hashlib.sha256(
        base_url.encode()).hexdigest()

    # cached_file_id_raw = await ctx.redis.hget(f"audio_content", object_name)
    cached_file_id_raw = await ctx.redis.get(f"audio_content:{object_name}")
    cached_file_id = (
        cached_file_id_raw.decode()
        if isinstance(cached_file_id_raw, bytes)
        else cached_file_id_raw
    )

    ctx.logger.info("data from redis", extra={
        'cached_file_id_raw': cached_file_id_raw,
        'cached_file_id': cached_file_id
    })

    def get_interest_accumulator_key():
        return f"ongoing_audio_content:{object_name}"

    # Setting up interest accumulation
    # TODO: potentially we need a delayed queue to handle this
    interest_accumulation_status_was_set = await ctx.redis.set(get_interest_accumulator_key(), "ongoing", ex=AUDIO_CACHE_INTEREST_ACC_TTL, nx=True)
    if not interest_accumulation_status_was_set and not cached_file_id:
        # If multiple handlers retry at the same time after 2s, you may still get contention. Add a small random delay:
        await asyncio.sleep(2 + random.uniform(0, 1))
        event = EventEnvelope(type='events.dl.audio.ready',
                              correlation_id=correlation_id,
                              timestamp=datetime.now(
                                  timezone.utc).isoformat(),
                              payload=payload,
                              version=1)
        event_as_json = event.to_json()
        await ctx.safe_publish(
            routing_key='telegram_events', body=event_as_json, exchange_name=''
        )
        return 'delayed for 2 seconds'

    start_time_raw = await ctx.redis.hget(
        f"correlation_id:{correlation_id}", "start_time"
    )

    ctx.logger.info(
        "Acquired interest lock", extra={
            'object_name': object_name,
            'interest_accumulation_status': interest_accumulation_status_was_set,
            'interest_key': get_interest_accumulator_key(),
            'interest_value': "ongoing",  # The value you set
            'ttl': AUDIO_CACHE_INTEREST_ACC_TTL,
            'object_name': object_name,
            'gateway_dispatch_time': start_time_raw
        })

    start_unix = float(start_time_raw)  # works even if still bytes

    def get_elapsed_time() -> ElapsedTime:
        now_unix = time()
        elapsed_seconds = now_unix - start_unix
        human_readable = (
            f"{elapsed_seconds * 1000:.2f} ms"
            if elapsed_seconds < 1
            else humanize.precisedelta(elapsed_seconds, format="%.5f")
        )
        return ElapsedTime({
            "now_unix": now_unix,
            "elapsed_seconds": elapsed_seconds,
            "human_readable_elapsed_time": human_readable,
        })

    if cached_file_id:
        cashed_file_metrics = get_elapsed_time()

        ctx.logger.info("Cached audio processed", extra={
            "start_time": f"{start_unix:.6f}",
            "now_time": f"{cashed_file_metrics['now_unix']:.6f}",
            "elapsed_seconds": round(cashed_file_metrics["elapsed_seconds"], 6),
            "elapsed_human": cashed_file_metrics["human_readable_elapsed_time"],
            'cached_file_id': cached_file_id,
        })

        final_caption = (
            f"🚀 **Download Complete**\n"
            f"Took: __{cashed_file_metrics['human_readable_elapsed_time']}__\n"
            f"ID: `{correlation_id}`"
        )

        try:
            await telegram_app.send_audio(chat_id, audio=cached_file_id, progress=progress, reply_to_message_id=message_id, caption=final_caption)
            # let it expire or lets catch this
            # await ctx.redis.delete(f"audio_content:{object_name}")
            await ctx.redis.delete(get_interest_accumulator_key())
            await optimistic_reply_cleanup(ctx=ctx, telegram_app=telegram_app)
            payload["_cleaup_correlation_id_start_time"] = True
            return "handled audio via redis cache"
        except Exception as e:
            ctx.logger.warning(
                "Cached file_id failed, retrying download...",
                extra={'error': str(e)}
            )

    file_path = os.path.join("downloads", object_name)
    # Check if file already exists
    if not os.path.exists(file_path):
        async with aiohttp.ClientSession() as session:
            async with session.get(pre_signed_url) as resp:
                if resp.status != 200:
                    ctx.logger.error(
                        "Failed to download audio.")
                    return
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

    initial_caption = (
        f"🚀 **Downloading**\n"
        f"ID: `{correlation_id}`"
    )

    msg = await telegram_app.send_audio(chat_id, audio=file_path, progress=progress, reply_to_message_id=message_id, caption=initial_caption)
    new_upload_file_metrics = get_elapsed_time()
    size = os.path.getsize(file_path)

    ctx.logger.info("Audio processed", extra={
        "start_time": f"{start_unix:.6f}",
        "now_time": f"{new_upload_file_metrics['now_unix']:.6f}",
        "elapsed_seconds": round(new_upload_file_metrics['elapsed_seconds'], 6),
        "elapsed_human": new_upload_file_metrics['human_readable_elapsed_time'],
        "file_size": humanize.naturalsize(size)
    })

    final_caption = (
        f"🚀 **Download Complete**\n"
        f"Took: __{new_upload_file_metrics['human_readable_elapsed_time']}__\n"
        f"ID: `{correlation_id}`"
    )

    await telegram_app.edit_message_caption(
        chat_id,
        message_id=msg.id,
        caption=final_caption,
    )

    file_id = getattr(msg.audio, "file_id", None)

    if not file_id:
        ctx.logger.error("No audio in Telegram message!")
        return

    # await ctx.redis.hset(
    #     f"audio_content", object_name, file_id)
    await ctx.redis.set(f"audio_content:{object_name}", file_id, ex=AUDIO_CACHE_TTL)
    await ctx.redis.delete(get_interest_accumulator_key())
    await optimistic_reply_cleanup(ctx=ctx, telegram_app=telegram_app)
    ctx.logger.info("File uploaded to telegram, and cached locally", extra={
        'file_id': file_id
    })
    payload["_cleaup_correlation_id_start_time"] = True
    return 'normallly downloaded to disk and reuploaded to telegram'
