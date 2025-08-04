import hashlib
import os
from typing import Optional, TypedDict
import urllib
import aiofiles
import aiohttp
import humanize
from hydrogram import Client
from src.core.logging_context import get_correlation_id
from src.core.service_container import ServiceContainer
from time import time


class ElapsedTime(TypedDict):
    now_unix: float
    elapsed_seconds: float
    human_readable_elapsed_time: str


async def video_ready_event_handler(ctx: ServiceContainer, telegram_app: Client, payload: object):
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

    start_time_raw = await ctx.redis.hget(
        f"correlation_id:{correlation_id}", "start_time"
    )
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

    cached_file_id_raw = await ctx.redis.hget(f"video_content", object_name)
    cached_file_id = (
        cached_file_id_raw.decode()
        if isinstance(cached_file_id_raw, bytes)
        else cached_file_id_raw
    )

    ctx.logger.info("data from redis", extra={
        'cached_file_id_raw': cached_file_id_raw,
        'cached_file_id': cached_file_id
    })

    if cached_file_id:
        cashed_file_metrics = get_elapsed_time()

        ctx.logger.info("Cached video processed", extra={
            "start_time": f"{start_unix:.6f}",
            "now_time": f"{cashed_file_metrics['now_unix']:.6f}",
            "elapsed_seconds": round(cashed_file_metrics["elapsed_seconds"], 6),
            "elapsed_human": cashed_file_metrics["human_readable_elapsed_time"],
            'cached_file_id': cached_file_id,
        })

        final_caption = (
            f"**Download Complete**\n"
            f"Took: __{cashed_file_metrics['human_readable_elapsed_time']}__\n"
            f"ID: `{correlation_id}`"
        )

        await telegram_app.send_video(chat_id, video=cached_file_id, progress=progress, reply_to_message_id=message_id, caption=final_caption)
        return

    file_path = os.path.join("downloads", object_name)
    # Check if file already exists
    if not os.path.exists(file_path):
        async with aiohttp.ClientSession() as session:
            async with session.get(pre_signed_url) as resp:
                if resp.status != 200:
                    ctx.logger.error(
                        "Failed to download video.")
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

    msg = await telegram_app.send_video(chat_id, video=file_path, progress=progress, reply_to_message_id=message_id, caption=initial_caption)
    new_upload_file_metrics = get_elapsed_time()

    ctx.logger.info("Video processed", extra={
        "start_time": f"{start_unix:.6f}",
        "now_time": f"{new_upload_file_metrics['now_unix']:.6f}",
        "elapsed_seconds": round(new_upload_file_metrics['elapsed_seconds'], 6),
        "elapsed_human": new_upload_file_metrics['human_readable_elapsed_time'],
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

    file_id = getattr(msg.video, "file_id", None)

    if not file_id:
        ctx.logger.error("No video in Telegram message!")
        return

    await ctx.redis.hset(
        f"video_content", object_name, file_id)
    ctx.logger.info("File uploaded to telegram, and cached locally", extra={
        'file_id': file_id
    })
