import hashlib
import os
from typing import Optional
import urllib
import aiofiles
import aiohttp
import humanize
from hydrogram import Client
from src.core.logging_context import get_correlation_id
from src.core.service_container import ServiceContainer
from time import time


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

    # await telegram_app.send_message("me", "this is a reply", reply_to_message_id=message_id)
    # Keep track of the progress while uploading

    # TODO: keep track of documents uploaded to telegram, so we can reuse them

    message_for_response = "Downloaded"
    start_time_raw = await ctx.redis.hget(
        f"correlation_id:{correlation_id}", "start_time")

    if start_time_raw:
        start_unix = int(start_time_raw)  # works even if still bytes
        now_unix = int(time())
        elapsed_seconds = now_unix - start_unix

        human_readable = humanize.precisedelta(elapsed_seconds, format="%0.3f")
        message_for_response = f"Downloaded in {human_readable}"

        ctx.logger.info("Audio processed", extra={
            "start_time": start_unix,
            "now_time": now_unix,
            "elapsed": human_readable,
        })
    else:
        ctx.logger.warning("start_time not found")

    async def progress(current, total):
        ctx.logger.debug(f"{current * 100 / total:.1f}%")

    await telegram_app.send_audio(chat_id, file_path, progress=progress, reply_to_message_id=message_id, caption=message_for_response)
