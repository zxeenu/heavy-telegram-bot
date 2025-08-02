import hashlib
import os
from typing import Optional
import urllib
import aiofiles
import aiohttp
from hydrogram import Client
from src.core.service_container import ServiceContainer


async def video_ready(ctx: ServiceContainer, telegram_app: Client, payload: object):
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

    ctx.logger.info(payload)
    # await telegram_app.send_message("me", "this is a reply", reply_to_message_id=message_id)
    # Keep track of the progress while uploading

    # TODO: keep track of documents uploaded to telegram, so we can reuse them

    async def progress(current, total):
        ctx.logger.info(f"{current * 100 / total:.1f}%")
    await telegram_app.send_video(chat_id, file_path, progress=progress, reply_to_message_id=message_id, caption='Downloaded...')
