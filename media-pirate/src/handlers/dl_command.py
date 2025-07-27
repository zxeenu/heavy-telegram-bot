import datetime
import hashlib
import os
from typing import Literal, Optional
from src.core.event_envelope import EventEnvelope
from src.core.service_container import ServiceContainer
from src.handlers.normalized_telegram_payload import NormalizedTelegramPayload
from src.yt_dlp_client import download_tiktok_video
from minio.error import S3Error


def extract_url(*candidates: Optional[str]) -> Optional[str]:
    for c in candidates:
        if c and c.strip().startswith(("http://", "https://")):
            return c.strip()
    return None


async def publish_presigned_event(ctx, correlation_id, presigned_url, payload: NormalizedTelegramPayload,):
    event = EventEnvelope(
        type="events.dl.video.ready",
        correlation_id=correlation_id,
        timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        payload={"presigned_url": presigned_url,
                 'message_id': payload["message_id"],
                 'chat_id': payload["chat_id"]
                 },
        version=1
    )
    await ctx.safe_publish(
        routing_key='telegram_events',
        body=event.to_json(),
        exchange_name=''
    )


async def dl_command(ctx: ServiceContainer, correlation_id: str, event_type: str, timestamp: str, version: int, payload: NormalizedTelegramPayload) -> None:
    minio = ctx.minio
    bucket_name = os.environ.get("S3_BUCKET_NAME")

    filtered_parts = payload["filtered_parts"]
    url_from_text: Optional[str] = filtered_parts[1] if len(
        filtered_parts) > 1 else None
    url_from_reply = payload["reply_text"]

    ctx.logger.info(
        f"Requesting a download for text from url: {url_from_text}")

    ctx.logger.info(
        f"Requesting a download for text from reply: {url_from_reply}")

    # need to check if the url strings are correct next
    # reply to the first truthy one

    mode: None | Literal["text"] | Literal["reply"] = None

    url_1 = extract_url(url_from_text)
    if url_1:
        mode = "text"

    url_2 = extract_url(url_from_reply)
    if url_2:
        mode = "reply"

    url = url_1 or url_2
    if url is None:
        ctx.logger.error(
            f"Does not contain a valid URL")
        # handle raising event to signify if failed
        return

    ctx.logger.info(
        f"Proceeding to download.",
        extra={"url": url, "mode": mode})

    hash_url = hashlib.sha256(url.encode()).hexdigest()
    filename_stub = hash_url  # We'll add extension after downloading

    # Check if file already exists in MinIO
    try:
        # Optional: use wildcard match if not hardcoded
        minio.stat_object(bucket_name, f"{filename_stub}.mp4")
        ctx.logger.info("File already exists in MinIO. Skipping download and upload.",
                        extra={"object_name": f"{filename_stub}.mp4"})

        presigned_url = minio.presigned_get_object(
            bucket_name=bucket_name,
            object_name=f"{filename_stub}.mp4",
            expires=datetime.timedelta(minutes=5)
        )
        await publish_presigned_event(ctx, correlation_id, presigned_url, payload=payload)
        ctx.logger.info("Published events.dl.video.ready event",
                        extra={"presigned_url": presigned_url})
        return
    except S3Error as e:
        if e.code != "NoSuchKey":
            raise
        ctx.logger.info("File not found in MinIO. Proceeding to download.",
                        extra={"url": url})

    path_to_file = download_tiktok_video(url=url)
    ctx.logger.info(
        f"File downloaded", extra={"path_to_file": path_to_file})

    # ext includes the dot, e.g., ".mp4"
    _, ext = os.path.splitext(path_to_file)
    hashed_filename = hash_url + ext

    minio.fput_object(bucket_name=bucket_name,
                      object_name=hashed_filename,
                      file_path=path_to_file)
    ctx.logger.info("File uploaded to MinIO", extra={
                    "object_name": hashed_filename})

    presigned_url = minio.presigned_get_object(
        bucket_name=bucket_name,
        object_name=hashed_filename,
        expires=datetime.timedelta(minutes=5)  # or minutes=15 etc.
    )

    await publish_presigned_event(ctx, correlation_id, presigned_url, payload=payload)

    try:
        os.remove(path_to_file)
    except OSError as e:
        ctx.logger.warning("Failed to clean up temp file",
                           extra={"error": str(e)})

    ctx.logger.info("Published events.dl.video.ready event",
                    extra={"presigned_url": presigned_url})
    return
