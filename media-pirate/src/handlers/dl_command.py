import datetime
import hashlib
import mimetypes
import os
from typing import Literal, Optional
from src.core.event_envelope import EventEnvelope
from src.core.service_container import ServiceContainer
from src.handlers.normalized_telegram_payload import NormalizedTelegramPayload
from src.yt_dlp_client import download_video, download_audio
from minio.error import S3Error
from urllib.parse import urlparse
from urllib.parse import quote


def sanitize_metadata(meta: dict) -> dict:
    sanitized = {}
    for k, v in meta.items():
        if not isinstance(v, str):
            v = str(v)
        k = k.lower()
        sanitized[k] = quote(v, safe='')
    return sanitized


def get_cleaned_url(url: str) -> str:
    """Remove query parameters and normalize URL for consistent hashing"""
    from urllib.parse import urlparse, urlunparse

    parsed = urlparse(url)
    # Remove query parameters and fragment
    cleaned = urlunparse((
        parsed.scheme,
        parsed.netloc.lower(),  # Normalize domain case
        parsed.path.rstrip('/'),  # Remove trailing slashes
        '',  # No params
        '',  # No query
        ''   # No fragment
    ))
    return cleaned


def generate_friendly_filename(url: str, extension: str, hash_prefix: str = None) -> str:
    """Generate a user-friendly filename from URL"""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)

        # Try to extract meaningful name from URL
        if 'tiktok.com' in parsed.netloc:
            return f"tiktok_{hash_prefix or 'video'}{extension}"
        elif 'youtube.com' in parsed.netloc or 'youtu.be' in parsed.netloc:
            return f"youtube_{hash_prefix or 'video'}{extension}"
        elif 'instagram.com' in parsed.netloc:
            return f"instagram_{hash_prefix or 'video'}{extension}"
        else:
            return f"download_{hash_prefix or 'file'}{extension}"
    except:
        return f"download_{hash_prefix or 'file'}{extension}"


def extract_url(*candidates: Optional[str]) -> Optional[str]:
    for c in candidates:
        if c and c.strip().startswith(("http://", "https://")):
            return c.strip()
    return None


async def publish_presigned_video_ready_event(ctx: ServiceContainer, correlation_id: str, presigned_url: str, payload: NormalizedTelegramPayload,):
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
        routing_key='gateway_events',
        body=event.to_json(),
        exchange_name=''
    )


async def publish_presigned_audio_ready_event(ctx: ServiceContainer, correlation_id: str, presigned_url: str, payload: NormalizedTelegramPayload,):
    event = EventEnvelope(
        type="events.dl.audio.ready",
        correlation_id=correlation_id,
        timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        payload={"presigned_url": presigned_url,
                 'message_id': payload["message_id"],
                 'chat_id': payload["chat_id"]
                 },
        version=1
    )
    await ctx.safe_publish(
        routing_key='gateway_events',
        body=event.to_json(),
        exchange_name=''
    )


async def video_dl_command(ctx: ServiceContainer, correlation_id: str, event_type: str, timestamp: str, version: int, payload: NormalizedTelegramPayload) -> None:
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

    # Content-addressable key (no extension)
    cleaned_url = get_cleaned_url(url)
    hash_url = hashlib.sha256(cleaned_url.encode()).hexdigest()
    hash_url = f'video/{hash_url}'

    # Check if file already exists in MinIO
    try:
        stat_result = minio.stat_object(bucket_name, hash_url)

        # Extract file info from metadata
        extension = stat_result.metadata.get('x-amz-meta-extension', '.mp4')
        mime_type = stat_result.metadata.get(
            'x-amz-meta-content-type', 'video/mp4')
        original_filename = stat_result.metadata.get(
            'x-amz-meta-original-name', f'video{extension}')
        original_url = stat_result.metadata.get(
            'x-amz-meta-original-url', 'unknown')
        cleaned_url = stat_result.metadata.get(
            'x-amz-meta-cleaned-url', 'unknown')

        ctx.logger.info("File already exists in MinIO. Skipping download.",
                        extra={
                            "object_name": hash_url,
                            "extension": extension,
                            "mime_type": mime_type,
                            "original_url": original_url,
                            "cleaned_url": cleaned_url
                        })

        # Generate presigned URL with proper content disposition
        presigned_url = minio.presigned_get_object(
            bucket_name=bucket_name,
            object_name=hash_url,
            expires=datetime.timedelta(minutes=5),
            response_headers={
                'Content-Type': mime_type,
                'Content-Disposition': f'attachment; filename="{original_filename}"'
            }
        )

        await publish_presigned_video_ready_event(ctx, correlation_id, presigned_url, payload=payload)
        ctx.logger.info("Published events.dl.video.ready event",
                        extra={"presigned_url": presigned_url})
        return

    except S3Error as e:
        if e.code != "NoSuchKey":
            raise
        ctx.logger.info("File not found in MinIO. Proceeding to download.",
                        extra={"url": url})

    # Download the file
    path_to_file = download_video(url=url)
    ctx.logger.info("File downloaded", extra={"path_to_file": path_to_file})

    # Extract file information
    _, ext = os.path.splitext(path_to_file)

    # Determine MIME type
    mime_type, _ = mimetypes.guess_type(path_to_file)

    if mime_type is None:
        # Fallback based on extension
        mime_type = {
            '.mp4': 'video/mp4',
            '.webm': 'video/webm',
            '.mkv': 'video/x-matroska',
            '.avi': 'video/x-msvideo',
            '.mov': 'video/quicktime',
            '.m4a': 'audio/mp4',
            '.mp3': 'audio/mpeg',
            '.wav': 'audio/wav'
        }.get(ext.lower(), 'application/octet-stream')

    # Generate a nice filename for downloads
    original_filename = generate_friendly_filename(url, ext, hash_url[:8])

    # Get the cleaned URL (what we actually hashed)
    cleaned_url = get_cleaned_url(url)  # Remove query params, normalize

    raw_meta = {
        'extension': ext,
        # 'content-type': mime_type, # breaks it for some reason
        'original-name': original_filename,
        'source-url-hash': hash_url,
        'download-timestamp': timestamp,
        'original-url': url,
        'cleaned-url': cleaned_url,
        'url-domain': urlparse(url).netloc
    }

    metadata = sanitize_metadata(raw_meta)

    # Upload with comprehensive metadata
    minio.fput_object(
        bucket_name=bucket_name,
        object_name=hash_url,  # Clean hash, no extension
        file_path=path_to_file,
        content_type=mime_type,  # Set proper content type on object
        metadata=metadata
    )

    ctx.logger.info("File uploaded to MinIO", extra=metadata)

    # Generate presigned URL with proper headers
    presigned_url = minio.presigned_get_object(
        bucket_name=bucket_name,
        object_name=hash_url,
        expires=datetime.timedelta(minutes=5),
        response_headers={
            'Content-Type': mime_type,
            'Content-Disposition': f'attachment; filename="{original_filename}"'
        }
    )

    await publish_presigned_video_ready_event(ctx, correlation_id, presigned_url, payload=payload)

    # Cleanup
    try:
        os.remove(path_to_file)
    except OSError as e:
        ctx.logger.warning("Failed to clean up temp file",
                           extra={"error": str(e)})

    ctx.logger.info("Published events.dl.video.ready event",
                    extra={"presigned_url": presigned_url})
    return


async def audio_dl_command(ctx: ServiceContainer, correlation_id: str, event_type: str, timestamp: str, version: int, payload: NormalizedTelegramPayload) -> None:
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

    # Content-addressable key (no extension)
    cleaned_url = get_cleaned_url(url)
    hash_url = hashlib.sha256(cleaned_url.encode()).hexdigest()
    hash_url = f'audio/{hash_url}'

    # Check if file already exists in MinIO
    try:
        stat_result = minio.stat_object(bucket_name, hash_url)

        # Extract file info from metadata
        extension = stat_result.metadata.get('x-amz-meta-extension', '.mp3')
        mime_type = stat_result.metadata.get(
            'x-amz-meta-content-type', 'audio/mp4')
        original_filename = stat_result.metadata.get(
            'x-amz-meta-original-name', f'audio{extension}')
        original_url = stat_result.metadata.get(
            'x-amz-meta-original-url', 'unknown')
        cleaned_url = stat_result.metadata.get(
            'x-amz-meta-cleaned-url', 'unknown')

        ctx.logger.info("File already exists in MinIO. Skipping download.",
                        extra={
                            "object_name": hash_url,
                            "extension": extension,
                            "mime_type": mime_type,
                            "original_url": original_url,
                            "cleaned_url": cleaned_url
                        })

        # Generate presigned URL with proper content disposition
        presigned_url = minio.presigned_get_object(
            bucket_name=bucket_name,
            object_name=hash_url,
            expires=datetime.timedelta(minutes=5),
            response_headers={
                'Content-Type': mime_type,
                'Content-Disposition': f'attachment; filename="{original_filename}"'
            }
        )

        await publish_presigned_audio_ready_event(ctx, correlation_id, presigned_url, payload=payload)
        ctx.logger.info("Published events.dl.audio.ready event",
                        extra={"presigned_url": presigned_url})
        return

    except S3Error as e:
        if e.code != "NoSuchKey":
            raise
        ctx.logger.info("File not found in MinIO. Proceeding to download.",
                        extra={"url": url})

    # Download the file
    path_to_file = download_audio(url=url)
    ctx.logger.info("File downloaded", extra={"path_to_file": path_to_file})

    # Extract file information
    _, ext = os.path.splitext(path_to_file)

    # Determine MIME type
    mime_type, _ = mimetypes.guess_type(path_to_file)

    if mime_type is None:
        # Fallback based on extension
        mime_type = {
            '.mp4': 'video/mp4',
            '.webm': 'video/webm',
            '.mkv': 'video/x-matroska',
            '.avi': 'video/x-msvideo',
            '.mov': 'video/quicktime',
            '.m4a': 'audio/mp4',
            '.mp3': 'audio/mpeg',
            '.wav': 'audio/wav'
        }.get(ext.lower(), 'application/octet-stream')

    # Generate a nice filename for downloads
    original_filename = generate_friendly_filename(url, ext, hash_url[:8])

    # Get the cleaned URL (what we actually hashed)
    cleaned_url = get_cleaned_url(url)  # Remove query params, normalize

    raw_meta = {
        'extension': ext,
        # 'content-type': mime_type, # breaks it for some reason
        'original-name': original_filename,
        'source-url-hash': hash_url,
        'download-timestamp': timestamp,
        'original-url': url,
        'cleaned-url': cleaned_url,
        'url-domain': urlparse(url).netloc
    }

    metadata = sanitize_metadata(raw_meta)

    # Upload with comprehensive metadata
    minio.fput_object(
        bucket_name=bucket_name,
        object_name=hash_url,  # Clean hash, no extension
        file_path=path_to_file,
        content_type=mime_type,  # Set proper content type on object
        metadata=metadata
    )

    ctx.logger.info("File uploaded to MinIO", extra=metadata)

    # Generate presigned URL with proper headers
    presigned_url = minio.presigned_get_object(
        bucket_name=bucket_name,
        object_name=hash_url,
        expires=datetime.timedelta(minutes=5),
        response_headers={
            'Content-Type': mime_type,
            'Content-Disposition': f'attachment; filename="{original_filename}"'
        }
    )

    await publish_presigned_audio_ready_event(ctx, correlation_id, presigned_url, payload=payload)

    # Cleanup
    try:
        os.remove(path_to_file)
    except OSError as e:
        ctx.logger.warning("Failed to clean up temp file",
                           extra={"error": str(e)})

    ctx.logger.info("Published events.dl.audio.ready event",
                    extra={"presigned_url": presigned_url})
    return
