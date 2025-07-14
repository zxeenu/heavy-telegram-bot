from typing import Literal, Optional
from src.app_context import AsyncAppContext
from src.handlers.normalized_telegram_payload import NormalizedTelegramPayload
from src.yt_dlp_client import download_tiktok_video


def extract_url(*candidates: Optional[str]) -> Optional[str]:
    for c in candidates:
        if c and c.strip().startswith(("http://", "https://")):
            return c.strip()
    return None


async def dl_command(ctx: AsyncAppContext, correlation_id: str, event_type: str, timestamp: str, version: int, payload: NormalizedTelegramPayload) -> None:
    filtered_parts = payload["filtered_parts"]
    url_from_text: Optional[str] = filtered_parts[1] if len(
        filtered_parts) > 1 else None
    url_from_reply = payload["reply_text"]

    ctx.logger.info(
        f"Event id: {correlation_id} is requesting a download for text from url: {url_from_text}")

    ctx.logger.info(
        f"Event id: {correlation_id} is requesting a download for text from reply: {url_from_reply}")

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
            f"Event id: {correlation_id} did not contain a valid URL")
        # handle raising event to signify if failed
        return

    ctx.logger.info(
        f"Event id: {correlation_id} proceeding to download: {url} from the {mode}")

    path_to_file = download_tiktok_video(url=url)
    ctx.logger.info(
        f"Event id: {correlation_id} downloaded to {path_to_file}")
    pass
