import asyncio
from src.app_context import AsyncAppContext
import json
import os
from typing import Optional
from src.handlers.dl_command import dl_command
from src.handlers.normalized_telegram_payload import NormalizedTelegramPayload


ctx = AsyncAppContext()


def normalize_telegram_payload(payload: dict) -> NormalizedTelegramPayload:
    from_user = payload.get('from_user') or {}
    reply_to_message = payload.get('reply_to_message') or {}

    try:
        from_user_id = int(from_user.get('id')) if from_user.get(
            'id') is not None else None
    except (TypeError, ValueError):
        from_user_id = None

    text = str(payload.get('text') or '')
    parts = text.split()
    filtered_parts = list(filter(None, parts))

    return {
        "message_id": payload.get('id', ''),
        "text": text,
        "filtered_parts": filtered_parts,
        "from_user_id": from_user_id,
        "from_user_name": str(from_user.get('username', '')),
        "reply_to_message_id": payload.get('reply_to_message_id'),
        "reply_text": str(reply_to_message.get('text', '')),
    }


# all telegram commands that are added here should accept the same arguments
TELEGRAM_COMMAND_HANDLERS = {
    '.dl': dl_command,
}


async def main() -> None:
    await ctx.connect()

    admin_user_id = int(os.environ["TELEGRAM_ADMIN_USER_ID"])
    allowed_user_ids = set([admin_user_id])

    ctx.logger.info(
        f"MediaPirate Service started, admin_user_id: {admin_user_id}")

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
                            f"Invalid JSON in message: {correlation_id}")
                        # Do something with the malformed JSON's later
                        continue
                    except Exception as e:
                        ctx.logger.error(
                            f"Error processing {correlation_id}: {e}")
                        continue

                    event_type: str = body.get('type', '')
                    version = int(body.get('version')) if body.get(
                        'version') is not None else None
                    correlation_id: str = body.get('correlation_id', '')
                    timestamp: str = body.get('timestamp', '')

                    if version is None:
                        ctx.logger.info(
                            f"Event received id: {correlation_id} does not have a version. Malformed event payload.")
                        continue

                    # Proper formatting with placeholders
                    ctx.logger.info(
                        f"Event received id: {correlation_id}, type: {event_type}, timestamp: {timestamp}, version: {version}")
                    if event_type == 'events.telegram.raw':
                        payload = body.get('payload', {})
                        data = normalize_telegram_payload(payload)

                        message_id = data["message_id"]
                        from_user_name = data["from_user_name"]
                        from_user_id = data["from_user_id"]

                        # pprint.pprint(payload, indent=2, width=60)
                        # ctx.logger.info(payload)

                        reply_to_message_id = data["reply_to_message_id"]
                        reply_text = data["reply_text"]

                        ctx.logger.info(
                            f"Event id: {correlation_id}, user_id: {from_user_name} user_name: {from_user_id}, message_id: {message_id}")
                        ctx.logger.info(
                            f"Event id: {correlation_id}, reply_to_message_id: {reply_to_message_id} reply_text: {reply_text}")

                        if from_user_id not in allowed_user_ids:
                            ctx.logger.info(
                                f"Event id: {correlation_id} does not originate from an allowed user. Abort")
                            continue

                        filtered_parts = data["filtered_parts"]
                        ctx.logger.info(
                            f"Event id: {correlation_id} originates from allowed user. Parts: {filtered_parts}")

                        command_word: Optional[str] = filtered_parts[0] if filtered_parts else None
                        ctx.logger.info(
                            f"Event id: {correlation_id} command_word: {command_word}")

                        if not command_word:
                            ctx.logger.info(
                                f"Event id: {correlation_id} does not have any actionable keywords")
                            continue

                        handler = TELEGRAM_COMMAND_HANDLERS.get(command_word)
                        if handler is None:
                            ctx.logger.warning(
                                f"Event id: {correlation_id} command_word: {command_word} has no associated handler.")
                            continue

                        try:
                            await handler(ctx=ctx, correlation_id=correlation_id, event_type=event_type, timestamp=timestamp, version=version, payload=data)
                        except Exception:
                            ctx.logger.exception(
                                f"Event id: {correlation_id}, handler for {command_word} failed")
                        continue


if __name__ == "__main__":
    asyncio.run(main())
