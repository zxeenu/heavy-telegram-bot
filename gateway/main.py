import asyncio
import os
import json
from hydrogram import Client
from hydrogram.handlers import MessageHandler
from hydrogram.types import Message
from src.app_context import AppContext
import uuid
from datetime import datetime, timezone


# Decorator: inject shared AppContext into handlers
def with_app_context(func):
    ctx = AppContext()  # One instance shared

    async def wrapper(client, message):
        try:
            await func(ctx, client, message)
        except Exception:
            ctx.logger.exception("Error in handler")
            raise
    return wrapper


# Convert message to serializable JSON
def to_serializable(obj):
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    elif isinstance(obj, list):
        return [to_serializable(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: to_serializable(v) for k, v in obj.items()}
    elif hasattr(obj, "__dict__"):
        return {k: to_serializable(v) for k, v in vars(obj).items() if not k.startswith("_")}
    elif hasattr(obj, "_asdict"):
        return to_serializable(obj._asdict())
    elif hasattr(obj, "isoformat"):
        return obj.isoformat()
    else:
        return str(obj)


# Handler with context injection
@with_app_context
async def event_bus_handler(ctx: AppContext, client: Client, message: Message):
    message_dict = to_serializable(obj=message)

    event = {
        "type": "telegram.message",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": message_dict,
    }
    json_str = json.dumps(event, indent=2)
    ctx.safe_publish(
        exchange='', routing_key='telegram_events', body=json_str)
    ctx.logger.info(json_str)


# Background task example
async def background_task(ctx: AppContext):
    while True:
        ctx.logger.info("Doing other stuff...")
        await asyncio.sleep(5)


# Main entry point — directly manages the context lifecycle
async def main():
    ctx = AppContext()
    try:
        ctx.logger.info("Service started")
        telegramApp = Client(
            "account_session",
            api_id=os.environ["TELEGRAM_ID"],
            api_hash=os.environ["TELEGRAM_HASH"]
        )

        ctx.channel.queue_declare(queue='telegram_events', durable=False)
        telegramApp.add_handler(MessageHandler(event_bus_handler))
        await telegramApp.start()

        await asyncio.gather(
            background_task(ctx=ctx),
            asyncio.Event().wait()
        )
    finally:
        await telegramApp.stop()
        ctx.close()


if __name__ == '__main__':
    asyncio.run(main())
