from typing import TypedDict, Optional


class NormalizedTelegramPayload(TypedDict):
    message_id: str
    chat_id: str
    text: str
    filtered_parts: list[str]
    from_user_id: Optional[int]
    from_user_name: str
    reply_to_message_id: Optional[int]
    reply_text: str
    reply_user_id: Optional[str]
    reply_user_name: Optional[str]
