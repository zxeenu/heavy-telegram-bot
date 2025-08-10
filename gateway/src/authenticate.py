import os

from src.core.service_container import ServiceContainer


class Authenticator:
    def __init__(self):
        self.admin_user_id = int(os.environ["TELEGRAM_ADMIN_USER_ID"])
        # self.admin_user_id = 223123

    def is_admin(self, user_id: int) -> bool:
        return self.admin_user_id == user_id

    async def is_allowed(self, user_id: str, chat_id: str, ctx: ServiceContainer) -> bool:
        try:
            user_id_int = int(user_id)
            chat_id_int = int(chat_id)
        except ValueError:
            return False  # or raise an error/log it

        is_admin = self.is_admin(user_id_int)
        is_allowed_in_chat = await ctx.redis.get(f"graced_chat:{chat_id_int}")

        # Redis returns bytes or None, so convert to bool:
        is_allowed_in_chat = bool(is_allowed_in_chat)

        return is_admin or is_allowed_in_chat
