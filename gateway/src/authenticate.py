import os


class Authenticator:
    def __init__(self):
        self.admin_user_id = int(os.environ["TELEGRAM_ADMIN_USER_ID"])

    def _is_admin(self, user_id: int) -> bool:
        return self.admin_user_id == user_id

    def is_allowed(self, user_id: str) -> bool:
        try:
            user_id_int = int(user_id)
        except ValueError:
            return False  # or raise an error/log it
        return self._is_admin(user_id_int)
