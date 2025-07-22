import contextvars

correlation_id_var = contextvars.ContextVar("correlation_id", default="-")


def set_correlation_id(corr_id: str) -> None:
    correlation_id_var.set(corr_id)


def get_correlation_id() -> str:
    return correlation_id_var.get()
