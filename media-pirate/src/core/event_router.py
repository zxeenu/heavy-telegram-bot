from collections import defaultdict
from typing import Callable, Awaitable, Dict, Any, Optional, List
from src.core.event_envelope import EventEnvelope
from src.core.logging_context import get_correlation_id
from src.core.service_container import ServiceContainer
import inspect


def has_params(func: Callable, required_params: list[str]) -> bool:
    try:
        sig = inspect.signature(func)
        params = sig.parameters.keys()
        return all(p in params for p in required_params)
    except (ValueError, TypeError):
        return False


# Type aliases
HandlerFunc = Callable[[ServiceContainer, Any, dict], Awaitable[Optional[Any]]]
MiddlewareFunc = Callable[[EventEnvelope,
                           ServiceContainer], Awaitable[Optional[Any]]]


class EventRouter:
    def __init__(self):
        # routes and meta data attached
        self.routes: Dict[str, Dict[int, HandlerFunc]] = defaultdict(dict)
        self.route_meta_data: Dict[str, Dict[int, Dict]] = defaultdict(dict)

        self.middlewares_before: List[str] = []
        self.middlewares_after: List[str] = []
        self.middlewares: Dict[str, MiddlewareFunc] = {}

    def route(self, event_type: str, version: int = 1, meta_data: Optional[dict] = None) -> Callable[[HandlerFunc], HandlerFunc]:
        def decorator(func: HandlerFunc) -> HandlerFunc:
            self.routes[event_type][version] = func
            self.route_meta_data[event_type][version] = meta_data
            return func
        return decorator

    def _validate_middleware(self, actual_name: str):
        if not actual_name:
            raise ValueError(
                "Middleware must have a non-empty 'name' parameter.")

        if actual_name in self.middlewares:
            raise ValueError(
                f"Middleware name '{actual_name}' is already registered.")

    def register_middleware(self, name: str = None) -> Callable[[MiddlewareFunc], MiddlewareFunc]:
        """
        These are regitered, but not active. Please use the `meta_data` in the `route` decorator to opt into middlewares
        """

        self._validate_middleware(actual_name=name)

        def decorator(func: MiddlewareFunc) -> MiddlewareFunc:
            middleware_is_valid = has_params(
                func, ["ctx", "envelope"])

            if not middleware_is_valid:
                raise ValueError(
                    f"Middleware '{name}' does not implement required parameters")

            self.middlewares[name] = func
            return func
        return decorator

    def register_before_middleware(self, name: str) -> Callable[[MiddlewareFunc], MiddlewareFunc]:
        """
        These are globally effective middlewares
        """

        self._validate_middleware(actual_name=name)

        def decorator(func: MiddlewareFunc) -> MiddlewareFunc:

            middleware_is_valid = has_params(
                func, ["ctx", "envelope"])

            if not middleware_is_valid:
                raise ValueError(
                    f"Middleware '{name}' does not implement required parameters")

            self.middlewares[name] = func
            self.middlewares_before.append(name)
            return func
        return decorator

    def register_after_middleware(self, name: str) -> Callable[[MiddlewareFunc], MiddlewareFunc]:
        """
        These are globally effective middlewares
        """

        self._validate_middleware(actual_name=name)

        def decorator(func: MiddlewareFunc) -> MiddlewareFunc:

            middleware_is_valid = has_params(
                func, ["ctx", "envelope"])

            if not middleware_is_valid:
                raise ValueError(
                    f"Middleware '{name}' does not implement required parameters")

            self.middlewares[name] = func
            self.middlewares_after.append(name)
            return func
        return decorator

    async def dispatch(
        self, envelope: EventEnvelope, ctx: ServiceContainer
    ) -> Dict[str, Any]:
        handler = self.routes.get(envelope.type, {}).get(envelope.version)
        if not handler:
            ctx.logger.warning(f"No handler registered for {envelope.type}")
            raise ValueError(
                f"No handler for {envelope.type} v{envelope.version}")

        handler_is_valid = has_params(
            handler, ["ctx", "envelope", "meta_data"])
        if not handler_is_valid:
            raise ValueError(
                f"handler for {envelope.type} does not implement required parameters")

        meta_data = self.route_meta_data.get(
            envelope.type, {}).get(envelope.version, {})
        extra_middleware_before: List[str] = meta_data.get(
            'middleware_before', [])
        all_middleware_before = self.middlewares_before + extra_middleware_before
        unique_middleware_before = list(dict.fromkeys(all_middleware_before))

        extra_middleware_after: List[str] = meta_data.get(
            'middleware_after', [])
        all_middleware_after = self.middlewares_after + extra_middleware_after
        unique_middleware_after = list(dict.fromkeys(all_middleware_after))

        middlewares_before_results = {}
        for middleware_name in unique_middleware_before:
            middleware = self.middlewares.get(middleware_name)
            if not middleware:
                raise ValueError(
                    f"No before middleware for {middleware_name}")

            result = await middleware(envelope=envelope, ctx=ctx)

            if not result:
                raise RuntimeError(
                    f"before handler {middleware_name} failed! You must return a truthy value!")

            middlewares_before_results[middleware_name] = result

        handler_result = await handler(
            ctx=ctx, envelope=envelope, meta_data=meta_data
        )

        middlewares_after_results = {}
        for middleware_name in unique_middleware_after:
            middleware = self.middlewares.get(middleware_name)
            if not middleware:
                raise ValueError(
                    f"No after middleware for {middleware_name}")

            result = await middleware(envelope=envelope, ctx=ctx)

            if not result:
                raise RuntimeError(
                    f"after handler {middleware_name} failed! You must return a truthy value!")

            middlewares_after_results[middleware_name] = result

        return {
            "handler_result": handler_result,
            "correlation_id": get_correlation_id(),
            "middlewares_before_result": middlewares_before_results,
            "middlewares_after_result": middlewares_after_results,
        }
