from collections import defaultdict
from dataclasses import dataclass, field
import logging
from typing import Callable, Awaitable, Dict, Any, Generic, Optional, List, Type, TypeVar, TypedDict, Protocol, runtime_checkable
from src.core.event_envelope import EventEnvelope
from src.core.logging_context import get_correlation_id
from src.core.service_container import ServiceContainer
import inspect


class EventRouterError(Exception):
    """Base exception class for EventRouter errors."""
    pass


class RouteNotFoundError(EventRouterError):
    """Raised when no route handler is found for a given event type and version."""

    def __init__(self, event_type: str, version: int):
        super().__init__(
            f"No handler for event '{event_type}' version {version}")
        self.event_type = event_type
        self.version = version


class MiddlewareRegistrationError(EventRouterError):
    """Raised when middleware registration fails (e.g. duplicate name or invalid signature)."""

    def __init__(self, name: str, message: str):
        super().__init__(f"Middleware '{name}' registration error: {message}")
        self.name = name


class MiddlewareExecutionError(EventRouterError):
    """Raised when middleware execution returns falsy or fails."""

    def __init__(self, name: str, phase: str):
        super().__init__(
            f"Middleware '{name}' failed during {phase} phase. It must return a truthy value.")
        self.name = name
        self.phase = phase


class HandlerSignatureError(EventRouterError):
    """Raised when a handler does not implement required parameters."""

    def __init__(self, event_type: str):
        super().__init__(
            f"Handler for event '{event_type}' does not implement required parameters")
        self.event_type = event_type


def has_params(func: Callable, required_params: list[str]) -> bool:
    """allows you to check if a given function implements certain keyword paramters"""
    try:
        sig = inspect.signature(func)
        params = sig.parameters.keys()
        return all(p in params for p in required_params)
    except (ValueError, TypeError):
        return False


class RouteOptionDict(TypedDict):
    """type hinted dictionary to provide options when declaring routes"""
    middleware_before: List[str] = []
    middleware_after: List[str] = []
    retry_attempt: int = 0


@dataclass
class RouteOption:
    """for the runtime validation, and actual options usage within the EventRouter"""
    middleware_before: List[str] = field(default_factory=list)
    middleware_after: List[str] = field(default_factory=list)
    retry_attempt: int = 0


ContextType = TypeVar('T')

HandlerFunc = Callable[[ContextType, EventEnvelope],
                       Awaitable[Optional[Any]]]
MiddlewareFunc = Callable[[EventEnvelope,
                           ContextType], Awaitable[Optional[Any]]]


class EventRouter(Generic[ContextType]):
    """
    `ContextType` the depdendency container to be injected 
    into the handler and middlewar methods
    """

    logger: logging.Logger
    _context_type: Optional[Type[ContextType]]

    def __init__(self):
        # routes and meta data attached
        self.routes: Dict[str, Dict[int, HandlerFunc]] = defaultdict(dict)
        self.route_options: Dict[str,
                                 Dict[int, RouteOption]] = defaultdict(dict)

        self.middlewares_before: List[str] = []
        self.middlewares_after: List[str] = []
        self.middlewares: Dict[str, MiddlewareFunc] = {}
        self.logger = logging.getLogger(__name__)
        self._context_type = None  # delayed until service container is available

    def set_logger(self, logger: logging.Logger):
        """Must be called once the service container is available."""
        self.logger = logger
        self.logger.debug(f"Logger set to {logger}")

    def set_context_type(self, context_type: Type[ContextType]):
        """Must be called once the service container is available."""
        self._context_type = context_type
        self.logger.debug(f"Context type set to {context_type}")

    def route(self, event_type: str, version: int = 1, options: RouteOptionDict | None = None) -> Callable[[HandlerFunc], HandlerFunc]:

        # Normalize to RouteOption instance
        route_options = RouteOption(**(options or {}))

        def decorator(func: HandlerFunc) -> HandlerFunc:
            self.routes[event_type][version] = func
            self.route_options[event_type][version] = route_options
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
        These are regitered, but not active. Please use the `options` in the `route` decorator 
        to enable them for routes
        """

        self._validate_middleware(actual_name=name)

        def decorator(func: MiddlewareFunc) -> MiddlewareFunc:
            middleware_is_valid = has_params(
                func, ["context", "envelope"])

            if not middleware_is_valid:
                raise ValueError(
                    f"Middleware '{name}' does not implement required parameters")

            self.middlewares[name] = func
            return func
        return decorator

    def register_before_middleware(self, name: str) -> Callable[[MiddlewareFunc], MiddlewareFunc]:
        """These are globally effective middlewares"""

        self._validate_middleware(actual_name=name)

        def decorator(func: MiddlewareFunc) -> MiddlewareFunc:

            middleware_is_valid = has_params(
                func, ["context", "envelope"])

            if not middleware_is_valid:
                raise ValueError(
                    f"Middleware '{name}' does not implement required parameters")

            self.middlewares[name] = func
            self.middlewares_before.append(name)
            return func
        return decorator

    def register_after_middleware(self, name: str) -> Callable[[MiddlewareFunc], MiddlewareFunc]:
        """These are globally effective middlewares"""

        self._validate_middleware(actual_name=name)

        def decorator(func: MiddlewareFunc) -> MiddlewareFunc:

            middleware_is_valid = has_params(
                func, ["context", "envelope"])

            if not middleware_is_valid:
                raise ValueError(
                    f"Middleware '{name}' does not implement required parameters")

            self.middlewares[name] = func
            self.middlewares_after.append(name)
            return func
        return decorator

    async def dispatch(
        self, envelope: EventEnvelope, context: ContextType
    ) -> Dict[str, Any]:
        """
        Dispatches an event to the appropriate handler.
        Checks that the context is of the correct type.
        """
        if self._context_type is not None and not isinstance(context, self._context_type):
            raise TypeError(
                f"Context must be of type {self._context_type.__name__}, "
                f"got {type(context).__name__}"
            )
        elif self._context_type is None:
            self.logger.warning(
                "Dispatching without context type check (context type not set)")

        handler = self.routes.get(envelope.type, {}).get(envelope.version)
        if not handler:
            self.logger.warning(f"No handler registered for {envelope.type}")
            raise RouteNotFoundError(envelope.type, envelope.version)

        handler_is_valid = has_params(
            handler, ["context", "envelope"])
        if not handler_is_valid:
            raise HandlerSignatureError(envelope.type)

        options = self.route_options.get(
            envelope.type, {}).get(envelope.version)
        if options is None:
            options = RouteOption()

        extra_middleware_before = options.middleware_before
        all_middleware_before = self.middlewares_before + extra_middleware_before
        unique_middleware_before = list(dict.fromkeys(all_middleware_before))

        extra_middleware_after = options.middleware_after
        all_middleware_after = self.middlewares_after + extra_middleware_after
        unique_middleware_after = list(dict.fromkeys(all_middleware_after))

        middlewares_before_results = {}
        for middleware_name in unique_middleware_before:
            middleware = self.middlewares.get(middleware_name)
            if not middleware:
                raise MiddlewareRegistrationError(
                    middleware_name, "No before middleware registered")

            result = await middleware(envelope=envelope, context=context)

            if not result:
                raise MiddlewareExecutionError(middleware_name, "before")

            middlewares_before_results[middleware_name] = result

        handler_result = await handler(
            context=context, envelope=envelope
        )

        middlewares_after_results = {}
        for middleware_name in unique_middleware_after:
            middleware = self.middlewares.get(middleware_name)
            if not middleware:
                raise MiddlewareRegistrationError(
                    middleware_name, "No after middleware registered")

            result = await middleware(envelope=envelope, context=context)

            if not result:
                raise MiddlewareExecutionError(middleware_name, "after")

            middlewares_after_results[middleware_name] = result

        return {
            "handler_result": handler_result,
            "correlation_id": get_correlation_id(),
            "middlewares_before_result": middlewares_before_results,
            "middlewares_after_result": middlewares_after_results,
        }
