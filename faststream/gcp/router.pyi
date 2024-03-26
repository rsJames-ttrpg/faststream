from typing import (
    Any,
    Callable,
    Sequence,
)

from fast_depends.dependencies import Depends
from typing_extensions import override

from faststream.broker.core.asynchronous import default_filter
from faststream.broker.middlewares import BaseMiddleware
from faststream.broker.router import BrokerRouter
from faststream.broker.types import (
    CustomDecoder,
    CustomParser,
    Filter,
    P_HandlerParams,
    T_HandlerReturn,
)
from faststream.broker.wrapper import HandlerCallWrapper
from faststream.gcp.asyncapi import Publisher
from faststream.gcp.message import AnyPubSubDict, PubSubMessage
from faststream.gcp.schemas import PubSubBatchSub, PubSubSub
from faststream.gcp.shared.router import PubSubRoute
from faststream.types import AnyDict

class PubSubRouter(BrokerRouter[str, AnyPubSubDict]):
    _publishers: dict[str, Publisher]  # type: ignore[assignment]

    def __init__(
        self,
        prefix: str = "",
        handlers: Sequence[PubSubRoute] = (),
        *,
        dependencies: Sequence[Depends] = (),
        parser: CustomParser[AnyPubSubDict, PubSubMessage] | None = None,
        decoder: CustomDecoder[PubSubMessage] | None = None,
        middlewares: Sequence[Callable[[AnyPubSubDict], BaseMiddleware]] | None = None,
        include_in_schema: bool = True,
    ) -> None: ...

    @override
    @staticmethod
    def _get_publisher_key(publisher: Publisher) -> str:  # type: ignore[override]
        ...

    @override
    @staticmethod
    def _update_publisher_prefix(  # type: ignore[override]
        prefix: str,
        publisher: Publisher,
    ) -> Publisher: ...

    @override
    def subscriber(  # type: ignore[override]
        self,
        topic: str | PubSubSub | None = None,
        *,
        batch_topic: str | PubSubBatchSub | None = None,
        # broker arguments
        dependencies: Sequence[Depends] = (),
        parser: CustomParser[AnyPubSubDict, PubSubMessage] | None = None,
        decoder: CustomDecoder[PubSubMessage] | None = None,
        middlewares: Sequence[Callable[[AnyPubSubDict], BaseMiddleware]] | None = None,
        filter: Filter[PubSubMessage] = default_filter,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        **__service_kwargs: Any,
    ) -> Callable[
        [Callable[P_HandlerParams, T_HandlerReturn]],
        HandlerCallWrapper[Any, P_HandlerParams, T_HandlerReturn],
    ]: ...

    @override
    def publisher(  # type: ignore[override]
        self,
        topic: str | PubSubSub | None = None,
        batch_topic: str | PubSubBatchSub | None = None,
        headers: AnyDict | None = None,
        reply_to: str = "",
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> Publisher: ...
