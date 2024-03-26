import logging
from types import TracebackType
from typing import (
    Any,
    AsyncContextManager,
    Awaitable,
    Callable,
    Sequence,
)

from fast_depends.dependencies import Depends
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
from typing_extensions import TypeAlias, override

from faststream.asyncapi import schema as asyncapi
from faststream.broker.core.asynchronous import BrokerAsyncUsecase, default_filter
from faststream.broker.message import StreamMessage
from faststream.broker.middlewares import BaseMiddleware
from faststream.broker.types import (
    CustomDecoder,
    CustomParser,
    Filter,
    P_HandlerParams,
    T_HandlerReturn,
    WrappedReturn,
)
from faststream.broker.wrapper import HandlerCallWrapper
from faststream.gcp.asyncapi import Handler, Publisher
from faststream.gcp.message import AnyPubSubDict, PubSubMessage
from faststream.gcp.producer import PubSubFastProducer
from faststream.gcp.schemas import PubSubBatchSub, PubSubSub
from faststream.gcp.shared.logging import PubSubLoggingMixin
from faststream.log import access_logger
from faststream.security import BaseSecurity
from faststream.types import AnyDict, DecodedMessage, SendableMessage

Topic: TypeAlias = str

class PubSubBroker(
    PubSubLoggingMixin,
    BrokerAsyncUsecase[AnyPubSubDict, PublisherClient, SubscriberClient],
):
    project_id: str
    handlers: dict[str, Handler]
    _publishers: dict[str, Publisher]

    _producer: PubSubFastProducer | None

    def __init__(
        self,
        project_id: str,
        *,
        security: BaseSecurity | None = None,
        # broker args
        graceful_timeout: float | None = None,
        apply_types: bool = True,
        validate: bool = True,
        dependencies: Sequence[Depends] = (),
        parser: CustomParser[AnyPubSubDict, PubSubMessage] | None = None,
        decoder: CustomDecoder[PubSubMessage] | None = None,
        middlewares: Sequence[Callable[[AnyPubSubDict], BaseMiddleware]] | None = None,
        # AsyncAPI args
        asyncapi_url: str | None = None,
        protocol: str | None = None,
        protocol_version: str | None = "custom",
        description: str | None = None,
        tags: Sequence[asyncapi.Tag] | None = None,
        # logging args
        logger: logging.Logger | None = access_logger,
        log_level: int = logging.INFO,
        log_fmt: str | None = None,
    ) -> None: ...
    async def connect(
        self,
        project_id: str,
        security: BaseSecurity | None = None,
    ) -> tuple[PublisherClient, SubscriberClient]: ...
    @override
    async def _connect(  # type: ignore[override]
        self,
        project_id: str,
        security: BaseSecurity | None = None,
    ) -> tuple[PublisherClient, SubscriberClient]: ...
    async def _close(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exec_tb: TracebackType | None = None,
    ) -> None: ...
    async def start(self) -> None: ...
    def _process_message(
        self,
        func: Callable[[StreamMessage[Any]], Awaitable[T_HandlerReturn]],
        watcher: Callable[..., AsyncContextManager[None]],
        **kwargs: Any,
    ) -> Callable[
        [StreamMessage[Any]],
        Awaitable[WrappedReturn[T_HandlerReturn]],
    ]: ...
    @override
    def subscriber(  # type: ignore[override]
        self,
        topic: Topic | PubSubSub | None = None,
        *,
        batch_topic: Topic | PubSubBatchSub | None = None,
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
        topic: Topic | PubSubSub | None = None,
        batch_topic: Topic | PubSubBatchSub | None = None,
        headers: AnyDict | None = None,
        reply_to: str = "",
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> Publisher: ...
    @override
    async def publish(  # type: ignore[override]
        self,
        message: SendableMessage,
        topic: str | None = None,
        reply_to: str = "",
        headers: AnyDict | None = None,
        correlation_id: str | None = None,
        *,
        batch_topic: str | None = None,
        rpc: bool = False,
        rpc_timeout: float | None = 30.0,
        raise_timeout: bool = False,
    ) -> DecodedMessage | None: ...
    async def publish_batch(
        self,
        *msgs: SendableMessage,
        batch_topic: str,
    ) -> None: ...
