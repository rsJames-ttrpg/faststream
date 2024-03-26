from functools import partial, wraps
from types import TracebackType
from typing import (
    Any,
    AsyncContextManager,
    Awaitable,
    Callable,
    Dict,
    Optional,
    Sequence,
    Type,
    Union,
)

from fast_depends.dependencies import Depends
from google.api_core import gapic_v1, retry
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
from typing_extensions import TypeAlias, override

from faststream.broker.core.asynchronous import BrokerAsyncUsecase, default_filter
from faststream.broker.message import StreamMessage
from faststream.broker.middlewares import BaseMiddleware
from faststream.broker.types import (
    AsyncPublisherProtocol,
    CustomDecoder,
    CustomParser,
    Filter,
    P_HandlerParams,
    T_HandlerReturn,
    WrappedReturn,
)
from faststream.broker.wrapper import FakePublisher, HandlerCallWrapper
from faststream.exceptions import NOT_CONNECTED_YET
from faststream.gcp.asyncapi import Handler, Publisher
from faststream.gcp.message import AnyPubSubDict, PubSubMessage
from faststream.gcp.producer import PubSubFastProducer
from faststream.gcp.schemas import INCORRECT_SETUP_MSG, PubSubBatchSub, PubSubSub
from faststream.gcp.security import parse_security
from faststream.gcp.shared.logging import PubSubLoggingMixin
from faststream.security import BaseSecurity
from faststream.types import AnyDict, DecodedMessage
from faststream.utils.context.repository import context

Topic: TypeAlias = str

class PubSubBroker(
    PubSubLoggingMixin,
    BrokerAsyncUsecase[AnyPubSubDict, PublisherClient, SubscriberClient],
):
    """Pub/Sub broker."""

    project_id: str
    handlers: Dict[str, Handler]
    _publishers: Dict[str, Publisher]

    _producer: Optional[PubSubFastProducer]

    def __init__(
        self,
        project_id: str,
        *,
        protocol: Optional[str] = None,
        protocol_version: Optional[str] = "custom",
        security: Optional[BaseSecurity] = None,
        **kwargs: Any,
    ) -> None:
        """Pub/Sub broker.

        Args:
            project_id : ID of the Google Cloud project
            protocol : protocol of the Pub/Sub broker (default: None)
            protocol_version : protocol version of the Pub/Sub broker (default: "custom")
            security : security settings for the Pub/Sub broker (default: None)
            kwargs : additional keyword arguments
        """
        self._producer = None

        super().__init__(
            project_id=project_id,
            protocol_version=protocol_version,
            security=security,
            **kwargs,
        )

        self.protocol = protocol or "pubsub"

    async def connect(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[PublisherClient, SubscriberClient]:
        """Connect to the Pub/Sub broker.

        Args:
            args : additional positional arguments
            kwargs : additional keyword arguments
        """
        connection = await super().connect(*args, **kwargs)
        for p in self._publishers.values():
            p._producer = self._producer
        return connection

    @override
    async def _connect(  # type: ignore[override]
        self,
        project_id: str,
        **kwargs: Any,
    ) -> tuple[PublisherClient, SubscriberClient]:
        security_options = parse_security(self.security)
        publisher = gapic_v1.method.wrap_method(
            PublisherClient.from_service_account_info,
            default_retry=retry.Retry(),
            default_timeout=5.0,
            client_info=gapic_v1.client_info.ClientInfo(client_library_version="1.0.0"),
        )(
            info={"project_id": project_id, **security_options},
        )
        subscriber = gapic_v1.method.wrap_method(
            SubscriberClient.from_service_account_info,
            default_retry=retry.Retry(),
            default_timeout=5.0,
            client_info=gapic_v1.client_info.ClientInfo(client_library_version="1.0.0"),
        )(
            info={"project_id": project_id, **security_options},
        )
        self._producer = PubSubFastProducer(
            parser=self._global_parser,  # type: ignore[arg-type]
            decoder=self._global_parser,  # type: ignore[arg-type]
        )
        return publisher, subscriber

    async def _close(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exec_tb: Optional[TracebackType] = None,
    ) -> None:
        if self._connection is not None:
            await self._connection[0].transport.grpc_channel.close()
            await self._connection[1].transport.grpc_channel.close()

        await super()._close(exc_type, exc_val, exec_tb)

    async def start(self) -> None:
        context.set_global(
            "default_log_context",
            self._get_log_context(None, ""),
        )

        await super().start()
        assert self._connection, NOT_CONNECTED_YET  # nosec B101

        for handler in self.handlers.values():
            c = self._get_log_context(None, handler.subscription_name)
            self._log(f"`{handler.call_name}` waiting for messages", extra=c)
            await handler.start()

    def _process_message(
        self,
        func: Callable[[StreamMessage[Any]], Awaitable[T_HandlerReturn]],
        watcher: Callable[..., AsyncContextManager[None]],
        **kwargs: Any,
    ) -> Callable[
        [StreamMessage[Any]],
        Awaitable[WrappedReturn[T_HandlerReturn]],
    ]:
        @wraps(func)
        async def process_wrapper(
            message: StreamMessage[Any],
        ) -> WrappedReturn[T_HandlerReturn]:
            async with watcher(
                message,
            ):
                r = await func(message)

                pub_response: Optional[AsyncPublisherProtocol]
                if message.reply_to:
                    pub_response = FakePublisher(
                        partial(self.publish, topic=message.reply_to)
                    )
                else:
                    pub_response = None

                return r, pub_response

        return process_wrapper

    @override
    def subscriber(  # type: ignore[override]
        self,
        topic: Union[Topic, PubSubSub, None] = None,
        *,
        batch_topic: Union[Topic, PubSubBatchSub, None] = None,
        # broker arguments
        dependencies: Sequence[Depends] = (),
        parser: Optional[CustomParser[AnyPubSubDict, PubSubMessage]] = None,
        decoder: Optional[CustomDecoder[PubSubMessage]] = None,
        middlewares: Optional[
            Sequence[Callable[[AnyPubSubDict], BaseMiddleware]]
        ] = None,
        filter: Filter[PubSubMessage] = default_filter,
        # AsyncAPI information
        title: Optional[str] = None,
        description: Optional[str] = None,
        include_in_schema: bool = True,
        **original_kwargs: Any,
    ) -> Callable[
        [Callable[P_HandlerParams, T_HandlerReturn]],
        HandlerCallWrapper[Any, P_HandlerParams, T_HandlerReturn],
    ]:
        topic = PubSubSub.validate(topic)
        batch_topic = PubSubBatchSub.validate(batch_topic)

        if (any_of := topic or batch_topic) is None:
            raise ValueError(INCORRECT_SETUP_MSG)

        if all((topic, batch_topic)):
            raise ValueError("You can't use `PubSubSub` and `PubSubBatchSub` both")

        self._setup_log_context(topic=any_of.name)
        super().subscriber()

        key = any_of.name
        handler = self.handlers[key] = self.handlers.get(
            key,
            Handler(  # type: ignore[abstract]
                log_context_builder=partial(
                    self._get_log_context,
                    topic=any_of.name,
                ),
                graceful_timeout=self.graceful_timeout,
                # Pub/Sub
                subscription=topic,
                batch_subscription=batch_topic,
                # AsyncAPI
                title=title,
                description=description,
                include_in_schema=include_in_schema,
            ),
        )

        def consumer_wrapper(
            func: Callable[P_HandlerParams, T_HandlerReturn],
        ) -> HandlerCallWrapper[
            AnyPubSubDict,
            P_HandlerParams,
            T_HandlerReturn,
        ]:
            handler_call, dependant = self._wrap_handler(
                func,
                extra_dependencies=dependencies,
                **original_kwargs,
            )

            handler.add_call(
                handler=handler_call,
                filter=filter,
                middlewares=middlewares,
                parser=parser or self._global_parser,  # type: ignore[arg-type]
                decoder=decoder or self._global_decoder,  # type: ignore[arg-type]
                dependant=dependant,
            )

            return handler_call

        return consumer_wrapper

    @override
    def publisher(  # type: ignore[override]
        self,
        topic: Union[Topic, PubSubSub, None] = None,
        batch_topic: Union[Topic, PubSubBatchSub, None] = None,
        headers: Optional[AnyDict] = None,
        reply_to: str = "",
        # AsyncAPI information
        title: Optional[str] = None,
        description: Optional[str] = None,
        schema: Optional[Any] = None,
        include_in_schema: bool = True,
    ) -> Publisher:
        topic = PubSubSub.validate(topic)
        batch_topic = PubSubBatchSub.validate(batch_topic)

        any_of = topic or batch_topic
        if any_of is None:
            raise ValueError(INCORRECT_SETUP_MSG)

        key = any_of.name
        publisher = self._publishers.get(
            key,
            Publisher(
                topic=topic,
                batch_topic=batch_topic,
                headers=headers,
                reply_to=reply_to,
                # AsyncAPI
                title=title,
                _description=description,
                _schema=schema,
                include_in_schema=include_in_schema,
            ),
        )
        super().publisher(key, publisher)
        if self._producer is not None:
            publisher._producer = self._producer
        return publisher

    @override
    async def publish(  # type: ignore[override]
        self,
        *args: Any,
        **kwargs: Any,
    ) -> Optional[DecodedMessage]:
        assert self._producer, NOT_CONNECTED_YET  # nosec B101
        return await self._producer.publish(*args, **kwargs)

    async def publish_batch(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        assert self._producer, NOT_CONNECTED_YET  # nosec B101
        return await self._producer.publish_batch(*args, **kwargs)
