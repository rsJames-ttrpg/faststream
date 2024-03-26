import asyncio
from contextlib import suppress
from functools import partial
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Hashable,
    List,
    Optional,
    Sequence,
    Union,
)

import anyio
from fast_depends.core import CallModel
from google.cloud import pubsub_v1
from typing_extensions import override

from faststream._compat import json_loads
from faststream.broker.handler import AsyncHandler
from faststream.broker.message import StreamMessage
from faststream.broker.middlewares import BaseMiddleware
from faststream.broker.parsers import resolve_custom_func
from faststream.broker.types import (
    CustomDecoder,
    CustomParser,
    Filter,
    P_HandlerParams,
    T_HandlerReturn,
)
from faststream.broker.wrapper import HandlerCallWrapper
from faststream.gcp.message import (
    AnyPubSubDict,
    PubSubMessage,
)
from faststream.gcp.parser import PubSubParser
from faststream.gcp.schemas import INCORRECT_SETUP_MSG, PubSubBatchSub, PubSubSub


class LogicPubSubHandler(AsyncHandler[AnyPubSubDict]):
    """A class to represent a Pub/Sub handler."""

    subscriber: Optional[pubsub_v1.SubscriberClient]
    task: Optional["asyncio.Task[Any]"]

    def __init__(
        self,
        *,
        log_context_builder: Callable[[StreamMessage[Any]], Dict[str, str]],
        graceful_timeout: Optional[float] = None,
        # Pub/Sub info
        subscription: Optional[PubSubSub] = None,
        batch_subscription: Optional[PubSubBatchSub] = None,
        # AsyncAPI information
        description: Optional[str] = None,
        title: Optional[str] = None,
        include_in_schema: bool = True,
    ) -> None:
        """Initialize the Pub/Sub handler.

        Args:
            log_context_builder: The log context builder.
            graceful_timeout: The graceful timeout.
            subscription: The subscription.
            batch_subscription: The batch subscription.
            description: The description.
            title: The title.
            include_in_schema: Whether to include in schema.
        """
        self.subscription = subscription
        self.batch_subscription = batch_subscription

        self.subscriber = None
        self.task = None

        super().__init__(
            log_context_builder=log_context_builder,
            description=description,
            title=title,
            include_in_schema=include_in_schema,
            graceful_timeout=graceful_timeout,
        )

    @property
    def subscription_name(self) -> str:
        any_of = self.subscription or self.batch_subscription
        assert any_of, INCORRECT_SETUP_MSG  # nosec B101
        return any_of.name

    def add_call(
        self,
        *,
        handler: HandlerCallWrapper[AnyPubSubDict, P_HandlerParams, T_HandlerReturn],
        dependant: CallModel[P_HandlerParams, T_HandlerReturn],
        parser: Optional[CustomParser[AnyPubSubDict, PubSubMessage]],
        decoder: Optional[CustomDecoder[PubSubMessage]],
        filter: Filter[PubSubMessage],
        middlewares: Optional[Sequence[Callable[[AnyPubSubDict], BaseMiddleware]]],
    ) -> None:
        super().add_call(
            handler=handler,
            parser=resolve_custom_func(parser, PubSubParser.parse_message),  # type: ignore[arg-type]
            decoder=resolve_custom_func(decoder, PubSubParser.decode_message),
            filter=filter,  # type: ignore[arg-type]
            dependant=dependant,
            middlewares=middlewares,
        )

    @override
    async def start(self) -> None:  # type: ignore[override]
        self.started = anyio.Event()

        consume: Union[
            Callable[[], Awaitable[Optional[AnyPubSubDict]]],
            Callable[[], Awaitable[Optional[Sequence[AnyPubSubDict]]]],
        ]
        sleep: float

        if (batch_subscription := self.batch_subscription) is not None:
            sleep = batch_subscription.polling_interval
            consume = partial(
                self._consume_batch_msg,
            )
            self.started.set()

        elif (subscription := self.subscription) is not None:
            self.subscriber = pubsub_v1.SubscriberClient()
            consume = partial(
                self._consume_msg,
            )
            sleep = subscription.polling_interval
            self.started.set()

        else:
            raise AssertionError("unreachable")

        await super().start()
        self.task = asyncio.create_task(self._consume(consume, sleep))
        # wait until Pub/Sub starts to consume
        await anyio.sleep(0.01)
        await self.started.wait()

    async def close(self) -> None:
        await super().close()

        if self.task is not None:
            if not self.task.done():
                self.task.cancel()
            self.task = None

        if self.subscriber is not None:
            await self.subscriber.close()
            self.subscriber = None

    @staticmethod
    def get_routing_hash(subscription: Hashable) -> int:
        return hash(subscription)

    async def _consume(
        self,
        consume: Union[
            Callable[[], Awaitable[Optional[AnyPubSubDict]]],
            Callable[[], Awaitable[Optional[Sequence[AnyPubSubDict]]]],
        ],
        sleep: float,
    ) -> None:
        connected = True

        while self.running:
            with suppress(Exception):
                try:
                    m = await consume()

                except Exception:
                    if connected is True:
                        connected = False
                    await anyio.sleep(5)

                else:
                    if connected is False:
                        connected = True

                    if msgs := (
                        (m,) if isinstance(m, dict) else m
                    ):  # pragma: no branch
                        for i in msgs:
                            await self.consume(i)

                    else:
                        await anyio.sleep(sleep)

    async def _consume_msg(
        self,
    ) -> Optional[AnyPubSubDict]:
        subscription = self.subscription
        assert subscription  # nosec B101
        assert self.subscriber  # nosec B101

        response = self.subscriber.pull(
            subscription=subscription.name,
            max_messages=1,
            return_immediately=True,
        )

        if response.received_messages:
            message = response.received_messages[0].message
            data = message.data
            with suppress(Exception):
                data = json_loads(data)

            return AnyPubSubDict(
                type="message",
                topic=subscription.name,
                data=data,
                message_id=message.message_id,
            )

        return None

    async def _consume_batch_msg(
        self,
    ) -> Optional[AnyPubSubDict]:
        subscription = self.batch_subscription
        assert subscription  # nosec B101
        assert self.subscriber  # nosec B101

        response = self.subscriber.pull(
            subscription=subscription.name,
            max_messages=subscription.max_records,
            return_immediately=True,
        )

        if response.received_messages:
            parsed: List[Any] = []
            ids = []
            for message in response.received_messages:
                ids.append(message.message.message_id)
                data = message.message.data
                with suppress(Exception):
                    data = json_loads(data)
                parsed.append(data)

            return AnyPubSubDict(
                type="batch",
                topic=subscription.name,
                data=parsed,
                message_id=ids[0],
                message_ids=ids,
            )

        return None
