import re
from typing import Any, Optional, Sequence, Union

from faststream.broker.test import TestBroker, call_handler
from faststream.broker.wrapper import HandlerCallWrapper
from faststream.gcp.asyncapi import Publisher
from faststream.gcp.broker import PubSubBroker
from faststream.gcp.message import AnyPubSubDict
from faststream.gcp.producer import PubSubFastProducer
from faststream.gcp.schemas import INCORRECT_SETUP_MSG
from faststream.types import AnyDict, SendableMessage

__all__ = ("TestPubSubBroker",)


class TestPubSubBroker(TestBroker[PubSubBroker]):
    """A class to test Pub/Sub brokers."""

    @staticmethod
    def patch_publisher(
        broker: PubSubBroker,
        publisher: Any,
    ) -> None:
        publisher._producer = broker._producer

    @staticmethod
    def create_publisher_fake_subscriber(
        broker: PubSubBroker,
        publisher: Publisher,
    ) -> HandlerCallWrapper[Any, Any, Any]:
        @broker.subscriber(
            topic=publisher.topic,
            batch_topic=publisher.batch_topic,
            _raw=True,
        )
        def f(msg: Any) -> None:
            pass

        return f

    @staticmethod
    async def _fake_connect(
        broker: PubSubBroker,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        broker._producer = FakeProducer(broker)  # type: ignore[assignment]

    @staticmethod
    def remove_publisher_fake_subscriber(
        broker: PubSubBroker,
        publisher: Publisher,
    ) -> None:
        any_of = publisher.topic or publisher.batch_topic
        assert any_of  # nosec B101
        broker.handlers.pop(any_of.name, None)


class FakeProducer(PubSubFastProducer):
    def __init__(self, broker: PubSubBroker) -> None:
        self.broker = broker

    async def publish(
        self,
        message: SendableMessage,
        topic: Optional[str] = None,
        reply_to: str = "",
        headers: Optional[AnyDict] = None,
        correlation_id: Optional[str] = None,
        *,
        batch_topic: Optional[str] = None,
        rpc: bool = False,
        rpc_timeout: Optional[float] = 30.0,
        raise_timeout: bool = False,
    ) -> Optional[Any]:
        any_of = topic or batch_topic
        if any_of is None:
            raise ValueError(INCORRECT_SETUP_MSG)

        for handler in self.broker.handlers.values():  # pragma: no branch
            call = False
            batch = False

            if topic and (sub := handler.subscription) is not None:
                call = bool(
                    (not sub.pattern and sub.name == topic)
                    or (
                        sub.pattern
                        and re.match(
                            sub.name.replace(".", "\\.").replace("*", ".*"),
                            topic,
                        )
                    )
                )

            if batch_topic and (batch_sub := handler.batch_subscription) is not None:
                batch = True
                call = batch_topic == batch_sub.name

            if call:
                r = await call_handler(
                    handler=handler,
                    message=build_message(
                        message=[message] if batch else message,
                        topic=any_of,
                        headers=headers,
                        correlation_id=correlation_id,
                        reply_to=reply_to,
                    ),
                    rpc=rpc,
                    rpc_timeout=rpc_timeout,
                    raise_timeout=raise_timeout,
                )

                if rpc:  # pragma: no branch
                    return r

        return None

    async def publish_batch(
        self,
        *msgs: SendableMessage,
        batch_topic: str,
    ) -> None:
        for handler in self.broker.handlers.values():  # pragma: no branch
            if handler.batch_subscription and handler.batch_subscription.name == batch_topic:
                await call_handler(
                    handler=handler,
                    message=build_message(
                        message=msgs,
                        topic=batch_topic,
                    ),
                )

        return None


def build_message(
    message: Union[Sequence[SendableMessage], SendableMessage],
    topic: str,
    *,
    reply_to: str = "",
    correlation_id: Optional[str] = None,
    headers: Optional[AnyDict] = None,
) -> AnyPubSubDict:
    return AnyPubSubDict(
        topic=topic,
        data=message,
        type="message",
        message_id=correlation_id,
        headers=headers,
    )
