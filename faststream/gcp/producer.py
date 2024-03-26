from typing import Any, Optional, Union, overload
from uuid import uuid4

from google.cloud import pubsub_v1

from faststream.broker.parsers import encode_message, resolve_custom_func
from faststream.broker.types import (
    AsyncCustomDecoder,
    AsyncCustomParser,
    AsyncDecoder,
    AsyncParser,
)
from faststream.exceptions import WRONG_PUBLISH_ARGS
from faststream.gcp.message import (
    AnyPubSubDict,
    BatchMessage,
    BatchPubSubMessage,
    OneMessage,
    OnePubSubMessage,
)
from faststream.gcp.parser import PubSubParser
from faststream.types import AnyDict, SendableMessage
from faststream.utils.functions import timeout_scope


class PubSubFastProducer:
    """A class to represent a Pub/Sub producer."""

    _publisher: "pubsub_v1.PublisherClient"
    _subscriber: "pubsub_v1.SubscriberClient"
    _decoder: AsyncDecoder[Any]
    _parser: AsyncParser[AnyPubSubDict, Any]

    @overload
    def __init__(
        self,
        parser: Optional[AsyncCustomParser[OneMessage, OnePubSubMessage]],
        decoder: Optional[AsyncCustomDecoder[OnePubSubMessage]],
    ) -> None:
        pass

    @overload
    def __init__(
        self,
        parser: Optional[AsyncCustomParser[BatchMessage, BatchPubSubMessage]],
        decoder: Optional[AsyncCustomDecoder[BatchPubSubMessage]],
    ) -> None:
        pass

    def __init__(
        self,
        parser: Union[
            None,
            AsyncCustomParser[OneMessage, OnePubSubMessage],
            AsyncCustomParser[BatchMessage, BatchPubSubMessage],
        ],
        decoder: Union[
            None,
            AsyncCustomDecoder[OnePubSubMessage],
            AsyncCustomDecoder[BatchPubSubMessage],
        ],
    ) -> None:
        """Initialize the Pub/Sub producer.

        Args:
            parser: The parser.
            decoder: The decoder.
        """
        self._publisher = pubsub_v1.PublisherClient()
        self._subscriber = pubsub_v1.SubscriberClient()
        self._parser = resolve_custom_func(
            parser,  # type: ignore[arg-type,assignment]
            PubSubParser.parse_message,
        )
        self._decoder = resolve_custom_func(decoder, PubSubParser.decode_message)

    async def publish(
        self,
        message: SendableMessage,
        topic: str,
        reply_to: str = "",
        headers: Optional[AnyDict] = None,
        correlation_id: Optional[str] = None,
        *,
        rpc: bool = False,
        rpc_timeout: Optional[float] = 30.0,
        raise_timeout: bool = False,
    ) -> Optional[Any]:
        subscription: Optional[str] = None
        if rpc:
            if reply_to:
                raise WRONG_PUBLISH_ARGS

            reply_to = str(uuid4())
            subscription = reply_to
            sub_path = self._subscriber.subscription_path(
                project="my-project", subscription=subscription
            )
            self._subscriber.create_subscription(
                name=sub_path, topic=topic, ack_deadline_seconds=60
            )

        msg = encode_message(message)[0]

        future = self._publisher.publish(topic, msg, **headers or {})
        await future.result()

        if subscription is None:
            return None

        else:
            m = None
            with timeout_scope(rpc_timeout, raise_timeout):
                response = self._subscriber.pull(
                    subscription=subscription,
                    max_messages=1,
                    return_immediately=True,
                )

                if response.received_messages:
                    m = response.received_messages[0].message

            self._subscriber.delete_subscription(subscription)

            if m is None:
                if raise_timeout:
                    raise TimeoutError()
                else:
                    return None
            else:
                return await self._decoder(await self._parser(m))

    async def publish_batch(
        self,
        *msgs: SendableMessage,
        topic: str,
    ) -> None:
        batch = [
            pubsub_v1.types.PubsubMessage(data=encode_message(msg)[0])
            for msg in msgs
        ]
        future = self._publisher.publish(topic, messages=batch)
        await future.result()
