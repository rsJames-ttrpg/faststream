from typing import TYPE_CHECKING, Any, List, Literal, Optional, TypeVar, Union

from typing_extensions import NotRequired, TypedDict, override

from faststream.broker.message import StreamMessage
from faststream.utils.context.repository import context

if TYPE_CHECKING:
    from google.cloud import pubsub_v1

    from faststream.gcp.asyncapi import Handler


class PubSubMessage(TypedDict):
    """A class to represent a PubSub message."""
    topic: str
    data: Union[bytes, List[bytes]]
    type: str
    message_id: NotRequired[str]
    message_ids: NotRequired[List[str]]

class OneMessage(PubSubMessage):
    """A class to represent a PubSub message."""
    type: Literal["stream", "list", "message"]  # type: ignore[misc]
    data: bytes  # type: ignore[misc]

class BatchMessage(PubSubMessage):
    """A class to represent a PubSub message."""
    type: Literal["batch"]  # type: ignore[misc]
    data: List[bytes]  # type: ignore[misc]

class AnyPubSubDict(PubSubMessage):
    """A class to represent a PubSub message."""
    type: Literal["stream", "list", "message", "batch"]  # type: ignore[misc]
    data: Union[bytes, List[bytes]]  # type: ignore[misc]

MsgType = TypeVar("MsgType", OneMessage, BatchMessage, AnyPubSubDict)

class PubSubAckMixin(StreamMessage[MsgType]):
    """A class to represent a PubSub ACK mixin."""

    @override
    async def ack(  # type: ignore[override]
        self,
        subscriber: "pubsub_v1.SubscriberClient",
        **kwargs: Any,
    ) -> None:
        handler: Optional["Handler"]
        if (
            not self.committed
            and (ids := self.raw_message.get("message_ids"))
            and (handler := context.get_local("handler_")) is not None
            and (sub := handler.subscription)
        ):
            subscriber.acknowledge(sub, ids)  # type: ignore[no-untyped-call]
        await super().ack()

class PubSubMessage(PubSubAckMixin[AnyPubSubDict]):
    """A class to represent a PubSub message."""
    pass

class OnePubSubMessage(PubSubAckMixin[OneMessage]):
    """A class to represent a PubSub message."""
    pass

class BatchPubSubMessage(PubSubAckMixin[BatchMessage]):
    """A class to represent a PubSub batch of messages."""
    pass
