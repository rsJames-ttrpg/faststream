from dataclasses import dataclass, field
from typing import Optional, Sequence, Union, cast

from typing_extensions import override

from faststream.broker.publisher import BasePublisher
from faststream.exceptions import NOT_CONNECTED_YET
from faststream.gcp.message import AnyPubSubDict
from faststream.gcp.producer import PubSubFastProducer
from faststream.gcp.schemas import INCORRECT_SETUP_MSG, PubSubBatchSub, PubSubSub
from faststream.types import AnyDict, DecodedMessage, SendableMessage


@dataclass
class LogicPublisher(BasePublisher[AnyPubSubDict]):
    """A class to represent a Pub/Sub publisher."""

    topic: Optional[PubSubSub] = field(default=None)
    batch_topic: Optional[PubSubBatchSub] = field(default=None)
    reply_to: str = field(default="")
    headers: Optional[AnyDict] = field(default=None)
    _producer: Optional[PubSubFastProducer] = field(default=None, init=False)

    @override
    async def publish(  # type: ignore[override]
        self,
        message: SendableMessage,
        topic: Union[str, PubSubSub, None] = None,
        reply_to: str = "",
        headers: Optional[AnyDict] = None,
        correlation_id: Optional[str] = None,
        *,
        batch_topic: Union[str, PubSubBatchSub, None] = None,
        rpc: bool = False,
        rpc_timeout: Optional[float] = 30.0,
        raise_timeout: bool = False,
    ) -> Optional[DecodedMessage]:
        assert self._producer, NOT_CONNECTED_YET  # nosec B101

        topic = PubSubSub.validate(topic or self.topic)
        batch_topic = PubSubBatchSub.validate(batch_topic or self.batch_topic)

        assert any((topic, batch_topic)), "You have to specify outgoing topic"  # nosec B101

        headers_to_send = (self.headers or {}).copy()
        if headers is not None:
            headers_to_send.update(headers)

        if batch_topic:
            await self._producer.publish_batch(
                *cast(Sequence[SendableMessage], message),
                topic=batch_topic.name,  # type: ignore[union-attr]
            )
            return None
        else:
            return await self._producer.publish(
                message=message,
                topic=topic.name,  # type: ignore[union-attr]
                reply_to=reply_to or self.reply_to,
                correlation_id=correlation_id,
                headers=headers_to_send,
                rpc=rpc,
                rpc_timeout=rpc_timeout,
                raise_timeout=raise_timeout,
            )

    @property
    def topic_name(self) -> str:
        any_of = self.topic or self.batch_topic
        assert any_of, INCORRECT_SETUP_MSG  # nosec B101
        return any_of.name
