from typing import Any, Callable, Optional, Sequence, Union

from fastapi import Depends
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
from typing_extensions import Annotated, override

from faststream.broker.fastapi.context import Context, ContextRepo, Logger
from faststream.broker.fastapi.router import StreamRouter
from faststream.broker.types import P_HandlerParams, T_HandlerReturn
from faststream.broker.wrapper import HandlerCallWrapper
from faststream.gcp.broker import PubSubBroker as PSB
from faststream.gcp.message import AnyPubSubDict
from faststream.gcp.message import PubSubMessage as PSM
from faststream.gcp.schemas import INCORRECT_SETUP_MSG, PubSubBatchSub, PubSubSub

__all__ = (
    "Context",
    "Logger",
    "ContextRepo",
    "PubSubRouter",
    "PubSubMessage",
    "PubSubBroker",
    "Publisher",
    "Subscriber",
)

PubSubMessage = Annotated[PSM, Context("message")]
PubSubBroker = Annotated[PSB, Context("broker")]
Publisher = Annotated[PublisherClient, Context("broker._connection[0]")]
Subscriber = Annotated[SubscriberClient, Context("broker._connection[1]")]

class PubSubRouter(StreamRouter[AnyPubSubDict]):
    """A class to represent a Pub/Sub router."""

    broker_class = PSB

    def subscriber(
        self,
        topic: Union[str, PubSubSub, None] = None,
        *,
        batch_topic: Union[str, PubSubBatchSub, None] = None,
        dependencies: Optional[Sequence[Depends]] = None,
        **broker_kwargs: Any,
    ) -> Callable[
        [Callable[P_HandlerParams, T_HandlerReturn]],
        HandlerCallWrapper[AnyPubSubDict, P_HandlerParams, T_HandlerReturn],
    ]:
        topic = PubSubSub.validate(topic)
        batch_topic = PubSubBatchSub.validate(batch_topic)

        if (any_of := topic or batch_topic) is None:
            raise ValueError(INCORRECT_SETUP_MSG)

        return super().subscriber(
            path=any_of.name,
            topic=topic,
            batch_topic=batch_topic,
            dependencies=dependencies,
            **broker_kwargs,
        )

    @override
    @staticmethod
    def _setup_log_context(  # type: ignore[override]
        main_broker: PSB,
        including_broker: PSB,
    ) -> None:
        for h in including_broker.handlers.values():
            main_broker._setup_log_context(h.subscription_name)
