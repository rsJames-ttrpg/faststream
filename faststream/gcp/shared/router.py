from typing import Any, Callable, Sequence, Union

from typing_extensions import TypeAlias, override

from faststream._compat import model_copy
from faststream.broker.router import BrokerRoute as PubSubRoute
from faststream.broker.router import BrokerRouter
from faststream.broker.types import P_HandlerParams, T_HandlerReturn
from faststream.broker.wrapper import HandlerCallWrapper
from faststream.gcp.message import AnyPubSubDict
from faststream.gcp.schemas import PubSubBatchSub, PubSubSub
from faststream.types import SendableMessage

__all__ = (
    "PubSubRouter",
    "PubSubRoute",
)

Topic: TypeAlias = str

class PubSubRouter(BrokerRouter[str, AnyPubSubDict]):
    """A class to represent a Pub/Sub router."""

    def __init__(
        self,
        prefix: str = "",
        handlers: Sequence[PubSubRoute[AnyPubSubDict, SendableMessage]] = (),
        **kwargs: Any,
    ) -> None:
        """Initialize the Pub/Sub router.

        Args:
            prefix: The prefix.
            handlers: The handlers.
            **kwargs: The keyword arguments.
        """
        for h in handlers:
            if topic := h.kwargs.pop("topic", None):
                topic, h.args = h.args[0], h.args[1:]
                h.args = (prefix + topic, *h.args)
            elif batch_topic := h.kwargs.pop("batch_topic", None):
                h.kwargs["batch_topic"] = prefix + batch_topic

        super().__init__(prefix, handlers, **kwargs)

    @override
    def subscriber(  # type: ignore[override]
        self,
        topic: Union[Topic, PubSubSub, None] = None,
        *,
        batch_topic: Union[Topic, PubSubBatchSub, None] = None,
        **broker_kwargs: Any,
    ) -> Callable[
        [Callable[P_HandlerParams, T_HandlerReturn]],
        HandlerCallWrapper[AnyPubSubDict, P_HandlerParams, T_HandlerReturn],
    ]:
        topic = PubSubSub.validate(topic)
        batch_topic = PubSubBatchSub.validate(batch_topic)

        return self._wrap_subscriber(
            subscription=model_copy(topic, update={"name": self.prefix + topic.name})
            if topic
            else None,
            batch_subscription=model_copy(batch_topic, update={"name": self.prefix + batch_topic.name})
            if batch_topic
            else None,
            **broker_kwargs,
        )
