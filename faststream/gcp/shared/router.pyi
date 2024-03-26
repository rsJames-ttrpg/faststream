from typing import (
    Any,
    Callable,
    Sequence,
)

from fast_depends.dependencies import Depends
from typing_extensions import TypeAlias

from faststream.broker.core.asynchronous import default_filter
from faststream.broker.middlewares import BaseMiddleware
from faststream.broker.types import (
    CustomDecoder,
    CustomParser,
    Filter,
    T_HandlerReturn,
)
from faststream.gcp.message import AnyPubSubDict, PubSubMessage
from faststream.gcp.schemas import PubSubBatchSub, PubSubSub

Topic: TypeAlias = str

class PubSubRoute:
    """Delayed `PubSubBroker.subscriber()` registration object."""

    def __init__(
        self,
        call: Callable[..., T_HandlerReturn],
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
    ) -> None: ...
