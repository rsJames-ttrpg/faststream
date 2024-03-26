import logging
from enum import Enum
from typing import (
    Any,
    Callable,
    Sequence,
)

from fast_depends.dependencies import Depends
from fastapi import params
from fastapi.datastructures import Default
from fastapi.routing import APIRoute
from fastapi.utils import generate_unique_id
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
from starlette import routing
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Lifespan
from typing_extensions import Annotated, TypeAlias, override

from faststream.asyncapi import schema as asyncapi
from faststream.broker.core.asynchronous import default_filter
from faststream.broker.fastapi.context import Context, ContextRepo, Logger
from faststream.broker.fastapi.router import StreamRouter
from faststream.broker.middlewares import BaseMiddleware
from faststream.broker.types import (
    CustomDecoder,
    CustomParser,
    Filter,
    P_HandlerParams,
    T_HandlerReturn,
)
from faststream.broker.wrapper import HandlerCallWrapper
from faststream.gcp.asyncapi import Publisher as GCP_Publisher
from faststream.gcp.broker import PubSubBroker as PSB
from faststream.gcp.message import AnyPubSubDict
from faststream.gcp.message import PubSubMessage as PSM
from faststream.gcp.schemas import PubSubBatchSub, PubSubSub
from faststream.log import access_logger
from faststream.security import BaseSecurity
from faststream.types import AnyDict

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

Topic: TypeAlias = str

class PubSubRouter(StreamRouter[AnyPubSubDict]):
    broker_class: type[PSB]
    broker: PSB

    def __init__(
        self,
        project_id: str,
        *,
        security: BaseSecurity | None = None,
        # broker args
        graceful_timeout: float | None = None,
        apply_types: bool = True,
        validate: bool = True,
        parser: CustomParser[AnyPubSubDict, PSM] | None = None,
        decoder: CustomDecoder[PSM] | None = None,
        middlewares: Sequence[Callable[[AnyPubSubDict], BaseMiddleware]] | None = None,
        # AsyncAPI args
        asyncapi_url: str | None = None,
        protocol: str | None = None,
        protocol_version: str | None = "custom",
        description: str | None = None,
        asyncapi_tags: Sequence[asyncapi.Tag] | None = None,
        schema_url: str | None = "/asyncapi",
        setup_state: bool = True,
        # logging args
        logger: logging.Logger | None = access_logger,
        log_level: int = logging.INFO,
        log_fmt: str | None = None,
        # FastAPI kwargs
        prefix: str = "",
        tags: list[str | Enum] | None = None,
        dependencies: Sequence[params.Depends] | None = None,
        default_response_class: type[Response] = Default(JSONResponse),
        responses: dict[int | str, dict[str, Any]] | None = None,
        callbacks: list[routing.BaseRoute] | None = None,
        routes: list[routing.BaseRoute] | None = None,
        redirect_slashes: bool = True,
        default: ASGIApp | None = None,
        dependency_overrides_provider: Any | None = None,
        route_class: type[APIRoute] = APIRoute,
        on_startup: Sequence[Callable[[], Any]] | None = None,
        on_shutdown: Sequence[Callable[[], Any]] | None = None,
        deprecated: bool | None = None,
        include_in_schema: bool = True,
        lifespan: Lifespan[Any] | None = None,
        generate_unique_id_function: Callable[[APIRoute], str] = Default(
            generate_unique_id
        ),
    ) -> None: ...
    @override
    @staticmethod
    def _setup_log_context(  # type: ignore[override]
        main_broker: PSB,
        including_broker: PSB,
    ) -> None: ...
    @override
    def subscriber(  # type: ignore[override]
        self,
        topic: Topic | PubSubSub | None = None,
        *,
        batch_topic: Topic | PubSubBatchSub | None = None,
        # broker arguments
        dependencies: Sequence[Depends] = (),
        parser: CustomParser[AnyPubSubDict, PSM] | None = None,
        decoder: CustomDecoder[PSM] | None = None,
        middlewares: Sequence[Callable[[AnyPubSubDict], BaseMiddleware]] | None = None,
        filter: Filter[PSM] = default_filter,
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
    ) -> GCP_Publisher: ...
