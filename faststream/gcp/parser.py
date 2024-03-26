from typing import TYPE_CHECKING, Optional, Sequence, Tuple, Union, overload
from uuid import uuid4

from pydantic import BaseModel, Field

from faststream._compat import dump_json, model_parse, model_to_json
from faststream.broker.parsers import decode_message, encode_message
from faststream.gcp.message import (
    BatchMessage,
    BatchPubSubMessage,
    OneMessage,
    OnePubSubMessage,
)
from faststream.types import AnyDict, DecodedMessage, SendableMessage
from faststream.utils.context.repository import context

if TYPE_CHECKING:
    from faststream.gcp.asyncapi import Handler

DATA_KEY = "__data__"
bDATA_KEY = DATA_KEY.encode()  # noqa: N816


class RawMessage(BaseModel):
    """A class to represent a raw Pub/Sub message."""

    data: bytes
    headers: AnyDict = Field(default_factory=dict)

    @classmethod
    def build(
        cls,
        message: Union[Sequence[SendableMessage], SendableMessage],
        reply_to: str = "",
        headers: Optional[AnyDict] = None,
        correlation_id: Optional[str] = None,
    ) -> "RawMessage":
        payload, content_type = encode_message(message)

        headers_to_send = {
            "correlation_id": correlation_id or str(uuid4()),
        }

        if content_type:
            headers_to_send["content-type"] = content_type

        if reply_to:
            headers_to_send["reply_to"] = reply_to

        if headers is not None:
            headers_to_send.update(headers)

        return cls(
            data=payload,
            headers=headers_to_send,
        )

    @classmethod
    def encode(
        cls,
        message: Union[Sequence[SendableMessage], SendableMessage],
        reply_to: str = "",
        headers: Optional[AnyDict] = None,
        correlation_id: Optional[str] = None,
    ) -> str:
        return model_to_json(
            cls.build(
                message=message,
                reply_to=reply_to,
                headers=headers,
                correlation_id=correlation_id,
            )
        )


class PubSubParser:
    """A class to represent a Pub/Sub parser."""

    @classmethod
    @overload
    async def parse_message(
        cls,
        message: OneMessage,
    ) -> OnePubSubMessage:
        pass

    @classmethod
    @overload
    async def parse_message(
        cls,
        message: BatchMessage,
    ) -> BatchPubSubMessage:
        pass

    @classmethod
    async def parse_message(
        cls,
        message: Union[OneMessage, BatchMessage],
    ) -> Union[OnePubSubMessage, BatchPubSubMessage]:
        id_ = str(uuid4())

        if message["type"] == "batch":
            data = dump_json([cls.parse_one_msg(x)[0] for x in message["data"]])

            return BatchPubSubMessage(
                raw_message=message,
                body=data,
                content_type="application/json",
                message_id=id_,
                correlation_id=id_,
            )

        else:
            data, headers = cls.parse_one_msg(message["data"])

            handler: Optional["Handler"] = context.get_local("handler_")
            if (
                handler is not None
                and handler.subscription is not None
                and (path_re := handler.subscription.path_regex) is not None
                and (match := path_re.match(handler.subscription.name)) is not None
            ):
                path = match.groupdict()
            else:
                path = {}

            return OnePubSubMessage(
                raw_message=message,
                body=data,
                path=path,
                headers=headers,
                reply_to=headers.get("reply_to", ""),
                content_type=headers.get("content-type", ""),
                message_id=message.get("message_id", id_),
                correlation_id=headers.get("correlation_id", id_),
            )

    @staticmethod
    def parse_one_msg(raw_data: bytes) -> Tuple[bytes, AnyDict]:
        try:
            obj = model_parse(RawMessage, raw_data)
        except Exception:
            # Raw Pub/Sub message format
            data = raw_data
            headers: AnyDict = {}
        else:
            # FastStream message format
            data = obj.data
            headers = obj.headers

        return data, headers

    @staticmethod
    async def decode_message(
        msg: OnePubSubMessage,
    ) -> DecodedMessage:
        return decode_message(msg)
