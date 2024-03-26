from typing import Dict

from faststream.asyncapi.schema import (
    Channel,
    ChannelBinding,
    CorrelationId,
    Message,
    Operation,
)
from faststream.asyncapi.schema.bindings import gcp
from faststream.asyncapi.utils import resolve_payloads
from faststream.gcp.handler import LogicPubSubHandler
from faststream.gcp.publisher import LogicPublisher


class Handler(LogicPubSubHandler):
    """A class to represent a Pub/Sub handler."""

    @property
    def name(self) -> str:
        return self._title or f"{self.subscription_name}:{self.call_name}"

    def schema(self) -> Dict[str, Channel]:
        if not self.include_in_schema:
            return {}

        payloads = self.get_payloads()

        method = None
        if self.batch_subscription is not None or self.subscription is not None:
            method = "pull"
        else:
            raise AssertionError("unreachable")

        return {
            self.name: Channel(
                description=self.description,
                subscribe=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads),
                        correlationId=CorrelationId(
                            location="$message.header#/correlation_id"
                        ),
                    ),
                ),
                bindings=ChannelBinding(
                    gcp=gcp.ChannelBinding(
                        subscription=self.subscription_name,
                        method=method,
                    )
                ),
            )
        }

class Publisher(LogicPublisher):
    """A class to represent a Pub/Sub publisher."""

    def schema(self) -> Dict[str, Channel]:
        if not self.include_in_schema:
            return {}

        payloads = self.get_payloads()

        method = None
        if self.batch_topic is not None or self.topic is not None:
            method = "publish"
        else:
            raise AssertionError("unreachable")

        return {
            self.name: Channel(
                description=self.description,
                publish=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads, "Publisher"),
                        correlationId=CorrelationId(
                            location="$message.header#/correlation_id"
                        ),
                    ),
                ),
                bindings=ChannelBinding(
                    gcp=gcp.ChannelBinding(
                        topic=self.topic_name,
                        method=method,
                    )
                ),
            )
        }

    @property
    def name(self) -> str:
        return self.title or f"{self.topic_name}:Publisher"
