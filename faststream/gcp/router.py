from typing import Any, Dict, Optional, Union

from typing_extensions import override

from faststream._compat import model_copy
from faststream.gcp.asyncapi import Publisher
from faststream.gcp.schemas import INCORRECT_SETUP_MSG, PubSubBatchSub, PubSubSub
from faststream.gcp.shared.router import PubSubRouter as BaseRouter
from faststream.types import AnyDict


class PubSubRouter(BaseRouter):
    """A class to represent a Pub/Sub router."""

    _publishers: Dict[str, Publisher]  # type: ignore[assignment]

    @override
    @staticmethod
    def _get_publisher_key(publisher: Publisher) -> str:  # type: ignore[override]
        any_of = publisher.topic or publisher.batch_topic
        if any_of is None:
            raise ValueError(INCORRECT_SETUP_MSG)
        return any_of.name

    @override
    @staticmethod
    def _update_publisher_prefix(  # type: ignore[override]
        prefix: str,
        publisher: Publisher,
    ) -> Publisher:
        if publisher.topic is not None:
            publisher.topic = model_copy(
                publisher.topic, update={"name": prefix + publisher.topic.name}
            )
        elif publisher.batch_topic is not None:
            publisher.batch_topic = model_copy(
                publisher.batch_topic, update={"name": prefix + publisher.batch_topic.name}
            )
        else:
            raise AssertionError("unreachable")
        return publisher

    @override
    def publisher(  # type: ignore[override]
        self,
        topic: Union[str, PubSubSub, None] = None,
        batch_topic: Union[str, PubSubBatchSub, None] = None,
        headers: Optional[AnyDict] = None,
        reply_to: str = "",
        # AsyncAPI information
        title: Optional[str] = None,
        description: Optional[str] = None,
        schema: Optional[Any] = None,
        include_in_schema: bool = True,
    ) -> Publisher:
        if not any((topic, batch_topic)):
            raise ValueError(INCORRECT_SETUP_MSG)

        new_publisher = self._update_publisher_prefix(
            self.prefix,
            Publisher(
                topic=PubSubSub.validate(topic),
                batch_topic=PubSubBatchSub.validate(batch_topic),
                reply_to=reply_to,
                headers=headers,
                title=title,
                _description=description,
                _schema=schema,
                include_in_schema=(
                    include_in_schema
                    if self.include_in_schema is None
                    else self.include_in_schema
                ),
            ),
        )

        publisher_key = self._get_publisher_key(new_publisher)
        publisher = self._publishers[publisher_key] = self._publishers.get(
            publisher_key, new_publisher
        )

        return publisher
