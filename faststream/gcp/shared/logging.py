import logging
from typing import Any, Optional

from typing_extensions import override

from faststream.broker.core.mixins import LoggingMixin
from faststream.broker.message import StreamMessage
from faststream.log import access_logger
from faststream.types import AnyDict


class PubSubLoggingMixin(LoggingMixin):
    """A class to represent a Pub/Sub logging mixin."""

    _max_topic_name: int

    def __init__(
        self,
        *args: Any,
        logger: Optional[logging.Logger] = access_logger,
        log_level: int = logging.INFO,
        log_fmt: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the Pub/Sub logging mixin.

        Args:
            *args: The arguments.
            logger: The logger.
            log_level: The log level.
            log_fmt: The log format.
            **kwargs: The keyword arguments.
        """
        super().__init__(
            *args,
            logger=logger,
            log_level=log_level,
            log_fmt=log_fmt,
            **kwargs,
        )
        self._message_id_ln = 15
        self._max_topic_name = 4

    @override
    def _get_log_context(  # type: ignore[override]
        self,
        message: Optional[StreamMessage[Any]],
        topic: str,
    ) -> AnyDict:
        return {
            "topic": topic,
            **super()._get_log_context(message),
        }

    @property
    def fmt(self) -> str:
        return self._fmt or (
            "%(asctime)s %(levelname)s - "
            f"%(topic)-{self._max_topic_name}s | "
            f"%(message_id)-{self._message_id_ln}s - %(message)s"
        )

    def _setup_log_context(
        self,
        topic: Optional[str] = None,
    ) -> None:
        if topic is not None:  # pragma: no branch
            self._max_topic_name = max((self._max_topic_name, len(topic)))
