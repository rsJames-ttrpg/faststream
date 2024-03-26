from typing import Optional, Pattern

from pydantic import PositiveFloat, PositiveInt

from faststream._compat import PYDANTIC_V2
from faststream.broker.schemas import NameRequired
from faststream.utils.path import compile_path


class PubSubSub(NameRequired):
    """A class to represent a Google Cloud Pub/Sub subscription."""

    polling_interval: PositiveFloat = 1.0
    path_regex: Optional[Pattern[str]] = None
    pattern: bool = False

    def __init__(
        self,
        topic: str,
        subscription: str,
        pattern: bool = False,
        polling_interval: PositiveFloat = 1.0,
    ) -> None:
        """Google Cloud Pub/Sub subscription parameters.

        Args:
            topic: (str): Pub/Sub topic name.
            subscription: (str): Pub/Sub subscription name.
            pattern: (bool): use pattern matching.
            polling_interval: (float): wait message block.
        """
        reg, path = compile_path(
            topic,
            replace_symbol="*",
            patch_regex=lambda x: x.replace(r"\*", ".*"),
        )

        if reg is not None:
            pattern = True

        super().__init__(
            name=subscription,
            path_regex=reg,
            pattern=pattern,
            polling_interval=polling_interval,
        )

    if PYDANTIC_V2:
        model_config = {"arbitrary_types_allowed": True}
    else:

        class Config:
            arbitrary_types_allowed = True

    def __hash__(self) -> int:
        return hash("pubsub" + self.name)


class PubSubBatchSub(NameRequired):
    """A class to represent a Google Cloud Pub/Sub batch subscriber."""

    polling_interval: PositiveFloat = 0.1
    max_records: PositiveInt = 10

    def __init__(
        self,
        subscription: str,
        max_records: PositiveInt = 10,
        polling_interval: PositiveFloat = 0.1,
    ) -> None:
        """Google Cloud Pub/Sub batch subscriber parameters.

        Args:
            subscription: (str): Pub/Sub subscription name.
            max_records: (int): max records per batch.
            polling_interval: (float): wait message block.
        """
        super().__init__(
            name=subscription,
            max_records=max_records,
            polling_interval=polling_interval,
        )

    @property
    def records(self) -> Optional[PositiveInt]:
        return self.max_records

    def __hash__(self) -> int:
        return hash("pubsub_batch" + self.name)


INCORRECT_SETUP_MSG = "You have to specify `subscription`"
