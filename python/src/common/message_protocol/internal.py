import json

from .internal_messages import (
    AggregationEOFBarrierMessage,
    DataMessage,
    EOFMessage,
    InternalMessageType,
    PartialResultMessage,
    ResultMessage,
    SumEOFBarrierMessage,
    parse_message,
)


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))


__all__ = [
    "serialize",
    "deserialize",
    "parse_message",
    "InternalMessageType",
    "DataMessage",
    "EOFMessage",
    "SumEOFBarrierMessage",
    "AggregationEOFBarrierMessage",
    "PartialResultMessage",
    "ResultMessage",
]
