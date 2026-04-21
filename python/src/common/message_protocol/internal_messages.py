from dataclasses import dataclass
from enum import Enum
from typing import ClassVar


class InternalMessageType(str, Enum):
    DATA = "data"
    EOF = "eof"
    SUM_EOF_BARRIER = "sum_eof_barrier"
    AGG_EOF_BARRIER = "agg_eof_barrier"
    PARTIAL_RESULT = "partial_result"
    RESULT = "result"


@dataclass
class InternalMessage:
    message_type: ClassVar[InternalMessageType]

    def to_dict(self):
        raise NotImplementedError


@dataclass
class DataMessage(InternalMessage):
    message_type: ClassVar[InternalMessageType] = InternalMessageType.DATA

    client_id: str
    fruit: str
    amount: int

    def to_dict(self):
        return {
            "type": self.message_type,
            "client_id": self.client_id,
            "fruit": self.fruit,
            "amount": self.amount,
        }


@dataclass
class EOFMessage(InternalMessage):
    message_type: ClassVar[InternalMessageType] = InternalMessageType.EOF

    client_id: str

    def to_dict(self):
        return {
            "type": self.message_type,
            "client_id": self.client_id,
        }


@dataclass
class SumEOFBarrierMessage(InternalMessage):
    message_type: ClassVar[InternalMessageType] = InternalMessageType.SUM_EOF_BARRIER

    client_id: str

    def to_dict(self):
        return {
            "type": self.message_type,
            "client_id": self.client_id,
        }


@dataclass
class AggregationEOFBarrierMessage(InternalMessage):
    message_type: ClassVar[InternalMessageType] = InternalMessageType.AGG_EOF_BARRIER

    client_id: str
    sum_id: int

    def to_dict(self):
        return {
            "type": self.message_type,
            "client_id": self.client_id,
            "sum_id": self.sum_id,
        }


@dataclass
class PartialResultMessage(InternalMessage):
    message_type: ClassVar[InternalMessageType] = InternalMessageType.PARTIAL_RESULT

    client_id: str
    aggregation_id: int
    fruit_top: list[list]

    def to_dict(self):
        return {
            "type": self.message_type,
            "client_id": self.client_id,
            "aggregation_id": self.aggregation_id,
            "fruit_top": self.fruit_top,
        }


@dataclass
class ResultMessage(InternalMessage):
    message_type: ClassVar[InternalMessageType] = InternalMessageType.RESULT

    client_id: str
    fruit_top: list[list]

    def to_dict(self):
        return {
            "type": self.message_type,
            "client_id": self.client_id,
            "fruit_top": self.fruit_top,
        }


def parse_message(payload):
    if not isinstance(payload, dict):
        raise TypeError("Internal message payload must be a dict")

    msg_type_value = payload.get("type")
    if msg_type_value is None:
        raise ValueError("Internal message is missing 'type' field")

    try:
        msg_type = InternalMessageType(msg_type_value)
    except ValueError as exc:
        raise ValueError(f"Unknown internal message type: {msg_type_value}") from exc

    if msg_type is InternalMessageType.DATA:
        return DataMessage(
            client_id=payload["client_id"],
            fruit=payload["fruit"],
            amount=int(payload["amount"]),
        )
    if msg_type is InternalMessageType.EOF:
        return EOFMessage(client_id=payload["client_id"])
    if msg_type is InternalMessageType.SUM_EOF_BARRIER:
        return SumEOFBarrierMessage(client_id=payload["client_id"])
    if msg_type is InternalMessageType.AGG_EOF_BARRIER:
        return AggregationEOFBarrierMessage(
            client_id=payload["client_id"],
            sum_id=int(payload["sum_id"]),
        )
    if msg_type is InternalMessageType.PARTIAL_RESULT:
        return PartialResultMessage(
            client_id=payload["client_id"],
            aggregation_id=int(payload["aggregation_id"]),
            fruit_top=payload["fruit_top"],
        )
    return ResultMessage(
        client_id=payload["client_id"],
        fruit_top=payload["fruit_top"],
    )