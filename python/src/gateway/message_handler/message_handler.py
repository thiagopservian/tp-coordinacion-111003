import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        self._client_id = str(uuid.uuid4())
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        data_msg = message_protocol.internal.DataMessage(self._client_id, fruit, amount)
        return message_protocol.internal.serialize(data_msg.to_dict())

    def serialize_eof_message(self, message):
        eof_msg = message_protocol.internal.EOFMessage(self._client_id)
        return message_protocol.internal.serialize(eof_msg.to_dict())

    def deserialize_result_message(self, message):
        payload = message_protocol.internal.deserialize(message)
        try:
            result_msg = message_protocol.internal.parse_message(payload)
        except (TypeError, ValueError, KeyError):
            return None
        if result_msg.message_type != message_protocol.internal.InternalMessageType.RESULT:
            return None
        if result_msg.client_id != self._client_id:
            return None
        return result_msg.fruit_top
