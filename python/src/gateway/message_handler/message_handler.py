import uuid
import logging

from common import message_protocol


class MessageHandler:

    def __init__(self):
        # Se agrega el client_id para poder identificar por cliente los mensajes
        self.client_id = uuid.uuid4().hex
        logging.info(f"Nueva sesion de cliente: {self.client_id}")

    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize(
            message_protocol.internal.data_message(self.client_id, fruit, amount)
        )

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize(
            message_protocol.internal.eof_message(self.client_id)
        )

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        return fields["top"]
