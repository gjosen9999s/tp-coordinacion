import json

# Tipo de mensaje para protocolo interno
class MsgType:
    DATA = "data"
    EOF = "eof"
    TOP = "top"


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))


def data_message(client_id, fruit, amount):
    return {
        "type": MsgType.DATA,
        "client_id": client_id,
        "fruit": fruit,
        "amount": amount,
    }


def eof_message(client_id):
    return {"type": MsgType.EOF, "client_id": client_id}


def top_message(client_id, fruit_top):
    return {"type": MsgType.TOP, "client_id": client_id, "top": fruit_top}
