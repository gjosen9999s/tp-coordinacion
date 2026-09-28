import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        # TOP y contador por client = sesion
        self.fruit_top_by_client = {}
        self.eof_count_by_client = {}



    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")

        fruit_top = self.fruit_top_by_client.setdefault(client_id, [])
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                # 
                bisect.insort(
                    fruit_top,
                    fruit_top.pop(i) + fruit_item.FruitItem(fruit, amount),
                )
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))


    def _process_eof(self, client_id):
        eof_count = self.eof_count_by_client.get(client_id, 0) + 1
        self.eof_count_by_client[client_id] = eof_count
        if eof_count < SUM_AMOUNT:
            logging.info(
                f"Partial EOF from {client_id} "
                f"({eof_count}/{SUM_AMOUNT}) - still missing the other Sums"
            )
            return

        logging.info(f"EOF from every Sum for {client_id} - sending result")
        fruit_top = self.fruit_top_by_client.pop(client_id, [])
        fruit_chunk = list(fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top_result = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )

        
        self.output_queue.send(
            message_protocol.internal.serialize(
                message_protocol.internal.top_message(client_id, fruit_top_result)
            )
        )
        # Borro el count x sesion para que no crezca la memoria sin liberar
        del self.eof_count_by_client[client_id]


    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if fields["type"] == message_protocol.internal.MsgType.EOF:
            self._process_eof(fields["client_id"])
        else:
            self._process_data(fields["client_id"], fields["fruit"], fields["amount"])
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
