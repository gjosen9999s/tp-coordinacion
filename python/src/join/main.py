import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:
    def __init__(self):
            self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, INPUT_QUEUE
            )
            self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, OUTPUT_QUEUE
            )
            # TOPs parciales por sesion: si fuera una lista sola, la combinacion de
            # un cliente terminaria mezclada con la de los demas.
            self.partials_by_client = {}

    def _combine(self, partials):
        combined = [
            fruit_item.FruitItem(fruit, amount)
            for partial in partials
            for (fruit, amount) in partial
        ]
    
        combined.sort(reverse=True)
        return [(item.fruit, item.amount) for item in combined[:TOP_SIZE]]

    def process_messsage(self, message, ack, nack):
        logging.info("Received partial top")
        fields = message_protocol.internal.deserialize(message)
        client_id = fields["client_id"]

        partials = self.partials_by_client.setdefault(client_id, [])
        partials.append(fields["top"])

        if len(partials) == AGGREGATION_AMOUNT:
            logging.info(f"Complete top from every Aggregator for {client_id}")
            final_top = self._combine(partials)
            
            del self.partials_by_client[client_id]
            self.output_queue.send(
                message_protocol.internal.serialize(
                    message_protocol.internal.top_message(client_id, final_top)
                )
            )
        else:
            logging.info(
                f"Partial top from {client_id} "
                f"({len(partials)}/{AGGREGATION_AMOUNT}) - "
                f"still missing the other Aggregators"
            )

        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
