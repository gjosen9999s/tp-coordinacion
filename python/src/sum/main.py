import os
import logging
import signal
import threading
import zlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
THREAD_JOIN_TIMEOUT = 10

# Distribucion de frutas a agregadores (N : 1)
def _aggregation_index(fruit):
    return zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT

class SumFilter:
    def __init__(self):

        self._control_thread = None
        
        # Exchange de control compartido por todos los SUM. Cada SUM declara la
        # cola de control de todos los demas: asi el exchange tiene destino
        # aunque un SUM todavia no se haya levantado. Evito condicion de carrera por EOF.
        self.eof_publish_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE]
        )  # la usa el hilo de datos para retransmitir (send)
        for i in range(SUM_AMOUNT):
            self.eof_publish_exchange.declare_queue(
                f"{SUM_PREFIX}_{i}_control"
            )

        self.eof_consume_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE],
            queue_name=f"{SUM_PREFIX}_{ID}_control"
        )

         
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        
        self.amount_by_fruit_by_client = {}

        self.lock = threading.Lock()

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")

        with self.lock:
            amount_by_fruit = self.amount_by_fruit_by_client.setdefault(client_id, {})
            amount_by_fruit[fruit] = amount_by_fruit.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Sending partial totals from {client_id}")

        with self.lock:
            # Recurso compartido
            amount_by_fruit = self.amount_by_fruit_by_client.pop(client_id, {})

        for final_fruit_item in amount_by_fruit.values():
            # Se envia al agregator correspondiente segun el criterio de division 
            self.data_output_exchanges[
                _aggregation_index(final_fruit_item.fruit)
            ].send(
                message_protocol.internal.serialize(
                    message_protocol.internal.data_message(
                        client_id,
                        final_fruit_item.fruit,
                        final_fruit_item.amount,
                    )
                )
            )

        # EOF a los aggregator
        logging.info(f"Broadcasting EOF message from {client_id}")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(
                message_protocol.internal.serialize(
                    message_protocol.internal.eof_message(client_id)
                )
            )

    def process_data_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if fields["type"] == message_protocol.internal.MsgType.EOF:
            # Si encuentro EOF lo republico al Exchange compartido de todos los SUM
            self.eof_publish_exchange.send(
                message_protocol.internal.serialize(
                    message_protocol.internal.eof_message(fields["client_id"])
                )
            )
        else:
            self._process_data(fields["client_id"], fields["fruit"], fields["amount"])

        ack()

    # Helper
    def _broadcast_eof(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        self._process_eof(fields["client_id"])
        ack()

    def start(self):
        self._control_thread = threading.Thread(
            target=self.eof_consume_exchange.start_consuming,
            args=(self._broadcast_eof,))
        self._control_thread.start()
        self.input_queue.start_consuming(self.process_data_message)

    def stop(self):
        logging.info("SIGTERM recibido, detengo los consumidores")
        self.input_queue.stop_consuming_threadsafe()
        self.eof_consume_exchange.stop_consuming_threadsafe()

    def close(self):
        # El hilo de control puede seguir vivo lo espero antes de cerrarle la conexion
        if self._control_thread is not None:
            self._control_thread.join(timeout=THREAD_JOIN_TIMEOUT)

        # Orden inverso al del init, no se borra la cola porque se comparte
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.close()
        self.input_queue.close()
        self.eof_consume_exchange.close()
        self.eof_publish_exchange.close()

    
def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    
    signal.signal(signal.SIGTERM, lambda signum, frame: sum_filter.stop())
    
    try:
        sum_filter.start()
    finally:
        sum_filter.close()
    return 0

if __name__ == "__main__":
    main()
