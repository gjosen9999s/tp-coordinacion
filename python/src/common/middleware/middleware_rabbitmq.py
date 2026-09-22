import pika
from .middleware import (
                        MessageMiddlewareCloseError,
                        MessageMiddlewareDisconnectedError,
                        MessageMiddlewareExchange,
                        MessageMiddlewareMessageError,
                        MessageMiddlewareQueue,
                        )

# Helpers 

# Wrapper para invocar el start_consuming dado que la funcion de callback no tiene los parametros requeridos por pika
def _wrap_callback(channel, on_message_callback):

    def callback(ch, method, properties, body):

        def ack():
            channel.basic_ack(delivery_tag=method.delivery_tag)

        def nack():
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        on_message_callback(body, ack, nack)

    return callback

# Manejo de errores unificado evitando duplicidad
def _handle_pika_error(e):

    if isinstance(e, pika.exceptions.AMQPConnectionError):
        raise MessageMiddlewareDisconnectedError() from e

    raise MessageMiddlewareMessageError() from e

# Cierre unificado evitando duplicidad de codigo
def _close_channel_connection(channel, connection):

    try:
        if channel is not None and channel.is_open:
            channel.close()

    finally:
        if connection is not None and connection.is_open:
            connection.close()

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):

        connection = None
        channel = None 

        self._is_consuming = False

        try:
            #conexion al host
            connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host))

            #canal
            channel = connection.channel()

            #cola
            self.queue_name = queue_name 
            channel.queue_declare(queue=queue_name, durable=True)
            
        except pika.exceptions.AMQPError as e:
            _close_channel_connection(channel, connection)
            _handle_pika_error(e)

        self.channel = channel
        self.connection = connection

    #Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
    #cada mensaje de datos o de control con el cuerpo del mensaje.
    # on_message_callback tiene como parámetros:
    # message - El valor tal y como lo recibe el método send de esta clase.
    # ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
    # nack - Función que al invocarse realiza nack al mensaje que se está consumiendo. 
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback):

        if self._is_consuming:
            raise MessageMiddlewareMessageError()

        self._is_consuming = True

        try:

            self.channel.basic_consume(queue=self.queue_name, 
                                        on_message_callback=_wrap_callback(self.channel, on_message_callback))

            self.channel.start_consuming()

        except pika.exceptions.AMQPError as e:
            _handle_pika_error(e)
            
        finally:
            self._is_consuming = False
        

    #Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
    #no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):

        try:

            self.channel.stop_consuming()

        except pika.exceptions.AMQPError as e:
            _handle_pika_error(e)

        finally:
            self._is_consuming = False

	#Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message):

        try:

            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
                )

        except pika.exceptions.AMQPError as e:
            _handle_pika_error(e)
       
	#Se desconecta de la cola o exchange al que estaba conectado.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):

        #orden inverso al del init, no se borra la cola porque se comparte
        try:
            _close_channel_connection(self.channel, self.connection)

        except Exception as e:
            raise MessageMiddlewareCloseError() from e
    
class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):

        connection = None
        channel = None 

        self._is_consuming = False
        self.queue_name = None

        try:

            self.routing_keys = routing_keys

            #conexion al host
            connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host))

            #canal
            channel = connection.channel()

            #exchange
            self.exchange_name = exchange_name

            channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')
                
        except pika.exceptions.AMQPError as e:
            _close_channel_connection(channel, connection)
            _handle_pika_error(e)

        self.channel = channel
        self.connection = connection

    #Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
    #cada mensaje de datos o de control con el cuerpo del mensaje.
    # on_message_callback tiene como parámetros:
    # message - El valor tal y como lo recibe el método send de esta clase.
    # ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
    # nack - Función que al invocarse realiza nack al mensaje que se está consumiendo. 
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback):

        if self._is_consuming:
            raise MessageMiddlewareMessageError()

        self._is_consuming = True

        try:

            #cola a asociar
            result = self.channel.queue_declare(queue='', exclusive=True)
            self.queue_name = result.method.queue

            #bind
            for routing_key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name, 
                                        queue=self.queue_name, routing_key=routing_key)
                
            self.channel.basic_consume(queue=self.queue_name, 
                                        on_message_callback=_wrap_callback(self.channel, on_message_callback))

            self.channel.start_consuming()

        except pika.exceptions.AMQPError as e:
            try:
                if self.queue_name:
                    self.channel.queue_delete(queue=self.queue_name)
            except pika.exceptions.AMQPError:
                # borrado de cola best-effort, si falla suprimo el error e informo el original
                pass

            _handle_pika_error(e)

        finally:
            self._is_consuming = False
        
    #Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
    #no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):

        try:
            self.channel.stop_consuming()
        
        except pika.exceptions.AMQPError as e:
            _handle_pika_error(e)

        finally:
            self._is_consuming = False
        
    #Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message):

        if not self.routing_keys:
            raise MessageMiddlewareMessageError()
        
        try:

            for routing_key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
                    )

        except pika.exceptions.AMQPError as e:
            _handle_pika_error(e)
        
    #Se desconecta de la cola o exchange al que estaba conectado.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):

        #orden inverso al del init
        try:
            _close_channel_connection(self.channel, self.connection)

        except Exception as e:
            raise MessageMiddlewareCloseError() from e
