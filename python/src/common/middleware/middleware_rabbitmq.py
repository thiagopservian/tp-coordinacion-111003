import pika
import random
import string
from pika.exceptions import AMQPConnectionError, AMQPError

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)


def _build_callback(on_message_callback):
    def callback(ch, method, properties, body):
        def ack():
            ch.basic_ack(delivery_tag=method.delivery_tag)

        def nack():
            ch.basic_nack(delivery_tag=method.delivery_tag)

        on_message_callback(body, ack, nack)

    return callback


def _random_queue_name(prefix):
    suffix = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(12))
    return f"{prefix}_{suffix}"

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self._channel = self._connection.channel()
            self._queue_name = queue_name
            self._channel.queue_declare(queue=self._queue_name, durable=True)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e

    def start_consuming(self, on_message_callback, second_callback=None, second_queue_name=None):
        try:
            self._channel.basic_qos(prefetch_count=1)
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=_build_callback(on_message_callback),
                auto_ack=False,
            )
            if second_callback is not None and second_queue_name is not None:
                self._channel.basic_consume(
                    queue=second_queue_name,
                    on_message_callback=_build_callback(second_callback),
                    auto_ack=False,
                )
            self._channel.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def stop_consuming(self):
        try:
            self._channel.stop_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
            )
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def close(self):
        try:
            self._connection.close()
        except AMQPError as e:
            raise MessageMiddlewareCloseError() from e

    def get_queue_name(self):
        return self._queue_name

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        try:
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self._channel = self._connection.channel()
            self._exchange_name = exchange_name
            self._routing_keys = routing_keys
            self._channel.exchange_declare(
                exchange=self._exchange_name,
                exchange_type="direct",
                durable=True,
            )

            queue_name = _random_queue_name(f"{self._exchange_name}_consumer")
            self._queue_name = queue_name
            self._channel.queue_declare(queue=self._queue_name, durable=True)

            for routing_key in self._routing_keys:
                self._channel.queue_bind(
                    exchange=self._exchange_name,
                    queue=self._queue_name,
                    routing_key=routing_key,
                )
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e

    def start_consuming(self, on_message_callback):
        try:
            self._channel.basic_qos(prefetch_count=1)
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=_build_callback(on_message_callback),
                auto_ack=False,
            )
            self._channel.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def stop_consuming(self):
        try:
            self._channel.stop_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def send(self, message):
        try:
            for routing_key in self._routing_keys:
                self._channel.basic_publish(
                    exchange=self._exchange_name,
                    routing_key=routing_key,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
                )
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def close(self):
        try:
            self._connection.close()
        except AMQPError as e:
            raise MessageMiddlewareCloseError() from e

    def get_queue_name(self):
        return self._queue_name
