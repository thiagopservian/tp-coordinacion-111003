import os
import logging
import signal
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

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.control_exchange_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE]
        )
        self.control_exchange_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE]
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_client = {}
        self.flushed_clients = set()
        self.is_shutting_down = False

    def _get_aggregation_id(self, client_id, fruit):
        key = f"{client_id}|{fruit}".encode("utf-8")
        return zlib.crc32(key) % AGGREGATION_AMOUNT

    def _process_data(self, client_id, fruit, amount):
        if client_id in self.flushed_clients:
            return
        amount_by_fruit = self.amount_by_client.setdefault(client_id, {})
        amount_by_fruit[fruit] = amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _publish_client_eof(self, client_id):
        eof_msg = message_protocol.internal.SumEOFBarrierMessage(client_id)
        self.control_exchange_publisher.send(message_protocol.internal.serialize(eof_msg.to_dict()))

    def _flush_client(self, client_id):
        if client_id in self.flushed_clients:
            return
        fruits = self.amount_by_client.get(client_id, {})
        self.flushed_clients.add(client_id)

        for final_fruit_item in fruits.values():
            aggregation_id = self._get_aggregation_id(client_id, final_fruit_item.fruit)
            data_msg = message_protocol.internal.DataMessage(
                client_id,
                final_fruit_item.fruit,
                final_fruit_item.amount,
            )
            self.data_output_exchanges[aggregation_id].send(
                message_protocol.internal.serialize(data_msg.to_dict())
            )

        eof_msg = message_protocol.internal.AggregationEOFBarrierMessage(client_id, ID)
        serialized_eof = message_protocol.internal.serialize(eof_msg.to_dict())
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(serialized_eof)

        self.amount_by_client.pop(client_id, None)

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        msg = message_protocol.internal.parse_message(fields)
        if msg.message_type == message_protocol.internal.InternalMessageType.DATA:
            self._process_data(msg.client_id, msg.fruit, msg.amount)
        elif msg.message_type == message_protocol.internal.InternalMessageType.EOF:
            self._publish_client_eof(msg.client_id)
            self._flush_client(msg.client_id)
        ack()

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        msg = message_protocol.internal.parse_message(fields)
        if msg.message_type == message_protocol.internal.InternalMessageType.SUM_EOF_BARRIER:
            self._flush_client(msg.client_id)
        ack()

    def start(self):
        self.input_queue.start_consuming(
            self.process_data_messsage,
            self.process_control_message,
            self.control_exchange_consumer.get_queue_name(),
        )

    def shutdown(self):
        if self.is_shutting_down:
            return
        self.is_shutting_down = True
        try:
            self.input_queue.stop_consuming()
        except Exception:
            pass
        try:
            self.control_exchange_consumer.stop_consuming()
        except Exception:
            pass
        try:
            self.input_queue.close()
        except Exception:
            pass
        try:
            self.control_exchange_consumer.close()
        except Exception:
            pass
        try:
            self.control_exchange_publisher.close()
        except Exception:
            pass
        for exchange in self.data_output_exchanges:
            try:
                exchange.close()
            except Exception:
                pass

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: sum_filter.shutdown())
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
