import os
import logging
import bisect
import signal

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
        self.fruit_top_by_client = {}
        self.sum_eof_by_client = {}
        self.is_shutting_down = False

    def _process_data(self, client_id, fruit, amount):
        fruit_top = self.fruit_top_by_client.setdefault(client_id, [])
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                updated_fruit_item = fruit_top.pop(i) + fruit_item.FruitItem(
                    fruit, amount
                )
                bisect.insort(fruit_top, updated_fruit_item)
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id, sum_id):
        eof_sums = self.sum_eof_by_client.setdefault(client_id, set())
        eof_sums.add(sum_id)
        if len(eof_sums) < SUM_AMOUNT:
            return

        fruit_top = self.fruit_top_by_client.get(client_id, [])
        fruit_chunk = list(fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        partial_result = message_protocol.internal.PartialResultMessage(client_id, ID, top)
        self.output_queue.send(message_protocol.internal.serialize(partial_result.to_dict()))
        self.fruit_top_by_client.pop(client_id, None)
        self.sum_eof_by_client.pop(client_id, None)

    def process_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        msg = message_protocol.internal.parse_message(fields)
        if msg.message_type == message_protocol.internal.InternalMessageType.DATA:
            self._process_data(msg.client_id, msg.fruit, msg.amount)
        elif msg.message_type == message_protocol.internal.InternalMessageType.AGG_EOF_BARRIER:
            self._process_eof(msg.client_id, msg.sum_id)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)

    def shutdown(self):
        if self.is_shutting_down:
            return
        self.is_shutting_down = True
        try:
            self.input_exchange.stop_consuming()
        except Exception:
            pass
        try:
            self.input_exchange.close()
        except Exception:
            pass
        try:
            self.output_queue.close()
        except Exception:
            pass


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: aggregation_filter.shutdown())
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
