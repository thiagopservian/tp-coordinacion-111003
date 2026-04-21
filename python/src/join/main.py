import os
import logging
import bisect
import signal

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
        self.partial_count_by_client = {}
        self.seen_aggregations_by_client = {}
        self.fruit_top_by_client = {}
        self.is_shutting_down = False

    def _add_fruit(self, client_id, fruit, amount):
        fruit_top = self.fruit_top_by_client.setdefault(client_id, [])
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                fruit_top[i] = fruit_top[i] + fruit_item.FruitItem(fruit, amount)
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _merge_partial(self, client_id, fruit_top):
        for fruit, amount in fruit_top:
            self._add_fruit(client_id, fruit, amount)

    def _emit_result(self, client_id):
        fruit_top = self.fruit_top_by_client.get(client_id, [])
        fruit_chunk = list(fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        top = list(map(lambda item: (item.fruit, item.amount), fruit_chunk))
        result_msg = message_protocol.internal.ResultMessage(client_id, top)
        self.output_queue.send(message_protocol.internal.serialize(result_msg.to_dict()))
        self.partial_count_by_client.pop(client_id, None)
        self.seen_aggregations_by_client.pop(client_id, None)
        self.fruit_top_by_client.pop(client_id, None)

    def process_messsage(self, message, ack, nack):
        payload = message_protocol.internal.deserialize(message)
        msg = message_protocol.internal.parse_message(payload)
        if msg.message_type == message_protocol.internal.InternalMessageType.PARTIAL_RESULT:
            seen = self.seen_aggregations_by_client.setdefault(msg.client_id, set())
            if msg.aggregation_id in seen:
                ack()
                return
            seen.add(msg.aggregation_id)
            self.partial_count_by_client[msg.client_id] = self.partial_count_by_client.get(msg.client_id, 0) + 1
            self._merge_partial(msg.client_id, msg.fruit_top)
            if self.partial_count_by_client[msg.client_id] == AGGREGATION_AMOUNT:
                self._emit_result(msg.client_id)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

    def shutdown(self):
        if self.is_shutting_down:
            return
        self.is_shutting_down = True
        try:
            self.input_queue.stop_consuming()
        except Exception:
            pass
        try:
            self.input_queue.close()
        except Exception:
            pass
        try:
            self.output_queue.close()
        except Exception:
            pass


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: join_filter.shutdown())
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
