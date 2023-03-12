import argparse
from typing import Dict, List
from confluent_kafka import Consumer

from settings import read_ccloud_config, GREEN_INPUT_DATA_PATH, GREEN_CONSUME_TOPIC_RIDES


class RideCSVConsumer:
    def __init__(self, props: Dict):
        self.consumer = Consumer(**props)

    def consume_from_kafka(self, topics: List[str]):
        self.consumer.subscribe(topics=topics)
        print('Consuming from Kafka started')
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                msg = self.consumer.poll(1.0)
                if msg is not None and msg.error() is None:
                    print("key = {key:12} value = {value:12}".format(key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
            except KeyboardInterrupt:
                break
        self.consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Kafka Consumer')
    parser.add_argument('--topic', type=str, default=GREEN_CONSUME_TOPIC_RIDES)
    args = parser.parse_args()

    topic = args.topic
    conf = read_ccloud_config("client.properties")
    conf["group.id"] = "python-group-1"
    conf["auto.offset.reset"] = "earliest"

    csv_consumer = RideCSVConsumer(props=conf)
    csv_consumer.consume_from_kafka(topics=[topic])
