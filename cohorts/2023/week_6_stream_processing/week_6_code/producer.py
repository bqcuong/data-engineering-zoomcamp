import csv
import pandas as pd
from time import sleep
from typing import Dict
from confluent_kafka import Producer

from settings import read_ccloud_config, GREEN_INPUT_DATA_PATH, GREEN_PRODUCE_TOPIC_RIDES


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


class RideCSVProducer:
    def __init__(self, props: Dict):
        self.producer = Producer(props)

    @staticmethod
    def read_records(resource_path: str):
        records, ride_keys = [], []
        
        df = pd.read_csv(resource_path)
        df = df.rename(columns={"lpep_pickup_datetime": "tpep_pickup_datetime", "lpep_dropoff_datetime": "tpep_dropoff_datetime"})
        df = df.head(10)
        df = df[['VendorID', 'PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'payment_type', 'total_amount']]

        df["json"] = df.apply(lambda x: x.to_json(), axis=1)

        ride_keys = map(str, df['VendorID'].tolist())
        records = map(str, df['json'].tolist())

        return zip(ride_keys, records)

    def publish(self, topic: str, records: [str, str]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.produce(topic=topic, key=key, value=value)
                print(f"[{topic}] - Producing record for <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()


if __name__ == "__main__":
    conf = read_ccloud_config("client.properties")
    producer = RideCSVProducer(props=conf)
    ride_records = producer.read_records(resource_path=GREEN_INPUT_DATA_PATH)
    producer.publish(topic=GREEN_PRODUCE_TOPIC_RIDES, records=ride_records)
