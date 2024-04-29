from kafka import KafkaConsumer
import os
import sys
import numpy as np

MAX_WORKERS = 10
BOOTSTRAP_SERVERS = ['localhost:9092']
BATCH_SIZE = 28 * 28 # MNIST image dimension
WAIT_MS = 10
EVENTS_TOPIC = 'mnist-transfer-events'


consumer_config_kwargs = {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
        "fetch_min_bytes": BATCH_SIZE,
        "fetch_max_wait_ms": WAIT_MS,
    }


def consume_images():
    file_consumer = KafkaConsumer(EVENTS_TOPIC, **consumer_config_kwargs)
    for chunk in file_consumer:
        # this is example code for doing any actions on the MNIST image
        data = np.frombuffer(chunk.value, dtype=np.uint8).astype(np.float32).reshape(1, 28, 28, 1)
        image = np.asarray(data[0]).squeeze()
        
        for i in image:
            for j in i:
                if j == 0:
                    print(" ", end='')
                else:
                    print("#", end='')
            print()


if __name__ == '__main__':

    consume_images()

