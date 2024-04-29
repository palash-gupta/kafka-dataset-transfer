from kafka import KafkaConsumer
from kafka.errors import KafkaError
import concurrent.futures
import os
import sys

MAX_WORKERS = 10
BOOTSTRAP_SERVERS = ['localhost:9092']
BATCH_SIZE = 32768
WAIT_MS = 10
EVENTS_TOPIC = 'file-transfer-events'


consumer_config_kwargs = {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
        "fetch_min_bytes": BATCH_SIZE,
        "fetch_max_wait_ms": WAIT_MS,
    }


def deformatted_path(formatted_path):
    return formatted_path.replace('___', '/').replace('__', '\x00').replace('_', '.').replace('\x00', '_')


def consume_file(filepath, topic):
    try:
        os.makedirs(os.path.dirname(filepath))
    except FileExistsError:
        pass

    file_consumer = KafkaConsumer(topic, **consumer_config_kwargs)
    file_consumer.seek_to_beginning()
    with open(filepath, 'wb') as f:
        for chunk in file_consumer:
            if chunk.key == b"DATA":
                f.write(chunk.value)
            else:
                break


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("No target directory specified, using current directory.", file=sys.stderr)
        target_dir = os.getcwd()
    elif len(sys.argv) != 2:
        print("Too many arguments, exiting.", file=sys.stderr)
        exit(1)
    else:
        target_dir = sys.argv[1]
        if not os.path.isdir(target_dir):
            if os.path.isfile(target_dir):
                print(f"{target_dir} is already a file, Please specify a directory.", file=sys.stderr)
                exit(1)
            else:
                try:
                    os.mkdir(target_dir)
                except FileExistsError:
                    pass


    consumer = KafkaConsumer(EVENTS_TOPIC, bootstrap_servers=BOOTSTRAP_SERVERS)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for message in consumer:
            path = deformatted_path(message.key.decode())
            if message.value == b'start':
                executor.submit(
                        consume_file, os.path.join(target_dir, path), message.key.decode()
                    )

