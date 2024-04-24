from kafka import KafkaProducer
from kafka.errors import KafkaError
import os
import sys

FILE_BUF_SIZE = 256

producer_config = {
        "bootstrap_servers": ['localhost:9092'],
        "compression_type": "gzip",
    }

producer = KafkaProducer(**producer_config)
topic_name = "file-transfer-events"


def file_generator(filepath: str, buffer_size: int):
    with open(filepath, 'rb') as f:
        try:
            while buf := f.read(buffer_size):
                yield buf
        except:
            yield buf

def file_transfer(filepath: str):
    for data in file_generator(filepath, FILE_BUF_SIZE):
        future = producer.send(topic_name, key=filepath.encode(), value=data)

        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            print("error")
            pass


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify file(s) to transfer.", file=sys.stderr)
        exit(1)

    file_paths = []
    for path in sys.argv[1:]:
        if not os.path.exists(path):
            print(f"{path} does not exist.", file=sys.stderr)
            exit(1)
        elif os.path.isdir(path):
            for file_path in [os.path.join(root, f) for root, dirs, filenames in os.walk(path) for f in filenames]: # list all files in any directory
                file_paths.append(file_path)

    for path in file_paths:
        file_transfer(os.path.abspath(path))
    



