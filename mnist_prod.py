from kafka import KafkaProducer
import os
import sys

BATCH_SIZE = 28 * 28 # MNIST Dimensions
REPLICATION_FACTOR = 1 # number of available brokers
BOOTSTRAP_SERVERS = ['localhost:9092']
EVENTS_TOPIC = 'mnist-transfer-events'

#--------------------------------PRODUCER FUNCTIONS---------------------------------------

producer_config = {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
        "compression_type": "gzip",
        "batch_size": BATCH_SIZE,
        "linger_ms": 10, # 10ms added latency for better batching
    }

producer = KafkaProducer(**producer_config)

def file_generator(filepath: str, buffer_size: int):
    """generate chunks of mnist image data"""

    with open(filepath, 'rb') as f:
        f.seek(16) # skip past the header bytes
        try:
            while buf := f.read(buffer_size):
                yield buf
        except EOFError:
            yield buf

def file_transfer(filepath: str):
    """transfer the generated data via producer"""

    for data in file_generator(filepath, BATCH_SIZE):
        future = producer.send(EVENTS_TOPIC, data)


#-------------------------------------MAIN------------------------------------------------

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
        else:
            file_paths.append(path)

    for path in file_paths:
        file_transfer(os.path.abspath(path), BATCH_SIZE)



