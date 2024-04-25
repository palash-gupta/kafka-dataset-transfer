from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import KafkaError, TopicAlreadyExistsError
from kafka.admin.new_topic import NewTopic
import concurrent.futures
import os
import sys

BATCH_SIZE = 32768 # 32 KB
REPLICATION_FACTOR = 1 # number of available brokers
MAX_WORKERS = 10 # maximum number of workers for concurrent.futures.ThreadPoolExecutor
BOOTSTRAP_SERVERS = ['localhost:9092']

#----------------------------------ADMIN FUNCTIONS----------------------------------------

admin_config = {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
    }

admin = KafkaAdminClient(**admin_config)

def create_path_topics(paths):
    """create individual topics for every file that is transferred"""

    for path in paths:
        try:
            admin.create_topics(
                    [NewTopic(formatted_path(path), 1, REPLICATION_FACTOR)] # 1 is the number of partitions here
                )
        except TopicAlreadyExistsError:
            continue

def formatted_path(path):
    """replacing path characters to comply with Kafka topic naming convention"""

    return path.replace('_', '__').replace('/', '___').replace('.', '_') 


#--------------------------------PRODUCER FUNCTIONS---------------------------------------

producer_config = {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
        "compression_type": "gzip",
        "batch_size": BATCH_SIZE,
        "linger_ms": 10, # 10ms added latency for better batching
    }

producer = KafkaProducer(**producer_config)

def file_generator(filepath: str, buffer_size: int):
    """generate chunks of file data"""

    with open(filepath, 'rb') as f:
        try:
            while buf := f.read(buffer_size):
                yield buf
        except EOFError:
            yield buf

def file_transfer(filepath: str):
    """transfer the generated data via producer"""

    topic_name = formatted_path(filepath)
    for data in file_generator(filepath, BATCH_SIZE):
        future = producer.send(topic_name, key=filepath.encode(), value=data)



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

    create_path_topics(file_paths) # create separate topics for every file that needs to be transferred

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for path in file_paths:
            executor.submit(
                file_transfer, os.path.abspath(path)
            )


