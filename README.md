## Start Zookeeper and Kafka Server with:
```sh
./start_kafka.sh
```

## How to use
- `producer.py` and `consumer.py` are multithreaded programs which may be used to transfer entire files
    1. Start `consumer.py` first:
    ```sh
    python consumer.py target_dir/
    ```

    The transferred files will be saved in the `target_directory`.

    2. Start `producer.py`:
    ```sh
    python producer.py filepath1 directory1 filepath2 filepath3 directory2 ...
    ```
    All specified files and directories will be sent by the producer

- `mnist_prod.py` and `mnist_consumer.py` are specific to MNIST datasets
    1. Start `mnist_consumer.py`
    ```sh
    python mnist_consumer.py 
    ```
    2. Start `mnist_prod.py`
    ```sh
    python mnist_prod.py archive/t10k-images.idx3-ubyte
    ```
    This will send the MNIST dataset image-by-image.    
