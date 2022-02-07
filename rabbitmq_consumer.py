from pymongo.mongo_client import MongoClient
from thread_worker import ThreadWorker
from typing import List
from pika.adapters.blocking_connection import BlockingChannel
from queue import Queue
from pika.spec import Basic
from os import getenv

class RabbitMqWorker:
    """Rabbit MQ Worker that spawns threads to handle the messages"""
    worker_num = 10

    def __init__(self) -> None:
        self.workers = []
        print(getenv("MONGO_HOST", "localhost"))
        self.mongo_pool = MongoClient(host=getenv("MONGO_HOST", "localhost"), port=27017, minPoolSize=10)

    def spawn(self):
        """Spawns the worker threads"""
        threads: List[ThreadWorker] = []
        for i in range(self.worker_num):
            queue_obj = Queue()
            worker = ThreadWorker(i, queue_obj, self.mongo_pool, f"Thread {i}")
            worker.setDaemon(1)
            worker.start()
            threads.append(worker)
            self.workers = threads

    def get_idle_worker_index(self) -> int:
        """Returns the index of the idle worker"""
        for index in range(self.worker_num):
            if self.workers[index].is_idle():
                return index
        return -1

    def dispatcher(self, channel: BlockingChannel, method: Basic.Deliver, body: str):
        """Dispatches the message to the worker thread"""
        print(body)
        index = self.get_idle_worker_index()
        if index == -1:
            return
        worker = self.workers[index]
        worker.queue.put(
            item={"message": body, "channel": channel}, block=False)
        channel.basic_ack(method.delivery_tag)
