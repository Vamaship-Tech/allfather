from pika.adapters.blocking_connection import BlockingChannel
from pymongo.mongo_client import MongoClient
from migrator import Migrator
import json
import threading
from queue import Queue
from time import sleep
from typing import Dict


class ThreadWorker(threading.Thread):
    def __init__(self, thread_id, queue: Queue, mongo_connection: MongoClient, name: str) -> None:
        threading.Thread.__init__(self, group=None)
        self.busy = False
        self.queue = queue
        self.setName(name)
        self.thread_id = thread_id
        self.mongo_connection = mongo_connection

    def run(self):
        print(f"[x] Spawned Thread : {self.getName()}")
        while True:
            sleep(1)
            data = self.queue.get()
            message = data['message']
            channel = data['channel']
            self.busy = True
            print(f"[x]Processing on Thread ID: {self.getName()}")
            try:
                decoded = json.loads(message)
                self.process(decoded)
            except Exception as e:
                print(str(e))
                exchange = "mongo_syncer"
                routingKey = ""
                channel.basic_publish(
                    exchange=exchange, routing_key=routingKey, body=message)
            finally:
                self.busy = False

    def process(self, body: Dict):
        schema = body['schema']
        collection = body['table']
        rows = body['rows']
        action = body['type']
        migrator = Migrator()
        migrator.handle(self.mongo_connection, action,
                        schema, collection, rows)

    def get_queue(self):
        return self.queue

    def get_thread_id(self):
        return self.thread_id

    def is_idle(self):
        return self.busy == False
