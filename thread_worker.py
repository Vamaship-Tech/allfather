from pika.adapters.blocking_connection import BlockingChannel
from logger import Logger
from migrator import Migrator
import json
import threading
from queue import Queue
from time import sleep
from typing import Dict, List


class ThreadWorker(threading.Thread):
    def __init__(self, threadId, queue: Queue, name: str) -> None:
        self.busy = False
        threading.Thread.__init__(self, group=None)
        self.queue = queue
        self.name = name
        self.threadId = threadId

    def run(self):
        Logger.getLogger().info(f"[x] Acknowledged from : {self.threadId}")
        while True:
            sleep(1)
            data = self.queue.get()
            message = data['message']
            channel = data['channel']
            self.busy = True
            Logger.getLogger().info(f"[x] {self.threadId} status: Busy")
            try:
                decoded = json.loads(message)
                self.process(decoded, channel)
            except Exception as e:
                Logger.getLogger().error(str(e))
                exchange = "mongo_syncer"
                routingKey = ""
                channel.basic_publish(
                    exchange=exchange, routing_key=routingKey, body=message)
            finally:
                self.busy = False
                Logger.getLogger().info(f"[x] Thread ID {self.threadId} status: Idle")

    def process(self, body: Dict, channel: BlockingChannel):
        schema = body['schema']
        collection = body['table']
        rows = body['rows']
        type = body['type']
        migrator = Migrator()
        unMigratedRows: List[Dict] = []
        Logger.getLogger().info(
            f"[x] {type} - {collection}: {len(rows)} rows --- [Thread ID: {self.threadId}]")
        for row in rows:
            result = migrator.handle(type, schema, collection, row)
            if result == False:
                unMigratedRows.append(row)

        unMigrated = {"schema": schema, "table": collection,
                      "rows": unMigratedRows, "type": type}

        if len(unMigrated['rows']) > 0:
            exchange = "mongo_syncer"
            routingKey = ""
            channel.basic_publish(
                exchange=exchange, routing_key=routingKey, body=json.dumps(unMigrated))

    def getQueue(self):
        return self.queue

    def getThreadId(self):
        return self.threadId

    def isIdle(self):
        return self.busy == False
