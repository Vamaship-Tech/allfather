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
            masterQueue = data['queue']
            try:
                self.busy = True
                decoded = json.loads(message)
                unMigratedRows = self.process(decoded)
                masterQueue.put(item=unMigratedRows, block=False)
                self.busy = False
            except Exception as e:
                Logger.getLogger().error(str(e))
                decoded = json.loads(message)
                masterQueue.put(item=decoded, block=False)

    def process(self, body: Dict):
        schema = body['schema']
        collection = body['table']
        rows = body['rows']
        type = body['type']
        migrator = Migrator()
        unMigrated: List[Dict] = []
        Logger.getLogger().info(f"[x] {type} - {collection}: {len(rows)} rows")
        for row in rows:
            result = migrator.handle(type, schema, collection, row)
            if result == False:
                unMigrated.append(row)

        return {"schema": schema, "table": collection, "rows": unMigrated, "type": type}

    def getQueue(self):
        return self.queue

    def getThreadId(self):
        return self.threadId

    def isIdle(self):
        return self.busy == False
