from logger import Logger
from thread_worker import ThreadWorker
from typing import List
from pika.adapters.blocking_connection import BlockingChannel
from queue import Queue
from pika.spec import Basic


class RabbitMqWorker:
    workerNum = 10

    def spawn(self):
        threads: List[ThreadWorker] = []
        for i in range(self.workerNum):
            queueObj = Queue()
            worker = ThreadWorker(i, queueObj, f"Thread {i}")
            worker.setDaemon(1)
            Logger.getLogger().info(f"Spawning thread: Thread {i}")
            worker.start()
            Logger.getLogger().info(f"Spawned thread: Thread {i}")
            threads.append(worker)
            self.workers = threads

    def getIdleWorkerIndex(self) -> int:
        for index in range(self.workerNum):
            if self.workers[index].isIdle():
                return index
        return -1

    def dispatcher(self, channel: BlockingChannel, method: Basic.Deliver, body: str):
        index = self.getIdleWorkerIndex()
        if index == -1:
            return
        worker = self.workers[index]
        worker.queue.put(
            item={"message": body, "channel": channel}, block=False)
        channel.basic_ack(method.delivery_tag)
