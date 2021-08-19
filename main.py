from logger import Logger
import pika
from rabbitmq_consumer import RabbitMqWorker

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

rabbitMqWorker = RabbitMqWorker()
rabbitMqWorker.spawn()

try:
    channel = connection.channel()
    channel.basic_consume(queue="shipments", on_message_callback=lambda channel,
                          method, _, body: rabbitMqWorker.dispatcher(channel, method, body))
    print("[x] In days of peace, and nights of war, obey the All Father forever more!")
    channel.start_consuming()
    channel.close()
    connection.close()
except Exception as e:
    Logger.getLogger().error(str(e))
    for thread in rabbitMqWorker.workers:
        if thread.is_alive():
            thread.join()
            Logger.getLogger().info(f"Closing threads: {thread.getThreadId()}")
