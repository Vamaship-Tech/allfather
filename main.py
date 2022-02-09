from dotenv import load_dotenv
load_dotenv()
import pika
from typing import List
from pika.adapters.blocking_connection import BlockingChannel
from rabbitmq_consumer import RabbitMqWorker
from logger import Logger
from os import getenv

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=getenv('RABBITMQ_HOST', 'localhost')))

rabbitMqWorker = RabbitMqWorker()
rabbitMqWorker.spawn()

tables = (
    "shipments",
    "shipment_inputs",
    "transactions",
    "shipment_tracking_details",
    "cod_transactions",
    "shipment_addresses",
    "shipment_line_items",
    "shipment_packages",
    "shipment_cost_breakups",
    "shipment_milestone_dates",
)

try:
    channels: List[BlockingChannel] = []
    for queue in tables:
        channel = connection.channel()
        channel.queue_declare(queue, False, True)
        channel.basic_consume(
            queue=queue,
            on_message_callback=lambda channel, method, _, body: rabbitMqWorker.dispatcher(channel, method, body)
        )
        channels.append(channel)
    print("[x] In days of peace, and nights of war, obey the All Father forever more!")
    channel.start_consuming()
except pika.exceptions.AMQPConnectionError as e:
    message = "AMQP Exception:" + str(e)
    Logger.get_logger().error(message)
    for channel in channels:
        channel.close()
    connection.close()
    for thread in rabbitMqWorker.workers:
        if thread.is_alive():
            thread.join()
            print(f"Closing threads: {thread.getName()}")
except Exception as e:
    message = "Exception:" + str(e)
    Logger.get_logger().exception(message)

exit(1)