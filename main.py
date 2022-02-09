from dotenv import load_dotenv
load_dotenv()
import pika
from rabbitmq_consumer import RabbitMqWorker
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
    channel = connection.channel()
    channel.queue_declare(queue='shipment_queue', durable=True)
    channel = connection.channel()
    for queue in tables:
        channel.queue_declare(queue, False, True)
        channel.basic_consume(
            queue=queue,
            on_message_callback=lambda channel, method, _, body: rabbitMqWorker.dispatcher(channel, method, body)
        )
    print("[x] In days of peace, and nights of war, obey the All Father forever more!")
    channel.start_consuming()
except pika.exceptions.AMQPConnectionError as e:
    print(str(e))
    for thread in rabbitMqWorker.workers:
        if thread.is_alive():
            thread.join()
            print(f"Closing threads: {thread.getName()}")
except KeyboardInterrupt:
    print('[x] Exiting')
    channel.close()
    connection.close()
except Exception as e:
    channel.close()
    connection.close()
    print(str(e))
    for thread in rabbitMqWorker.workers:
        if thread.is_alive():
            thread.join()
            print(f"Closing threads: {thread.getName()}")

exit(1)